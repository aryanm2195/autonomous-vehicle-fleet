"""Shared runtime helpers for the Python microservices.

* ``Settings``      - environment-driven configuration shared by all services
* ``LatencyWindow`` - sliding reservoir for exact p50/p95/p99 over recent samples
* ``serve_http``    - tiny asyncio HTTP server for /health, /ready, /metrics, /stats
* ``make_consumer`` - aiokafka consumer with manual commits (at-least-once)
* ``parse_ts``      - ISO-8601 -> epoch seconds
"""
from __future__ import annotations

import asyncio
import collections
import json
import logging
import os
import time
from datetime import datetime, timezone

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest


def env(name: str, default: str | None = None) -> str | None:
    v = os.getenv(name)
    return default if v in (None, "") else v


class Settings:
    def __init__(self, service: str):
        self.service = service
        self.kafka_broker = env("KAFKA_BROKER", "localhost:29092")
        self.mongodb_uri = env("MONGODB_URI", "mongodb://localhost:27017")
        self.mongodb_db = env("MONGODB_DB", "fleet")
        self.redis_uri = env("REDIS_URI", "redis://localhost:6379/0")
        self.topic_telemetry = env("KAFKA_TOPIC_TELEMETRY", "vehicle.telemetry")
        self.topic_events = env("KAFKA_TOPIC_EVENTS", "vehicle.events")
        self.topic_alerts = env("KAFKA_TOPIC_ALERTS", "vehicle.alerts")
        self.low_battery = float(env("LOW_BATTERY_THRESHOLD", "20"))
        self.critical_battery = float(env("CRITICAL_BATTERY_THRESHOLD", "10"))
        self.http_port = int(env("SERVICE_HTTP_PORT", "8001"))
        self.batch_size = int(env("CONSUMER_BATCH_SIZE", "2000"))
        self.log_level = env("LOG_LEVEL", "INFO").upper()
        logging.basicConfig(level=self.log_level, format=f"%(asctime)s %(levelname)-7s {service}: %(message)s")


def parse_ts(value) -> float | None:
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class LatencyWindow:
    """Keeps the last ``size`` samples (ms) and returns exact percentiles on demand."""

    def __init__(self, size: int = 50_000):
        self.samples: collections.deque = collections.deque(maxlen=size)
        self.count = 0

    def add(self, ms: float) -> None:
        self.samples.append(ms)
        self.count += 1

    def reset(self) -> None:
        self.samples.clear()

    def summary(self) -> dict:
        s = sorted(self.samples)
        if not s:
            return {"samples": 0, "p50": None, "p95": None, "p99": None, "max": None, "mean": None}

        def p(q):
            return round(s[min(len(s) - 1, int(q * len(s)))], 2)
        return {"samples": len(s), "p50": p(0.50), "p95": p(0.95), "p99": p(0.99),
                "max": round(s[-1], 2), "mean": round(sum(s) / len(s), 2)}


async def serve_http(port: int, stats_fn, ready_fn=None, reset_fn=None):
    """Minimal HTTP/1.1 server so services need no web framework."""
    log = logging.getLogger("http")

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            line = await asyncio.wait_for(reader.readline(), 5)
            while (await reader.readline()) not in (b"\r\n", b"\n", b""):
                pass
            parts = line.decode(errors="ignore").split()
            path = parts[1] if len(parts) > 1 else "/"
            code, ctype = 200, "application/json"
            if path.startswith("/metrics"):
                body, ctype = generate_latest(), CONTENT_TYPE_LATEST
            elif path.startswith("/health"):
                body = b'{"status":"ok"}'
            elif path.startswith("/ready"):
                ok = await ready_fn() if ready_fn else True
                code = 200 if ok else 503
                body = json.dumps({"ready": bool(ok)}).encode()
            elif path.startswith("/stats"):
                if "reset=1" in path and reset_fn:
                    reset_fn()
                body = json.dumps(stats_fn(), default=str).encode()
            else:
                code, body = 404, b'{"error":"not found"}'
            reason = {200: "OK", 404: "Not Found", 503: "Service Unavailable"}[code]
            writer.write(f"HTTP/1.1 {code} {reason}\r\nContent-Type: {ctype}\r\nContent-Length: {len(body)}\r\n"
                         f"Connection: close\r\n\r\n".encode() + body)
            await writer.drain()
        except Exception as exc:  # pragma: no cover
            log.debug("http error %s", exc)
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "0.0.0.0", port)
    logging.getLogger("http").info("health/metrics/stats on :%d", port)
    return server


async def make_consumer(settings: Settings, topics: list[str], group: str):
    from aiokafka import AIOKafkaConsumer

    delay = 1.0
    while True:
        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=settings.kafka_broker,
            group_id=group,
            enable_auto_commit=False,          # commit only after successful processing
            auto_offset_reset="earliest",
            max_poll_records=settings.batch_size,
            max_poll_interval_ms=600_000,
            session_timeout_ms=30_000,
            heartbeat_interval_ms=3_000,
            fetch_max_wait_ms=100,
            value_deserializer=lambda b: b,
        )
        try:
            await consumer.start()
            logging.getLogger("kafka").info("consumer %s subscribed to %s via %s", group, topics, settings.kafka_broker)
            return consumer
        except Exception as exc:
            logging.getLogger("kafka").warning("Kafka not reachable (%s); retrying in %.0fs", exc, delay)
            try:
                await consumer.stop()
            except Exception:
                pass
            await asyncio.sleep(delay)
            delay = min(delay * 2, 15)


async def retry_forever(coro_fn, what: str, counter=None, max_delay: float = 10.0):
    """Run ``coro_fn`` until it succeeds (exponential back-off). Used around DB writes so a
    temporarily unavailable database pauses consumption instead of losing data."""
    delay = 0.5
    attempt = 0
    while True:
        try:
            return await coro_fn()
        except Exception as exc:
            attempt += 1
            if counter is not None:
                counter.inc()
            logging.getLogger("retry").warning("%s failed (attempt %d): %s - retrying in %.1fs",
                                               what, attempt, exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)


class Breaker:
    """Very small circuit breaker: after a failure, skip the dependency for ``cooldown`` seconds."""

    def __init__(self, cooldown: float = 5.0):
        self.cooldown = cooldown
        self.open_until = 0.0

    @property
    def closed(self) -> bool:
        return time.time() >= self.open_until

    def trip(self) -> None:
        self.open_until = time.time() + self.cooldown

