"""Alert Service

Consumes ``vehicle.events`` (vehicle-reported emergencies) and
``vehicle.telemetry`` (platform-side rule evaluation), de-duplicates, and
for every new alert:

  1. stores it in MongoDB ``alerts`` (durable, queryable, acknowledgeable)
  2. pushes it to Redis (``alerts:recent`` list + ``alerts:live`` pub/sub
     channel -> the backend WebSocket -> the dashboard, in real time)
  3. republishes it on Kafka ``vehicle.alerts`` for downstream consumers
     (notification, maintenance, charging services)

It measures the end-to-end latency from the vehicle's timestamp to the moment
the alert is stored - the key number for the "critical payload" requirement.
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
import uuid
from datetime import datetime, timezone

from prometheus_client import Counter, Histogram

from common.runtime import LatencyWindow, Settings, make_consumer, parse_ts, retry_forever, serve_http

from .rules import COOLDOWN_S, SEVERITY_RANK, dedupe_key, evaluate_telemetry

log = logging.getLogger("alerts")

CONSUMED = Counter("alerts_consumed_total", "Messages consumed", ["topic"])
CREATED = Counter("alerts_created_total", "New alerts created", ["event_type", "severity", "source"])
MERGED = Counter("alerts_merged_total", "Detections merged into an existing alert (dedupe)")
ERRORS = Counter("alerts_errors_total", "Processing errors", ["stage"])
LATENCY = Histogram("alerts_e2e_latency_seconds", "Vehicle timestamp -> alert stored & published", ["severity"],
                    buckets=(.005, .01, .025, .05, .075, .1, .2, .3, .5, 1, 2, 5))


class LocalCooldown:
    """In-memory fallback for de-duplication while Redis is unavailable."""

    def __init__(self):
        self.until: dict[str, tuple[float, str]] = {}

    def claim(self, key: str, alert_id: str, ttl: int) -> str | None:
        now = time.time()
        cur = self.until.get(key)
        if cur and cur[0] > now:
            return cur[1]
        self.until[key] = (now + ttl, alert_id)
        if len(self.until) > 200_000:
            self.until = {k: v for k, v in self.until.items() if v[0] > now}
        return None


class AlertService:
    def __init__(self, settings: Settings):
        self.s = settings
        self.latency = {"CRITICAL": LatencyWindow(20000), "HIGH": LatencyWindow(20000), "ALL": LatencyWindow(50000)}
        self.local = LocalCooldown()
        self.running = True
        self.producer = None
        self.created = 0
        self.active: dict[str, tuple[float, str]] = {}   # hot cache of open dedupe windows
        self.pending_merges: dict[str, int] = {}
        self._sources_seen: set = set()

    async def connect(self):
        from aiokafka import AIOKafkaProducer
        from motor.motor_asyncio import AsyncIOMotorClient
        import redis.asyncio as aioredis

        self.mongo = AsyncIOMotorClient(self.s.mongodb_uri, serverSelectionTimeoutMS=5000)
        self.db = self.mongo[self.s.mongodb_db]
        self.redis = aioredis.from_url(self.s.redis_uri, decode_responses=True, socket_timeout=2, socket_connect_timeout=2)

        async def idx():
            a = self.db.alerts
            await a.create_index("alert_id", unique=True)
            await a.create_index([("created_at", -1)])
            await a.create_index([("vehicle_id", 1), ("created_at", -1)])
            await a.create_index([("status", 1), ("severity_rank", -1), ("created_at", -1)])
        await retry_forever(idx, "alerts index creation")

        async def start_producer():
            p = AIOKafkaProducer(bootstrap_servers=self.s.kafka_broker, acks="all", linger_ms=5)
            await p.start()
            return p
        self.producer = await retry_forever(start_producer, "Kafka producer start")

    # ------------------------------------------------------------------ dedupe
    async def claim(self, key: str, alert_id: str, ttl: int) -> str | None:
        """Returns None if this is a new alert, else the id of the existing alert."""
        try:
            ok = await self.redis.set(key, alert_id, nx=True, ex=ttl)
            if ok:
                return None
            return await self.redis.get(key) or alert_id
        except Exception:
            ERRORS.labels("redis_dedupe").inc()
            return self.local.claim(key, alert_id, ttl)

    # ------------------------------------------------------------------ core
    async def raise_alert(self, vehicle_id: str, event_type: str, severity: str, source: str, details: dict,
                          generated_ts: float | None, location: dict | None, vehicle_status: str | None,
                          suffix: str | None = None, event_id: str | None = None, ingest: dict | None = None):
        alert_id = f"alr-{uuid.uuid4().hex[:12]}"
        key = dedupe_key(vehicle_id, event_type, details, suffix)
        ttl = COOLDOWN_S.get(event_type, 60)
        hot = self.active.get(key)
        now_dt = datetime.now(timezone.utc)
        if hot and hot[0] > time.time():
            # repeat detection inside the window: count it, flush to MongoDB in bulk later
            MERGED.inc()
            self.pending_merges[hot[1]] = self.pending_merges.get(hot[1], 0) + 1
            if (source, hot[1]) not in self._sources_seen:
                self._sources_seen.add((source, hot[1]))
                try:
                    await self.db.alerts.update_one({"alert_id": hot[1]}, {"$addToSet": {"sources": source}})
                except Exception:
                    ERRORS.labels("mongo_merge").inc()
            return None
        existing = await self.claim(key, alert_id, ttl)
        if existing:
            MERGED.inc()
            self.active[key] = (time.time() + ttl, existing)
            self.pending_merges[existing] = self.pending_merges.get(existing, 0) + 1
            try:
                await self.db.alerts.update_one({"alert_id": existing}, {"$addToSet": {"sources": source}})
            except Exception:
                ERRORS.labels("mongo_merge").inc()
            return None
        self.active[key] = (time.time() + ttl, alert_id)
        self._sources_seen.add((source, alert_id))

        doc = {
            "alert_id": alert_id, "vehicle_id": vehicle_id, "event_type": event_type, "severity": severity,
            "severity_rank": SEVERITY_RANK.get(severity, 0), "status": "OPEN", "sources": [source],
            "details": details, "location": location, "vehicle_status": vehicle_status, "event_id": event_id,
            "generated_at": datetime.fromtimestamp(generated_ts, tz=timezone.utc) if generated_ts else None,
            "created_at": now_dt, "first_seen": now_dt, "last_seen": now_dt, "occurrences": 1,
            "simulated": True,
        }
        await retry_forever(lambda: self.db.alerts.insert_one(dict(doc)), "MongoDB alert insert")
        stored = time.time()
        latency_ms = (stored - generated_ts) * 1000 if generated_ts else None
        out = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in doc.items()}
        out["pipeline_latency_ms"] = round(latency_ms, 2) if latency_ms is not None else None
        if ingest:
            out["edge_latency_ms"] = ingest.get("edge_latency_ms")
        payload = json.dumps(out, default=str)
        try:
            pipe = self.redis.pipeline(transaction=False)
            pipe.lpush("alerts:recent", payload)
            pipe.ltrim("alerts:recent", 0, 199)
            pipe.hincrby("alerts:counts", event_type, 1)
            pipe.publish("alerts:live", payload)
            await pipe.execute()
        except Exception:
            ERRORS.labels("redis_publish").inc()
        try:
            await self.producer.send(self.s.topic_alerts, payload.encode(), key=vehicle_id.encode())
        except Exception:
            ERRORS.labels("kafka_publish").inc()
        if latency_ms is not None:
            await self.db.alerts.update_one({"alert_id": alert_id}, {"$set": {"pipeline_latency_ms": round(latency_ms, 2)}})
            LATENCY.labels(severity).observe(latency_ms / 1000)
            self.latency["ALL"].add(latency_ms)
            if severity in self.latency:
                self.latency[severity].add(latency_ms)
        CREATED.labels(event_type, severity, source).inc()
        self.created += 1
        log.info("ALERT %-18s %-8s %s via %s latency=%sms", event_type, severity, vehicle_id, source,
                 round(latency_ms, 1) if latency_ms is not None else "?")
        return alert_id

    async def handle_event(self, e: dict):
        await self.raise_alert(e["vehicle_id"], e["event_type"], e["severity"], "VEHICLE", e.get("details") or {},
                               parse_ts(e.get("timestamp")), e.get("location"), e.get("vehicle_status"),
                               event_id=e.get("event_id"), ingest=e.get("ingest"))

    async def handle_telemetry(self, t: dict):
        for hit in evaluate_telemetry(t, self.s.low_battery, self.s.critical_battery):
            loc = t.get("location") or {}
            await self.raise_alert(t["vehicle_id"], hit["event_type"], hit["severity"], "PLATFORM", hit["details"],
                                   parse_ts(t.get("timestamp")),
                                   {"latitude": loc.get("latitude"), "longitude": loc.get("longitude")},
                                   (t.get("vehicle") or {}).get("status"), suffix=hit.get("dedupe_suffix"),
                                   ingest=t.get("ingest"))

    async def flush_merges_loop(self):
        from pymongo import UpdateOne
        while self.running:
            await asyncio.sleep(5)
            now = time.time()
            self.active = {k: v for k, v in self.active.items() if v[0] > now}
            if len(self._sources_seen) > 100_000:
                self._sources_seen.clear()
            if not self.pending_merges:
                continue
            merges, self.pending_merges = self.pending_merges, {}
            ts = datetime.now(timezone.utc)
            ops = [UpdateOne({"alert_id": a}, {"$inc": {"occurrences": n}, "$set": {"last_seen": ts}})
                   for a, n in merges.items()]
            try:
                await self.db.alerts.bulk_write(ops, ordered=False)
            except Exception:
                ERRORS.labels("mongo_merge").inc()

    async def run(self):
        await self.connect()
        asyncio.create_task(self.flush_merges_loop())
        consumer = await make_consumer(self.s, [self.s.topic_events, self.s.topic_telemetry], "alert-service")
        try:
            while self.running:
                batches = await consumer.getmany(timeout_ms=100, max_records=self.s.batch_size)
                if not batches:
                    continue
                # vehicle-reported events first: they are the most time-critical
                for tp in sorted(batches, key=lambda tp: tp.topic != self.s.topic_events):
                    for r in batches[tp]:
                        CONSUMED.labels(tp.topic).inc()
                        try:
                            m = json.loads(r.value)
                            if tp.topic == self.s.topic_events:
                                await self.handle_event(m)
                            else:
                                await self.handle_telemetry(m)
                        except (ValueError, KeyError, TypeError) as exc:
                            ERRORS.labels("decode").inc()
                            log.debug("bad message skipped: %s", exc)
                await consumer.commit()
        finally:
            await consumer.stop()
            if self.producer:
                await self.producer.stop()

    def stats(self) -> dict:
        return {"service": "alert-service", "alerts_created": self.created, "merged": MERGED._value.get(),
                "latency_ms": {k: v.summary() for k, v in self.latency.items()}}

    def reset(self):
        for v in self.latency.values():
            v.reset()

    async def ready(self) -> bool:
        try:
            await self.mongo.admin.command("ping")
            return True
        except Exception:
            return False


async def amain():
    s = Settings("alert-service")
    svc = AlertService(s)
    await serve_http(s.http_port, svc.stats, svc.ready, svc.reset)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: setattr(svc, "running", False))
        except NotImplementedError:
            pass
    await svc.run()


if __name__ == "__main__":
    asyncio.run(amain())
