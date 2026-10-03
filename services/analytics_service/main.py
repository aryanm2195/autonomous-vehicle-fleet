"""Analytics Service

Consumes ``vehicle.telemetry`` in its own consumer group (Kafka fan-out: the
same stream feeds telemetry, alerts and analytics independently) and every
``ANALYTICS_WINDOW_S`` seconds computes fleet-level aggregates:

* active vehicles, status / traffic / activity distribution
* average speed & battery, low-battery count, passengers on board
* sensor health (failed / degraded counts)
* ingest throughput (msg/s) for the window
* demand hotspots: k-means clustering of vehicle positions (project plan §10.4)

Results go to Redis (``analytics:summary``, ``analytics:hotspots``,
``analytics:throughput`` time series) for the dashboard and to MongoDB
``analytics_snapshots`` for history.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import time
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge

from common.runtime import Settings, make_consumer, parse_ts, serve_http

log = logging.getLogger("analytics")

CONSUMED = Counter("analytics_consumed_total", "Telemetry messages consumed")
SNAPSHOTS = Counter("analytics_snapshots_total", "Aggregation windows published")
FLEET_ACTIVE = Gauge("fleet_active_vehicles", "Vehicles that reported within the activity window")
FLEET_STATUS = Gauge("fleet_vehicles_by_status", "Active vehicles per status", ["status"])
FLEET_MSG_RATE = Gauge("fleet_ingest_messages_per_second", "Telemetry msg/s observed by analytics")
FLEET_AVG_BATTERY = Gauge("fleet_avg_battery_pct", "Average battery of active vehicles")

STATUSES = ["AVAILABLE", "IN_SERVICE", "IDLE", "CHARGING", "MAINTENANCE", "EMERGENCY"]
TRAFFIC = ["LOW", "MODERATE", "HIGH", "CONGESTED"]


def kmeans(points: list[tuple[float, float]], k: int = 5, iters: int = 10, seed: int = 0) -> list[dict]:
    """Plain k-means (lat/lon treated as planar - fine at city scale)."""
    if not points:
        return []
    k = min(k, len(points))
    rng = random.Random(seed)
    cents = rng.sample(points, k)
    assign = [0] * len(points)
    for _ in range(iters):
        for i, (y, x) in enumerate(points):
            assign[i] = min(range(k), key=lambda c: (y - cents[c][0]) ** 2 + (x - cents[c][1]) ** 2)
        sums = [[0.0, 0.0, 0] for _ in range(k)]
        for (y, x), c in zip(points, assign):
            sums[c][0] += y
            sums[c][1] += x
            sums[c][2] += 1
        cents = [(s[0] / s[2], s[1] / s[2]) if s[2] else cents[i] for i, s in enumerate(sums)]
    sizes = [0] * k
    for c in assign:
        sizes[c] += 1
    return sorted([{"latitude": round(c[0], 6), "longitude": round(c[1], 6), "vehicles": sizes[i]}
                   for i, c in enumerate(cents)], key=lambda h: -h["vehicles"])


def summarise(latest: dict[str, dict], now: float, active_window: float, window_msgs: int, window_s: float) -> dict:
    status = {s: 0 for s in STATUSES}
    traffic = {t: 0 for t in TRAFFIC}
    activity: dict[str, int] = {}
    n = speed = battery = 0.0
    low = critical = passengers = failed = degraded = 0
    for m in latest.values():
        if now - m["_seen"] > active_window:
            continue
        n += 1
        v, b, loc = m.get("vehicle", {}), m.get("battery", {}), m.get("location", {})
        status[v.get("status", "AVAILABLE")] = status.get(v.get("status", "AVAILABLE"), 0) + 1
        traffic[(m.get("traffic") or {}).get("status", "LOW")] += 1
        activity[v.get("activity", "?")] = activity.get(v.get("activity", "?"), 0) + 1
        speed += loc.get("speed_kmh", 0) or 0
        pct = b.get("percentage", 0) or 0
        battery += pct
        low += pct < 20
        critical += pct < 10
        passengers += v.get("passenger_count", 0) or 0
        for sensor in ("lidar", "radar", "camera"):
            st = (m.get(sensor) or {}).get("status")
            failed += st == "FAILED"
            degraded += st == "DEGRADED"
        if loc.get("gps_status") == "FAILED":
            failed += 1
    return {
        "timestamp": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "active_vehicles": int(n),
        "known_vehicles": len(latest),
        "status": status, "traffic": traffic, "activity": activity,
        "avg_speed_kmh": round(speed / n, 1) if n else 0,
        "avg_battery_pct": round(battery / n, 1) if n else 0,
        "low_battery_vehicles": low, "critical_battery_vehicles": critical,
        "passengers_on_board": passengers,
        "sensors_failed": failed, "sensors_degraded": degraded,
        "messages_per_second": round(window_msgs / window_s, 1) if window_s else 0,
        "simulated": True,
    }


class AnalyticsService:
    def __init__(self, settings: Settings):
        self.s = settings
        self.window_s = float(os.getenv("ANALYTICS_WINDOW_S", "5"))
        # a vehicle counts as "active" if it reported within ~1.5 reporting intervals (min 30 s);
        # a fixed 30 s window under-counts when TELEMETRY_INTERVAL is long (e.g. 300 s -> only ~10 %)
        interval = float(os.getenv("TELEMETRY_INTERVAL", "1") or 1)
        self.active_window = float(os.getenv("ACTIVE_WINDOW_S") or max(30.0, 1.5 * interval))
        self.latest: dict[str, dict] = {}
        self.window_msgs = 0
        self.running = True
        self.last_summary: dict = {}

    async def connect(self):
        from motor.motor_asyncio import AsyncIOMotorClient
        import redis.asyncio as aioredis
        self.mongo = AsyncIOMotorClient(self.s.mongodb_uri, serverSelectionTimeoutMS=5000)
        self.db = self.mongo[self.s.mongodb_db]
        self.redis = aioredis.from_url(self.s.redis_uri, decode_responses=True, socket_timeout=2)
        try:
            await self.db.analytics_snapshots.create_index("ts", expireAfterSeconds=7 * 86400)
        except Exception as exc:
            log.warning("index creation deferred: %s", exc)

    async def publish_loop(self):
        last = time.time()
        while self.running:
            await asyncio.sleep(self.window_s)
            now = time.time()
            msgs, self.window_msgs = self.window_msgs, 0
            summary = summarise(self.latest, now, self.active_window, msgs, now - last)
            last = now
            pts = [(m["location"]["latitude"], m["location"]["longitude"]) for m in self.latest.values()
                   if now - m["_seen"] <= self.active_window and m.get("location")]
            if len(pts) > 3000:
                pts = random.sample(pts, 3000)
            hotspots = await asyncio.get_running_loop().run_in_executor(None, kmeans, pts, 5, 8)
            summary["hotspots"] = hotspots
            self.last_summary = summary
            FLEET_ACTIVE.set(summary["active_vehicles"])
            FLEET_MSG_RATE.set(summary["messages_per_second"])
            FLEET_AVG_BATTERY.set(summary["avg_battery_pct"])
            for st, c in summary["status"].items():
                FLEET_STATUS.labels(st).set(c)
            point = json.dumps({"t": summary["timestamp"], "mps": summary["messages_per_second"],
                                "active": summary["active_vehicles"]})
            try:
                pipe = self.redis.pipeline(transaction=False)
                pipe.set("analytics:summary", json.dumps(summary))
                pipe.set("analytics:hotspots", json.dumps(hotspots))
                pipe.rpush("analytics:throughput", point)
                pipe.ltrim("analytics:throughput", -240, -1)
                await pipe.execute()
            except Exception as exc:
                log.warning("Redis write failed (%s)", exc)
            try:
                await self.db.analytics_snapshots.insert_one({**summary, "ts": datetime.now(timezone.utc)})
            except Exception as exc:
                log.warning("Mongo snapshot failed (%s)", exc)
            SNAPSHOTS.inc()
            # forget vehicles silent for 10 minutes
            self.latest = {k: v for k, v in self.latest.items() if now - v["_seen"] < 600}

    async def run(self):
        await self.connect()
        consumer = await make_consumer(self.s, [self.s.topic_telemetry], "analytics-service")
        asyncio.create_task(self.publish_loop())
        try:
            while self.running:
                batches = await consumer.getmany(timeout_ms=200, max_records=self.s.batch_size)
                now = time.time()
                n = 0
                for recs in batches.values():
                    for r in recs:
                        try:
                            m = json.loads(r.value)
                        except ValueError:
                            continue
                        m["_seen"] = parse_ts(m.get("timestamp")) or now
                        self.latest[m["vehicle_id"]] = m
                        n += 1
                if n:
                    CONSUMED.inc(n)
                    self.window_msgs += n
                    await consumer.commit()
        finally:
            await consumer.stop()

    def stats(self):
        return {"service": "analytics-service", "consumed": CONSUMED._value.get(), "summary": self.last_summary}

    async def ready(self):
        try:
            await self.mongo.admin.command("ping")
            return True
        except Exception:
            return False


async def amain():
    s = Settings("analytics-service")
    svc = AnalyticsService(s)
    await serve_http(s.http_port, svc.stats, svc.ready)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: setattr(svc, "running", False))
        except NotImplementedError:
            pass
    await svc.run()


if __name__ == "__main__":
    asyncio.run(amain())
