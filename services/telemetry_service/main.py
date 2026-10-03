"""Telemetry Service

Kafka ``vehicle.telemetry``  ->  MongoDB ``telemetry`` (history, event-sourcing style)
                             ->  Redis    ``vehicle:{id}`` hash + ``fleet:geo`` (latest state, geo index)

Delivery semantics: at-least-once. Offsets are committed only after the batch
is durably written to MongoDB. Replays are harmless because ``message_id`` has
a unique index (duplicates are counted and skipped).

Failure behaviour:
* MongoDB unavailable -> the batch is retried with back-off; consumption pauses
  (Kafka retains the data, consumer lag grows, nothing is lost).
* Redis unavailable   -> circuit breaker skips Redis; MongoDB remains the source
  of truth and the ``vehicle_state`` snapshot collection keeps the API working.
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, Histogram

from common.runtime import Breaker, LatencyWindow, Settings, make_consumer, parse_ts, retry_forever, serve_http

log = logging.getLogger("telemetry")

CONSUMED = Counter("telemetry_consumed_total", "Telemetry messages consumed from Kafka")
INSERTED = Counter("telemetry_inserted_total", "Telemetry documents inserted into MongoDB")
DUPLICATES = Counter("telemetry_duplicates_total", "Messages skipped by the unique message_id index")
BAD = Counter("telemetry_bad_messages_total", "Undecodable messages skipped")
MONGO_RETRIES = Counter("telemetry_mongo_retries_total", "MongoDB write retries")
REDIS_ERRORS = Counter("telemetry_redis_errors_total", "Redis update failures (degraded mode)")
BATCH_SECONDS = Histogram("telemetry_mongo_batch_seconds", "insert_many duration per batch",
                          buckets=(.002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5))
BATCH_SIZE = Histogram("telemetry_batch_size", "Messages per processed batch", buckets=(1, 10, 50, 100, 250, 500, 1000, 2000, 5000))
E2E = Histogram("telemetry_e2e_latency_seconds", "Vehicle timestamp -> persisted in MongoDB",
                buckets=(.01, .025, .05, .1, .2, .3, .5, .75, 1, 2, 5, 10, 30))
ACTIVE = Gauge("telemetry_vehicles_seen", "Distinct vehicles with state in the current process")


def to_document(msg: dict) -> dict:
    """Kafka message -> MongoDB document (adds a real datetime and a GeoJSON point)."""
    doc = dict(msg)
    ts = parse_ts(msg.get("timestamp"))
    doc["ts"] = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(timezone.utc)
    loc = msg.get("location") or {}
    if isinstance(loc.get("longitude"), (int, float)) and isinstance(loc.get("latitude"), (int, float)):
        doc["geo"] = {"type": "Point", "coordinates": [loc["longitude"], loc["latitude"]]}
    return doc


def to_state(msg: dict) -> dict:
    """Compact latest-state record stored in Redis (flat fields + full JSON)."""
    loc, bat, veh = msg.get("location", {}), msg.get("battery", {}), msg.get("vehicle", {})
    return {
        "vehicle_id": msg["vehicle_id"],
        "timestamp": msg.get("timestamp", ""),
        "status": veh.get("status", ""),
        "passengers": veh.get("passenger_count", 0),
        "battery": bat.get("percentage", 0),
        "charging": int(bool(bat.get("charging"))),
        "lat": loc.get("latitude", 0),
        "lon": loc.get("longitude", 0),
        "speed_kmh": loc.get("speed_kmh", 0),
        "heading": loc.get("heading", 0),
        "traffic": (msg.get("traffic") or {}).get("status", ""),
        "data": json.dumps(msg, separators=(",", ":")),
    }


class TelemetryService:
    def __init__(self, settings: Settings):
        self.s = settings
        self.latency = LatencyWindow()
        self.stage_latency = {"edge": LatencyWindow(20000), "kafka_to_service": LatencyWindow(20000)}
        self.mongo_batch_ms = LatencyWindow(5000)
        self.redis_breaker = Breaker(5.0)
        self.latest: dict[str, dict] = {}
        self.dirty: set[str] = set()
        self.running = True
        self.last_batch_at = 0.0
        self.mongo = None
        self.redis = None

    # ------------------------------------------------------------------ setup
    async def connect(self):
        from motor.motor_asyncio import AsyncIOMotorClient
        import redis.asyncio as aioredis

        self.mongo = AsyncIOMotorClient(self.s.mongodb_uri, serverSelectionTimeoutMS=5000, retryWrites=True)
        self.db = self.mongo[self.s.mongodb_db]
        self.redis = aioredis.from_url(self.s.redis_uri, decode_responses=True, socket_timeout=2, socket_connect_timeout=2)
        await retry_forever(self.ensure_indexes, "MongoDB index creation")

    async def ensure_indexes(self):
        ttl_days = int(__import__("os").getenv("TELEMETRY_TTL_DAYS", "7"))
        t = self.db.telemetry
        await t.create_index("message_id", unique=True)
        await t.create_index([("vehicle_id", 1), ("ts", -1)])
        await t.create_index([("geo", "2dsphere")])
        await t.create_index("ts", expireAfterSeconds=ttl_days * 86400, name="ttl_ts")  # time-based retention
        await self.db.vehicle_state.create_index([("geo", "2dsphere")])
        await self.db.vehicle_state.create_index("vehicle.status")

    # ------------------------------------------------------------------ processing
    async def handle_batch(self, records: list) -> None:
        docs, states = [], {}
        for r in records:
            try:
                m = json.loads(r.value)
                docs.append(to_document(m))
                states[m["vehicle_id"]] = m
            except (ValueError, KeyError, TypeError):
                BAD.inc()
        CONSUMED.inc(len(records))
        BATCH_SIZE.observe(len(records))
        if not docs:
            return

        async def write():
            from pymongo.errors import BulkWriteError
            t0 = time.perf_counter()
            try:
                res = await self.db.telemetry.insert_many(docs, ordered=False)
                INSERTED.inc(len(res.inserted_ids))
            except BulkWriteError as bwe:
                errs = bwe.details.get("writeErrors", [])
                dups = sum(1 for e in errs if e.get("code") == 11000)
                others = [e for e in errs if e.get("code") != 11000]
                DUPLICATES.inc(dups)
                INSERTED.inc(bwe.details.get("nInserted", 0))
                if others:
                    raise RuntimeError(f"{len(others)} non-duplicate write errors, first: {others[0].get('errmsg')}")
            BATCH_SECONDS.observe(time.perf_counter() - t0)
            self.mongo_batch_ms.add((time.perf_counter() - t0) * 1000)
        # retry until MongoDB accepts the batch (do not commit offsets before that)
        await retry_forever(write, "MongoDB insert_many", MONGO_RETRIES)

        now = time.time()
        for d in docs:
            gen = d["ts"].timestamp()
            ms = (now - gen) * 1000
            self.latency.add(ms)
            E2E.observe(ms / 1000)
            ing = d.get("ingest") or {}
            if isinstance(ing.get("edge_latency_ms"), (int, float)):
                self.stage_latency["edge"].add(ing["edge_latency_ms"])
            rec = parse_ts(ing.get("received_at"))
            if rec:
                self.stage_latency["kafka_to_service"].add((now - rec) * 1000)

        self.latest.update(states)
        self.dirty.update(states)
        ACTIVE.set(len(self.latest))
        await self.update_redis(states)
        self.last_batch_at = now

    async def update_redis(self, states: dict[str, dict]) -> None:
        if not self.redis_breaker.closed:
            return
        try:
            pipe = self.redis.pipeline(transaction=False)
            for vid, m in states.items():
                st = to_state(m)
                pipe.hset(f"vehicle:{vid}", mapping=st)
                if st["lat"] and st["lon"]:
                    pipe.geoadd("fleet:geo", (st["lon"], st["lat"], vid))
                pipe.zadd("fleet:last_seen", {vid: parse_ts(st["timestamp"]) or time.time()})
            pipe.incrby("stats:telemetry_persisted", len(states))
            await pipe.execute()
        except Exception as exc:
            REDIS_ERRORS.inc()
            self.redis_breaker.trip()
            log.warning("Redis unavailable (%s) - degraded mode for %.0fs; MongoDB remains source of truth",
                        exc, self.redis_breaker.cooldown)

    async def snapshot_loop(self):
        """Every 15 s upsert the latest state of changed vehicles into MongoDB ``vehicle_state``
        (fallback read model when Redis is down, and a durable fleet registry)."""
        from pymongo import ReplaceOne
        while self.running:
            await asyncio.sleep(15)
            if not self.dirty:
                continue
            ids, self.dirty = list(self.dirty), set()
            ops = []
            for vid in ids:
                d = to_document(self.latest[vid])
                d["_id"] = vid
                d.pop("message_id", None)
                ops.append(ReplaceOne({"_id": vid}, d, upsert=True))
            try:
                for i in range(0, len(ops), 1000):
                    await self.db.vehicle_state.bulk_write(ops[i:i + 1000], ordered=False)
            except Exception as exc:
                log.warning("vehicle_state snapshot failed: %s", exc)
                self.dirty.update(ids)

    # ------------------------------------------------------------------ loop
    async def run(self):
        await self.connect()
        consumer = await make_consumer(self.s, [self.s.topic_telemetry], "telemetry-service")
        asyncio.create_task(self.snapshot_loop())
        try:
            while self.running:
                batches = await consumer.getmany(timeout_ms=200, max_records=self.s.batch_size)
                records = [r for recs in batches.values() for r in recs]
                if not records:
                    continue
                await self.handle_batch(records)
                await consumer.commit()
        finally:
            await consumer.stop()

    def stats(self) -> dict:
        return {
            "service": "telemetry-service",
            "consumed": CONSUMED._value.get(), "inserted": INSERTED._value.get(),
            "duplicates": DUPLICATES._value.get(), "bad": BAD._value.get(),
            "mongo_retries": MONGO_RETRIES._value.get(), "redis_errors": REDIS_ERRORS._value.get(),
            "vehicles_seen": len(self.latest),
            "e2e_latency_ms": self.latency.summary(),
            "stage_latency_ms": {k: v.summary() for k, v in self.stage_latency.items()},
            "mongo_insert_batch_ms": self.mongo_batch_ms.summary(),
            "redis_degraded": not self.redis_breaker.closed,
        }

    def reset(self):
        self.latency.reset()
        self.mongo_batch_ms.reset()
        for v in self.stage_latency.values():
            v.reset()

    async def ready(self) -> bool:
        try:
            await self.mongo.admin.command("ping")
            return True
        except Exception:
            return False


async def amain():
    s = Settings("telemetry-service")
    svc = TelemetryService(s)
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
