"""Data access for the API: Redis first (latest state), MongoDB for history and as fall-back."""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone

log = logging.getLogger("store")

STATUSES = ["AVAILABLE", "IN_SERVICE", "IDLE", "CHARGING", "MAINTENANCE", "EMERGENCY"]


def _clean(doc: dict | None) -> dict | None:
    if doc is None:
        return None
    doc = dict(doc)
    if not isinstance(doc.get("_id"), str):
        doc.pop("_id", None)
    for k, v in list(doc.items()):
        if isinstance(v, datetime):
            if v.tzinfo is None:          # BSON dates are UTC; make that explicit for browsers
                v = v.replace(tzinfo=timezone.utc)
            doc[k] = v.isoformat()
    return doc


class Store:
    def __init__(self, mongo=None, redis=None, db_name: str | None = None):
        self.mongo = mongo
        self.redis = redis
        self.db_name = db_name or os.getenv("MONGODB_DB", "fleet")
        self.db = mongo[self.db_name] if mongo is not None else None

    async def connect(self):
        from motor.motor_asyncio import AsyncIOMotorClient
        import redis.asyncio as aioredis
        self.mongo = AsyncIOMotorClient(os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
                                        serverSelectionTimeoutMS=3000, tz_aware=True)
        self.db = self.mongo[self.db_name]
        self.redis = aioredis.from_url(os.getenv("REDIS_URI", "redis://localhost:6379/0"), decode_responses=True,
                                       socket_timeout=2, socket_connect_timeout=2)

    async def close(self):
        try:
            await self.redis.aclose()
        except Exception:
            pass
        if self.mongo is not None:
            self.mongo.close()

    async def health(self) -> dict:
        async def mongo_ok():
            try:
                await asyncio.wait_for(self.db.command("ping"), 2)
                return True
            except Exception:
                return False

        async def redis_ok():
            try:
                return bool(await asyncio.wait_for(self.redis.ping(), 2))
            except Exception:
                return False
        m, r = await asyncio.gather(mongo_ok(), redis_ok())
        return {"mongodb": m, "redis": r}

    # ---------------------------------------------------------------- fleet
    async def summary(self) -> dict:
        try:
            raw = await self.redis.get("analytics:summary")
            if raw:
                s = json.loads(raw)
                s["source"] = "redis"
                return s
        except Exception as exc:
            log.warning("redis summary failed (%s); falling back to MongoDB", exc)
        # degraded mode: compute from the durable vehicle_state snapshot
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=max(60.0, 1.5 * float(os.getenv("TELEMETRY_INTERVAL", "1") or 1)))
        pipeline = [{"$match": {"ts": {"$gte": cutoff}}},
                    {"$group": {"_id": "$vehicle.status", "n": {"$sum": 1},
                                "battery": {"$avg": "$battery.percentage"}, "speed": {"$avg": "$location.speed_kmh"},
                                "pax": {"$sum": "$vehicle.passenger_count"}}}]
        status = {s: 0 for s in STATUSES}
        n = pax = 0
        bsum = ssum = 0.0
        try:
            async for g in self.db.vehicle_state.aggregate(pipeline):
                status[g["_id"]] = g["n"]
                n += g["n"]
                pax += g["pax"]
                bsum += (g["battery"] or 0) * g["n"]
                ssum += (g["speed"] or 0) * g["n"]
        except Exception as exc:
            log.warning("mongo summary failed: %s", exc)
            return {"active_vehicles": 0, "status": status, "source": "unavailable"}
        return {"active_vehicles": n, "status": status, "passengers_on_board": pax,
                "avg_battery_pct": round(bsum / n, 1) if n else 0, "avg_speed_kmh": round(ssum / n, 1) if n else 0,
                "source": "mongodb-fallback", "simulated": True}

    async def positions(self, limit: int = 1500) -> list[dict]:
        """Compact positions for the map (most recently seen first)."""
        try:
            ids = await self.redis.zrevrange("fleet:last_seen", 0, limit - 1)
            if not ids:
                return []
            pipe = self.redis.pipeline(transaction=False)
            for vid in ids:
                pipe.hmget(f"vehicle:{vid}", "lat", "lon", "status", "battery", "speed_kmh", "heading", "passengers")
            rows = await pipe.execute()
            out = []
            for vid, r in zip(ids, rows):
                if r and r[0]:
                    out.append({"id": vid, "lat": float(r[0]), "lon": float(r[1]), "s": r[2], "b": float(r[3] or 0),
                                "v": float(r[4] or 0), "h": float(r[5] or 0), "p": int(r[6] or 0)})
            return out
        except Exception:
            cur = self.db.vehicle_state.find({}, {"location": 1, "vehicle": 1, "battery": 1}).limit(limit)
            return [{"id": d["_id"], "lat": d["location"]["latitude"], "lon": d["location"]["longitude"],
                     "s": d["vehicle"]["status"], "b": d["battery"]["percentage"], "v": d["location"]["speed_kmh"],
                     "h": d["location"].get("heading", 0), "p": d["vehicle"]["passenger_count"]} async for d in cur]

    async def vehicles(self, status: str | None, limit: int, offset: int) -> dict:
        try:
            total = await self.redis.zcard("fleet:last_seen")
            ids = await self.redis.zrange("fleet:last_seen", 0, -1) if status else \
                await self.redis.zrange("fleet:last_seen", offset, offset + limit - 1)
            pipe = self.redis.pipeline(transaction=False)
            for vid in ids:
                pipe.hgetall(f"vehicle:{vid}")
            rows = [r for r in await pipe.execute() if r]
            for r in rows:
                r.pop("data", None)
            if status:
                rows = [r for r in rows if r.get("status") == status]
                total = len(rows)
                rows = rows[offset:offset + limit]
            return {"total": total, "items": sorted(rows, key=lambda r: r["vehicle_id"]), "source": "redis"}
        except Exception:
            q = {"vehicle.status": status} if status else {}
            total = await self.db.vehicle_state.count_documents(q)
            cur = self.db.vehicle_state.find(q).sort("_id", 1).skip(offset).limit(limit)
            return {"total": total, "items": [_clean(d) async for d in cur], "source": "mongodb-fallback"}

    async def vehicle(self, vid: str) -> dict | None:
        try:
            raw = await self.redis.hget(f"vehicle:{vid}", "data")
            if raw:
                return json.loads(raw)
        except Exception:
            pass
        return _clean(await self.db.vehicle_state.find_one({"_id": vid}))

    async def near(self, lat: float, lon: float, radius_m: float, limit: int) -> list[dict]:
        try:
            res = await self.redis.geosearch("fleet:geo", longitude=lon, latitude=lat, radius=radius_m, unit="m",
                                             withdist=True, sort="ASC", count=limit)
            return [{"vehicle_id": vid, "distance_m": round(float(d), 1)} for vid, d in res]
        except Exception:
            cur = self.db.vehicle_state.find({"geo": {"$nearSphere": {"$geometry": {"type": "Point",
                                              "coordinates": [lon, lat]}, "$maxDistance": radius_m}}}).limit(limit)
            return [{"vehicle_id": d["_id"]} async for d in cur]

    async def history(self, vid: str, minutes: int, limit: int) -> list[dict]:
        since = datetime.now(timezone.utc) - timedelta(minutes=minutes)
        cur = self.db.telemetry.find({"vehicle_id": vid, "ts": {"$gte": since}},
                                     {"_id": 0, "timestamp": 1, "location": 1, "battery.percentage": 1,
                                      "vehicle.status": 1, "vehicle.passenger_count": 1, "traffic.status": 1,
                                      "safety": 1}).sort("ts", -1).limit(limit)
        return [_clean(d) async for d in cur]

    # ---------------------------------------------------------------- alerts
    async def alerts(self, status, severity, vehicle_id, limit) -> list[dict]:
        q = {}
        if status:
            q["status"] = status
        if severity:
            q["severity"] = severity
        if vehicle_id:
            q["vehicle_id"] = vehicle_id
        try:
            cur = self.db.alerts.find(q, {"_id": 0}).sort("created_at", -1).limit(limit)
            return [_clean(d) async for d in cur]
        except Exception:
            raw = await self.redis.lrange("alerts:recent", 0, limit - 1)
            return [json.loads(r) for r in raw]

    async def alert_stats(self) -> dict:
        pipeline = [{"$group": {"_id": {"status": "$status", "severity": "$severity"}, "n": {"$sum": 1}}}]
        out = {"by_status": {}, "by_severity": {}, "open_by_severity": {}}
        async for g in self.db.alerts.aggregate(pipeline):
            st, sev = g["_id"]["status"], g["_id"]["severity"]
            out["by_status"][st] = out["by_status"].get(st, 0) + g["n"]
            out["by_severity"][sev] = out["by_severity"].get(sev, 0) + g["n"]
            if st == "OPEN":
                out["open_by_severity"][sev] = out["open_by_severity"].get(sev, 0) + g["n"]
        lat = [d["pipeline_latency_ms"] async for d in self.db.alerts.find(
            {"pipeline_latency_ms": {"$ne": None}}, {"pipeline_latency_ms": 1}).sort("created_at", -1).limit(500)]
        lat.sort()
        if lat:
            out["latency_ms"] = {"samples": len(lat), "p50": lat[len(lat) // 2],
                                 "p95": lat[min(len(lat) - 1, int(len(lat) * 0.95))], "max": lat[-1]}
        return out

    async def update_alert(self, alert_id: str, new_status: str, by: str) -> bool:
        res = await self.db.alerts.update_one(
            {"alert_id": alert_id},
            {"$set": {"status": new_status, f"{new_status.lower()}_by": by,
                      f"{new_status.lower()}_at": datetime.now(timezone.utc)}})
        return res.matched_count == 1

    async def throughput(self) -> list[dict]:
        try:
            return [json.loads(x) for x in await self.redis.lrange("analytics:throughput", 0, -1)]
        except Exception:
            return []

    async def alert_stream(self, stop: asyncio.Event):
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("alerts:live")
        try:
            while not stop.is_set():
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg.get("type") == "message":
                    yield json.loads(msg["data"])
        finally:
            await pubsub.unsubscribe("alerts:live")
            await pubsub.aclose()
