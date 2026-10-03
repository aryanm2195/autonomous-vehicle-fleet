"""Service logic tests using in-memory stand-ins for MongoDB (mongomock-motor) and Redis (fakeredis).
Kafka is not needed: we feed records directly into the batch handlers."""
import json
import time
import types
from datetime import datetime, timezone

import fakeredis.aioredis
import pytest
from mongomock_motor import AsyncMongoMockClient

from alert_service.main import AlertService
from alert_service.rules import dedupe_key, evaluate_telemetry
from analytics_service.main import kmeans, summarise
from common.runtime import LatencyWindow, Settings
from telemetry_service.main import TelemetryService, to_document, to_state


def telemetry(vid="vehicle-00001", **over):
    t = {
        "schema_version": "1.1.0", "message_id": f"m-{time.time_ns()}", "vehicle_id": vid, "sequence": 1,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "location": {"latitude": -37.81, "longitude": 144.96, "speed_kmh": 42.0, "heading": 90, "gps_status": "OK"},
        "battery": {"percentage": 75.0, "charging": False},
        "lidar": {"status": "OK", "obstacle_detected": False, "distance_m": 50, "object_count": 1},
        "radar": {"status": "OK", "object_detected": False, "distance_m": None, "relative_speed_mps": 0},
        "camera": {"status": "OK", "object_detected": False, "object_type": None, "confidence": 0},
        "safety": {"time_to_collision_s": None, "emergency_braking": False},
        "vehicle": {"status": "IN_SERVICE", "passenger_count": 2, "route_deviation_m": 3.0},
        "traffic": {"status": "MODERATE"},
        "ingest": {"received_at": datetime.now(timezone.utc).isoformat(), "edge_latency_ms": 3},
    }
    for k, v in over.items():
        t[k] = {**t.get(k, {}), **v} if isinstance(v, dict) else v
    return t


def rec(obj, topic="vehicle.telemetry"):
    return types.SimpleNamespace(value=json.dumps(obj).encode(), topic=topic)


# ---------------------------------------------------------------- pure functions
def test_to_document_adds_geojson_and_datetime():
    d = to_document(telemetry())
    assert d["geo"] == {"type": "Point", "coordinates": [144.96, -37.81]}
    assert isinstance(d["ts"], datetime)


def test_to_state_is_flat_and_redis_safe():
    st = to_state(telemetry())
    assert all(isinstance(v, (str, int, float)) for v in st.values())
    assert json.loads(st["data"])["vehicle_id"] == "vehicle-00001"


def test_rules_detect_each_condition():
    types_ = lambda t: {h["event_type"] for h in evaluate_telemetry(t)}
    assert types_(telemetry()) == set()
    assert "LOW_BATTERY" in types_(telemetry(battery={"percentage": 15.0}))
    assert "CRITICAL_BATTERY" in types_(telemetry(battery={"percentage": 5.0}))
    assert types_(telemetry(battery={"percentage": 5.0, "charging": True})) == set()
    assert "COLLISION_WARNING" in types_(telemetry(safety={"time_to_collision_s": 1.2, "emergency_braking": True}))
    assert "SENSOR_FAILURE" in types_(telemetry(lidar={"status": "FAILED"}))
    assert "ABNORMAL_SPEED" in types_(telemetry(location={"speed_kmh": 140.0}))
    assert "ROUTE_DEVIATION" in types_(telemetry(vehicle={"route_deviation_m": 420.0}))


def test_dedupe_key_distinguishes_sensors():
    assert dedupe_key("v", "SENSOR_FAILURE", {"sensor": "lidar"}) != dedupe_key("v", "SENSOR_FAILURE", {"sensor": "radar"})


def test_latency_window_percentiles():
    w = LatencyWindow()
    for i in range(1, 101):
        w.add(i)
    s = w.summary()
    assert (s["p50"], s["p95"], s["p99"]) == (51, 96, 100)


def test_kmeans_finds_two_clusters():
    pts = [(-37.80 + i * 1e-4, 144.90) for i in range(50)] + [(-37.85 + i * 1e-4, 145.00) for i in range(50)]
    hs = kmeans(pts, k=2)
    assert sorted(h["vehicles"] for h in hs) == [50, 50]


def test_summarise_counts_only_active():
    now = time.time()
    a = telemetry("vehicle-00001"); a["_seen"] = now
    b = telemetry("vehicle-00002", vehicle={"status": "CHARGING", "passenger_count": 0}); b["_seen"] = now - 120
    s = summarise({"a": a, "b": b}, now, 30, 100, 5)
    assert s["active_vehicles"] == 1 and s["status"]["IN_SERVICE"] == 1 and s["messages_per_second"] == 20


# ---------------------------------------------------------------- telemetry service
@pytest.fixture
def settings():
    return Settings("test")


async def make_telemetry_service(settings):
    svc = TelemetryService(settings)
    svc.mongo = AsyncMongoMockClient()
    svc.db = svc.mongo["fleet"]
    svc.redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    await svc.db.telemetry.create_index("message_id", unique=True)
    return svc


async def test_telemetry_batch_persists_and_updates_redis(settings):
    svc = await make_telemetry_service(settings)
    msgs = [telemetry(f"vehicle-0000{i}") for i in range(1, 6)]
    await svc.handle_batch([rec(m) for m in msgs])
    assert await svc.db.telemetry.count_documents({}) == 5
    h = await svc.redis.hgetall("vehicle:vehicle-00003")
    assert h["status"] == "IN_SERVICE" and float(h["battery"]) == 75.0
    assert len(await svc.redis.geosearch("fleet:geo", longitude=144.96, latitude=-37.81, radius=1, unit="km")) == 5
    assert svc.stats()["e2e_latency_ms"]["samples"] == 5


async def test_telemetry_replay_is_idempotent(settings):
    svc = await make_telemetry_service(settings)
    m = telemetry()
    await svc.handle_batch([rec(m)])
    await svc.handle_batch([rec(m), rec(telemetry("vehicle-00002"))])   # Kafka redelivery after a crash
    assert await svc.db.telemetry.count_documents({}) == 2


async def test_redis_outage_degrades_gracefully(settings):
    svc = await make_telemetry_service(settings)

    class Broken:
        def pipeline(self, **_):
            raise ConnectionError("redis down")
    svc.redis = Broken()
    await svc.handle_batch([rec(telemetry())])
    assert await svc.db.telemetry.count_documents({}) == 1   # data still persisted
    assert svc.stats()["redis_degraded"] is True


async def test_bad_message_is_skipped(settings):
    svc = await make_telemetry_service(settings)
    bad = types.SimpleNamespace(value=b"{not json", topic="vehicle.telemetry")
    await svc.handle_batch([bad, rec(telemetry())])
    assert await svc.db.telemetry.count_documents({}) == 1


# ---------------------------------------------------------------- alert service
class FakeProducer:
    def __init__(self):
        self.sent = []

    async def send(self, topic, value, key=None):
        self.sent.append((topic, json.loads(value)))


async def make_alert_service(settings):
    svc = AlertService(settings)
    svc.mongo = AsyncMongoMockClient()
    svc.db = svc.mongo["fleet"]
    svc.redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    svc.producer = FakeProducer()
    return svc


def event(vid="vehicle-00007", etype="COLLISION_WARNING", sev="CRITICAL"):
    return {"vehicle_id": vid, "event_type": etype, "severity": sev, "event_id": "evt-1",
            "timestamp": datetime.now(timezone.utc).isoformat(), "details": {"time_to_collision_s": 1.1},
            "location": {"latitude": -37.81, "longitude": 144.96}, "vehicle_status": "IN_SERVICE"}


async def test_emergency_event_flows_to_mongo_redis_and_kafka(settings):
    svc = await make_alert_service(settings)
    pubsub = svc.redis.pubsub()
    await pubsub.subscribe("alerts:live")
    await svc.handle_event(event())
    doc = await svc.db.alerts.find_one({"vehicle_id": "vehicle-00007"})
    assert doc["event_type"] == "COLLISION_WARNING" and doc["status"] == "OPEN"
    assert doc["pipeline_latency_ms"] >= 0
    assert json.loads((await svc.redis.lrange("alerts:recent", 0, 0))[0])["severity"] == "CRITICAL"
    assert svc.producer.sent[0][0] == settings.topic_alerts
    msg = None
    for _ in range(10):
        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)
        if msg:
            break
    assert msg and json.loads(msg["data"])["vehicle_id"] == "vehicle-00007"


async def test_vehicle_and_platform_detection_are_merged(settings):
    svc = await make_alert_service(settings)
    await svc.handle_event(event())
    await svc.handle_telemetry(telemetry("vehicle-00007", safety={"time_to_collision_s": 1.0, "emergency_braking": True}))
    await svc.handle_event(event())
    assert await svc.db.alerts.count_documents({}) == 1
    doc = await svc.db.alerts.find_one({})
    assert set(doc["sources"]) == {"VEHICLE", "PLATFORM"}
    assert sum(svc.pending_merges.values()) == 2


async def test_dedupe_falls_back_to_memory_when_redis_down(settings):
    svc = await make_alert_service(settings)

    class Down:
        async def set(self, *a, **k):
            raise ConnectionError("down")

        def pipeline(self, **_):
            raise ConnectionError("down")
    svc.redis = Down()
    svc.active.clear()
    await svc.handle_event(event(etype="SENSOR_FAILURE", sev="HIGH"))
    svc.active.clear()   # force the fallback path to be exercised
    await svc.handle_event(event(etype="SENSOR_FAILURE", sev="HIGH"))
    assert await svc.db.alerts.count_documents({}) == 1
