#!/usr/bin/env python3
"""Checks every documented function of the RUNNING stack and prints PASS / FAIL / SKIP.

    docker compose up -d --build          # wait ~2 minutes until everything is healthy
    python scripts/verify_all.py          # add --quick to skip the 20-second wait for DB snapshots

Each check exercises the real component (no mocks): it publishes to Mosquitto,
reads Node-RED / service stats, queries Kafka, MongoDB and Redis directly,
triggers scenarios through the simulator control API and calls the backend API
and WebSocket. The results are also saved to results/verify-<timestamp>.json.

Requires: pip install -r scripts/requirements-tools.txt
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------- helpers
def load_env() -> dict:
    env = {}
    for name in (".env", ".env.example"):
        p = ROOT / name
        if p.exists():
            for line in p.read_text().splitlines():
                line = line.split("#", 1)[0].strip()
                if "=" in line:
                    k, v = line.split("=", 1)
                    env.setdefault(k.strip(), v.strip())
    return env


ENV = load_env()
USERS = {u.split(":")[2]: (u.split(":")[0], u.split(":")[1]) for u in ENV.get("API_USERS", "").split(",") if u.count(":") == 2}
HOST = "localhost"
SIM, NODERED, API = f"http://{HOST}:9105", f"http://{HOST}:1880", f"http://{HOST}:8000"
SVC = {"telemetry": f"http://{HOST}:8001", "alert": f"http://{HOST}:8002", "analytics": f"http://{HOST}:8003"}
KAFKA = f"{HOST}:29092"
MONGO_URI = ENV.get("ATLAS_URI") or f"mongodb://{ENV.get('MONGO_ROOT_USER', 'root')}:{ENV.get('MONGO_ROOT_PASSWORD', 'root-dev-password')}@{HOST}:27017/admin"
REDIS_URI = f"redis://:{ENV.get('REDIS_PASSWORD', 'redis-dev-password')}@{HOST}:6379/0"


class Skip(Exception):
    pass


def http(method, url, body=None, token=None, timeout=6):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, method=method, headers=headers,
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            try:
                return r.status, json.loads(raw)
            except ValueError:
                return r.status, raw.decode(errors="ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode(errors="ignore")


def get(url, **kw):
    code, body = http("GET", url, **kw)
    assert code == 200, f"GET {url} -> HTTP {code}"
    return body


def wait_for(fn, timeout=15, interval=0.5, what="condition"):
    t0 = time.time()
    last = None
    while time.time() - t0 < timeout:
        try:
            last = fn()
            if last:
                return last
        except Exception as exc:  # keep polling
            last = exc
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for {what} (last: {str(last)[:120]})")


# ---------------------------------------------------------------- mqtt helpers
def mqtt_client(user=None, pwd=None, cid=None):
    import paho.mqtt.client as mqtt
    c = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2, client_id=cid or f"verify-{uuid.uuid4().hex[:6]}")
    if user:
        c.username_pw_set(user, pwd)
    return c


def mqtt_connect_rc(user=None, pwd=None) -> int:
    """Return the CONNACK reason code (0 = accepted)."""
    result = {}
    ev = threading.Event()
    c = mqtt_client(user, pwd)

    def on_connect(cl, ud, flags, rc, props=None):
        result["rc"] = rc.value if hasattr(rc, "value") else rc
        ev.set()
    c.on_connect = on_connect
    try:
        c.connect(HOST, int(ENV.get("MQTT_PORT", 1883)), 10)
    except Exception as exc:
        raise AssertionError(f"cannot reach MQTT broker: {exc}")
    c.loop_start()
    ev.wait(5)
    c.loop_stop()
    c.disconnect()
    return result.get("rc", -1)


def mqtt_collect(topic, seconds, user="admin", pwd="admin-dev-password", limit=None):
    msgs = []
    c = mqtt_client(user, pwd)
    c.on_message = lambda cl, ud, m: msgs.append((m.topic, m.payload))
    c.on_connect = lambda cl, ud, f, rc, p=None: cl.subscribe(topic, qos=1)
    c.connect(HOST, int(ENV.get("MQTT_PORT", 1883)), 10)
    c.loop_start()
    t0 = time.time()
    while time.time() - t0 < seconds and (limit is None or len(msgs) < limit):
        time.sleep(0.1)
    c.loop_stop()
    c.disconnect()
    return msgs


# ---------------------------------------------------------------- result bookkeeping
RESULTS: list[dict] = []
COLORS = {"PASS": "\033[32m", "FAIL": "\033[31m", "SKIP": "\033[33m"}


def check(section: str, name: str):
    def deco(fn):
        def run():
            t0 = time.time()
            try:
                detail = fn() or ""
                status = "PASS"
            except Skip as s:
                status, detail = "SKIP", str(s)
            except AssertionError as a:
                status, detail = "FAIL", str(a)
            except Exception as e:
                status, detail = "FAIL", f"{type(e).__name__}: {e}"
                if ARGS.verbose:
                    traceback.print_exc()
            RESULTS.append({"section": section, "check": name, "status": status, "detail": str(detail),
                            "seconds": round(time.time() - t0, 2)})
            col = COLORS[status] if sys.stdout.isatty() else ""
            end = "\033[0m" if col else ""
            print(f"  {col}{status:4}{end}  {name:<58} {str(detail)[:110]}", flush=True)
        run.section = section
        CHECKS.append(run)
        return fn
    return deco


CHECKS = []
STATE: dict = {}


def token(role):
    if role in STATE.get("tokens", {}):
        return STATE["tokens"][role]
    if role not in USERS:
        raise Skip(f"no {role} user in API_USERS")
    u, p = USERS[role]
    code, body = http("POST", f"{API}/api/auth/login", {"username": u, "password": p})
    assert code == 200, f"login as {u} failed: HTTP {code} {str(body)[:80]}"
    STATE.setdefault("tokens", {})[role] = body["access_token"]
    return body["access_token"]


# ================================================================ 1. containers
@check("1 Infrastructure", "All docker compose services running / healthy")
def _():
    import subprocess
    try:
        out = subprocess.run(["docker", "compose", "ps", "-a", "--format", "json"], cwd=ROOT, capture_output=True,
                             text=True, timeout=30).stdout.strip()
    except FileNotFoundError:
        raise Skip("docker CLI not found (running outside docker?)")
    if not out:
        raise AssertionError("no compose services found - run: docker compose up -d --build")
    rows = [json.loads(l) for l in out.splitlines()] if out.startswith("{") else json.loads(out)
    bad = []
    for r in rows:
        svc, state, health = r.get("Service"), r.get("State"), r.get("Health", "")
        if svc == "kafka-init":
            if state != "exited" or r.get("ExitCode", 0) != 0:
                bad.append(f"kafka-init {state} (exit {r.get('ExitCode')})")
        elif state != "running" or health == "unhealthy":
            bad.append(f"{svc}={state}/{health or '-'}")
    assert not bad, "; ".join(bad)
    return f"{len(rows)} services OK"


# ================================================================ 2. MQTT security
@check("2 MQTT / Mosquitto", "Anonymous connection is refused")
def _():
    rc = mqtt_connect_rc()
    assert rc != 0, "anonymous client was accepted"
    return f"CONNACK rc={rc} (not authorised)"


@check("2 MQTT / Mosquitto", "Wrong password is refused")
def _():
    rc = mqtt_connect_rc("simulator", "wrong-password")
    assert rc != 0, "bad password accepted"
    return f"CONNACK rc={rc}"


@check("2 MQTT / Mosquitto", "Simulator + Node-RED users authenticate")
def _():
    a = mqtt_connect_rc(ENV.get("MQTT_SIM_USERNAME", "simulator"), ENV.get("MQTT_SIM_PASSWORD"))
    b = mqtt_connect_rc(ENV.get("MQTT_NODERED_USERNAME", "nodered"), ENV.get("MQTT_NODERED_PASSWORD"))
    assert a == 0 and b == 0, f"simulator rc={a}, nodered rc={b}"
    return "both accepted"


@check("2 MQTT / Mosquitto", "ACL: 'nodered' user cannot publish vehicle data")
def _():
    topic = f"fleet/vehicle-99999/telemetry"
    marker = f"acl-test-{uuid.uuid4().hex}"
    got = []
    sub = mqtt_client("admin", "admin-dev-password")
    sub.on_message = lambda c, u, m: got.append(m.payload.decode(errors="ignore"))
    sub.connect(HOST, 1883)
    sub.subscribe(topic)
    sub.loop_start()
    time.sleep(0.5)
    pub = mqtt_client(ENV.get("MQTT_NODERED_USERNAME", "nodered"), ENV.get("MQTT_NODERED_PASSWORD"))
    pub.connect(HOST, 1883)
    pub.loop_start()
    pub.publish(topic, marker, qos=1)
    time.sleep(1.5)
    pub.loop_stop(); pub.disconnect(); sub.loop_stop(); sub.disconnect()
    assert marker not in got, "nodered user was able to publish (ACL not enforced)"
    return "publish silently dropped by broker ACL"


# ================================================================ 3. simulator
@check("3 Simulator", "Control API /status responds with mode + vehicle count")
def _():
    s = get(f"{SIM}/status")
    STATE["sim"] = s
    assert s["vehicles"] > 0
    return f"mode={s['mode']} vehicles={s['vehicles']} mqtt_up={s['mqtt_connections_up']} statuses={s['status_counts']}"


@check("3 Simulator", "Telemetry published on fleet/{id}/telemetry and valid vs JSON Schema")
def _():
    import jsonschema
    schema = json.loads((ROOT / "schemas" / "telemetry.schema.json").read_text())
    v = jsonschema.Draft202012Validator(schema)
    msgs = mqtt_collect("fleet/+/telemetry", 8, limit=200)
    assert msgs, "no telemetry received in 8 s"
    bad = 0
    parsed = []
    for topic, p in msgs:
        try:
            m = json.loads(p)
        except ValueError:
            continue  # a deliberately malformed frame from another test
        parsed.append(m)
        errs = list(v.iter_errors(m))
        bad += bool(errs)
    STATE["telemetry_sample"] = parsed
    assert bad == 0, f"{bad}/{len(parsed)} messages violate the schema"
    return f"{len(parsed)} messages, all schema-valid, e.g. {parsed[0]['vehicle_id']}"


@check("3 Simulator", "Every message is flagged simulated=true")
def _():
    s = sample()
    assert all(m.get("simulation", {}).get("simulated") is True for m in s)
    return "OK"


@check("3 Simulator", "GPS moves gradually (no teleporting between messages)")
def _():
    vid = (STATE.get("telemetry_sample") or [{}])[0].get("vehicle_id") or "vehicle-00001"
    msgs = [json.loads(p) for _, p in mqtt_collect(f"fleet/{vid}/telemetry", 7, limit=6)]
    assert len(msgs) >= 3, f"only {len(msgs)} messages for {vid}"

    def dist(a, b):
        R = 6371000
        la1, la2 = math.radians(a["latitude"]), math.radians(b["latitude"])
        dl = math.radians(b["longitude"] - a["longitude"])
        return 2 * R * math.asin(math.sqrt(math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin(dl / 2) ** 2))
    jumps = [dist(a["location"], b["location"]) for a, b in zip(msgs, msgs[1:])]
    speeds = [m["location"]["speed_kmh"] for m in msgs]
    assert max(jumps) < 60, f"position jumped {max(jumps):.0f} m in one interval"
    return f"{vid}: steps {[round(j, 1) for j in jumps]} m, speeds {speeds} km/h"


@check("3 Simulator", "Speed changes smoothly (no 40->183->2 jumps)")
def _():
    by = {}
    for m in sample():
        by.setdefault(m["vehicle_id"], []).append(m["location"]["speed_kmh"])
    deltas = [abs(b - a) for s in by.values() for a, b in zip(s, s[1:])]
    if not deltas:
        raise Skip("need >=2 messages per vehicle")
    assert max(deltas) <= 35, f"max speed change {max(deltas)} km/h in one interval"
    return f"max change between consecutive messages: {max(deltas):.1f} km/h"


@check("3 Simulator", "Passenger count follows status (IN_SERVICE 1-6, else 0)")
def _():
    bad = [m["vehicle_id"] for m in sample()
           if (m["vehicle"]["status"] == "IN_SERVICE" and not 1 <= m["vehicle"]["passenger_count"] <= 6)
           or (m["vehicle"]["status"] in ("AVAILABLE", "IDLE", "CHARGING", "MAINTENANCE") and m["vehicle"]["passenger_count"] != 0)]
    assert not bad, f"violations: {bad[:5]}"
    return "OK"


@check("3 Simulator", "LiDAR / radar / camera readings are consistent")
def _():
    n = 0
    for m in sample():
        l, r = m["lidar"], m["radar"]
        if l.get("obstacle_detected") and r.get("object_detected") and l["distance_m"] and r["distance_m"]:
            n += 1
            assert abs(l["distance_m"] - r["distance_m"]) < 5, f"{m['vehicle_id']}: lidar {l['distance_m']} vs radar {r['distance_m']}"
    return f"{n} messages with both sensors tracking an object, distances agree"


@check("3 Simulator", "Traffic uses LOW/MODERATE/HIGH/CONGESTED")
def _():
    seen = {m["traffic"]["status"] for m in sample()}
    assert seen <= {"LOW", "MODERATE", "HIGH", "CONGESTED"} and seen
    return f"levels seen: {sorted(seen)}"


# ================================================================ 4. node-red
@check("4 Node-RED", "Receives and forwards telemetry (/stats counters increase)")
def _():
    a = get(f"{NODERED}/stats")
    time.sleep(3)
    b = get(f"{NODERED}/stats")
    d = b["received"]["telemetry"] - a["received"]["telemetry"]
    assert d > 0, "no telemetry received by Node-RED in 3 s"
    return f"+{d} received, +{b['forwarded']['telemetry'] - a['forwarded']['telemetry']} forwarded in 3 s; edge p95 {b['edge_latency_ms']['telemetry']['p95']} ms"


@check("4 Node-RED", "Kafka producer connected")
def _():
    s = get(f"{NODERED}/stats")["kafka"]
    assert s["enabled"] and s["connected"], f"kafka state: {s}"
    return f"acked {s['sent']} msgs, errors {s['errors']}"


@check("4 Node-RED", "Malformed messages rejected (-> DLQ)")
def _():
    a = get(f"{NODERED}/stats")["rejected"]
    code, r = http("POST", f"{SIM}/failure", {"failure": "malformed", "count": 10})
    assert code == 200 and r.get("ok"), r
    b = wait_for(lambda: (get(f"{NODERED}/stats")["rejected"] - a >= 10) and get(f"{NODERED}/stats")["rejected"], 10,
                 what="10 rejections")
    STATE["dlq_expected"] = True
    return f"rejected +{b - a} of 10 sent"


@check("4 Node-RED", "Duplicate messages dropped")
def _():
    a = get(f"{NODERED}/stats")["duplicates"]
    http("POST", f"{SIM}/failure", {"failure": "duplicate", "count": 10})
    b = wait_for(lambda: (get(f"{NODERED}/stats")["duplicates"] - a >= 10) and get(f"{NODERED}/stats")["duplicates"], 10,
                 what="10 duplicates dropped")
    return f"dropped +{b - a} of 10 duplicates"


@check("4 Node-RED", "Prometheus /metrics endpoint")
def _():
    code, body = http("GET", f"{NODERED}/metrics")
    assert code == 200 and "nodered_messages_received_total" in body
    return "OK"


# ================================================================ 5. kafka
async def _kafka_topics():
    from aiokafka.admin import AIOKafkaAdminClient
    a = AIOKafkaAdminClient(bootstrap_servers=KAFKA, request_timeout_ms=5000)
    await a.start()
    try:
        return await a.list_topics()
    finally:
        await a.close()


async def _kafka_tail(topic, seconds=8, from_seconds_ago=60):
    from aiokafka import AIOKafkaConsumer
    c = AIOKafkaConsumer(topic, bootstrap_servers=KAFKA, auto_offset_reset="latest", enable_auto_commit=False,
                         group_id=None)
    await c.start()
    try:
        parts = c.assignment() or set()
        for _ in range(20):
            parts = c.assignment()
            if parts:
                break
            await asyncio.sleep(0.2)
        ts = int((time.time() - from_seconds_ago) * 1000)
        offs = await c.offsets_for_times({tp: ts for tp in parts})
        for tp, o in offs.items():
            if o is not None:
                c.seek(tp, o.offset)
        out = []
        t0 = time.time()
        while time.time() - t0 < seconds:
            batch = await c.getmany(timeout_ms=500)
            for recs in batch.values():
                out += [r.value for r in recs]
            if out and time.time() - t0 > 2:
                break
        return out
    finally:
        await c.stop()


@check("5 Kafka", "Topics exist: telemetry, events, alerts, dlq")
def _():
    topics = set(asyncio.run(_kafka_topics()))
    need = {ENV.get("KAFKA_TOPIC_TELEMETRY", "vehicle.telemetry"), ENV.get("KAFKA_TOPIC_EVENTS", "vehicle.events"),
            ENV.get("KAFKA_TOPIC_ALERTS", "vehicle.alerts"), ENV.get("KAFKA_TOPIC_DLQ", "vehicle.dlq")}
    missing = need - topics
    assert not missing, f"missing topics: {missing}"
    return ", ".join(sorted(need))


@check("5 Kafka", "vehicle.telemetry carries enriched messages (ingest block)")
def _():
    recs = asyncio.run(_kafka_tail("vehicle.telemetry", 6, 10))
    assert recs, "no records in the last 10 s"
    m = json.loads(recs[-1])
    assert "ingest" in m and "edge_latency_ms" in m["ingest"], "ingest enrichment missing"
    return f"{len(recs)} recent records; ingest={m['ingest']}"


@check("5 Kafka", "Rejected messages land in vehicle.dlq with a reason")
def _():
    if not STATE.get("dlq_expected"):
        raise Skip("malformed test did not run")
    recs = asyncio.run(_kafka_tail("vehicle.dlq", 6, 120))
    assert recs, "DLQ empty for the last 2 minutes"
    reasons = sorted({json.loads(r).get("reason") for r in recs})
    return f"{len(recs)} DLQ records, reasons {reasons}"


@check("5 Kafka", "Consumer groups are keeping up (lag)")
def _():
    sys.path.insert(0, str(ROOT / "scripts"))
    from loadtest import kafka_lag
    lag = asyncio.run(kafka_lag(KAFKA, ["telemetry-service", "alert-service", "analytics-service"]))
    assert lag and "error" not in lag, f"lag query failed: {lag}"
    missing = [g for g, v in lag.items() if v is None]
    assert not missing, f"no committed offsets for {missing}"
    return f"lag per group: {lag}"


# ================================================================ 6. telemetry service + stores
@check("6 Telemetry service", "Consumes and stores in MongoDB (counts rising)")
def _():
    a = get(f"{SVC['telemetry']}/stats")
    time.sleep(3)
    b = get(f"{SVC['telemetry']}/stats")
    assert b["inserted"] > a["inserted"], "inserted counter not increasing"
    e = b["e2e_latency_ms"]
    return f"+{b['inserted'] - a['inserted']} inserted in 3 s; e2e p50/p95/p99 = {e['p50']}/{e['p95']}/{e['p99']} ms"


def mongo():
    from pymongo import MongoClient
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=4000)["fleet"]


@check("6 Telemetry service", "MongoDB indexes: unique message_id, vehicle+ts, 2dsphere, TTL")
def _():
    idx = mongo().telemetry.index_information()
    keys = {n: v for n, v in idx.items()}
    assert any(v.get("unique") and v["key"] == [("message_id", 1)] for v in keys.values()), "no unique message_id"
    assert any(v["key"] == [("geo", "2dsphere")] for v in keys.values()), "no 2dsphere"
    assert any("expireAfterSeconds" in v for v in keys.values()), "no TTL index"
    return ", ".join(sorted(keys))


@check("6 Telemetry service", "Stored document has ts (date) + GeoJSON geo")
def _():
    d = mongo().telemetry.find_one(sort=[("ts", -1)])
    assert d, "telemetry collection empty"
    assert d["geo"]["type"] == "Point" and isinstance(d["ts"], datetime)
    return f"latest: {d['vehicle_id']} at {d['ts'].isoformat()}"


@check("6 Telemetry service", "Geospatial query ($nearSphere) works")
def _():
    d = mongo().telemetry.find_one(sort=[("ts", -1)])
    lon, lat = d["geo"]["coordinates"]
    n = len(list(mongo().telemetry.find({"geo": {"$nearSphere": {"$geometry": {"type": "Point", "coordinates": [lon, lat]},
                                                                  "$maxDistance": 500}}}).limit(20)))
    assert n >= 1
    return f"{n} documents within 500 m"


@check("6 Telemetry service", "vehicle_state snapshot collection populated (15 s cycle)")
def _():
    if ARGS.quick:
        raise Skip("--quick")
    n = wait_for(lambda: mongo().vehicle_state.count_documents({}), 25, 2, "vehicle_state documents")
    return f"{n} vehicles in vehicle_state"


def rds():
    import redis
    return redis.Redis.from_url(REDIS_URI, decode_responses=True, socket_timeout=3)


@check("6 Telemetry service", "Redis latest state: vehicle:{id}, fleet:geo, fleet:last_seen")
def _():
    r = rds()
    n_geo, n_seen = r.zcard("fleet:geo"), r.zcard("fleet:last_seen")
    vid = r.zrevrange("fleet:last_seen", 0, 0)[0]
    h = r.hgetall(f"vehicle:{vid}")
    assert h.get("status") and h.get("data"), "vehicle hash incomplete"
    near = r.geosearch("fleet:geo", longitude=float(h["lon"]), latitude=float(h["lat"]), radius=1, unit="km")
    return f"{n_geo} vehicles in geo index, {n_seen} last_seen; {vid} status={h['status']}; {len(near)} within 1 km"


# ================================================================ 7. alert service
def _new_alert(vehicle, scenario, event_type, timeout=15):
    since = datetime.now(timezone.utc)
    code, r = http("POST", f"{SIM}/scenario", {"scenario": scenario, "vehicle_id": vehicle})
    assert code == 200 and r.get("ok"), f"scenario rejected: {r}"
    db = mongo()
    a = wait_for(lambda: db.alerts.find_one({"vehicle_id": vehicle, "event_type": event_type,
                                              "created_at": {"$gte": since.replace(tzinfo=None)}}),
                 timeout, 0.5, f"{event_type} alert for {vehicle}")
    return a


def sample():
    s = STATE.get("telemetry_sample") or []
    if not s:
        raise Skip("no telemetry sample (earlier MQTT check failed)")
    return s


def _pick_vehicle(prefer_moving=True):
    s = STATE.get("telemetry_sample") or []
    for m in s:
        if m["vehicle"]["status"] in ("IN_SERVICE", "AVAILABLE") and m["location"]["speed_kmh"] > 10:
            return m["vehicle_id"]
    return s[0]["vehicle_id"] if s else "vehicle-00001"


@check("7 Alert service", "COLLISION_WARNING travels vehicle->MQTT->Node-RED->Kafka->alert->MongoDB")
def _():
    vid = _pick_vehicle()
    a = _new_alert(vid, "collision", "COLLISION_WARNING")
    STATE["collision_alert"] = a
    return f"{vid}: severity={a['severity']} sources={a['sources']} pipeline_latency={a.get('pipeline_latency_ms')} ms"


@check("7 Alert service", "Vehicle + platform detections merged into ONE alert")
def _():
    a = STATE.get("collision_alert")
    if not a:
        raise Skip("collision test failed")
    time.sleep(4)
    db = mongo()
    docs = list(db.alerts.find({"vehicle_id": a["vehicle_id"], "event_type": "COLLISION_WARNING",
                                "created_at": {"$gte": a["created_at"]}}))
    doc = db.alerts.find_one({"alert_id": a["alert_id"]})
    assert len(docs) == 1, f"{len(docs)} separate alerts created"
    return f"1 alert, sources={doc['sources']}, occurrences={doc.get('occurrences')}"


@check("7 Alert service", "Alert pushed to Redis (alerts:recent) and Kafka vehicle.alerts")
def _():
    a = STATE.get("collision_alert")
    if not a:
        raise Skip("collision test failed")
    recent = [json.loads(x)["alert_id"] for x in rds().lrange("alerts:recent", 0, 50)]
    assert a["alert_id"] in recent, "not in alerts:recent"
    recs = asyncio.run(_kafka_tail("vehicle.alerts", 5, 60))
    ids = [json.loads(r)["alert_id"] for r in recs]
    assert a["alert_id"] in ids, "not found on vehicle.alerts"
    return "found in both"


for _scn, _evt in [("sensor_failure", "SENSOR_FAILURE"), ("abnormal_speed", "ABNORMAL_SPEED"),
                   ("route_deviation", "ROUTE_DEVIATION"), ("low_battery", "LOW_BATTERY"),
                   ("critical_battery", "CRITICAL_BATTERY"), ("emergency_stop", "EMERGENCY_STOP"),
                   ("obstacle", "OBSTACLE_DETECTED")]:
    def _make(scn=_scn, evt=_evt):
        @check("7 Alert service", f"Scenario {scn} -> {evt} alert")
        def _():
            s = STATE.get("telemetry_sample") or []
            # use a different vehicle per scenario so cooldown windows do not merge them
            pool = [m["vehicle_id"] for m in s if m["vehicle"]["status"] in ("IN_SERVICE", "AVAILABLE")]
            pool = sorted(set(pool)) or ["vehicle-00002"]
            order = ["sensor_failure", "abnormal_speed", "route_deviation", "low_battery", "critical_battery", "emergency_stop", "obstacle"]
            vid = pool[(order.index(scn) + 1) % len(pool)]
            a = _new_alert(vid, scn, evt, timeout=30 if scn in ("route_deviation", "low_battery", "obstacle") else 15)
            return f"{vid}: {a['severity']} via {a['sources']} latency={a.get('pipeline_latency_ms')} ms"
    _make()


# ================================================================ 8. analytics
@check("8 Analytics service", "Fleet summary in Redis matches the fleet size")
def _():
    s = json.loads(rds().get("analytics:summary") or "{}")
    assert s, "analytics:summary missing"
    expected = (STATE.get("sim") or {}).get("vehicles")
    ok = expected is None or abs(s["active_vehicles"] - expected) <= max(2, expected * 0.05)
    assert ok, f"active_vehicles={s['active_vehicles']} but simulator has {expected}"
    return f"active={s['active_vehicles']} mps={s['messages_per_second']} avg_battery={s['avg_battery_pct']} status={s['status']}"


@check("8 Analytics service", "k-means demand hotspots computed")
def _():
    h = json.loads(rds().get("analytics:hotspots") or "[]")
    assert h, "no hotspots"
    return f"{len(h)} clusters, largest has {h[0]['vehicles']} vehicles"


@check("8 Analytics service", "Snapshots stored in MongoDB analytics_snapshots")
def _():
    n = mongo().analytics_snapshots.count_documents({})
    assert n > 0
    return f"{n} snapshots"


# ================================================================ 9. backend API
@check("9 Backend API / security", "Login works; wrong password -> 401")
def _():
    token("operator")
    code, _ = http("POST", f"{API}/api/auth/login", {"username": USERS["operator"][0], "password": "nope"})
    assert code == 401, f"bad password gave HTTP {code}"
    return "JWT issued; bad password rejected"


@check("9 Backend API / security", "No token / forged token -> 401")
def _():
    c1, _ = http("GET", f"{API}/api/fleet/summary")
    c2, _ = http("GET", f"{API}/api/fleet/summary", token="forged.token.value")
    assert c1 == 401 and c2 == 401, f"got {c1}/{c2}"
    return "both rejected"


@check("9 Backend API / security", "RBAC: viewer cannot acknowledge, operator can")
def _():
    a = STATE.get("collision_alert")
    alert_id = a["alert_id"] if a else "alr-does-not-exist"
    c_view, _ = http("POST", f"{API}/api/alerts/{alert_id}/ack", token=token("viewer"))
    assert c_view == 403, f"viewer got HTTP {c_view}"
    if not a:
        raise Skip("viewer blocked (403) OK; no alert available to test operator ack")
    c_op, _ = http("POST", f"{API}/api/alerts/{alert_id}/ack", token=token("operator"))
    assert c_op == 200, f"operator got HTTP {c_op}"
    st = mongo().alerts.find_one({"alert_id": alert_id})["status"]
    assert st == "ACKNOWLEDGED"
    return "viewer 403, operator 200, status=ACKNOWLEDGED"


@check("9 Backend API / security", "Fleet summary, vehicles list, vehicle detail")
def _():
    t = token("viewer")
    s = get(f"{API}/api/fleet/summary", token=t)
    v = get(f"{API}/api/vehicles?limit=5", token=t)
    vid = v["items"][0]["vehicle_id"]
    d = get(f"{API}/api/vehicles/{vid}", token=t)
    assert d["vehicle_id"] == vid
    return f"summary source={s.get('source')} active={s.get('active_vehicles')}; {v['total']} vehicles; detail {vid} OK"


@check("9 Backend API / security", "Vehicles near a point (Redis GEOSEARCH)")
def _():
    t = token("viewer")
    n = get(f"{API}/api/vehicles/near?lat=-37.8136&lon=144.9631&radius_m=8000", token=t)
    assert n, "no vehicles within 8 km of CBD"
    return f"{len(n)} vehicles, nearest {n[0]}"


@check("9 Backend API / security", "Vehicle history from MongoDB")
def _():
    t = token("viewer")
    vid = get(f"{API}/api/vehicles?limit=1", token=t)["items"][0]["vehicle_id"]
    h = get(f"{API}/api/vehicles/{vid}/history?minutes=10&limit=20", token=t)
    assert h, "no history"
    return f"{len(h)} points for {vid}"


@check("9 Backend API / security", "Alerts list + stats (latency p95)")
def _():
    t = token("viewer")
    a = get(f"{API}/api/alerts?limit=10", token=t)
    s = get(f"{API}/api/alerts/stats", token=t)
    return f"{len(a)} alerts listed; by_status={s.get('by_status')} latency={s.get('latency_ms')}"


@check("9 Backend API / security", "Pipeline health: every stage up")
def _():
    h = get(f"{API}/api/pipeline/health", token=token("viewer"), timeout=10)
    stages = ["simulator", "mqtt", "nodered", "kafka", "telemetry-service", "alert-service", "analytics-service", "mongodb", "redis"]
    down = [s for s in stages if not h.get(s, {}).get("up")]
    assert not down, f"down: {down}"
    return "all 9 stages up"


@check("9 Backend API / security", "Dashboard trigger endpoint forwards to simulator (operator)")
def _():
    c, r = http("POST", f"{API}/api/simulator/trigger", {"scenario": "sensor_degraded"}, token=token("operator"))
    assert c == 200 and r.get("ok"), f"HTTP {c} {r}"
    c2, _ = http("POST", f"{API}/api/simulator/trigger", {"scenario": "collision"}, token=token("viewer"))
    assert c2 == 403, f"viewer allowed to trigger (HTTP {c2})"
    return "operator OK, viewer 403"


@check("9 Backend API / security", "WebSocket: snapshot pushed + live alert delivered")
def _():
    try:
        import websockets
    except ImportError:
        raise Skip("pip install websockets")

    async def run():
        uri = f"ws://{HOST}:8000/ws?token={token('operator')}"
        got_snap = got_alert = False
        async with websockets.connect(uri, open_timeout=5) as ws:
            http("POST", f"{SIM}/scenario", {"scenario": "emergency_stop", "vehicle_id": _pick_vehicle()})
            t0 = time.time()
            while time.time() - t0 < 15 and not (got_snap and got_alert):
                msg = json.loads(await asyncio.wait_for(ws.recv(), 15))
                got_snap |= msg["type"] == "snapshot" and bool(msg.get("positions"))
                got_alert |= msg["type"] == "alert"
        return got_snap, got_alert
    snap, alert = asyncio.run(run())
    assert snap, "no snapshot with positions"
    assert alert, "no live alert within 15 s"
    return "snapshot + alert received"


@check("9 Backend API / security", "Dashboard page served")
def _():
    code, body = http("GET", f"{API}/")
    assert code == 200 and "AV Fleet Operations" in body
    return "OK"


# ================================================================ 10. monitoring
@check("10 Monitoring", "Service /health and /metrics endpoints")
def _():
    for name, url in SVC.items():
        assert get(f"{url}/health")["status"] == "ok", name
        code, body = http("GET", f"{url}/metrics")
        assert code == 200 and "# HELP" in body, name
    assert http("GET", f"{SIM}/metrics")[0] == 200 and http("GET", f"{API}/metrics")[0] == 200
    return "simulator, 3 services, backend OK"


@check("10 Monitoring", "Prometheus scrape targets up")
def _():
    code, r = http("GET", f"http://{HOST}:9090/api/v1/targets")
    if code != 200:
        raise AssertionError(f"Prometheus not reachable (HTTP {code})")
    t = r["data"]["activeTargets"]
    down = sorted({x["labels"]["job"] for x in t if x["health"] != "up" and x["labels"]["job"] != "simulator-host"})
    up = sorted({x["labels"]["job"] for x in t if x["health"] == "up"})
    assert not down, f"down: {down} (up: {up})"
    return f"up: {', '.join(up)}"


@check("10 Monitoring", "Grafana healthy + AV Fleet dashboard provisioned")
def _():
    import base64
    code, h = http("GET", f"http://{HOST}:3000/api/health")
    assert code == 200, f"Grafana HTTP {code}"
    req = urllib.request.Request(f"http://{HOST}:3000/api/dashboards/uid/av-fleet", headers={
        "Authorization": "Basic " + base64.b64encode(f"admin:{ENV.get('GRAFANA_ADMIN_PASSWORD', 'admin')}".encode()).decode()})
    with urllib.request.urlopen(req, timeout=5) as r:
        d = json.loads(r.read())
    return f"dashboard '{d['dashboard']['title']}' with {len(d['dashboard']['panels'])} panels"


# ================================================================ 11. resilience (optional)
@check("11 Resilience", "MQTT disconnect 10 s -> buffered and replayed")
def _():
    if not ARGS.chaos:
        raise Skip("use --chaos")
    sys.path.insert(0, str(ROOT / "scripts"))
    import chaos
    r = chaos.mqtt_disconnect(10)
    assert r["flushed_after_reconnect"] and r["flushed_after_reconnect"] > 0, r
    return json.dumps(r)


for _name in ["kafka_consumer_failure", "database_unavailable", "redis_unavailable", "microservice_crash"]:
    def _mk(name=_name):
        @check("11 Resilience", f"chaos: {name}")
        def _():
            if not ARGS.chaos:
                raise Skip("use --chaos (stops/starts containers)")
            sys.path.insert(0, str(ROOT / "scripts"))
            import chaos
            r = getattr(chaos, name)()
            if name in ("kafka_consumer_failure", "database_unavailable"):
                assert r.get("recovery_to_lag_below_500_s") is not None, f"did not recover: {r}"
            if name == "redis_unavailable":
                assert r.get("api_summary_source_during_outage") == "mongodb-fallback", r
            if name == "microservice_crash":
                assert r.get("back_healthy_after_s") is not None, r
            return json.dumps(r, default=str)[:300]
    _mk()


# ================================================================ main
def main():
    global ARGS
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", help="run only sections whose name contains this text, e.g. --only Kafka")
    ap.add_argument("--quick", action="store_true", help="skip checks that wait for 15-second cycles")
    ap.add_argument("--chaos", action="store_true", help="also run resilience scenarios (stops/starts containers)")
    ap.add_argument("-v", "--verbose", action="store_true")
    ARGS = ap.parse_args()
    print(f"AV Fleet verification - {datetime.now().isoformat(timespec='seconds')}  (virtual vehicles, real pipeline)\n")
    current = None
    for c in CHECKS:
        if ARGS.only and ARGS.only.lower() not in c.section.lower():
            continue
        if c.section != current:
            current = c.section
            print(f"\n[{current}]")
        c()
    counts = {s: sum(r["status"] == s for r in RESULTS) for s in ("PASS", "FAIL", "SKIP")}
    print(f"\nSummary: {counts['PASS']} passed, {counts['FAIL']} failed, {counts['SKIP']} skipped")
    (ROOT / "results").mkdir(exist_ok=True)
    out = ROOT / "results" / f"verify-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    out.write_text(json.dumps({"ran_at": datetime.now(timezone.utc).isoformat(), "summary": counts, "checks": RESULTS}, indent=2))
    print(f"Saved {out.relative_to(ROOT)}")
    sys.exit(1 if counts["FAIL"] else 0)


ARGS = None
if __name__ == "__main__":
    main()
