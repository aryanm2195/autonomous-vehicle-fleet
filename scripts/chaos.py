#!/usr/bin/env python3
"""Failure-simulation (chaos) runner.

Each scenario injects a SOFTWARE failure into the running stack, observes the
system through its real health/stats endpoints, and records what actually
happened (counts, lag, recovery time) into results/chaos-<timestamp>.json.

    python scripts/chaos.py --list
    python scripts/chaos.py mqtt_disconnect malformed duplicate            # simulator-level
    python scripts/chaos.py kafka_consumer_failure database_unavailable \\
                            redis_unavailable microservice_crash           # needs docker compose

Scenarios
---------
sensor_failure          simulator marks a sensor FAILED -> SENSOR_FAILURE event + platform rule alert
abnormal_behaviour      simulator forces 125-150 km/h -> ABNORMAL_SPEED alert
malformed               20 invalid frames -> rejected by Node-RED -> vehicle.dlq
duplicate               20 duplicated messages -> dropped by Node-RED de-duplication
mqtt_disconnect         all simulator MQTT sessions drop for 15 s -> store-and-forward replay
kafka_consumer_failure  telemetry-service stopped 30 s -> lag grows -> restarted -> catches up, no loss
database_unavailable    MongoDB stopped 20 s -> telemetry-service retries, lag grows -> recovers
redis_unavailable       Redis stopped 20 s -> services degrade, API falls back to MongoDB
microservice_crash      alert-service process killed -> container restart policy restarts it
"""
from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from loadtest import kafka_lag  # noqa: E402

SIM = "http://localhost:9105"
NODERED = "http://localhost:1880"
TELEMETRY = "http://localhost:8001"
ALERTS = "http://localhost:8002"
API = "http://localhost:8000"
KAFKA = "localhost:29092"


def get(url, timeout=3):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def post(url, body, headers=None, timeout=5):
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def compose(*args):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, capture_output=True, text=True)


def lag():
    res = asyncio.run(kafka_lag(KAFKA, ["telemetry-service"]))
    return (res or {}).get("telemetry-service")


def alerts_of(event_type: str, since_iso: str):
    """Query alerts through the real API (login as viewer)."""
    try:
        tok = post(f"{API}/api/auth/login", {"username": "viewer", "password": "viewer123"})["access_token"]
        req = urllib.request.Request(f"{API}/api/alerts?limit=200", headers={"Authorization": f"Bearer {tok}"})
        with urllib.request.urlopen(req, timeout=5) as r:
            items = json.loads(r.read())
        return [a for a in items if a["event_type"] == event_type and a["created_at"] >= since_iso]
    except Exception as exc:
        return {"error": str(exc)}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------- scenarios
def sensor_failure():
    t0 = now_iso()
    r = post(f"{SIM}/scenario", {"scenario": "sensor_failure", "vehicle_id": "vehicle-00002"})
    time.sleep(6)
    a = alerts_of("SENSOR_FAILURE", t0)
    return {"injected": r, "alerts_found": a if isinstance(a, dict) else len(a),
            "sources": sorted({s for x in a for s in x.get("sources", [])}) if isinstance(a, list) else None,
            "pipeline_latency_ms": [x.get("pipeline_latency_ms") for x in a][:5] if isinstance(a, list) else None}


def abnormal_behaviour():
    t0 = now_iso()
    r = post(f"{SIM}/scenario", {"scenario": "abnormal_speed", "vehicle_id": "vehicle-00004"})
    time.sleep(10)
    a = alerts_of("ABNORMAL_SPEED", t0)
    return {"injected": r, "alerts_found": a if isinstance(a, dict) else len(a)}


def malformed():
    before = get(f"{NODERED}/stats")
    post(f"{SIM}/failure", {"failure": "malformed", "count": 20})
    time.sleep(3)
    after = get(f"{NODERED}/stats")
    rb, ra = before["reject_reasons"], after["reject_reasons"]
    return {"sent": 20, "rejected_by_nodered": after["rejected"] - before["rejected"],
            "reasons_this_run": {k: ra[k] - rb.get(k, 0) for k in ra if ra[k] - rb.get(k, 0)}}


def duplicate():
    before = get(f"{NODERED}/stats")
    post(f"{SIM}/failure", {"failure": "duplicate", "count": 20})
    time.sleep(3)
    after = get(f"{NODERED}/stats")
    return {"duplicates_sent": 20, "dropped_by_nodered": after["duplicates"] - before["duplicates"]}


def _sim_metric(name):
    try:
        with urllib.request.urlopen(f"{SIM}/metrics", timeout=3) as r:
            txt = r.read().decode()
        return sum(float(l.rsplit(" ", 1)[1]) for l in txt.splitlines() if l.startswith(name) and not l.startswith("#"))
    except Exception:
        return None


def mqtt_disconnect(seconds=15):
    nr0 = get(f"{NODERED}/stats")["received"]["telemetry"]
    flushed0 = _sim_metric("sim_offline_buffer_flushed_total") or 0
    post(f"{SIM}/failure", {"failure": "mqtt_disconnect", "seconds": seconds})
    time.sleep(seconds / 2)
    nr_mid = get(f"{NODERED}/stats")["received"]["telemetry"]
    buffered_mid = _sim_metric("sim_offline_buffer_messages")
    time.sleep(seconds / 2 + 8)
    nr1 = get(f"{NODERED}/stats")["received"]["telemetry"]
    return {"disconnect_s": seconds, "buffered_while_offline": buffered_mid,
            "flushed_after_reconnect": (_sim_metric("sim_offline_buffer_flushed_total") or 0) - flushed0,
            "dropped_buffer_full": _sim_metric("sim_offline_buffer_dropped_total"),
            "nodered_received_during_outage_first_half": nr_mid - nr0,
            "nodered_received_total_delta": nr1 - nr0}


def kafka_consumer_failure(down_s=30):
    t_before = get(f"{TELEMETRY}/stats")
    compose("stop", "telemetry-service")
    time.sleep(down_s)
    lag_peak = lag()
    compose("start", "telemetry-service")
    t0 = time.time()
    rec = None
    while time.time() - t0 < 180:
        time.sleep(2)
        lg = lag()
        if lg is not None and lg < 500:
            rec = time.time() - t0
            break
    return {"stopped_for_s": down_s, "lag_when_restarted": lag_peak, "recovery_to_lag_below_500_s": round(rec, 1) if rec else None,
            "note": "offsets are committed after MongoDB writes, so the backlog is replayed; unique message_id makes replays idempotent",
            "consumed_before": (t_before or {}).get("consumed"), "stats_after": get(f"{TELEMETRY}/stats")}


def database_unavailable(down_s=20):
    s0 = get(f"{TELEMETRY}/stats") or {}
    compose("stop", "mongodb")
    time.sleep(down_s)
    s_mid = get(f"{TELEMETRY}/stats") or {}
    lag_mid = lag()
    compose("start", "mongodb")
    t0 = time.time()
    rec = None
    while time.time() - t0 < 240:
        time.sleep(3)
        lg = lag()
        if lg is not None and lg < 500:
            rec = time.time() - t0
            break
    s1 = get(f"{TELEMETRY}/stats") or {}
    return {"mongodb_down_s": down_s, "mongo_retries_during_outage": (s_mid.get("mongo_retries") or 0) - (s0.get("mongo_retries") or 0),
            "lag_during_outage": lag_mid, "recovery_to_lag_below_500_s": round(rec, 1) if rec else None,
            "inserted_before": s0.get("inserted"), "inserted_after": s1.get("inserted")}


def redis_unavailable(down_s=20):
    compose("stop", "redis")
    time.sleep(5)
    tok = post(f"{API}/api/auth/login", {"username": "viewer", "password": "viewer123"})["access_token"]
    req = urllib.request.Request(f"{API}/api/fleet/summary", headers={"Authorization": f"Bearer {tok}"})
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            summary = json.loads(r.read())
    except Exception as exc:
        summary = {"error": str(exc)}
    tstats = get(f"{TELEMETRY}/stats")
    time.sleep(max(0, down_s - 5))
    compose("start", "redis")
    time.sleep(10)
    return {"redis_down_s": down_s, "api_summary_source_during_outage": summary.get("source"),
            "telemetry_service_redis_degraded": (tstats or {}).get("redis_degraded"),
            "telemetry_inserted_kept_growing": (tstats or {}).get("inserted"),
            "after_recovery": (get(f"{TELEMETRY}/stats") or {}).get("redis_degraded")}


def microservice_crash():
    before = compose("ps", "--format", "json", "alert-service").stdout
    compose("exec", "-T", "alert-service", "python", "-c", "import os, signal; os.kill(1, signal.SIGKILL)")
    t0 = time.time()
    up = None
    time.sleep(2)
    while time.time() - t0 < 90:
        if get(f"{ALERTS}/health"):
            up = time.time() - t0
            break
        time.sleep(1)
    return {"killed": "alert-service (PID 1, SIGKILL)", "back_healthy_after_s": round(up, 1) if up else None,
            "restart_policy": "unless-stopped (docker compose) / Deployment + liveness probe (k8s)"}


SCENARIOS = {f.__name__: f for f in [sensor_failure, abnormal_behaviour, malformed, duplicate, mqtt_disconnect,
                                      kafka_consumer_failure, database_unavailable, redis_unavailable,
                                      microservice_crash]}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenarios", nargs="*")
    ap.add_argument("--list", action="store_true")
    args = ap.parse_args()
    if args.list or not args.scenarios:
        print("\n".join(SCENARIOS))
        return
    out = {"started_at": now_iso(), "results": {}}
    for name in args.scenarios:
        if name not in SCENARIOS:
            sys.exit(f"unknown scenario {name}; use --list")
        print(f"--- {name} ---", flush=True)
        try:
            res = SCENARIOS[name]()
        except Exception as exc:
            res = {"error": f"{type(exc).__name__}: {exc}"}
        print(json.dumps(res, indent=1, default=str))
        out["results"][name] = res
    (ROOT / "results").mkdir(exist_ok=True)
    path = ROOT / "results" / f"chaos-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    path.write_text(json.dumps(out, indent=2, default=str))
    print(f"\nwritten {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
