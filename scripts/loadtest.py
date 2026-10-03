#!/usr/bin/env python3
"""Load-test harness - measures the REAL pipeline with progressively larger virtual fleets.

It starts the simulator itself (MODE=load_test) as one or more local processes
pointed at the running MQTT broker, lets each stage warm up, resets latency
windows, measures for a fixed window, then drains and records:

  * messages generated / received by Node-RED / acked by Kafka / consumed / stored
  * messages per second at every stage
  * latency p50 / p95 / p99 (edge = vehicle->Node-RED, e2e = vehicle->MongoDB, alerts)
  * errors (publish errors, validation rejects, Kafka errors, Mongo retries, Redis errors, loss)
  * Kafka consumer lag (max during the window and after drain)
  * CPU and memory per component (docker stats, or psutil for native processes)
  * database performance (insert rate, insert_many batch latency, MongoDB opcounters)

Nothing is estimated or extrapolated: every number in the output was read from
a running component during the test. Components that are not running are
reported as "not measured".

Usage (full stack running via docker compose; stop the compose simulator first):
    docker compose stop simulator
    python scripts/loadtest.py --stages 10,100,1000,5000,10000 --duration 60

Edge-tier only (no Kafka/MongoDB available; Node-RED started with KAFKA_ENABLED=false):
    python scripts/loadtest.py --edge-only --stages 10,100,1000 --duration 30
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import platform
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"


def get_json(url: str, timeout: float = 3.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception:
        return None


def get_text(url: str, timeout: float = 3.0) -> str | None:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.read().decode()
    except Exception:
        return None


def prom_value(text: str | None, name: str, labels: str = "") -> float:
    """Sum of a counter/gauge from Prometheus text exposition."""
    if not text:
        return 0.0
    total = 0.0
    for line in text.splitlines():
        if line.startswith(name) and not line.startswith("#"):
            head, _, val = line.rpartition(" ")
            metric = head.split("{")[0]
            if metric != name:
                continue
            if labels and labels not in head:
                continue
            try:
                total += float(val)
            except ValueError:
                pass
    return total


# ---------------------------------------------------------------- resource sampling
class ResourceSampler(threading.Thread):
    """Samples CPU% and memory for each pipeline component every ``interval`` seconds."""

    NATIVE_MATCH = {
        "mosquitto": ["mosquitto"], "node-red": ["node-red", "node-red/red.js"], "kafka": ["kafka.Kafka"],
        "mongodb": ["mongod"], "redis": ["redis-server"],
        "telemetry-service": ["telemetry_service.main"], "alert-service": ["alert_service.main"],
        "analytics-service": ["analytics_service.main"], "backend": ["app.main:app"],
    }

    def __init__(self, mode: str, sim_pids: list[int], interval: float = 2.0):
        super().__init__(daemon=True)
        self.mode, self.sim_pids, self.interval = mode, sim_pids, interval
        self.samples: dict[str, list[tuple[float, float]]] = {}
        self.stop_evt = threading.Event()

    def _add(self, name, cpu, mem_mb):
        self.samples.setdefault(name, []).append((cpu, mem_mb))

    def run(self):
        import psutil
        procs: dict[int, psutil.Process] = {}
        while not self.stop_evt.is_set():
            if self.mode == "docker":
                try:
                    out = subprocess.run(["docker", "stats", "--no-stream", "--format",
                                          "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}"], capture_output=True, text=True,
                                         timeout=20).stdout
                    for line in out.strip().splitlines():
                        name, cpu, mem = line.split("|")
                        name = name.replace("av-fleet-", "").rsplit("-", 1)[0]
                        used = mem.split("/")[0].strip()
                        mult = {"KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "B": 1 / 1048576}
                        unit = next(u for u in ("KiB", "MiB", "GiB", "B") if used.endswith(u))
                        self._add(name, float(cpu.rstrip("%") or 0), float(used[:-len(unit)]) * mult[unit])
                except Exception:
                    pass
            else:
                found: dict[str, list] = {}
                for p in psutil.process_iter(["pid", "cmdline", "name"]):
                    cmd = " ".join(p.info.get("cmdline") or []) or (p.info.get("name") or "")
                    for comp, pats in self.NATIVE_MATCH.items():
                        if any(pt in cmd for pt in pats) and "loadtest.py" not in cmd:
                            found.setdefault(comp, []).append(p)
                            break
                    if p.info["pid"] in self.sim_pids:
                        found.setdefault("simulator", []).append(p)
                for comp, plist in found.items():
                    cpu = mem = 0.0
                    for p in plist:
                        try:
                            q = procs.setdefault(p.pid, p)
                            cpu += q.cpu_percent(None)
                            mem += q.memory_info().rss / 1048576
                        except Exception:
                            pass
                    self._add(comp, cpu, mem)
            if self.mode == "docker":  # simulator runs natively even in docker mode
                import psutil as _ps
                cpu = mem = 0.0
                for pid in self.sim_pids:
                    try:
                        q = procs.setdefault(pid, _ps.Process(pid))
                        cpu += q.cpu_percent(None)
                        mem += q.memory_info().rss / 1048576
                    except Exception:
                        pass
                self._add("simulator", cpu, mem)
            self.stop_evt.wait(self.interval)

    def summary(self) -> dict:
        out = {}
        for comp, s in sorted(self.samples.items()):
            s = s[1:] if len(s) > 2 else s   # first psutil sample is always 0
            if not s:
                continue
            cpus, mems = [c for c, _ in s], [m for _, m in s]
            out[comp] = {"cpu_pct_avg": round(statistics.mean(cpus), 1), "cpu_pct_max": round(max(cpus), 1),
                         "mem_mb_avg": round(statistics.mean(mems), 1), "mem_mb_max": round(max(mems), 1)}
        return out


# ---------------------------------------------------------------- kafka lag
async def kafka_lag(bootstrap: str, groups: list[str]) -> dict | None:
    try:
        from aiokafka import AIOKafkaConsumer
        from aiokafka.admin import AIOKafkaAdminClient
    except ImportError:
        return None
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5000)
    consumer = AIOKafkaConsumer(bootstrap_servers=bootstrap, request_timeout_ms=5000)
    try:
        await asyncio.wait_for(admin.start(), 6)
        await asyncio.wait_for(consumer.start(), 6)
        out = {}
        for g in groups:
            offsets = await admin.list_consumer_group_offsets(g)
            if not offsets:
                out[g] = None
                continue
            ends = await consumer.end_offsets(list(offsets.keys()))
            out[g] = int(sum(max(0, ends[tp] - (om.offset if om.offset >= 0 else 0)) for tp, om in offsets.items()))
        return out
    except Exception as exc:
        return {"error": str(exc)[:120]}
    finally:
        for c in (admin, consumer):
            try:
                await c.close() if hasattr(c, "close") else await c.stop()
            except Exception:
                pass


def lag(args) -> dict | None:
    if args.edge_only:
        return None
    return asyncio.run(kafka_lag(args.kafka, ["telemetry-service", "alert-service", "analytics-service"]))


def mongo_status(uri: str | None):
    if not uri:
        return None
    try:
        from pymongo import MongoClient
        c = MongoClient(uri, serverSelectionTimeoutMS=3000)
        ss = c.admin.command("serverStatus")
        return {"insert": ss["opcounters"]["insert"], "query": ss["opcounters"]["query"],
                "update": ss["opcounters"]["update"]}
    except Exception as exc:
        return {"error": str(exc)[:120]}


# ---------------------------------------------------------------- snapshots
def snapshot(args, sim_ports: list[int]) -> dict:
    snap = {"t": time.time(), "sim": {"telemetry": 0.0, "events": 0.0, "publish_errors": 0.0, "dropped": 0.0, "lag_s": 0.0}}
    snap["sim_read_failures"] = 0
    for port in sim_ports:
        txt = get_text(f"http://127.0.0.1:{port}/metrics", timeout=15)
        if txt is None:   # a CPU-saturated simulator may answer slowly; retry once
            txt = get_text(f"http://127.0.0.1:{port}/metrics", timeout=30)
        if txt is None:
            snap["sim_read_failures"] += 1
        snap["sim"]["telemetry"] += prom_value(txt, "sim_messages_published_total", 'kind="telemetry"')
        snap["sim"]["events"] += prom_value(txt, "sim_messages_published_total", 'kind="event"')
        snap["sim"]["publish_errors"] += prom_value(txt, "sim_publish_errors_total")
        snap["sim"]["dropped"] += prom_value(txt, "sim_offline_buffer_dropped_total")
        snap["sim"]["lag_s"] = max(snap["sim"]["lag_s"], prom_value(txt, "sim_schedule_lag_seconds"))
    snap["nodered"] = get_json(f"{args.nodered}/stats")
    if not args.edge_only:
        snap["telemetry"] = get_json(f"{args.telemetry}/stats")
        snap["alerts"] = get_json(f"{args.alerts}/stats")
        snap["mongo"] = mongo_status(args.mongo)
    return snap


def reset_windows(args):
    get_json(f"{args.nodered}/stats?reset=1")
    if not args.edge_only:
        get_json(f"{args.telemetry}/stats?reset=1")
        get_json(f"{args.alerts}/stats?reset=1")


def d(a, b, *path):
    """delta b - a following a key path; None if missing."""
    try:
        va, vb = a, b
        for k in path:
            va, vb = va[k], vb[k]
        return vb - va
    except (KeyError, TypeError):
        return None


# ---------------------------------------------------------------- one stage
def run_stage(args, n: int) -> dict:
    procs_n = args.sim_procs or max(1, math.ceil(n / args.vehicles_per_proc))
    per = math.ceil(n / procs_n)
    total_time = args.warmup + args.duration + args.drain + 10
    procs, ports, stat_files = [], [], []
    pre = snapshot(args, [])   # pipeline counters before any traffic of this stage
    for i in range(procs_n):
        count = min(per, n - i * per)
        if count <= 0:
            break
        port = 9105 + 10 * i
        env = dict(os.environ, MODE="load_test", VEHICLE_COUNT=str(count), VEHICLE_ID_OFFSET=str(i * per),
                   TELEMETRY_INTERVAL=str(args.interval), MQTT_BROKER=args.mqtt_host, MQTT_PORT=str(args.mqtt_port),
                   MQTT_USERNAME=args.mqtt_user, MQTT_PASSWORD=args.mqtt_password,
                   EMERGENCY_EVENT_RATE=str(args.emergency_rate), SIM_HTTP_PORT=str(port),
                   DURATION_SECONDS=str(args.warmup + args.duration), LOG_LEVEL="WARNING",
                   PYTHONPATH=str(ROOT / "simulator"), RANDOM_SEED=str(1000 + i),
                   SIM_STATS_FILE=str(RESULTS / f".sim-stats-{i}.json"))
        stat_files.append(RESULTS / f".sim-stats-{i}.json")
        logf = open(RESULTS / f".sim-{i}.log", "w")
        procs.append(subprocess.Popen([sys.executable, "-m", "fleetsim.main"], env=env, cwd=ROOT / "simulator",
                                      stdout=subprocess.DEVNULL, stderr=logf))
        ports.append(port)
    print(f"\n=== Stage: {n} virtual vehicles ({len(procs)} simulator process(es), interval {args.interval}s) ===")
    sampler = ResourceSampler("docker" if args.docker_stats else "native", [p.pid for p in procs])
    try:
        time.sleep(args.warmup)
        dead = [p for p in procs if p.poll() is not None]
        if dead:
            err = (RESULTS / ".sim-0.log").read_text()[-800:]
            raise RuntimeError(f"simulator process exited during warm-up (port busy? broker down?):\n{err}")
        reset_windows(args)
        before = snapshot(args, ports)
        sampler.start()
        lag_samples = []
        t_end = time.time() + args.duration
        while time.time() < t_end:
            time.sleep(min(5, max(0.1, t_end - time.time())))
            lg = lag(args)
            if lg and "error" not in lg:
                lag_samples.append(lg)
        after = snapshot(args, ports)
        sampler.stop_evt.set()
        # latency percentiles are read at the end of the window (before drain)
        latency = {
            "edge_vehicle_to_nodered_ms": (after["nodered"] or {}).get("edge_latency_ms", {}).get("telemetry"),
        }
        if not args.edge_only:
            latency["e2e_vehicle_to_mongodb_ms"] = (after.get("telemetry") or {}).get("e2e_latency_ms")
            latency["kafka_to_telemetry_service_ms"] = ((after.get("telemetry") or {}).get("stage_latency_ms") or {}).get("kafka_to_service")
            latency["alert_e2e_ms"] = ((after.get("alerts") or {}).get("latency_ms") or {}).get("ALL")
            latency["alert_e2e_critical_ms"] = ((after.get("alerts") or {}).get("latency_ms") or {}).get("CRITICAL")
        # drain: wait for the simulator to stop and consumers to catch up
        for p in procs:
            try:
                p.wait(timeout=args.drain + 30)
            except subprocess.TimeoutExpired:
                p.terminate()
        drained_lag = None
        if not args.edge_only:
            deadline = time.time() + args.drain
            while time.time() < deadline:
                drained_lag = lag(args)
                if drained_lag and "error" not in drained_lag and all((v or 0) == 0 for v in drained_lag.values()):
                    break
                time.sleep(2)
        if args.edge_only:
            # wait until Node-RED has drained its backlog (received counter stable for 3 s)
            last, stable_since, deadline = None, time.time(), time.time() + args.drain
            while time.time() < deadline:
                cur = (get_json(f"{args.nodered}/stats") or {}).get("received", {}).get("telemetry")
                if cur != last:
                    last, stable_since = cur, time.time()
                elif time.time() - stable_since >= 3:
                    break
                time.sleep(1)
        final = snapshot(args, [])  # simulator gone; pipeline counters only
    finally:
        for p in procs:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()

    gen_total = None
    try:
        gen_total = sum(json.loads(f.read_text())["telemetry"] for f in stat_files)
        for f in stat_files:
            f.unlink()
    except Exception:
        pass
    dur = after["t"] - before["t"]
    gen = after["sim"]["telemetry"] - before["sim"]["telemetry"]
    if before["sim_read_failures"] or after["sim_read_failures"]:
        print("WARNING: simulator metrics unreadable (CPU saturated); window generation count invalid")
        gen = float("nan")
    nr_rx = d(before, after, "nodered", "received", "telemetry")
    res = {
        "vehicles": n, "simulator_processes": len(procs), "telemetry_interval_s": args.interval,
        "measurement_window_s": round(dur, 1),
        "target_msgs_per_s": round(n / args.interval, 1),
        "messages": {
            "generated_telemetry": None if gen != gen else int(gen),
            "generated_events": int(after["sim"]["events"] - before["sim"]["events"]),
            "nodered_received": nr_rx,
            "nodered_forwarded": d(before, after, "nodered", "forwarded", "telemetry"),
            "kafka_acked": None if args.edge_only else d(before, after, "nodered", "kafka", "sent"),
            "telemetry_consumed": None if args.edge_only else d(before, after, "telemetry", "consumed"),
            "mongodb_inserted": None if args.edge_only else d(before, after, "telemetry", "inserted"),
        },
        "throughput_msgs_per_s": {
            "simulator": None if gen != gen else round(gen / dur, 1),
            "nodered": round(nr_rx / dur, 1) if nr_rx is not None else None,
        },
        "latency": latency,
        "errors": {
            "sim_publish_errors": int(after["sim"]["publish_errors"] - before["sim"]["publish_errors"]),
            "sim_offline_buffer_dropped": int(after["sim"]["dropped"] - before["sim"]["dropped"]),
            "nodered_rejected": d(before, after, "nodered", "rejected"),
            "nodered_duplicates": d(before, after, "nodered", "duplicates"),
            "kafka_send_errors": None if args.edge_only else d(before, after, "nodered", "kafka", "errors"),
            "mongo_retries": None if args.edge_only else d(before, after, "telemetry", "mongo_retries"),
            "redis_errors": None if args.edge_only else d(before, after, "telemetry", "redis_errors"),
        },
        "simulator_schedule_lag_s_end": None if gen != gen else round(after["sim"]["lag_s"], 3),
        "simulator_metrics_unreadable": bool(before["sim_read_failures"] or after["sim_read_failures"]),
        "resources": sampler.summary(),
    }
    if args.edge_only:
        rx_total = d(pre, final, "nodered", "received", "telemetry")
        res["delivery"] = {"generated_total_incl_warmup": gen_total, "nodered_received_total_after_drain": rx_total,
                           "not_received": (gen_total - rx_total) if (gen_total is not None and rx_total is not None) else None}
    if not args.edge_only:
        ins = res["messages"]["mongodb_inserted"]
        res["throughput_msgs_per_s"]["kafka_acked"] = round(res["messages"]["kafka_acked"] / dur, 1) if res["messages"]["kafka_acked"] is not None else None
        res["throughput_msgs_per_s"]["telemetry_service"] = round(res["messages"]["telemetry_consumed"] / dur, 1) if res["messages"]["telemetry_consumed"] is not None else None
        res["throughput_msgs_per_s"]["mongodb_inserts"] = round(ins / dur, 1) if ins is not None else None
        res["kafka_lag"] = {
            "max_during_window": {g: max((s.get(g) or 0) for s in lag_samples) for g in (lag_samples[0] if lag_samples else {})},
            "after_drain": drained_lag,
        }
        res["database"] = {
            "mongo_insert_batch_ms": (after.get("telemetry") or {}).get("mongo_insert_batch_ms"),
            "mongo_opcounter_inserts_per_s": round(d(before, after, "mongo", "insert") / dur, 1) if d(before, after, "mongo", "insert") is not None else None,
        }
        # loss accounting over the whole run (window + drain) using the final counters
        inserted_total = d(pre, final, "telemetry", "inserted")
        res["delivery"] = {
            "generated_total_incl_warmup": gen_total,
            "nodered_received_total": d(pre, final, "nodered", "received", "telemetry"),
            "consumed_total_after_drain": d(pre, final, "telemetry", "consumed"),
            "mongodb_inserted_total_after_drain": inserted_total,
            "lost": (gen_total - inserted_total) if (gen_total is not None and inserted_total is not None) else None,
        }
    print(json.dumps({k: res[k] for k in ("messages", "throughput_msgs_per_s", "latency", "errors")}, indent=1, default=str))
    return res


def env_info() -> dict:
    info = {"platform": platform.platform(), "python": platform.python_version(), "cpu_count": os.cpu_count()}
    try:
        import psutil
        info["memory_gb"] = round(psutil.virtual_memory().total / 2 ** 30, 1)
    except ImportError:
        pass
    return info


def to_markdown(run: dict) -> str:
    L = [f"# Load test results - {run['started_at']}", "",
         "> Measured on the running system by `scripts/loadtest.py`. Virtual (software-simulated) vehicles; "
         "nothing extrapolated.", "",
         f"- Environment: {run['environment']}", f"- Scope: **{run['scope']}**",
         f"- Window per stage: {run['args']['duration']} s after {run['args']['warmup']} s warm-up; "
         f"telemetry interval {run['args']['interval']} s", ""]
    cols = ["Vehicles", "Target msg/s", "Generated msg/s", "Node-RED msg/s"]
    full = run["scope"] == "full pipeline"
    if full:
        cols += ["Kafka acked msg/s", "Mongo inserts/s", "E2E p50 ms", "E2E p95 ms", "E2E p99 ms", "Max lag (telemetry)"]
    cols += ["Edge p50 ms", "Edge p95 ms", "Edge p99 ms", "Rejected", "Publish errors", "Sim lag s"]
    L += ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for s in run["stages"]:
        e = s["latency"].get("edge_vehicle_to_nodered_ms") or {}
        row = [s["vehicles"], s["target_msgs_per_s"], s["throughput_msgs_per_s"]["simulator"], s["throughput_msgs_per_s"]["nodered"]]
        if full:
            x = s["latency"].get("e2e_vehicle_to_mongodb_ms") or {}
            ml = (s.get("kafka_lag") or {}).get("max_during_window") or {}
            row += [s["throughput_msgs_per_s"].get("kafka_acked"), s["throughput_msgs_per_s"].get("mongodb_inserts"),
                    x.get("p50"), x.get("p95"), x.get("p99"), ml.get("telemetry-service")]
        unreadable = s["messages"]["generated_telemetry"] is None
        row += [e.get("p50"), e.get("p95"), e.get("p99"), s["errors"]["nodered_rejected"],
                None if unreadable else s["errors"]["sim_publish_errors"],
                None if unreadable else s["simulator_schedule_lag_s_end"]]
        L.append("| " + " | ".join("–" if v is None else str(v) for v in row) + " |")
    L += ["", "## Delivery accounting (whole stage incl. warm-up, after drain)", "",
          "| Vehicles | Generated by simulator | Received by Node-RED | Stored in MongoDB | Not delivered |", "|---|---|---|---|---|"]
    for s in run["stages"]:
        dl = s.get("delivery") or {}
        gen = dl.get("generated_total_incl_warmup")
        rx = dl.get("nodered_received_total_after_drain", dl.get("nodered_received_total"))
        st = dl.get("mongodb_inserted_total_after_drain")
        lost = dl.get("lost", dl.get("not_received"))
        pct = f" ({lost / gen * 100:.2f} %)" if (lost is not None and gen) else ""
        L.append(f"| {s['vehicles']} | {gen if gen is not None else '–'} | {rx if rx is not None else '–'} | "
                 f"{st if st is not None else '–'} | {lost if lost is not None else '–'}{pct} |")
    L += ["", "## Resource usage (average CPU % of one core / average RSS MB)", ""]
    comps = sorted({c for s in run["stages"] for c in s["resources"]})
    L += ["| Vehicles | " + " | ".join(comps) + " |", "|" + "---|" * (len(comps) + 1)]
    for s in run["stages"]:
        L.append(f"| {s['vehicles']} | " + " | ".join(
            f"{s['resources'][c]['cpu_pct_avg']}% / {s['resources'][c]['mem_mb_avg']} MB" if c in s["resources"] else "–"
            for c in comps) + " |")
    return "\n".join(L) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stages", default="10,100,1000,5000,10000")
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--warmup", type=float, default=15)
    ap.add_argument("--drain", type=float, default=60)
    ap.add_argument("--interval", type=float, default=1.0, help="telemetry interval per vehicle (s)")
    ap.add_argument("--emergency-rate", type=float, default=0.0001)
    ap.add_argument("--sim-procs", type=int, default=0, help="simulator processes (0 = auto)")
    ap.add_argument("--vehicles-per-proc", type=int, default=5000)
    ap.add_argument("--mqtt-host", default="localhost")
    ap.add_argument("--mqtt-port", type=int, default=1883)
    ap.add_argument("--mqtt-user", default="simulator")
    ap.add_argument("--mqtt-password", default=os.getenv("MQTT_SIM_PASSWORD", "sim-dev-password"))
    ap.add_argument("--nodered", default="http://localhost:1880")
    ap.add_argument("--telemetry", default="http://localhost:8001")
    ap.add_argument("--alerts", default="http://localhost:8002")
    ap.add_argument("--kafka", default="localhost:29092")
    ap.add_argument("--mongo", default=os.getenv("LOADTEST_MONGO_URI", "mongodb://root:root-dev-password@localhost:27017/admin"))
    ap.add_argument("--edge-only", action="store_true", help="measure simulator->MQTT->Node-RED only")
    ap.add_argument("--docker-stats", action="store_true",
                    help="sample container CPU/memory with `docker stats` (default: psutil on native processes)")
    ap.add_argument("--tag", default="")
    args = ap.parse_args()

    if not get_json(f"{args.nodered}/stats"):
        sys.exit(f"Node-RED stats endpoint {args.nodered}/stats not reachable - is the stack running?")
    if not args.edge_only and not get_json(f"{args.telemetry}/stats"):
        sys.exit("telemetry-service not reachable - start the full stack or use --edge-only")

    RESULTS.mkdir(exist_ok=True)
    started = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run = {"started_at": started, "scope": "edge tier only (simulator -> MQTT -> Node-RED)" if args.edge_only else "full pipeline",
           "environment": env_info(), "args": vars(args), "stages": []}
    for n in [int(x) for x in args.stages.split(",") if x.strip()]:
        run["stages"].append(run_stage(args, n))
        name = f"loadtest-{started}{'-' + args.tag if args.tag else ''}"
        (RESULTS / f"{name}.json").write_text(json.dumps(run, indent=2, default=str))
        (RESULTS / f"{name}.md").write_text(to_markdown(run))
        time.sleep(5)
    print(f"\nResults written to results/{name}.json and results/{name}.md")


if __name__ == "__main__":
    main()
