"""Virtual autonomous-vehicle fleet simulator - entry point.

    python -m fleetsim.main            # reads configuration from the environment

A single asyncio process simulates the whole fleet. The telemetry interval is
split into time slots and every vehicle is assigned to one slot, so the
publishing load is spread evenly over the interval instead of arriving as one
burst. Each vehicle is updated exactly once per interval (dt = interval).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import random
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import metrics as M
from .config import Config
from .publisher import FleetPublisher
from .scenarios import (FLEET_FAILURES, VEHICLE_SCENARIOS, demo_timeline, malformed_payloads,
                        pick_random_scenario)
from .traffic import TrafficModel
from .vehicle import STATUSES, VirtualVehicle

try:  # orjson is ~5x faster for serialisation; fall back to stdlib
    import orjson

    def dumps(obj) -> bytes:
        return orjson.dumps(obj)
except ImportError:  # pragma: no cover
    def dumps(obj) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode()

log = logging.getLogger("fleetsim")


class FleetSimulator:
    def __init__(self, cfg: Config, publisher=None):
        self.cfg = cfg
        self.rng = random.Random(cfg.random_seed)
        self.ids = cfg.vehicle_ids()
        self.traffic = TrafficModel(cfg.start_lat, self.rng, change_probability=cfg.traffic_change_probability)
        self.vehicles: dict[str, VirtualVehicle] = {}
        for vid in self.ids:
            self.vehicles[vid] = VirtualVehicle(
                vehicle_id=vid, rng=random.Random(self.rng.random()), traffic=self.traffic,
                center=(cfg.start_lat, cfg.start_lon), area_radius_m=cfg.area_radius_km * 1000,
                low_battery=cfg.low_battery_threshold, critical_battery=cfg.critical_battery_threshold,
                drain_multiplier=cfg.battery_drain_multiplier)
        self.publisher = publisher if publisher is not None else FleetPublisher(cfg, self.ids)
        self.commands: queue.Queue = queue.Queue()
        self.timeline = demo_timeline(self.ids) if cfg.demo_script else []
        self.started = 0.0
        self.running = True
        self.stats = {"telemetry": 0, "events": 0, "scenarios": 0, "malformed": 0, "duplicates": 0}
        self.recent_events: list[dict] = []
        M.VEHICLES.set(len(self.ids))

    # ------------------------------------------------------------------ commands
    def submit(self, cmd: dict) -> dict:
        """Thread-safe entry point used by the HTTP control API."""
        kind = cmd.get("scenario") or cmd.get("failure")
        if kind not in VEHICLE_SCENARIOS and kind not in FLEET_FAILURES:
            return {"ok": False, "error": f"unknown scenario/failure {kind!r}",
                    "scenarios": VEHICLE_SCENARIOS, "failures": FLEET_FAILURES}
        vid = cmd.get("vehicle_id")
        if vid and vid not in self.vehicles:
            return {"ok": False, "error": f"unknown vehicle {vid}"}
        self.commands.put(cmd)
        return {"ok": True, "queued": cmd}

    def _apply_command(self, cmd: dict) -> None:
        if cmd.get("scenario"):
            vid = cmd.get("vehicle_id") or self.rng.choice(self.ids)
            self._inject(vid, cmd["scenario"], source="api")
            return
        f = cmd["failure"]
        M.FAILURES.labels(f).inc()
        if f == "mqtt_disconnect":
            secs = float(cmd.get("seconds", 10))
            n = self.publisher.simulate_disconnect(secs, float(cmd.get("fraction", 1.0)))
            log.warning("FAILURE: %d MQTT connection(s) dropped for %.0fs (store-and-forward active)", n, secs)
        elif f == "malformed":
            for _ in range(int(cmd.get("count", 10))):
                self._send_malformed(self.rng.choice(self.ids))
        elif f == "duplicate":
            for _ in range(int(cmd.get("count", 10))):
                self._send_duplicate(self.rng.choice(self.ids))

    def _inject(self, vid: str, scenario: str, source: str) -> None:
        if self.vehicles[vid].inject(scenario):
            M.SCENARIOS.labels(scenario, source).inc()
            self.stats["scenarios"] += 1
            log.info("SCENARIO %-16s -> %s (%s)", scenario, vid, source)

    def _send_malformed(self, vid: str) -> None:
        v = self.vehicles[vid]
        good = v.to_telemetry(time.time(), self.cfg.mode)
        v.seq -= 1  # a corrupted frame should not consume a real sequence number
        self.publisher.raw(vid, "telemetry", malformed_payloads(vid, good, self.rng), "malformed")
        self.stats["malformed"] += 1

    def _send_duplicate(self, vid: str) -> None:
        payload = dumps(self.vehicles[vid].to_telemetry(time.time(), self.cfg.mode))
        self.publisher.raw(vid, "telemetry", payload, "telemetry")
        self.publisher.raw(vid, "telemetry", payload, "duplicate")  # identical message_id
        self.stats["duplicates"] += 1

    # ------------------------------------------------------------------ main loop
    def _process_vehicle(self, v: VirtualVehicle, dt: float, now: float) -> None:
        cfg = self.cfg
        if cfg.emergency_event_rate and self.rng.random() < cfg.emergency_event_rate:
            self._inject(v.vehicle_id, pick_random_scenario(self.rng), source="random")
        if cfg.failure_rate and self.rng.random() < cfg.failure_rate:
            self._inject(v.vehicle_id, self.rng.choice(["sensor_failure", "sensor_degraded"]), source="failure_rate")
        v.step(dt)
        tel = v.to_telemetry(now, cfg.mode)
        if cfg.malformed_rate and self.rng.random() < cfg.malformed_rate:
            self.publisher.raw(v.vehicle_id, "telemetry", malformed_payloads(v.vehicle_id, tel, self.rng), "malformed")
            self.stats["malformed"] += 1
            return
        payload = dumps(tel)
        self.publisher.telemetry(v.vehicle_id, payload)
        self.stats["telemetry"] += 1
        if cfg.duplicate_rate and self.rng.random() < cfg.duplicate_rate:
            self.publisher.raw(v.vehicle_id, "telemetry", payload, "duplicate")
            self.stats["duplicates"] += 1
        for ev in v.drain_events():
            msg = v.build_event(ev, now, cfg.mode)
            self.publisher.event(v.vehicle_id, dumps(msg))
            self.stats["events"] += 1
            M.EVENTS.labels(ev["event_type"], ev["severity"]).inc()
            self.recent_events = (self.recent_events + [msg])[-50:]
            log.info("EVENT %-18s %-8s %s", ev["event_type"], ev["severity"], v.vehicle_id)

    async def run(self) -> None:
        cfg = self.cfg
        interval = cfg.telemetry_interval
        slots = max(1, min(50, len(self.ids) // 20 or 1, int(interval * 50)))
        groups = [self.ids[i::slots] for i in range(slots)]
        slot_dt = interval / slots
        self.started = time.time()
        next_t = self.started
        cycle = 0
        last_log = self.started
        log.info("Simulating %d virtual vehicles | mode=%s | interval=%.2fs | slots=%d | emergency_rate=%g",
                 len(self.ids), cfg.mode, interval, slots, cfg.emergency_event_rate)
        while self.running:
            for si, group in enumerate(groups):
                t0 = time.perf_counter()
                now = time.time()
                while not self.commands.empty():
                    self._apply_command(self.commands.get_nowait())
                elapsed = now - self.started
                while self.timeline and self.timeline[0][0] <= elapsed:
                    _, vid, scenario = self.timeline.pop(0)
                    self._inject(vid, scenario, source="demo_script")
                for vid in group:
                    self._process_vehicle(self.vehicles[vid], interval, now)
                M.TICK.observe(time.perf_counter() - t0)
                next_t += slot_dt
                lag = time.time() - next_t
                M.LAG.set(max(0.0, lag))
                if lag < 0:
                    await asyncio.sleep(-lag)
                else:
                    await asyncio.sleep(0)
                    if lag > interval * 5:  # hopelessly behind: re-anchor instead of spiralling
                        log.warning("simulator %.1fs behind schedule - CPU saturated; re-anchoring", lag)
                        next_t = time.time()
            self.traffic.step(interval)
            cycle += 1
            if cfg.demo_script and not self.timeline and cfg.mode == "demo":
                # replay the demo storyline (after a 30 s gap) so late viewers still see every event
                el = time.time() - self.started
                self.timeline = [(el + 15 + t, v, s) for t, v, s in demo_timeline(self.ids)]
            if time.time() - last_log >= 10:
                last_log = time.time()
                counts = {s: 0 for s in STATUSES}
                for v in self.vehicles.values():
                    counts[v.status] += 1
                for s, c in counts.items():
                    M.STATUS.labels(s).set(c)
                log.info("cycle=%d telemetry=%d events=%d lag=%.3fs status=%s", cycle, self.stats["telemetry"],
                         self.stats["events"], max(0.0, time.time() - next_t), counts)
            if cfg.duration_seconds and time.time() - self.started >= cfg.duration_seconds:
                self.running = False

    def status(self) -> dict:
        counts = {s: 0 for s in STATUSES}
        for v in self.vehicles.values():
            counts[v.status] += 1
        return {"mode": self.cfg.mode, "vehicles": len(self.ids), "uptime_s": round(time.time() - self.started, 1),
                "stats": self.stats, "status_counts": counts,
                "mqtt_connections_up": sum(1 for l in getattr(self.publisher, "links", []) if l.connected.is_set()),
                "scenarios": VEHICLE_SCENARIOS, "failures": FLEET_FAILURES,
                "recent_events": self.recent_events[-10:]}


def serve_http(sim: FleetSimulator, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, body: bytes, ctype: str = "application/json"):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path.startswith("/metrics"):
                self._send(200, generate_latest(), CONTENT_TYPE_LATEST)
            elif self.path.startswith("/health"):
                self._send(200, b'{"status":"ok"}')
            elif self.path.startswith("/status"):
                self._send(200, json.dumps(sim.status(), default=str).encode())
            else:
                self._send(404, b'{"error":"not found"}')

        def do_POST(self):
            if self.path not in ("/scenario", "/failure"):
                return self._send(404, b'{"error":"not found"}')
            try:
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0)) or 0) or b"{}")
            except ValueError:
                return self._send(400, b'{"error":"invalid json"}')
            self._send(200, json.dumps(sim.submit(body)).encode())

        def log_message(self, *args):  # keep stdout clean
            pass

    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def main() -> None:
    cfg = Config.from_env()
    logging.basicConfig(level=cfg.log_level, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    log.info("SOFTWARE SIMULATION - all vehicles and sensor readings are generated in software; "
             "no physical vehicles or sensors are used.")
    sim = FleetSimulator(cfg)
    srv = serve_http(sim, cfg.http_port)
    log.info("control/metrics API on :%d  (GET /status, GET /metrics, POST /scenario, POST /failure)", cfg.http_port)
    sim.publisher.start()

    loop = asyncio.new_event_loop()

    def _stop(*_):
        sim.running = False
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:  # Windows
            signal.signal(sig, _stop)
    try:
        loop.run_until_complete(sim.run())
    finally:
        log.info("stopping: %s", sim.stats)
        sim.publisher.stop()
        if os.getenv("SIM_STATS_FILE"):  # used by scripts/loadtest.py for exact delivery accounting
            with open(os.environ["SIM_STATS_FILE"], "w") as fh:
                json.dump({**sim.stats, "vehicles": len(sim.ids)}, fh)
        srv.shutdown()


if __name__ == "__main__":
    main()
