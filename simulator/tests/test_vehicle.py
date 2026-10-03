"""Unit tests for the virtual vehicle model (pure software, no broker needed)."""
import json
import pathlib
import random
import time

import jsonschema
import pytest

from fleetsim import geo
from fleetsim.config import Config
from fleetsim.main import FleetSimulator
from fleetsim.traffic import TRAFFIC_LEVELS, TrafficModel
from fleetsim.vehicle import TRANSITIONS, VirtualVehicle

SCHEMAS = pathlib.Path(__file__).resolve().parents[2] / "schemas"


class FakePublisher:
    links = []

    def __init__(self):
        self.msgs = []

    def telemetry(self, vid, p):
        self.msgs.append(("telemetry", vid, json.loads(p)))

    def event(self, vid, p):
        self.msgs.append(("event", vid, json.loads(p)))

    def raw(self, vid, kind, p, label):
        self.msgs.append((label, vid, p))

    def simulate_disconnect(self, *a):
        return 0


def make_vehicle(seed=1):
    rng = random.Random(seed)
    return VirtualVehicle("vehicle-00001", random.Random(seed), TrafficModel(-37.81, rng), (-37.8136, 144.9631), 6000)


def run_sim(n=20, seconds=120, **overrides):
    cfg = Config.from_env()
    cfg.vehicle_count = n
    cfg.random_seed = 42
    for k, v in overrides.items():
        setattr(cfg, k, v)
    pub = FakePublisher()
    sim = FleetSimulator(cfg, pub)
    for sec in range(seconds):
        for t, vid, sc in [x for x in sim.timeline if x[0] == sec]:
            sim._inject(vid, sc, "test")
        for v in sim.vehicles.values():
            sim._process_vehicle(v, 1.0, time.time())
        sim.traffic.step(1.0)
    return sim, pub


def test_vehicle_ids_are_zero_padded_and_configurable():
    cfg = Config.from_env()
    cfg.vehicle_count = 3
    assert cfg.vehicle_ids() == ["vehicle-00001", "vehicle-00002", "vehicle-00003"]
    cfg.vehicle_count, cfg.vehicle_id_offset = 2, 99998
    assert cfg.vehicle_ids() == ["vehicle-99999", "vehicle-100000"]


def test_mode_presets(monkeypatch):
    monkeypatch.setenv("MODE", "load_test")
    cfg = Config.from_env()
    assert cfg.mode == "load_test" and cfg.mqtt_connection_mode == "shared" and not cfg.demo_script
    monkeypatch.setenv("VEHICLE_COUNT", "5000")
    assert Config.from_env().vehicle_count == 5000  # env var beats preset
    monkeypatch.setenv("MODE", "bogus")
    with pytest.raises(ValueError):
        Config.from_env()


def test_gps_moves_gradually():
    v = make_vehicle()
    v._start_trip()
    prev = (v.lat, v.lon)
    for _ in range(300):
        v.step(1.0)
        jump = geo.haversine_m(*prev, v.lat, v.lon)
        assert jump <= v.speed_mps * 1.0 + 20  # never teleports (> speed * dt)
        prev = (v.lat, v.lon)


def test_speed_changes_are_bounded():
    v = make_vehicle(7)
    v._start_trip()
    speeds = []
    for _ in range(300):
        v.step(1.0)
        speeds.append(v.speed_mps * 3.6)
    deltas = [abs(b - a) for a, b in zip(speeds, speeds[1:])]
    assert max(deltas) <= 8.0 * 3.6 + 1  # emergency braking is the largest allowed change
    assert max(speeds) < 100  # no abnormal speed unless injected


def test_battery_drains_monotonically_when_not_charging():
    v = make_vehicle(3)
    v._start_trip()
    last = v.battery_pct
    for _ in range(200):
        v.step(1.0)
        if not v.charging:
            assert v.battery_pct <= last + 1e-9
        last = v.battery_pct


def test_low_and_critical_battery_events_fire_once_on_crossing():
    v = make_vehicle(4)
    v._start_trip()
    v.battery_pct = 20.3
    v.drain_multiplier = 50
    types = []
    for _ in range(120):
        v.step(1.0)
        types += [e["event_type"] for e in v.drain_events()]
    assert types.count("LOW_BATTERY") == 1
    assert types.count("CRITICAL_BATTERY") <= 1


def test_passenger_rules_match_status():
    sim, pub = run_sim(n=50, seconds=150)
    for kind, vid, msg in pub.msgs:
        if kind != "telemetry":
            continue
        st, pc = msg["vehicle"]["status"], msg["vehicle"]["passenger_count"]
        if st == "IN_SERVICE":
            assert 1 <= pc <= 6
        elif st in ("AVAILABLE", "IDLE", "CHARGING", "MAINTENANCE"):
            assert pc == 0


def test_status_transitions_are_legal_and_not_flapping():
    sim, pub = run_sim(n=50, seconds=200)
    last = {}
    changes = {}
    for kind, vid, msg in pub.msgs:
        if kind != "telemetry":
            continue
        st = msg["vehicle"]["status"]
        if vid in last and last[vid] != st:
            assert st in TRANSITIONS[last[vid]] or last[vid] == "EMERGENCY", (last[vid], st)
            changes[vid] = changes.get(vid, 0) + 1
        last[vid] = st
    assert max(changes.values(), default=0) < 20  # no second-by-second switching


def test_demo_script_produces_every_required_event():
    cfg = Config.from_env()
    sim, pub = run_sim(n=20, seconds=110, demo_script=True)
    events = {(m["event_type"]) for k, _, m in pub.msgs if k == "event"}
    for required in ("LOW_BATTERY", "COLLISION_WARNING", "SENSOR_FAILURE", "ROUTE_DEVIATION", "ABNORMAL_SPEED"):
        assert required in events, f"{required} missing from demo storyline: {events}"


def test_sensor_streams_are_consistent():
    v = make_vehicle(5)
    v.inject("collision")
    v.step(0.1)
    t = v.to_telemetry(time.time(), "demo")
    assert t["lidar"]["obstacle_detected"] and t["radar"]["object_detected"]
    assert abs(t["lidar"]["distance_m"] - t["radar"]["distance_m"]) < 3
    assert t["camera"]["object_type"] == "pedestrian"
    assert t["radar"]["relative_speed_mps"] < 0  # closing


def test_failed_sensor_reports_nulls():
    v = make_vehicle(6)
    v.fail_sensor("lidar")
    t = v.to_telemetry(time.time(), "demo")
    assert t["lidar"] == {"status": "FAILED", "obstacle_detected": None, "distance_m": None, "object_count": None}
    assert any(e["event_type"] == "SENSOR_FAILURE" for e in v.drain_events())


def test_traffic_changes_one_level_at_a_time():
    tm = TrafficModel(-37.81, random.Random(1), change_probability=1.0)
    tm.level_at(-37.81, 144.96)
    prev = TRAFFIC_LEVELS.index(tm.level_at(-37.81, 144.96))
    for _ in range(100):
        tm.step(1.0)
        cur = TRAFFIC_LEVELS.index(tm.level_at(-37.81, 144.96))
        assert abs(cur - prev) <= 1
        prev = cur


def test_generated_messages_validate_against_json_schema():
    tv = jsonschema.Draft202012Validator(json.loads((SCHEMAS / "telemetry.schema.json").read_text()))
    ev = jsonschema.Draft202012Validator(json.loads((SCHEMAS / "event.schema.json").read_text()))
    sim, pub = run_sim(n=30, seconds=100, emergency_event_rate=0.02)
    tel = [m for k, _, m in pub.msgs if k == "telemetry"]
    evs = [m for k, _, m in pub.msgs if k == "event"]
    assert evs, "expected some events at this emergency rate"
    for m in random.Random(0).sample(tel, 500) + evs:
        (tv if "lidar" in m else ev).validate(m)


def test_malformed_and_duplicate_injection():
    sim, pub = run_sim(n=10, seconds=5, malformed_rate=0.5, duplicate_rate=0.5)
    labels = {k for k, _, _ in pub.msgs}
    assert "malformed" in labels and "duplicate" in labels


def test_large_time_step_does_not_create_false_route_deviation():
    """TELEMETRY_INTERVAL=60 s (100k-vehicle runs) must not overshoot corners."""
    import random
    from fleetsim.traffic import TrafficModel
    from fleetsim.vehicle import VirtualVehicle
    c = (-37.8136, 144.9631)
    tr = TrafficModel(c[0], random.Random(1))
    events = []
    for i in range(200):
        v = VirtualVehicle(vehicle_id=f"v{i}", rng=random.Random(i), traffic=tr, center=c, area_radius_m=4000)
        for _ in range(20):
            v.step(60.0)
            events += [e["event_type"] for e in v.drain_events()]
    assert "ROUTE_DEVIATION" not in events
