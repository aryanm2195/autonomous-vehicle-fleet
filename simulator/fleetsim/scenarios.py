"""Deliberate scenarios, demo timeline and software-failure payloads."""
from __future__ import annotations

import json
import random

# Scenarios the simulator can inject into a vehicle (see VirtualVehicle.inject)
VEHICLE_SCENARIOS = [
    "collision", "obstacle", "sensor_failure", "sensor_degraded", "abnormal_speed",
    "route_deviation", "low_battery", "critical_battery", "emergency_stop",
]

# Weights used when EMERGENCY_EVENT_RATE picks a random scenario
RANDOM_SCENARIO_WEIGHTS = {
    "collision": 25, "obstacle": 20, "sensor_failure": 10, "sensor_degraded": 10,
    "abnormal_speed": 10, "route_deviation": 15, "critical_battery": 5, "emergency_stop": 5,
}

# Fleet-level software failures (handled by the runner, not a single vehicle)
FLEET_FAILURES = ["mqtt_disconnect", "malformed", "duplicate"]


def demo_timeline(vehicle_ids: list[str]) -> list[tuple[float, str, str]]:
    """(seconds after start, vehicle_id, scenario) - guarantees every required demo event.

    Picks vehicles spread through the fleet so it works for any VEHICLE_COUNT >= 1.
    """
    n = len(vehicle_ids)

    def v(i: int) -> str:
        return vehicle_ids[min(n - 1, i)]

    return [
        (15, v(2), "low_battery"),        # LOW_BATTERY  (warning -> vehicle heads to charger)
        (30, v(6), "collision"),          # COLLISION_WARNING + SUDDEN_BRAKING -> EMERGENCY
        (45, v(10), "sensor_failure"),    # SENSOR_FAILURE -> safe stop -> MAINTENANCE
        (60, v(14), "route_deviation"),   # ROUTE_DEVIATION
        (75, v(17), "abnormal_speed"),    # ABNORMAL_SPEED
        (90, v(4), "obstacle"),           # OBSTACLE_DETECTED
    ]


def pick_random_scenario(rng: random.Random) -> str:
    names = list(RANDOM_SCENARIO_WEIGHTS)
    return rng.choices(names, weights=[RANDOM_SCENARIO_WEIGHTS[n] for n in names])[0]


def malformed_payloads(vehicle_id: str, good: dict, rng: random.Random) -> bytes:
    """Return one deliberately invalid telemetry message (Node-RED must reject it)."""
    kind = rng.choice(["not_json", "missing_fields", "out_of_range", "wrong_types", "truncated"])
    if kind == "not_json":
        return b"<<garbage telemetry frame 0xDEADBEEF>>"
    if kind == "truncated":
        return json.dumps(good).encode()[: rng.randint(20, 120)]
    bad = json.loads(json.dumps(good))
    if kind == "missing_fields":
        bad.pop(rng.choice(["vehicle_id", "timestamp", "location", "battery"]), None)
    elif kind == "out_of_range":
        choice = rng.choice(["lat", "battery", "speed"])
        if choice == "lat":
            bad["location"]["latitude"] = 123.456
        elif choice == "battery":
            bad["battery"]["percentage"] = 187.0
        else:
            bad["location"]["speed_kmh"] = -40
    elif kind == "wrong_types":
        bad["location"]["speed_kmh"] = "fast"
    bad.setdefault("simulation", {})["malformed_kind"] = kind
    return json.dumps(bad).encode()
