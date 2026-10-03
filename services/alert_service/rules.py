"""Platform-side alert rules.

Vehicles report their own events (``vehicle.events``), but the platform must not
trust the vehicle alone: these rules re-derive safety-relevant conditions from
raw telemetry, so an alert is still raised if an event message is lost or the
on-board logic misses it. When both fire, the alert service merges them into a
single alert corroborated by both sources.
"""
from __future__ import annotations

SEVERITY_RANK = {"INFO": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}

ABNORMAL_SPEED_KMH = 100.0
ROUTE_DEVIATION_M = 150.0
COLLISION_TTC_S = 2.0

# Cooldown (s) during which repeated detections update the existing alert instead of creating a new one
COOLDOWN_S = {
    "COLLISION_WARNING": 30, "SUDDEN_BRAKING": 30, "OBSTACLE_DETECTED": 60, "LOW_BATTERY": 900,
    "CRITICAL_BATTERY": 900, "SENSOR_FAILURE": 600, "ABNORMAL_SPEED": 60, "ROUTE_DEVIATION": 120,
    "EMERGENCY_STOP": 60,
}


def evaluate_telemetry(t: dict, low: float = 20.0, critical: float = 10.0) -> list[dict]:
    """Return a list of {event_type, severity, details, dedupe_suffix} detected in one telemetry message."""
    out = []
    bat = t.get("battery") or {}
    loc = t.get("location") or {}
    safety = t.get("safety") or {}
    veh = t.get("vehicle") or {}
    pct = bat.get("percentage")
    if isinstance(pct, (int, float)) and not bat.get("charging"):
        if pct < critical:
            out.append({"event_type": "CRITICAL_BATTERY", "severity": "CRITICAL",
                        "details": {"battery_pct": pct, "threshold": critical}})
        elif pct < low:
            out.append({"event_type": "LOW_BATTERY", "severity": "MEDIUM",
                        "details": {"battery_pct": pct, "threshold": low}})
    ttc = safety.get("time_to_collision_s")
    if isinstance(ttc, (int, float)) and ttc < COLLISION_TTC_S and safety.get("emergency_braking"):
        out.append({"event_type": "COLLISION_WARNING", "severity": "CRITICAL",
                    "details": {"time_to_collision_s": ttc, "speed_kmh": loc.get("speed_kmh")}})
    for sensor in ("lidar", "radar", "camera"):
        if (t.get(sensor) or {}).get("status") == "FAILED":
            out.append({"event_type": "SENSOR_FAILURE", "severity": "HIGH",
                        "details": {"sensor": sensor}, "dedupe_suffix": sensor})
    if loc.get("gps_status") == "FAILED":
        out.append({"event_type": "SENSOR_FAILURE", "severity": "HIGH", "details": {"sensor": "gps"},
                    "dedupe_suffix": "gps"})
    spd = loc.get("speed_kmh")
    if isinstance(spd, (int, float)) and spd > ABNORMAL_SPEED_KMH:
        out.append({"event_type": "ABNORMAL_SPEED", "severity": "HIGH",
                    "details": {"speed_kmh": spd, "threshold_kmh": ABNORMAL_SPEED_KMH}})
    dev = veh.get("route_deviation_m")
    if isinstance(dev, (int, float)) and dev > ROUTE_DEVIATION_M:
        out.append({"event_type": "ROUTE_DEVIATION", "severity": "HIGH",
                    "details": {"deviation_m": dev, "route_id": veh.get("route_id")}})
    return out


def dedupe_key(vehicle_id: str, event_type: str, details: dict | None = None, suffix: str | None = None) -> str:
    if suffix is None and event_type == "SENSOR_FAILURE" and details:
        suffix = details.get("sensor")
    return f"alert:dedupe:{vehicle_id}:{event_type}" + (f":{suffix}" if suffix else "")
