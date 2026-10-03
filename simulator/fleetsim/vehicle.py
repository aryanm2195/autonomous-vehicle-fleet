"""Stateful virtual autonomous vehicle.

Each ``VirtualVehicle`` keeps its own persistent state (position, speed,
heading, battery, passengers, status, sensor health, the object in front of it,
...) and ``step(dt)`` evolves that state physically plausibly:

* speed changes are bounded by acceleration / braking limits,
* heading changes are bounded by a turn-rate limit,
* position is integrated from speed and heading (no teleporting),
* battery drains with distance, load and idle consumption, and only rises
  while plugged in,
* the vehicle status follows a state machine with minimum dwell times,
* LiDAR, radar and camera all observe the *same* simulated object, so the
  three sensor streams are mutually consistent (the basis for sensor fusion).

Nothing here talks to the network: ``to_telemetry()`` builds the JSON-ready
payload and ``drain_events()`` returns on-board detected events.

All of this is SOFTWARE-GENERATED data. There is no physical vehicle or sensor.
"""
from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

from . import geo
from .traffic import TRAFFIC_HAZARD_RATE, TRAFFIC_INDEX, TRAFFIC_SPEED_KMH, TrafficModel

SCHEMA_VERSION = "1.1.0"

STATUSES = ("AVAILABLE", "IN_SERVICE", "IDLE", "CHARGING", "MAINTENANCE", "EMERGENCY")
SENSORS = ("gps", "lidar", "radar", "camera")
OBJECT_TYPES = ("pedestrian", "vehicle", "bicycle", "obstacle", "road_barrier")
CAMERA_ONLY_TYPES = ("traffic_light", "road_sign")

# Allowed transitions of the status state machine (documented in docs/ARCHITECTURE.md)
TRANSITIONS = {
    "AVAILABLE": {"IN_SERVICE", "IDLE", "CHARGING", "MAINTENANCE", "EMERGENCY"},
    "IN_SERVICE": {"AVAILABLE", "CHARGING", "MAINTENANCE", "EMERGENCY"},
    "IDLE": {"AVAILABLE", "CHARGING", "MAINTENANCE", "EMERGENCY"},
    "CHARGING": {"AVAILABLE", "MAINTENANCE", "EMERGENCY"},
    "MAINTENANCE": {"AVAILABLE"},
    "EMERGENCY": {"AVAILABLE", "IN_SERVICE", "MAINTENANCE", "CHARGING"},
}
MIN_DWELL_S = {"AVAILABLE": 20, "IN_SERVICE": 30, "IDLE": 20, "CHARGING": 30, "MAINTENANCE": 45, "EMERGENCY": 15}

# Physical limits
MAX_ACCEL = 2.5          # m/s^2
COMFORT_DECEL = 3.0      # m/s^2
EMERGENCY_DECEL = 8.0    # m/s^2
MAX_TURN_RATE = 40.0     # deg/s
WAYPOINT_RADIUS_M = 25.0
LARGE_STEP_S = 5.0       # steps longer than this follow the route geometry exactly (see step())
CITY_SPEED_LIMIT_KMH = 60.0
ABNORMAL_SPEED_KMH = 100.0
ROUTE_DEVIATION_M = 150.0
BATTERY_PCT_PER_KM = 0.22        # ~60 kWh pack, ~0.13 kWh/km
IDLE_DRAIN_PCT_PER_S = 0.00015   # HVAC + compute
CHARGE_PCT_PER_S = 0.08          # time-compressed DC fast charge (documented)
PASSENGER_WEIGHTS = [35, 30, 15, 12, 5, 3]  # P(1..6 passengers)

# Seven charging hubs around Melbourne CBD (lat, lon)
CHARGING_HUBS = [
    (-37.8183, 144.9671), (-37.8102, 144.9628), (-37.8226, 144.9580), (-37.8065, 144.9760),
    (-37.8300, 144.9700), (-37.7990, 144.9590), (-37.8150, 144.9450),
]

SEVERITY_ORDER = ("INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL")


def utc_iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class TrackedObject:
    """The single most relevant object in front of the vehicle (what the ADAS would track)."""
    obj_type: str
    distance_m: float
    ground_speed_mps: float      # object's own speed along our heading
    ttl_s: float                 # how long until it clears the path (crosses, turns off...)
    scripted: bool = False


@dataclass
class VirtualVehicle:
    vehicle_id: str
    rng: random.Random
    traffic: TrafficModel
    center: tuple[float, float]
    area_radius_m: float
    low_battery: float = 20.0
    critical_battery: float = 10.0
    drain_multiplier: float = 1.0

    # ---- dynamic state -------------------------------------------------
    lat: float = 0.0
    lon: float = 0.0
    altitude_m: float = 31.0
    speed_mps: float = 0.0
    heading: float = 0.0
    battery_pct: float = 100.0
    charging: bool = False
    odometer_km: float = 0.0
    status: str = "AVAILABLE"
    prev_status: str = "AVAILABLE"
    status_since: float = 0.0
    activity: str = "PARKED"
    passengers: int = 0
    trip_id: str | None = None
    route: list = field(default_factory=list)          # waypoints actually being followed
    planned_route: list = field(default_factory=list)  # route issued by dispatch (for deviation)
    route_index: int = 0
    destination: tuple[float, float] | None = None
    route_id: str | None = None
    sensor_status: dict = field(default_factory=lambda: {s: "OK" for s in SENSORS})
    obj: TrackedObject | None = None
    seq: int = 0
    sim_time: float = 0.0
    speed_personality: float = 1.0
    last_decel: float = 0.0
    emergency_braking: bool = False
    speed_fault_until: float = -1.0
    speed_fault_kmh: float = 0.0
    emergency_until: float = -1.0
    emergency_next: str | None = None
    maintenance_until: float = -1.0
    battery_flags: dict = field(default_factory=lambda: {"low": False, "critical": False})
    cooldowns: dict = field(default_factory=dict)
    pending_events: list = field(default_factory=list)
    background_returns: int = 0
    cruise_kmh: float = 40.0

    # ------------------------------------------------------------------
    def __post_init__(self):
        if not self.lat:
            self.lat, self.lon = geo.random_point(*self.center, self.area_radius_m * 0.8, self.rng)
        self.heading = self.rng.uniform(0, 360)
        self.battery_pct = round(self.rng.uniform(35, 100), 2)
        self.odometer_km = round(self.rng.uniform(500, 60_000), 1)
        self.speed_personality = self.rng.uniform(0.92, 1.08)
        self._base_drain = self.drain_multiplier
        # desynchronise initial status so the fleet looks like a real fleet
        r = self.rng.random()
        if r < 0.45:
            self._start_trip()
        elif r < 0.55:
            self._set_status("IDLE", force=True)
        # dwell-time clocks start "in the past" so transitions can happen soon
        self.status_since = -self.rng.uniform(0, 60)

    # ---------------------------------------------------------------- routes
    def _make_route(self, dest: tuple[float, float]) -> list[tuple[float, float]]:
        """Street-grid-like route: alternate N-S and E-W legs with a little jitter."""
        pts = []
        lat, lon = self.lat, self.lon
        legs = self.rng.randint(2, 4)
        for i in range(1, legs + 1):
            frac = i / legs
            tlat = self.lat + (dest[0] - self.lat) * frac
            tlon = self.lon + (dest[1] - self.lon) * frac
            if i % 2 == 1:
                pts.append((tlat, lon))  # north-south leg
            else:
                pts.append((lat, tlon))  # east-west leg
            pts.append((tlat, tlon))
            lat, lon = tlat, tlon
        pts[-1] = dest
        return pts

    def _set_route(self, dest: tuple[float, float]) -> None:
        self.destination = dest
        self.route = self._make_route(dest)
        self.planned_route = [(self.lat, self.lon)] + list(self.route)
        self.route_index = 0
        self.route_id = f"route-{uuid.uuid4().hex[:8]}"

    def _start_trip(self) -> None:
        dist = self.rng.uniform(1500, 7000)
        dest = geo.destination(self.lat, self.lon, self.rng.uniform(0, 360), dist)
        # keep destinations inside the service area
        if geo.haversine_m(*dest, *self.center) > self.area_radius_m:
            dest = geo.random_point(*self.center, self.area_radius_m * 0.7, self.rng)
        self._set_route(dest)
        self.trip_id = f"trip-{uuid.uuid4().hex[:10]}"
        self.passengers = self.rng.choices(range(1, 7), weights=PASSENGER_WEIGHTS)[0]
        self._set_status("IN_SERVICE", force=True)
        self.activity = "TRIP"

    def _end_trip(self) -> None:
        self.trip_id = None
        self.passengers = 0
        self.destination = None
        self.route = []
        self.planned_route = []

    def _go_charge(self) -> None:
        hub = min(CHARGING_HUBS, key=lambda h: geo.haversine_m(self.lat, self.lon, *h))
        self._end_trip()
        self._set_route(hub)
        self._set_status("CHARGING", force=True)
        self.activity = "EN_ROUTE_TO_CHARGER"

    def _reposition(self) -> None:
        dest = geo.random_point(*self.center, self.area_radius_m * 0.6, self.rng)
        self._set_route(dest)
        self.activity = "REPOSITIONING"

    # ---------------------------------------------------------------- status
    def _set_status(self, new: str, force: bool = False) -> bool:
        if new == self.status:
            return False
        if not force:
            if new not in TRANSITIONS[self.status]:
                return False
            if self.sim_time - self.status_since < MIN_DWELL_S[self.status]:
                return False
        self.prev_status = self.status
        self.status = new
        self.status_since = self.sim_time
        return True

    def _dwell(self) -> float:
        return self.sim_time - self.status_since

    def _update_status(self, dt: float) -> None:
        s = self.status
        if s == "EMERGENCY":
            if self.sim_time >= self.emergency_until:
                nxt = self.emergency_next or ("IN_SERVICE" if self.trip_id else "AVAILABLE")
                self.emergency_next = None
                if nxt == "MAINTENANCE":
                    self._enter_maintenance()
                else:
                    self._set_status(nxt, force=True)
                    self.activity = "TRIP" if nxt == "IN_SERVICE" else "CRUISING"
            return
        if s == "MAINTENANCE":
            if self.sim_time >= self.maintenance_until:
                self.sensor_status = {k: "OK" for k in SENSORS}
                self._set_status("AVAILABLE", force=True)
                self.activity = "PARKED"
            return
        if s == "CHARGING":
            if self.charging and self.battery_pct >= 90.0:
                self.charging = False
                self._end_trip()
                self._set_status("AVAILABLE", force=True)
                self.activity = "PARKED"
            return
        if s == "IN_SERVICE":
            if not self.route:
                self._arrived()
                return
            if self.battery_pct < self.critical_battery and self._dwell() > MIN_DWELL_S[s]:
                self._go_charge()  # safe early drop-off, then straight to a charger
            return
        # AVAILABLE / IDLE
        if self.battery_pct < self.low_battery:
            self._go_charge()
            return
        if s == "AVAILABLE":
            if self._dwell() > MIN_DWELL_S[s]:
                if self.rng.random() < dt / 90.0:
                    self._start_trip()
                elif self.rng.random() < dt / 400.0:
                    self._end_trip()
                    self._set_status("IDLE")
                    self.activity = "PARKED"
                elif not self.route and self.rng.random() < dt / 60.0:
                    self._reposition()
        elif s == "IDLE" and self._dwell() > MIN_DWELL_S[s] and self.rng.random() < dt / 90.0:
            self._set_status("AVAILABLE")
            self.activity = "PARKED"

    def _enter_maintenance(self, duration: float | None = None) -> None:
        self._end_trip()
        self._set_status("MAINTENANCE", force=True)
        self.maintenance_until = self.sim_time + (duration or self.rng.uniform(60, 150))
        self.activity = "OUT_OF_SERVICE"

    def enter_emergency(self, duration: float | None = None, then: str | None = None) -> None:
        self._set_status("EMERGENCY", force=True)
        self.emergency_until = self.sim_time + (duration or self.rng.uniform(20, 40))
        self.emergency_next = then
        self.activity = "EMERGENCY_STOP"

    # ---------------------------------------------------------------- events
    def _emit(self, event_type: str, severity: str, details: dict, cooldown: float = 20.0) -> None:
        last = self.cooldowns.get(event_type, -1e9)
        if self.sim_time - last < cooldown:
            return
        self.cooldowns[event_type] = self.sim_time
        self.pending_events.append({
            "event_type": event_type,
            "severity": severity,
            "details": details,
        })

    def drain_events(self) -> list[dict]:
        ev, self.pending_events = self.pending_events, []
        return ev

    # ---------------------------------------------------------------- objects
    def _update_object(self, dt: float, traffic_level: str) -> None:
        own = self.speed_mps
        if self.obj is None:
            moving = own > 1.0
            if moving and self.rng.random() < TRAFFIC_HAZARD_RATE[traffic_level] * dt:
                t = self.rng.choices(OBJECT_TYPES, weights=[25, 45, 15, 10, 5])[0]
                if t == "vehicle":
                    gs = max(0.0, own + self.rng.uniform(-3, 1.5))
                elif t == "bicycle":
                    gs = self.rng.uniform(3, 7)
                else:
                    gs = 0.0
                stop_d = own * own / (2 * COMFORT_DECEL * 0.8) + 8.0  # detected beyond planning distance
                self.obj = TrackedObject(t, self.rng.uniform(stop_d + 10, stop_d + 70), gs, self.rng.uniform(4, 15))
            return
        o = self.obj
        o.distance_m += (o.ground_speed_mps - own) * dt
        o.ttl_s -= dt
        if o.obj_type == "vehicle":
            o.ground_speed_mps = max(0.0, o.ground_speed_mps + self.rng.uniform(-0.6, 0.6))
        if o.ttl_s <= 0 or o.distance_m > 120 or o.distance_m < 0.5:
            self.obj = None

    def required_decel(self) -> float:
        """Deceleration (m/s^2) needed to avoid reaching the object (2 m safety buffer)."""
        if not self.obj:
            return 0.0
        closing = self.speed_mps - self.obj.ground_speed_mps
        if closing <= 0:
            return 0.0
        room = self.obj.distance_m - 2.0
        return float("inf") if room <= 0 else closing * closing / (2 * room)

    def time_to_collision(self) -> float | None:
        if not self.obj:
            return None
        closing = self.speed_mps - self.obj.ground_speed_mps
        if closing <= 0.1:
            return None
        return self.obj.distance_m / closing

    # ---------------------------------------------------------------- main step
    def step(self, dt: float) -> None:
        self.sim_time += dt
        traffic_level = self.traffic.level_at(self.lat, self.lon)
        self._update_status(dt)

        # ---- target speed -------------------------------------------------
        # cruise speed follows a slow random walk inside the traffic band, so consecutive
        # readings look like 40 -> 42 -> 44 -> 41 km/h rather than random jumps
        lo, hi = TRAFFIC_SPEED_KMH[traffic_level]
        lo, hi = lo * self.speed_personality, hi * self.speed_personality
        self.cruise_kmh += self.rng.gauss(0, 0.8) * dt
        if self.cruise_kmh < lo:
            self.cruise_kmh = min(lo, self.cruise_kmh + 3.0 * dt)
        elif self.cruise_kmh > hi:
            self.cruise_kmh = max(hi, self.cruise_kmh - 3.0 * dt)
        cruise = self.cruise_kmh / 3.6
        moving_states = {"IN_SERVICE", "AVAILABLE"} | ({"CHARGING"} if not self.charging else set())
        if self.status not in moving_states or not self.route:
            target = 0.0
        elif self.status == "AVAILABLE":
            target = cruise * 0.7
        else:
            target = cruise
        if self.sensor_status["lidar"] == "FAILED" and self.sensor_status["radar"] == "FAILED":
            target = 0.0  # minimal-risk manoeuvre

        # autonomous emergency braking / following
        decel_limit = COMFORT_DECEL
        ttc = self.time_to_collision()
        self.emergency_braking = False
        if self.obj is not None:
            rd = self.required_decel()
            if rd > 4.5:  # comfortable braking no longer enough -> autonomous emergency braking
                target, decel_limit = 0.0, EMERGENCY_DECEL
                self.emergency_braking = True
                if rd > 5.5 and ttc is not None and ttc < 3.0:
                    self._emit("COLLISION_WARNING", "CRITICAL", {
                        "object_type": self.obj.obj_type, "distance_m": round(self.obj.distance_m, 2),
                        "time_to_collision_s": round(ttc, 2), "speed_kmh": round(self.speed_mps * 3.6, 1),
                        "required_decel_mps2": round(min(rd, 99.0), 2)}, 30)
                    if self.obj.scripted and self.status not in ("EMERGENCY", "MAINTENANCE"):
                        self.enter_emergency()
            else:
                # anticipatory planning with one-step look-ahead: never faster than what allows a
                # comfortable stop / following with an 8 m standstill gap + 1 s headway
                gap = max(0.0, self.obj.distance_m - 8.0 - self.obj.ground_speed_mps * 1.0
                          - max(0.0, self.speed_mps - self.obj.ground_speed_mps) * dt)
                allowed = self.obj.ground_speed_mps + math.sqrt(2 * (COMFORT_DECEL * 0.6) * gap)
                if allowed < target:
                    target = allowed
                    decel_limit = 4.5

        # injected controller fault (abnormal speed scenario) overrides everything
        if self.sim_time < self.speed_fault_until:
            target = self.speed_fault_kmh / 3.6
            accel = 6.0
        else:
            accel = MAX_ACCEL

        prev = self.speed_mps
        if target > self.speed_mps:
            self.speed_mps = min(target, self.speed_mps + accel * dt)
        else:
            self.speed_mps = max(target, self.speed_mps - decel_limit * dt)
        if self.speed_mps > 0.5:
            self.speed_mps = max(0.0, self.speed_mps + self.rng.gauss(0, 0.08))
        self.last_decel = (prev - self.speed_mps) / dt if dt > 0 else 0.0
        # the tracked object moves relative to our *average* speed over the step
        avg = (prev + self.speed_mps) / 2
        cur, self.speed_mps = self.speed_mps, avg
        self._update_object(dt, traffic_level)
        self.speed_mps = cur
        ttc = self.time_to_collision()

        # ---- heading & position ------------------------------------------
        dist = self.speed_mps * dt
        if dt > LARGE_STEP_S and self.route and self.route_index < len(self.route):
            # Large time step (e.g. TELEMETRY_INTERVAL=60 s in load tests): integrating a straight
            # line for ~800 m would overshoot corners and create false ROUTE_DEVIATION events.
            # Instead, travel the same distance *along* the route, consuming waypoints.
            self._follow_route(dist)
        else:
            self._integrate_position(dt, dist)
        self.altitude_m = 31.0 + 8.0 * math.sin(self.lat * 900) + 5.0 * math.cos(self.lon * 700)
        self._after_motion(dt, dist, traffic_level, ttc)

    def _follow_route(self, dist: float) -> None:
        remaining = dist
        self.odometer_km += dist / 1000.0
        while remaining > 0 and self.route and self.route_index < len(self.route):
            wlat, wlon = self.route[self.route_index]
            d = geo.haversine_m(self.lat, self.lon, wlat, wlon)
            self.heading = geo.bearing_deg(self.lat, self.lon, wlat, wlon) if d > 0.5 else self.heading
            if d <= remaining:
                self.lat, self.lon = wlat, wlon
                remaining -= d
                self.route_index += 1
                if self.route_index >= len(self.route):
                    self._arrived()
                    break
            else:
                frac = remaining / d
                self.lat += (wlat - self.lat) * frac
                self.lon += (wlon - self.lon) * frac
                remaining = 0.0

    def _integrate_position(self, dt: float, dist: float) -> None:
        if self.route and self.route_index < len(self.route):
            wlat, wlon = self.route[self.route_index]
            desired = geo.bearing_deg(self.lat, self.lon, wlat, wlon)
            diff = geo.angle_diff(self.heading, desired)
            max_turn = MAX_TURN_RATE * dt
            self.heading = (self.heading + max(-max_turn, min(max_turn, diff))) % 360.0
        if dist > 0:
            # equirectangular integration: cheap and accurate over metres
            h = math.radians(self.heading)
            self.lat += (dist * math.cos(h)) / 111_320.0
            self.lon += (dist * math.sin(h)) / (111_320.0 * math.cos(math.radians(self.lat)))
            self.odometer_km += dist / 1000.0
        if self.route and self.route_index < len(self.route):
            wlat, wlon = self.route[self.route_index]
            if geo.haversine_m(self.lat, self.lon, wlat, wlon) < WAYPOINT_RADIUS_M + dist:
                self.route_index += 1
                if self.route_index >= len(self.route):
                    self._arrived()

    def _after_motion(self, dt: float, dist: float, traffic_level: str, ttc: float | None) -> None:
        # ---- battery ------------------------------------------------------
        if self.charging:
            self.battery_pct = min(100.0, self.battery_pct + CHARGE_PCT_PER_S * dt)
        else:
            load = 1.0 + 0.03 * self.passengers + (0.15 if traffic_level in ("HIGH", "CONGESTED") else 0.0)
            drain = (dist / 1000.0) * BATTERY_PCT_PER_KM * load + IDLE_DRAIN_PCT_PER_S * dt
            self.battery_pct = max(0.0, self.battery_pct - drain * self.drain_multiplier)
        self._check_battery()

        # ---- background LiDAR returns (parked cars, poles...) --------------
        base = {"LOW": 1, "MODERATE": 3, "HIGH": 5, "CONGESTED": 8}[traffic_level]
        self.background_returns = max(0, min(20, self.background_returns + self.rng.choice([-1, 0, 0, 1])))
        if abs(self.background_returns - base) > 3:
            self.background_returns = base

        self._detect_events(ttc)

    def _arrived(self) -> None:
        self.route = []
        if self.status == "IN_SERVICE":
            self._end_trip()
            self._set_status("AVAILABLE", force=True)
            self.activity = "PARKED"
        elif self.status == "CHARGING":
            self.charging = True
            self.activity = "CHARGING"
        else:
            self.activity = "PARKED"

    def _check_battery(self) -> None:
        b, f = self.battery_pct, self.battery_flags
        if not f["low"] and b < self.low_battery and not self.charging:
            f["low"] = True
            self._emit("LOW_BATTERY", "MEDIUM", {"battery_pct": round(b, 2), "threshold": self.low_battery}, 0)
            self.drain_multiplier = self._base_drain  # end of any injected fast-drain scenario
        if not f["critical"] and b < self.critical_battery and not self.charging:
            f["critical"] = True
            self._emit("CRITICAL_BATTERY", "CRITICAL", {"battery_pct": round(b, 2), "threshold": self.critical_battery}, 0)
        # hysteresis: re-arm only when comfortably above the threshold
        if f["low"] and b > self.low_battery + 5:
            f["low"] = False
        if f["critical"] and b > self.critical_battery + 5:
            f["critical"] = False

    def _detect_events(self, ttc: float | None) -> None:
        kmh = self.speed_mps * 3.6
        if self.last_decel > 6.0:
            self._emit("SUDDEN_BRAKING", "HIGH", {"deceleration_mps2": round(self.last_decel, 2),
                                                  "speed_kmh": round(kmh, 1)}, 15)
        if (self.obj is not None and self.obj.obj_type in ("obstacle", "road_barrier") and self.obj.distance_m < 20
                and kmh > 3 and self.sensor_status["lidar"] != "FAILED"):
            self._emit("OBSTACLE_DETECTED", "MEDIUM", {"object_type": self.obj.obj_type,
                                                       "distance_m": round(self.obj.distance_m, 2)}, 45)
        if kmh > ABNORMAL_SPEED_KMH:
            self._emit("ABNORMAL_SPEED", "HIGH", {"speed_kmh": round(kmh, 1),
                                                  "limit_kmh": CITY_SPEED_LIMIT_KMH}, 30)
        dev = self.route_deviation_m()
        if dev is not None and dev > ROUTE_DEVIATION_M:
            self._emit("ROUTE_DEVIATION", "HIGH", {"deviation_m": round(dev, 1), "route_id": self.route_id}, 60)

    # ---------------------------------------------------------------- route deviation
    def route_deviation_m(self) -> float | None:
        pr = self.planned_route
        if len(pr) < 2:
            return None
        lat0 = math.radians(self.lat)
        kx, ky = 111_320.0 * math.cos(lat0), 111_320.0

        def xy(p):
            return ((p[1] - self.lon) * kx, (p[0] - self.lat) * ky)

        best = float("inf")
        for a, b in zip(pr, pr[1:]):
            ax, ay = xy(a)
            bx, by = xy(b)
            dx, dy = bx - ax, by - ay
            L = dx * dx + dy * dy
            t = 0.0 if L == 0 else max(0.0, min(1.0, -(ax * dx + ay * dy) / L))
            px, py = ax + t * dx, ay + t * dy
            best = min(best, math.hypot(px, py))
        return best

    # ---------------------------------------------------------------- scenarios
    def inject(self, scenario: str) -> bool:
        """Deliberately create a situation. The resulting telemetry stays physically consistent
        and the event itself is raised by the normal on-board detection logic."""
        if scenario == "collision":
            if self.status in ("MAINTENANCE", "CHARGING", "EMERGENCY"):
                return False
            self.speed_mps = max(self.speed_mps, 40 / 3.6)
            if not self.route:
                self._start_trip()
            self.obj = TrackedObject("pedestrian", self.rng.uniform(10, 13), 0.0, 8.0, scripted=True)
        elif scenario == "obstacle":
            self.speed_mps = max(self.speed_mps, 30 / 3.6)
            if not self.route:
                self._reposition()
            # placed just beyond comfortable stopping distance: the AV should see it and stop smoothly
            d = self.speed_mps ** 2 / (2 * COMFORT_DECEL * 0.6) + 14.0
            self.obj = TrackedObject(self.rng.choice(["road_barrier", "obstacle"]), d, 0.0, 25.0, scripted=False)
        elif scenario == "sensor_failure":
            sensor = self.rng.choice(["lidar", "radar", "camera"])
            self.fail_sensor(sensor)
        elif scenario == "sensor_degraded":
            self.sensor_status[self.rng.choice(["lidar", "radar", "camera", "gps"])] = "DEGRADED"
        elif scenario == "abnormal_speed":
            self.speed_fault_until = self.sim_time + self.rng.uniform(6, 9)
            self.speed_fault_kmh = self.rng.uniform(125, 150)
            if not self.route:
                self._reposition()
        elif scenario == "route_deviation":
            if not self.route or self.status != "IN_SERVICE":
                self._start_trip()
            # wrong turn: insert a detour waypoint ~500 m perpendicular to the path
            detour = geo.destination(self.lat, self.lon, (self.heading + 90) % 360, 500)
            self.route.insert(self.route_index, detour)
        elif scenario == "low_battery":
            self.battery_pct = min(self.battery_pct, self.low_battery + 0.6)
            self.battery_flags["low"] = False
            self.drain_multiplier = max(self.drain_multiplier, 40.0)  # cross threshold within seconds
            if not self.route:
                self._start_trip()
        elif scenario == "critical_battery":
            self.battery_pct = min(self.battery_pct, self.critical_battery - 0.5)
            self.battery_flags["critical"] = False
        elif scenario == "emergency_stop":
            self.enter_emergency()
            self._emit("EMERGENCY_STOP", "CRITICAL", {"reason": "operator/scenario initiated"}, 0)
        else:
            return False
        return True

    def fail_sensor(self, sensor: str) -> None:
        if self.sensor_status.get(sensor) == "FAILED":
            return
        self.sensor_status[sensor] = "FAILED"
        self._emit("SENSOR_FAILURE", "HIGH", {"sensor": sensor}, 0)
        if sensor in ("lidar", "radar") and self.status not in ("MAINTENANCE",):
            # pull over safely then go to maintenance
            self.enter_emergency(duration=self.rng.uniform(10, 20), then="MAINTENANCE")

    # ---------------------------------------------------------------- telemetry
    def to_telemetry(self, now: float, mode: str) -> dict:
        self.seq += 1
        rng = self.rng
        s = self.sensor_status
        kmh = self.speed_mps * 3.6
        o = self.obj
        ttc = self.time_to_collision()
        lvl = self.traffic.level_at(self.lat, self.lon)

        # GPS: small noise, degraded => bigger noise, failed => dead reckoning (last estimate)
        gps_noise = {"OK": 2.5, "DEGRADED": 15.0, "FAILED": 0.0}[s["gps"]]
        nlat = self.lat + rng.gauss(0, gps_noise) / 111_320.0 if gps_noise else self.lat
        nlon = self.lon + rng.gauss(0, gps_noise) / (111_320.0 * math.cos(math.radians(self.lat))) if gps_noise else self.lon

        # LiDAR
        if s["lidar"] == "FAILED":
            lidar = {"status": "FAILED", "obstacle_detected": None, "distance_m": None, "object_count": None}
        else:
            noise = 0.05 if s["lidar"] == "OK" else 0.6
            detected = o is not None and o.distance_m <= 100
            dist = (o.distance_m if detected else rng.uniform(40, 100)) + rng.gauss(0, noise)
            lidar = {"status": s["lidar"], "obstacle_detected": detected, "distance_m": round(max(0.2, dist), 2),
                     "object_count": self.background_returns + (1 if detected else 0)}

        # Radar
        if s["radar"] == "FAILED":
            radar = {"status": "FAILED", "object_detected": None, "distance_m": None, "relative_speed_mps": None}
        else:
            noise = 0.3 if s["radar"] == "OK" else 2.0
            detected = o is not None and o.distance_m <= 150
            radar = {
                "status": s["radar"],
                "object_detected": detected,
                "distance_m": round(max(0.2, o.distance_m + rng.gauss(0, noise)), 2) if detected else None,
                "relative_speed_mps": round((o.ground_speed_mps - self.speed_mps) + rng.gauss(0, 0.1), 2) if detected else 0.0,
            }

        # Camera (metadata only - no video)
        if s["camera"] == "FAILED":
            camera = {"status": "FAILED", "object_detected": None, "object_type": None, "confidence": None}
        elif o is not None and o.distance_m <= 60:
            conf = 0.99 - o.distance_m / 250 + rng.gauss(0, 0.015)
            if s["camera"] == "DEGRADED":
                conf -= 0.3
            camera = {"status": s["camera"], "object_detected": True, "object_type": o.obj_type,
                      "confidence": round(max(0.05, min(0.999, conf)), 3)}
        elif rng.random() < 0.08:
            camera = {"status": s["camera"], "object_detected": True,
                      "object_type": rng.choice(CAMERA_ONLY_TYPES), "confidence": round(rng.uniform(0.8, 0.98), 3)}
        else:
            camera = {"status": s["camera"], "object_detected": False, "object_type": None, "confidence": 0.0}

        dev = self.route_deviation_m()
        return {
            "schema_version": SCHEMA_VERSION,
            "message_id": str(uuid.uuid4()),
            "vehicle_id": self.vehicle_id,
            "sequence": self.seq,
            "timestamp": utc_iso(now),
            "location": {
                "latitude": round(nlat, 6),
                "longitude": round(nlon, 6),
                "altitude_m": round(self.altitude_m, 1),
                "speed_kmh": round(kmh, 1),
                "heading": round(self.heading, 1),
                "accuracy_m": gps_noise or None,
                "gps_status": s["gps"],
                "fix": "DEAD_RECKONING" if s["gps"] == "FAILED" else "GNSS",
            },
            "battery": {
                "percentage": round(self.battery_pct, 2),
                "charging": self.charging,
                "estimated_range_km": round(self.battery_pct / BATTERY_PCT_PER_KM, 1),
            },
            "lidar": lidar,
            "radar": radar,
            "camera": camera,
            "safety": {
                "time_to_collision_s": round(ttc, 2) if ttc is not None else None,
                "emergency_braking": self.emergency_braking,
                "deceleration_mps2": round(max(0.0, self.last_decel), 2),
            },
            "vehicle": {
                "status": self.status,
                "activity": self.activity,
                "passenger_count": self.passengers,
                "trip_id": self.trip_id,
                "route_id": self.route_id,
                "route_deviation_m": round(dev, 1) if dev is not None else None,
                "odometer_km": round(self.odometer_km, 2),
            },
            "traffic": {
                "status": lvl,
                "congestion_index": TRAFFIC_INDEX[lvl],
            },
            "simulation": {"simulated": True, "mode": mode},
        }

    def build_event(self, ev: dict, now: float, mode: str) -> dict:
        return {
            "schema_version": SCHEMA_VERSION,
            "message_id": str(uuid.uuid4()),
            "event_id": f"evt-{uuid.uuid4().hex[:12]}",
            "vehicle_id": self.vehicle_id,
            "timestamp": utc_iso(now),
            "event_type": ev["event_type"],
            "severity": ev["severity"],
            "source": "VEHICLE",
            "location": {"latitude": round(self.lat, 6), "longitude": round(self.lon, 6)},
            "vehicle_status": self.status,
            "details": ev["details"],
            "simulation": {"simulated": True, "mode": mode},
        }
