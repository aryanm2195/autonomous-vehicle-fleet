"""Zone-based traffic model.

Traffic is a property of the *road network*, not of an individual vehicle, so
the city is divided into ~1 km grid cells. Each cell holds a traffic level that
changes periodically (configurable probability) and only ever moves one step at
a time (LOW <-> MODERATE <-> HIGH <-> CONGESTED). Vehicles in the same cell
therefore experience the same traffic and it evolves smoothly.
"""
from __future__ import annotations

import math
import random

TRAFFIC_LEVELS = ["LOW", "MODERATE", "HIGH", "CONGESTED"]

# (min, max) cruising speed in km/h for each traffic level
TRAFFIC_SPEED_KMH = {
    "LOW": (45.0, 60.0),
    "MODERATE": (30.0, 45.0),
    "HIGH": (15.0, 30.0),
    "CONGESTED": (3.0, 12.0),
}

# Probability per second that a new object/hazard appears in front of a vehicle
TRAFFIC_HAZARD_RATE = {"LOW": 0.010, "MODERATE": 0.025, "HIGH": 0.05, "CONGESTED": 0.08}

# Congestion index reported in telemetry (0 = free flow, 1 = gridlock)
TRAFFIC_INDEX = {"LOW": 0.15, "MODERATE": 0.4, "HIGH": 0.7, "CONGESTED": 0.92}


class TrafficModel:
    def __init__(self, center_lat: float, rng: random.Random, cell_m: float = 1000.0,
                 change_probability: float = 0.02):
        self.rng = rng
        self.cell_deg_lat = cell_m / 111_320.0
        self.cell_deg_lon = cell_m / (111_320.0 * math.cos(math.radians(center_lat)))
        self.change_probability = change_probability
        self.cells: dict[tuple[int, int], int] = {}

    def _key(self, lat: float, lon: float) -> tuple[int, int]:
        return int(math.floor(lat / self.cell_deg_lat)), int(math.floor(lon / self.cell_deg_lon))

    def level_at(self, lat: float, lon: float) -> str:
        key = self._key(lat, lon)
        idx = self.cells.get(key)
        if idx is None:
            # CBD-ish weighting: most cells start LOW/MODERATE
            idx = self.rng.choices([0, 1, 2, 3], weights=[35, 40, 18, 7])[0]
            self.cells[key] = idx
        return TRAFFIC_LEVELS[idx]

    def step(self, dt: float) -> None:
        """Evolve every known cell; called once per simulation tick."""
        p = min(1.0, self.change_probability * dt)
        for key, idx in self.cells.items():
            if self.rng.random() < p:
                # drift one level, slight bias back toward MODERATE
                if idx == 0:
                    idx = 1
                elif idx == 3:
                    idx = 2
                else:
                    idx += self.rng.choice([-1, 1]) if idx != 2 else self.rng.choice([-1, -1, 1])
                self.cells[key] = idx

    def snapshot(self) -> dict[str, int]:
        out = {lvl: 0 for lvl in TRAFFIC_LEVELS}
        for idx in self.cells.values():
            out[TRAFFIC_LEVELS[idx]] += 1
        return out
