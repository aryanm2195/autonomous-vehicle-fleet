"""Simulator configuration.

Every tunable is read from environment variables so the simulator never needs a
code change between a 20-vehicle lecture demo and a 10,000-vehicle load test.

MODE presets
------------
* ``demo``      – small fleet (default 20), per-vehicle MQTT connections, and a
                  scripted timeline that guarantees a low-battery event, an
                  emergency, a sensor anomaly and a route deviation.
* ``normal``    – steady-state simulation using the configured values.
* ``load_test`` – larger fleet, faster telemetry, shared (gateway) MQTT
                  connections, events at the configured rate.

Explicit environment variables always win over the preset defaults.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default, cast=str):
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    if cast is bool:
        return raw.strip().lower() in ("1", "true", "yes", "on")
    return cast(raw)


MODE_PRESETS = {
    "demo": {
        "VEHICLE_COUNT": 20,
        "TELEMETRY_INTERVAL": 1.0,
        "EMERGENCY_EVENT_RATE": 0.0,  # demo events are scripted, not random
        "MQTT_CONNECTION_MODE": "per_vehicle",
        "DEMO_SCRIPT": True,
    },
    "normal": {
        "VEHICLE_COUNT": 100,
        "TELEMETRY_INTERVAL": 1.0,
        "EMERGENCY_EVENT_RATE": 0.0005,
        "MQTT_CONNECTION_MODE": "shared",
        "DEMO_SCRIPT": False,
    },
    "load_test": {
        "VEHICLE_COUNT": 1000,
        "TELEMETRY_INTERVAL": 1.0,
        "EMERGENCY_EVENT_RATE": 0.0001,
        "MQTT_CONNECTION_MODE": "shared",
        "DEMO_SCRIPT": False,
    },
}


@dataclass
class Config:
    mode: str = "demo"
    vehicle_count: int = 20
    vehicle_id_offset: int = 0
    telemetry_interval: float = 1.0
    mqtt_broker: str = "localhost"
    mqtt_port: int = 1883
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    mqtt_tls: bool = False
    mqtt_ca_cert: str | None = None
    mqtt_topic_prefix: str = "fleet"
    mqtt_connection_mode: str = "shared"  # shared | per_vehicle
    vehicles_per_connection: int = 250
    mqtt_telemetry_qos: int = 0
    mqtt_event_qos: int = 1
    offline_buffer_size: int = 5000
    kafka_broker: str = "localhost:9092"  # not used by the simulator itself; kept for a single shared .env
    mongodb_uri: str = "mongodb://localhost:27017"
    redis_uri: str = "redis://localhost:6379/0"
    emergency_event_rate: float = 0.0005
    low_battery_threshold: float = 20.0
    critical_battery_threshold: float = 10.0
    battery_drain_multiplier: float = 1.0
    traffic_change_probability: float = 0.02
    failure_rate: float = 0.0  # probability per vehicle per tick of a software failure scenario
    duplicate_rate: float = 0.0
    malformed_rate: float = 0.0
    demo_script: bool = True
    duration_seconds: float = 0.0  # 0 = run forever
    random_seed: int | None = None
    start_lat: float = -37.8136
    start_lon: float = 144.9631
    area_radius_km: float = 6.0
    http_port: int = 9105  # /metrics, /health, /status and the scenario control API
    log_level: str = "INFO"
    extra: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "Config":
        mode = _env("MODE", "demo").lower()
        if mode not in MODE_PRESETS:
            raise ValueError(f"MODE must be one of {sorted(MODE_PRESETS)}, got {mode!r}")
        p = MODE_PRESETS[mode]
        seed = os.getenv("RANDOM_SEED")
        return cls(
            mode=mode,
            vehicle_count=_env("VEHICLE_COUNT", p["VEHICLE_COUNT"], int),
            vehicle_id_offset=_env("VEHICLE_ID_OFFSET", 0, int),
            telemetry_interval=_env("TELEMETRY_INTERVAL", p["TELEMETRY_INTERVAL"], float),
            mqtt_broker=_env("MQTT_BROKER", "localhost"),
            mqtt_port=_env("MQTT_PORT", 1883, int),
            mqtt_username=_env("MQTT_USERNAME", None),
            mqtt_password=_env("MQTT_PASSWORD", None),
            mqtt_tls=_env("MQTT_TLS", False, bool),
            mqtt_ca_cert=_env("MQTT_CA_CERT", None),
            mqtt_topic_prefix=_env("MQTT_TOPIC_PREFIX", "fleet"),
            mqtt_connection_mode=_env("MQTT_CONNECTION_MODE", p["MQTT_CONNECTION_MODE"]),
            vehicles_per_connection=_env("VEHICLES_PER_CONNECTION", 250, int),
            mqtt_telemetry_qos=_env("MQTT_TELEMETRY_QOS", 0, int),
            mqtt_event_qos=_env("MQTT_EVENT_QOS", 1, int),
            offline_buffer_size=_env("OFFLINE_BUFFER_SIZE", 5000, int),
            kafka_broker=_env("KAFKA_BROKER", "localhost:9092"),
            mongodb_uri=_env("MONGODB_URI", "mongodb://localhost:27017"),
            redis_uri=_env("REDIS_URI", "redis://localhost:6379/0"),
            emergency_event_rate=_env("EMERGENCY_EVENT_RATE", p["EMERGENCY_EVENT_RATE"], float),
            low_battery_threshold=_env("LOW_BATTERY_THRESHOLD", 20.0, float),
            critical_battery_threshold=_env("CRITICAL_BATTERY_THRESHOLD", 10.0, float),
            battery_drain_multiplier=_env("BATTERY_DRAIN_MULTIPLIER", 1.0, float),
            traffic_change_probability=_env("TRAFFIC_CHANGE_PROBABILITY", 0.02, float),
            failure_rate=_env("FAILURE_RATE", 0.0, float),
            duplicate_rate=_env("DUPLICATE_RATE", 0.0, float),
            malformed_rate=_env("MALFORMED_RATE", 0.0, float),
            demo_script=_env("DEMO_SCRIPT", p["DEMO_SCRIPT"], bool),
            duration_seconds=_env("DURATION_SECONDS", 0.0, float),
            random_seed=int(seed) if seed else None,
            start_lat=_env("START_LAT", -37.8136, float),
            start_lon=_env("START_LON", 144.9631, float),
            area_radius_km=_env("AREA_RADIUS_KM", 6.0, float),
            http_port=_env("SIM_HTTP_PORT", 9105, int),
            log_level=_env("LOG_LEVEL", "INFO").upper(),
        )

    def vehicle_ids(self) -> list[str]:
        return [f"vehicle-{i:05d}" for i in range(self.vehicle_id_offset + 1,
                                                        self.vehicle_id_offset + self.vehicle_count + 1)]
