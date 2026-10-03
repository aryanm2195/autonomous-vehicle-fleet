"""Prometheus metrics exposed by the simulator (GET /metrics on SIM_HTTP_PORT)."""
from prometheus_client import Counter, Gauge, Histogram

MESSAGES = Counter("sim_messages_published_total", "MQTT messages handed to the client", ["kind"])
PUBLISH_ERRORS = Counter("sim_publish_errors_total", "Publish calls that failed")
BUFFERED = Gauge("sim_offline_buffer_messages", "Messages held in the store-and-forward buffer")
BUFFER_DROPPED = Counter("sim_offline_buffer_dropped_total", "Messages dropped because the offline buffer was full")
BUFFER_FLUSHED = Counter("sim_offline_buffer_flushed_total", "Buffered messages re-sent after reconnect")
EVENTS = Counter("sim_events_generated_total", "On-board events generated", ["event_type", "severity"])
SCENARIOS = Counter("sim_scenarios_injected_total", "Deliberate scenarios injected", ["scenario", "source"])
FAILURES = Counter("sim_failures_injected_total", "Software failure injections", ["failure"])
VEHICLES = Gauge("sim_vehicles_active", "Virtual vehicles being simulated")
STATUS = Gauge("sim_vehicle_status", "Vehicles per status", ["status"])
CONNECTED = Gauge("sim_mqtt_connections_up", "MQTT connections currently connected")
TICK = Histogram("sim_cycle_seconds", "Wall time to update+publish one slot of vehicles",
                 buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2))
LAG = Gauge("sim_schedule_lag_seconds", "How far the simulator is behind its telemetry schedule")
