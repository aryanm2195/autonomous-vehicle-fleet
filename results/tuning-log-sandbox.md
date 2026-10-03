# Tuning log (edge tier, measured in the cloud sandbox)

Scope: simulator -> Mosquitto -> Node-RED only (Kafka/MongoDB could not be installed in the
sandbox, so Node-RED ran with `KAFKA_ENABLED=false`). 200 virtual vehicles at 1 msg/s each,
one 10-second measurement window per row after a 5-second warm-up
(`scripts/loadtest.py --edge-only --stages 200 --duration 10 --warmup 5`).
Environment: Linux container, 2 vCPU, 7.8 GB RAM.

| Change | Edge latency p50 | p95 | p99 |
|---|---|---|---|
| Baseline (defaults) | 4 ms | 46 ms | 48 ms |
| + `set_tcp_nodelay true` in mosquitto.conf | 4 ms | 6 ms | 46 ms |
| + `TCP_NODELAY` on the simulator's MQTT sockets | 4 ms | 7 ms | 8 ms |

Interpretation: the ~40 ms tail was Nagle's algorithm interacting with TCP delayed ACKs on
small MQTT frames, first on the broker -> subscriber hop and then on the publisher -> broker hop.
Both settings are now the defaults in this repository.
These are short single-window measurements; re-run on your own hardware for the report.
