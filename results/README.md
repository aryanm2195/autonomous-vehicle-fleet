# Measured results

Only measured output goes in this folder. Nothing here is estimated or edited by hand.
(Markdown tables are rendered from the JSON by `scripts/loadtest.py`.)

| File | What | Where it was measured |
|---|---|---|
| `loadtest-20260927T100621Z-sandbox-edge.*` | Progressive load 10 → 10,000 virtual vehicles, **edge tier only** (simulator → Mosquitto → Node-RED, Kafka producer disabled) | Cloud sandbox, 2 vCPU / 7.8 GB, every component on the same host |
| `loadtest-20260927T101832Z-overload-diagnosis.*` | Re-run of the 10,000 stage with broker connection logging, to explain the loss | Same |
| `shared-subscription-sandbox.md` | Two Node-RED replicas behind `$share/...`, 1,000 vehicles | Same |
| `chaos-20260927T102546Z.json` | malformed / duplicate / MQTT-disconnect scenarios, 1 Node-RED replica | Same |
| `chaos-20260927T102446Z-two-replicas.json` | Same scenarios while 2 replicas shared the subscription (shows the per-replica de-dup caveat) | Same |
| `tuning-log-sandbox.md` | Before/after latency for the TCP_NODELAY fix | Same |

Kafka and MongoDB could not be installed in the sandbox (network policy), so **no full-pipeline results are in this folder yet**. Running `make loadtest` and `make chaos` against the docker compose stack adds `loadtest-<timestamp>.md/.json` and `chaos-<timestamp>.json` files with Kafka lag, MongoDB throughput, end-to-end latency and delivery accounting.
