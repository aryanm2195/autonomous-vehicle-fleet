# Testing and results

> **Software-only prototype.** The prototype uses software-generated telemetry to simulate a fleet of autonomous vehicles. This lets us evaluate the architecture, event-processing pipeline, scalability mechanisms, database design, security mechanisms and monitoring infrastructure without physical autonomous vehicles or sensor hardware. Every load and failure test here uses **virtual vehicles**.
>
> **Rule for this document:** every number was measured on a running system and comes from a file in `results/`. Nothing is estimated or extrapolated, and 100,000 vehicles were never tested. Where a component could not run, this document says so and does not guess.

## 1. Test levels

| Level | Tool | What it covers | Status |
|---|---|---|---|
| Unit | `pytest` in `simulator/`, `services/`, `backend/` (35 tests) | Vehicle physics and state machine, passenger rules, battery threshold events, sensor consistency, traffic model, schema validity of generated data, malformed/duplicate injection; telemetry persistence + Redis state + idempotent replay + Redis outage; alert rules, VEHICLE+PLATFORM merge, dedupe fallback; k-means, aggregation; JWT/RBAC, API read paths, geo search, WebSocket auth | **35/35 pass** |
| Integration (edge) | Simulator → real Mosquitto → real Node-RED | Auth + ACL, validation, DLQ routing, de-duplication, enrichment, latency stats, demo storyline | Run in the sandbox (§3) |
| Integration (full) | `docker compose up` | Complete pipeline including Kafka, MongoDB, Redis, API, dashboard, Grafana | **To run on the report machine** (§6) |
| Load / scalability | `scripts/loadtest.py` | Progressive virtual fleets, per-stage throughput, latency p50/p95/p99, errors, loss, Kafka lag, CPU/memory, DB performance | Edge tier measured in the sandbox (§4). Full pipeline: run `make loadtest` |
| Failure / resilience | `scripts/chaos.py` | 9 software failure scenarios | 3 measured in the sandbox (§5). 6 need the full stack |
| Security | Unit tests + manual | Login rejection, missing/invalid token → 401, viewer → 403 on write, WebSocket without token rejected, MQTT anonymous access denied, ACL write/read split | Unit-tested. Pen-testing (plan §11) not performed |

Run everything:
```bash
pip install -r scripts/requirements-tools.txt
make test
docker compose up -d --build && make loadtest && make chaos
```

## 2. Where the measurements in this repository come from

The stack was developed in a cloud sandbox (Linux, **2 vCPU, 7.8 GB RAM**). Its network policy blocked Docker Hub, the Apache/Maven mirrors and the MongoDB download servers, so **Kafka and MongoDB could not be installed there**. Mosquitto (Ubuntu package), Node-RED (npm) and Redis were available.

The sandbox measurements therefore cover the **edge tier only: simulator → Mosquitto → Node-RED**, with Node-RED's Kafka producer switched off (`KAFKA_ENABLED=false`; messages are counted and discarded after enrichment). They are real measurements of that tier. They are **not** full-pipeline results.

The simulator, broker and Node-RED all ran on the same 2 vCPUs, so they competed for CPU and the numbers are lower bounds.

## 3. Integration: demo storyline through real MQTT and Node-RED (sandbox)

`MODE=demo`, 20 virtual vehicles with one MQTT session each, 110 s run, plus 25 malformed and 15 duplicate messages injected through the control API:

- **Scripted scenarios:** all six produced their on-board events: `LOW_BATTERY` (vehicle-00003), `COLLISION_WARNING` + `SUDDEN_BRAKING` (vehicle-00007), `SENSOR_FAILURE` (vehicle-00011), `ROUTE_DEVIATION` (vehicle-00015), `ABNORMAL_SPEED` (vehicle-00018), and an obstacle for vehicle-00005.
- **Node-RED counts:** received 2,195 telemetry and 11 event messages; forwarded 2,155 telemetry and 11 events.
- **Edge filtering:** rejected **25 of 25** malformed frames (invalid JSON, bad speed, bad vehicle id, bad coordinates) and dropped **15 of 15** duplicates.

A separate sandbox-only harness fed the live MQTT stream directly into the real telemetry, alert and analytics service code, with real Redis and in-memory MongoDB stand-ins, behind the real API and dashboard. It was used to check the API and dashboard code paths. It found and fixed two bugs (a missing WebSocket library in the non-docker environment, and naive UTC timestamps shown as local time). **It is not used for any reported performance figure** because it bypasses Kafka.

## 4. Load test: edge tier (sandbox, measured)

Source: `results/loadtest-20260927T100621Z-sandbox-edge.md` / `.json`.
Command: `scripts/loadtest.py --edge-only --stages 10,100,1000,5000,7500,10000 --duration 60 --warmup 15 --drain 90`.
Each stage runs a 60 s measurement window after a 15 s warm-up, with 1 msg/s per vehicle (about 1.0 KB each).

| Virtual vehicles | Target msg/s | Generated msg/s | Node-RED msg/s | Edge latency p50 | p95 | p99 | Not delivered (whole stage) |
|---|---|---|---|---|---|---|---|
| 10 | 10 | 10.0 | 10.0 | 3 ms | 5 ms | 6 ms | 0 of 750 |
| 100 | 100 | 100.0 | 100.0 | 4 ms | 6 ms | 8 ms | 0 of 7,500 |
| 1,000 | 1,000 | 999.9 | 999.9 | 3 ms | 6 ms | 7 ms | 0 of 75,000 |
| 5,000 | 5,000 | 5,000.0 | 4,999.1 | 10 ms | 18 ms | 43 ms | 0 of 375,000 |
| 7,500 | 7,500 | 7,499.2 | 7,520.8 | 855 ms | 975 ms | 1,020 ms | 0 of 562,500 |
| 10,000 | 10,000 | – (see note) | 6,565.2 | 19,254 ms | 19,393 ms | 19,459 ms | **163,929 of 750,000 (21.86 %)** |

Resource usage (average % of one core / average RSS):

| Vehicles | Mosquitto | Node-RED | Simulator |
|---|---|---|---|
| 1,000 | 1.5 % / 8 MB | 16.0 % / 232 MB | 14.1 % / 37 MB |
| 5,000 | 7.0 % / 8 MB | 63.7 % / 286 MB | 51.8 % / 64 MB |
| 7,500 | 8.2 % / 9 MB | 89.3 % / 1,169 MB | 74.9 % / 111 MB |
| 10,000 | 8.7 % / 112 MB | 86.3 % / 2,995 MB | 93.8 % / 126 MB |

What the table shows:

1. **Up to 5,000 virtual vehicles (5,000 msg/s), the edge tier keeps up with no loss.** At 5,000, p95 edge latency is 18 ms and p99 is 43 ms, well inside the plan's 200 ms target for standard telemetry. (That target is end-to-end, so the full pipeline still has to be measured.)
2. **Between 5,000 and 7,500 msg/s, a single Node-RED instance saturates on this hardware.** At 7,500 every message still arrived, but queueing pushed latency to about 1 s and Node-RED memory to about 1.2 GB. Simulator and Node-RED together used about 1.65 of the 2 cores.
3. **At 10,000 msg/s the tier is overloaded.** Node-RED plateaued at about 6,565 msg/s and latency grew to about 19 s. A diagnosis re-run (`results/loadtest-20260927T101832Z-overload-diagnosis.md`) with broker connection logging turned on showed the mechanism: Mosquitto logged `Outgoing messages are being dropped for client nodered…` once the subscriber's queue hit `max_queued_messages` (100,000). The saturated Node-RED client then closed and re-opened its connection. QoS 0 telemetry is shed at the broker so the broker's memory stays bounded, while events use QoS 1. The diagnosis run lost 39.48 %.
4. **The simulator is not the bottleneck up to 7,500.** Its schedule lag stayed at 0.0 s and there were no publish errors. At 10,000 its metrics endpoint stopped answering under CPU saturation, so that stage's generation rate is reported as unknown. The total generated (750,000) comes from the simulator's own exit statistics.

**Scaling conclusion (measured):** one Node-RED instance on one shared vCPU handles about 5,000 msg/s cleanly. To go further the tier must scale out horizontally. That is why the design uses MQTT shared subscriptions and a Node-RED HPA.

### 4.1 Horizontal scale-out check (sandbox, measured)

Source: `results/shared-subscription-sandbox.md`. Two Node-RED replicas shared `$share/nodered/fleet/+/telemetry` while 1,000 virtual vehicles ran for 30 s:

- The simulator generated 30,000 messages.
- Replica 1 received 15,000 and replica 2 received 15,000, with nothing lost and nothing duplicated.
- p95 edge latency was 6 ms on both replicas.

This proves the scale-out mechanism works without any configuration change. A throughput gain from replicas can't be shown on a 2-vCPU box where the CPU is already shared. Repeat the 10,000-vehicle stage on a machine with more cores and two or more replicas.

The same test exposed a design caveat. Mosquitto distributes shared-subscription messages **per message**, not per vehicle, so edge de-duplication and sequence tracking become per-replica. In a chaos run with two replicas (`results/chaos-20260927T102446Z-two-replicas.json`), only 1 of 20 duplicates was caught at the edge. The MongoDB unique `message_id` index is the backstop. A sticky shared-subscription strategy (EMQX `hash_clientid`) is the production fix.

### 4.2 Tuning found by measurement

Source: `results/tuning-log-sandbox.md`. At 200 vehicles, p99 edge latency was **48 ms before and 8 ms after** turning off Nagle's algorithm. The fix was `set_tcp_nodelay true` in Mosquitto plus `TCP_NODELAY` on the simulator's sockets. Nagle's algorithm was interacting with TCP delayed ACKs on the small MQTT frames. Both settings are now the defaults.

## 5. Failure simulation

| Scenario | How it is injected | Expected behaviour | Result |
|---|---|---|---|
| Malformed telemetry | `POST /failure {"failure":"malformed","count":20}` | Rejected by Node-RED, reason counted, sent to `vehicle.dlq` | **Measured (sandbox):** 20/20 rejected: `invalid_json` 9, `bad_speed` 5, `bad_battery` 2, `bad_vehicle_id` 2, `missing_location` 2. The breakdown is the difference between cumulative counters; the JSON file stores the cumulative `reasons_after`. `results/chaos-20260927T102546Z.json` |
| Duplicate messages | `POST /failure {"failure":"duplicate","count":20}` | Dropped by Node-RED de-duplication; MongoDB unique index as backstop | **Measured (sandbox, 1 replica):** 20/20 dropped. With 2 replicas: 1/20 at the edge (see §4.1) |
| MQTT disconnect | `POST /failure {"failure":"mqtt_disconnect","seconds":15}` | Simulator buffers while offline, replays on reconnect, nothing lost | **Measured (sandbox, 100 vehicles):** 740 buffered at mid-outage, 1,520 flushed after reconnect, 0 dropped. Node-RED received 2,300 messages in the 23 s observation window (15 s outage + 8 s), which matches 100 msg/s × 23 s |
| Sensor failure | `POST /scenario {"scenario":"sensor_failure"}` | `SENSOR_FAILURE` event, platform rule alert, vehicle → EMERGENCY → MAINTENANCE | Event generation verified in the sandbox (§3) and by unit tests. Alert path: run `chaos.py sensor_failure` on the full stack |
| Abnormal behaviour | `POST /scenario {"scenario":"abnormal_speed"}` | `ABNORMAL_SPEED` from vehicle and platform, merged | As above |
| Kafka consumer failure | `docker compose stop telemetry-service` for 30 s | Lag grows, replay from committed offsets after restart, no loss | **To run:** `chaos.py kafka_consumer_failure` (needs Kafka) |
| Database temporarily unavailable | `docker compose stop mongodb` for 20 s | telemetry-service retries without committing, lag grows then drains, no loss | **To run:** `chaos.py database_unavailable`. Retry logic unit-tested |
| Redis unavailable | `docker compose stop redis` for 20 s | Circuit breaker opens, MongoDB keeps accepting writes, API summary source becomes `mongodb-fallback` | **To run:** `chaos.py redis_unavailable`. Degraded mode unit-tested |
| Microservice crash | SIGKILL to alert-service PID 1 | Restart policy brings it back, consumer group resumes | **To run:** `chaos.py microservice_crash` |

## 6. Full-pipeline test procedure (report machine)

1. `cp .env.example .env && docker compose up -d --build`. Wait until `docker compose ps` shows everything healthy. The dashboard should show live data.
2. `docker compose stop simulator`. The harness starts its own simulator processes so it can size them per stage.
3. `python scripts/loadtest.py --stages 10,100,1000,5000,10000 --duration 60 --docker-stats`
4. `docker compose start simulator && python scripts/chaos.py sensor_failure abnormal_behaviour malformed duplicate mqtt_disconnect kafka_consumer_failure database_unavailable redis_unavailable microservice_crash`
5. Copy the tables from `results/loadtest-*.md` and the values from `results/chaos-*.json` into the report. Screenshot the Grafana dashboard and the web dashboard **from your own run**.

The full-pipeline report adds these columns automatically:
- Kafka-acked msg/s, MongoDB inserts/s
- E2E p50/p95/p99 (vehicle timestamp → MongoDB)
- Kafka-to-service latency
- Alert E2E (all and CRITICAL)
- Max Kafka lag per consumer group, and lag after drain
- MongoDB `insert_many` batch p50/p95 and opcounter inserts/s
- Mongo retries, Redis errors
- **Delivery accounting**: generated vs stored after drain

## 7. Requirement traceability (plan §4.2 / §11)

| Requirement | How it is measured | Status |
|---|---|---|
| p95 < 200 ms for standard telemetry | `telemetry_e2e_latency` (vehicle → MongoDB), loadtest column "E2E p95" | Edge tier alone: 18 ms p95 at 5,000 vehicles. **End-to-end not yet measured** |
| p95 < 50 ms for critical payloads | alert-service `alerts_e2e_latency{severity="CRITICAL"}`, loadtest "alert_e2e_critical_ms", dashboard KPI | **Not yet measured end-to-end** |
| Error rate < 0.1 % | Loadtest errors plus delivery accounting (lost ÷ generated) | Edge tier: 0 % up to 7,500 vehicles; 21.86 % at 10,000 (overload, §4) |
| Kafka queue depth < 1,000 per partition | `kafka_consumergroup_lag` (Grafana), loadtest "max lag" | **Not yet measured** (needs Kafka) |
| HPA triggers at 70 % CPU | `k8s/*` HPA definitions | Defined. **Not executed** (no cluster) |
| 99 % availability | – | Not measurable in a short prototype test |
| 100,000 vehicles | – | **Not tested.** Scaling path argued in ARCHITECTURE.md §7 |
