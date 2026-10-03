# Architecture

> **Software-only prototype.** The prototype uses software-generated telemetry to simulate a fleet of autonomous vehicles. This lets us evaluate the architecture, event-processing pipeline, scalability mechanisms, database design, security mechanisms and monitoring infrastructure without physical autonomous vehicles or sensor hardware.
> In this document, *vehicle* means a **virtual vehicle** and *sensor* means a **software-simulated sensor**.

## 1. Mapping the project plan to the implementation

| Project plan element | Implementation | Status |
|---|---|---|
| Autonomous fleet with GPS, LiDAR, radar, camera, battery (§1, §9) | `simulator/`: stateful virtual vehicles; LiDAR/radar/camera as consistent summaries, camera as metadata only | Implemented (software) |
| MQTT broker (§5.1) | Eclipse Mosquitto 2 with password auth and topic ACLs | Implemented |
| Node-RED edge filtering: subscribers, duplicate removal, bad-reading removal, enrichment (§7) | `nodered/flows.json`: validate → de-duplicate → filter/enrich → batched Kafka producer, DLQ | Implemented |
| Kafka bus (§5.1, §5.2) | Apache Kafka 3.8 (KRaft), 4 topics, key = vehicle_id | Implemented (single broker locally) |
| Microservices as independent consumers (§5.2, §8) | telemetry-service, alert-service (the plan's *Emergency* + *AI detection* role, rule-based), analytics-service | Implemented (3 of the 10 services in §8) |
| Charging, Maintenance, Routing, Billing, Fleet, Notification, AI Detection (CNN), Authentication services (§8) | Authentication lives in the backend API (JWT + RBAC). `vehicle.alerts` is the hand-off point for notification, charging and maintenance consumers | Not implemented as separate services (see LIMITATIONS) |
| MongoDB + Redis + S3, polyglot persistence (§6.2, §10) | MongoDB (history, alerts, snapshots) + Redis (latest state, geo, pub/sub). S3 described as the archive tier | MongoDB/Redis implemented. S3 is design only |
| Event sourcing (§6.2) | Every telemetry message is kept (append-only `telemetry` collection + Kafka log). `vehicle_state` is a derived projection | Implemented |
| Geospatial index, hashed sharding, time partitioning (§10.1) | `2dsphere` on `telemetry.geo` and `vehicle_state.geo`, Redis GEO, TTL index on `ts`, `scripts/mongo-sharding.js` | Indexes and TTL implemented. Sharding scripted, not run |
| API Gateway / ELB (§10.2) | FastAPI backend (single entry point for the UI). ALB Ingress in `k8s/` | Backend implemented. ALB is design only |
| JWT, RBAC, TLS, secrets (§10.3) | JWT (HS256) + 3 roles, bcrypt, MQTT ACLs, TLS/mTLS config + cert script, least-privilege Mongo user, secrets via env / K8s Secret | Implemented, except TLS off by default locally |
| K-means fleet allocation (§10.4) | analytics-service k-means hotspots over live positions | Implemented |
| Kalman filter, A*, CNN/LSTM (§10.4) | – | Not implemented (out of scope for the pipeline prototype) |
| Kubernetes HPA at 70 % CPU, EKS (§6.2, §10.2, §11) | `k8s/`: Deployments, HPA (70 %), PDBs, KEDA Kafka-lag scaler, sharded simulator StatefulSet | Manifests written, not executed |
| CloudWatch monitoring (§10.2) | Prometheus + Grafana (local equivalent) plus exporters for Kafka, Redis and MongoDB | Implemented |
| Testing strategy (§11) | Unit tests, load-test harness, chaos runner | Implemented. See TESTING.md |

**Deviation from the plan's diagram.** The plan places an "API Gateway" between MQTT and Kafka. Here Node-RED takes that position, which matches the plan's own §5.3 and §7. The API gateway role is played by the backend API on the consumer side.

## 2. Data flow

```
Virtual vehicle state ──step(dt)──► telemetry JSON (≈1.0 KB) ──MQTT QoS0──► Mosquitto
                     └─on-board detection─► event JSON ────────MQTT QoS1──┘
Mosquitto ──$share/nodered/fleet/+/telemetry──► Node-RED replica(s)
   Node-RED: parse+validate ─(invalid)─► vehicle.dlq
             de-duplicate (message_id) → filter/enrich (ingest time, edge latency, priority, speed_mps)
             → micro-batch (≤500 msgs / 20 ms) → Kafka (acks=all)
Kafka vehicle.telemetry (key=vehicle_id ⇒ per-vehicle ordering)
   ├─ group telemetry-service → MongoDB telemetry (insert_many, unique message_id) → Redis vehicle:{id}, fleet:geo
   ├─ group alert-service     → rules → dedupe (Redis SET NX EX) → MongoDB alerts → Redis alerts:live → Kafka vehicle.alerts
   └─ group analytics-service → 5 s windows → Redis analytics:* → MongoDB analytics_snapshots
Kafka vehicle.events → alert-service (processed before telemetry in each poll)
Backend API ← Redis (hot) / MongoDB (history, fallback) ── REST + WebSocket ──► Dashboard
```

### Emergency event path (what the demo shows)

1. The simulator injects a pedestrian 10–13 m ahead of `vehicle-00007` at 40 km/h.
2. On the next tick the vehicle's planner computes the deceleration it needs. That is more than 4.5 m/s², so autonomous emergency braking engages at 8 m/s² and a `COLLISION_WARNING` (CRITICAL) plus a `SUDDEN_BRAKING` (HIGH) event are published on `fleet/vehicle-00007/events` at QoS 1. The status becomes `EMERGENCY`.
3. Node-RED validates the event, de-duplicates it, stamps `ingest.received_at` and `edge_latency_ms`, and produces it to `vehicle.events`.
4. alert-service consumes it, claims the dedupe key in Redis, inserts the alert into MongoDB, publishes it on `alerts:live` and `vehicle.alerts`, and records `pipeline_latency_ms` (vehicle timestamp → alert stored).
5. At the same time, the telemetry for that tick (`safety.emergency_braking=true`, `time_to_collision_s<2`) reaches the alert-service rule engine. That detection is **merged** into the same alert (`sources: ["VEHICLE","PLATFORM"]`) instead of creating a duplicate.
6. The backend WebSocket relays `alerts:live` to the dashboard, which flashes the alert and shows its measured end-to-end latency.

## 3. Components

### 3.1 Virtual vehicle simulator (`simulator/fleetsim`)
- **Concurrency:** one asyncio process. The telemetry interval is split into up to 50 slots and vehicles are spread across the slots, so load is smooth rather than bursty. No thread or process per vehicle. Measured cost of the vehicle model: about 38 µs per vehicle-update on a 2 vCPU sandbox (update + JSON, no network).
- **Connection models:** `per_vehicle` (each vehicle is its own MQTT client with an LWT on `fleet/{id}/status`) or `shared` (gateway sessions of `VEHICLES_PER_CONNECTION` vehicles).
- **Store-and-forward:** a bounded offline buffer per MQTT session, flushed on reconnect.
- **Horizontal scaling:** `VEHICLE_ID_OFFSET` shards the ID space, so N processes or pods simulate disjoint ranges (`vehicle-00001…`, `vehicle-05001…`).
- **Control API** (`:9105`): `GET /status`, `GET /metrics`, `POST /scenario {vehicle_id, scenario}`, `POST /failure {failure, seconds|count}`.

#### Vehicle status state machine

```
             trip assigned                      arrived
 AVAILABLE ───────────────► IN_SERVICE ─────────────────► AVAILABLE
   │  ▲  │                     │ battery < critical
   │  │  └──► IDLE ──┐         ▼
   │  └──────────────┘      CHARGING ──(≥90 %)──► AVAILABLE
   │ battery < low            ▲
   └──────────────────────────┘
 any ──critical event──► EMERGENCY ──(20–40 s)──► previous state | MAINTENANCE (sensor failure)
 MAINTENANCE ──(60–150 s, sensors repaired)──► AVAILABLE
```

Minimum dwell times (15–45 s) stop the status from flapping. Transitions are validated in `vehicle.TRANSITIONS` and checked by unit tests.

### 3.2 Mosquitto
Anonymous access is disabled and a password file plus ACLs are required. `simulator` may only publish `fleet/#`, `nodered` may only subscribe. `set_tcp_nodelay true` is enabled. It removed a measured ~40 ms latency tail (see `results/tuning-log-sandbox.md`). A TLS/mTLS listener is ready in the config.

### 3.3 Node-RED
Source functions live in `nodered/functions/*.js` and are assembled by `nodered/build_flows.py`.
- **Validation:** hand-written checks that mirror the JSON Schema (types, ranges, enums, topic/vehicle match, clock skew). Full JSON-Schema validation per message would be too slow at thousands of msg/s in one Node.js thread.
- **De-duplication:** two rotating `Set` generations of message IDs (bounded memory). Sequence gaps and reordering are counted.
- **Enrichment:** `ingest.received_at`, `ingest.edge_latency_ms`, `ingest.priority`, `location.speed_mps`.
- **Kafka producer:** kafkajs `sendBatch`, `acks=-1`, micro-batching, a bounded retry buffer while Kafka is down.
- **Observability:** `GET /metrics` (Prometheus) and `GET /stats` (JSON with p50/p95/p99 edge latency).
- **Scaling:** MQTT v5 shared subscriptions (`$share/nodered/...`), so extra replicas split the load with no configuration change.

### 3.4 Kafka
| Topic | Partitions (local) | Key | Retention | Producers → consumers |
|---|---|---|---|---|
| `vehicle.telemetry` | 6 | vehicle_id | 24 h | Node-RED → telemetry, alert, analytics |
| `vehicle.events` | 3 | vehicle_id | 7 d | Node-RED → alert |
| `vehicle.alerts` | 3 | vehicle_id | default | alert-service → future notification/charging/maintenance services |
| `vehicle.dlq` | 1 | vehicle_id | 7 d | Node-RED (rejected messages with reason) |

Consumers disable auto-commit and commit only after processing. That gives **at-least-once** delivery, and idempotent writes (unique `message_id`) make replays harmless.

### 3.5 Microservices
| Service | Consumes | Writes | Notes |
|---|---|---|---|
| telemetry-service | `vehicle.telemetry` | MongoDB `telemetry`, `vehicle_state` (every 15 s); Redis `vehicle:{id}`, `fleet:geo`, `fleet:last_seen` | Retries MongoDB forever with back-off (no commit until stored). Redis behind a circuit breaker |
| alert-service | `vehicle.events`, `vehicle.telemetry` | MongoDB `alerts`; Redis `alerts:recent`, `alerts:live`, `alerts:counts`; Kafka `vehicle.alerts` | Vehicle events plus platform rules (battery, TTC + braking, sensor FAILED, >100 km/h, >150 m deviation). Redis `SET NX EX` dedupe with an in-memory fallback |
| analytics-service | `vehicle.telemetry` | Redis `analytics:summary`, `analytics:hotspots`, `analytics:throughput`; MongoDB `analytics_snapshots` | 5 s windows, k-means (k=5) demand hotspots |

All three expose `/health`, `/ready`, `/metrics` and `/stats` (exact percentiles over a sliding window).

### 3.6 Data stores
**MongoDB** (`fleet` database, least-privilege `fleet_app` user):
- `telemetry`: append-only history. Indexes: unique `message_id`, `{vehicle_id:1, ts:-1}`, `2dsphere(geo)`, TTL on `ts` (`TELEMETRY_TTL_DAYS`).
- `vehicle_state`: latest state per vehicle (`_id` = vehicle_id), `2dsphere`. Used when Redis is down.
- `alerts`: lifecycle `OPEN → ACKNOWLEDGED → RESOLVED`, `occurrences`, `sources`, `pipeline_latency_ms`.
- `analytics_snapshots`: 7-day TTL.

**Redis:** `vehicle:{id}` hash (flat fields plus the full JSON), `fleet:geo` (GEOSEARCH for "vehicles near"), `fleet:last_seen` sorted set, `alerts:*`, `analytics:*`.

### 3.7 Backend API and dashboard
FastAPI with a bcrypt-hashed user list, HS256 JWT (8 h), roles `viewer < operator < admin`, a login rate limit and security headers. REST endpoints: `/api/fleet/summary`, `/api/vehicles`, `/api/vehicles/near`, `/api/vehicles/{id}`, `/api/vehicles/{id}/history`, `/api/alerts`, `/api/alerts/stats`, `POST /api/alerts/{id}/ack|resolve` (operator), `/api/analytics/throughput`, `/api/pipeline/health`, `POST /api/simulator/trigger` (operator). WebSocket `/ws?token=`: alerts are pushed as they arrive, and a snapshot (summary plus up to 1,500 positions) is sent every 2 s. Every value on the dashboard comes from these endpoints.

## 4. Security architecture (prototype vs production)
| Concern | Prototype | Production (AWS) |
|---|---|---|
| Device auth | MQTT username/password + ACL | mTLS per vehicle (listener prepared, `scripts/gen-certs.sh`) or AWS IoT Core X.509 |
| Transport | Plain inside the docker network | TLS everywhere (MQTT 8883, ALB + ACM, Kafka TLS/SASL, Atlas TLS) |
| User auth | JWT + RBAC, bcrypt, rate-limited login | Cognito/OAuth2 OIDC, short-lived tokens |
| Least privilege | Separate MQTT users, `fleet_app` readWrite-only Mongo user, Redis password | IAM roles for service accounts, KMS encryption at rest |
| Secrets | `.env` (git-ignored) / K8s Secret | AWS Secrets Manager via External Secrets |
| Input hardening | Node-RED validation + DLQ, Pydantic request models, parameter limits | + WAF, API throttling |

## 5. Resilience design
Each failure case has a scripted test in `scripts/chaos.py`:
- **Duplicates / malformed input** are stopped at the edge (Node-RED) and counted, and rejects go to the DLQ.
- **MQTT outage:** the simulator buffers and replays (bounded). QoS 1 covers events.
- **Kafka outage:** Node-RED buffers up to `KAFKA_MAX_BUFFER` and retries.
- **Consumer crash:** Kafka retains the data, the lag grows, and the consumer resumes from the last committed offset.
- **MongoDB outage:** telemetry-service retries and does not commit, so consumption pauses and nothing is lost.
- **Redis outage:** the circuit breaker opens, MongoDB remains the source of truth, and the API falls back to `vehicle_state`.
- **Process crash:** the `restart: unless-stopped` compose policy restarts it; on K8s, Deployments with liveness probes and PDBs do the same.

## 6. Monitoring
Prometheus scrapes the simulator, Node-RED, the three services, the backend, and the exporters for Kafka (consumer lag), Redis and MongoDB. The Grafana dashboard *AV Fleet – pipeline overview* shows throughput per stage (divergence points to the bottleneck), end-to-end p50/p95/p99, alert latency, lag per consumer group, error and resilience counters, MongoDB batch latency and ops, process CPU/RSS, and fleet status.

## 7. Scaling toward 100,000 virtual vehicles

**What was measured and what is design.** 100,000 *virtual* vehicles were run on one AWS EC2 t3.large with the whole stack: stable at a 300 s reporting interval (≈ 333 msg/s, 0 loss, e2e p95 3.4 s) and saturated at 60 s and 120 s (results/aws-100k-20261001.md). 100,000 vehicles at 1 Hz (100,000 msg/s) has **not** been run. The prototype is tested with progressively larger virtual fleets (TESTING.md). This section is the engineering reasoning for how the same architecture scales out. It is written so the measured per-instance throughput from `results/` can be plugged into the formulas.

**Load model.** At 1 Hz, 100,000 vehicles × 1 msg/s = **100,000 msg/s**. Measured payload is about 1.0 KB (1.15 KB after enrichment), so roughly 100 MB/s ≈ 0.8 Gbit/s into the broker and about 115 MB/s into Kafka. (The plan's "~1 million/s" counts individual sensor readings. This design packs all of a vehicle's sensors into one message per second.)

| Tier | Scaling mechanism | Sizing rule (fill X from measured results) |
|---|---|---|
| Simulator | `VEHICLE_ID_OFFSET` shards, StatefulSet (`k8s/30-…`) | pods = 100,000 / vehicles-per-process measured without schedule lag |
| MQTT | Mosquitto is single-threaded. At 100k connections / 100k msg/s use a clustered broker (EMQX, HiveMQ, VerneMQ) or AWS IoT Core, keeping the same topics and shared subscriptions | broker nodes = 100k msg/s ÷ measured msg/s per node, × 1.5 headroom |
| Node-RED | Stateless replicas behind `$share/nodered/...`, HPA at 70 % CPU. The 50/50 split was measured with 2 replicas | replicas = 100,000 ÷ X_nodered × 1.5. Measured X_nodered ≈ 5,000 msg/s per instance, clean (sandbox, sharing 2 vCPU with the simulator; saturation between 5,000 and 7,500), so the *calculated* requirement is ≈ 30 replicas. This is a design estimate from one measured point and must be re-checked on target hardware |
| Kafka | 3 brokers, RF=3, `min.insync.replicas=2` (plan §13). Partitions ≥ max consumer replicas | partitions(telemetry) = max(100,000 ÷ X_consumer, HPA max), for example 48–96 |
| telemetry-service | Consumer-group replicas (HPA on CPU or KEDA on lag < 1,000/partition) | replicas = 100,000 ÷ X_consumer × 1.3 (≤ partitions) |
| MongoDB | Hashed shard key on `vehicle_id` (`scripts/mongo-sharding.js`), batched inserts | 100k inserts/s needs several shards. **Storage:** 100k × 86,400 × ~1.2 KB ≈ 10 TB/day raw, so at full rate the history tier must downsample (for example 1-in-10 plus all state changes, events and alerts) with raw data archived from Kafka to S3 (Parquet via Kafka Connect). The 7-day TTL on full-rate data is only viable at prototype scale |
| Redis | Pipelined writes. Redis Cluster / ElastiCache with cluster mode to split keys by hash slot | 100k HSET + GEOADD per second is within a small cluster's range. `fleet:geo` becomes geo-sharded by region at that size |
| Backend / WebSocket | Stateless replicas behind the ALB. The dashboard receives aggregates plus a capped position sample, never the raw stream | API load is independent of fleet size |

**Why the design is horizontally scalable:**
- Every hot path is partitioned by `vehicle_id`: MQTT topic, Kafka key, Mongo shard key, Redis key.
- Stages are decoupled by Kafka, so each scales independently and absorbs bursts as lag instead of loss.
- Consumers are stateless apart from bounded caches.
- Idempotent writes make scale-in, scale-out and rebalances safe.

The main known bottlenecks at 100k are the single-threaded broker, single-threaded Node-RED instances (hence replicas) and MongoDB write volume (hence sharding plus downsampling).
