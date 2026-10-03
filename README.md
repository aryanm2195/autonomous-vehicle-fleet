# AV Fleet: a software simulation of an autonomous vehicle IoT platform

SIT314 Software Architecture and Scalability for IoT: implementation of the *Autonomous Vehicle Fleet* project plan (Task 1.2D).

> **This is a software-only prototype.**
> The prototype uses software-generated telemetry to simulate a fleet of autonomous vehicles. This lets us evaluate the architecture, event-processing pipeline, scalability mechanisms, database design, security mechanisms and monitoring infrastructure without physical autonomous vehicles or sensor hardware.
>
> It uses no Arduino, Raspberry Pi, physical sensors, LiDAR, radar, camera, GPS hardware or vehicles. Wherever the project plan mentions *sensors* or *vehicles*, read them as **software-simulated virtual sensors and virtual vehicles**. The rest of the stack is real software: MQTT, Node-RED, Kafka, the microservices, MongoDB, Redis, the API, the dashboard and the monitoring.

---

## Architecture

```
            ┌─────────────────────────┐
            │ Virtual Vehicle Fleet   │  simulator/  (asyncio, 1 process simulates thousands of vehicles)
            │ Software Simulator      │
            └────────────┬────────────┘
                         │ MQTT  fleet/{vehicle_id}/telemetry (QoS0) · fleet/{vehicle_id}/events (QoS1)
                         ▼
                ┌─────────────────┐
                │    Mosquitto    │  auth + topic ACLs
                └────────┬────────┘
                         ▼
                ┌─────────────────┐
                │    Node-RED     │  validate → de-duplicate → filter/enrich → batch → Kafka
                │ Validation /    │  invalid → vehicle.dlq
                │ Filtering       │
                └────────┬────────┘
                         ▼
                ┌─────────────────┐
                │      Kafka      │  vehicle.telemetry (6 partitions, key = vehicle_id)
                │ Event Streaming │  vehicle.events · vehicle.alerts · vehicle.dlq
                └────────┬────────┘
         ┌───────────────┼────────────────┐   (independent consumer groups = fan-out)
         ▼               ▼                ▼
   Telemetry         Alert Service    Analytics
    Service          (events + rules)  (aggregates, k-means hotspots)
         └───────┬───────┴────────────────┘
         ┌───────┴────────┐
         ▼                ▼
    MongoDB             Redis         history / alerts / snapshots   ·   latest state, geo index, pub/sub
         └───────┬────────┘
                 ▼
             Backend API    FastAPI · JWT + RBAC · REST + WebSocket
                 ▼
         Web Dashboard      live map, KPIs, alerts, pipeline health, demo controls

   Prometheus + Grafana scrape every component · Kubernetes/AWS manifests in k8s/
```

Every major component handles real data from the simulator. The dashboard has **no hard-coded statistics**: each number comes from the API, which reads what the pipeline wrote to Redis and MongoDB.

## Quick start

Requirements: Docker Desktop (or Docker Engine with compose v2), about 6 GB of free RAM.

```bash
cp .env.example .env          # all settings are environment variables
make demo                     # or: MODE=demo VEHICLE_COUNT=20 docker compose up -d --build
```

| URL | What | Login |
|---|---|---|
| http://localhost:8000 | Fleet dashboard | `operator` / `operator123` (also `admin`, `viewer`) |
| http://localhost:1880 | Node-RED ingestion flow | – |
| http://localhost:3000 | Grafana "AV Fleet – pipeline overview" | `admin` / `admin` |
| http://localhost:9090 | Prometheus | – |
| http://localhost:9105/status | Simulator status and control API | – |

The first build downloads the images (Kafka, MongoDB, Node-RED and others), which takes a few minutes.

## Modes

| `MODE` | Vehicles (default) | MQTT sessions | Events |
|---|---|---|---|
| `demo` | 20 | one per vehicle (a true IoT device model, each with its own LWT) | scripted storyline: low battery (t+15 s), collision warning (t+30 s), sensor failure (t+45 s), route deviation (t+60 s), abnormal speed (t+75 s), obstacle (t+90 s), repeating |
| `normal` | 100 | shared gateway sessions | random at `EMERGENCY_EVENT_RATE` |
| `load_test` | 1,000 | shared gateway sessions (`VEHICLES_PER_CONNECTION`) | low random rate. Started by `scripts/loadtest.py` |

Any variable overrides the preset, for example `MODE=load_test VEHICLE_COUNT=10000`. No code change is ever needed.

### Main configuration (`.env`)

`VEHICLE_COUNT`, `TELEMETRY_INTERVAL`, `MQTT_BROKER`, `MQTT_PORT`, `KAFKA_BROKER`, `MONGODB_URI`, `REDIS_URI`, `EMERGENCY_EVENT_RATE`, `LOW_BATTERY_THRESHOLD`, `CRITICAL_BATTERY_THRESHOLD`, `LOG_LEVEL`, plus `FAILURE_RATE`, `DUPLICATE_RATE`, `MALFORMED_RATE`, `MQTT_CONNECTION_MODE`, `VEHICLES_PER_CONNECTION`, `RANDOM_SEED`. See `.env.example` for the full list with comments.

## What the simulator generates

Each virtual vehicle is a **stateful object**. Every telemetry cycle updates that state instead of drawing independent random values:

- **GPS**: position integrated from speed and heading along street-grid routes around Melbourne CBD (-37.8136, 144.9631), with 2.5 m GNSS noise. Speed follows a smooth random walk inside the traffic band (for example 41.6 → 41.9 → 41.6 → 43.0 km/h) and respects acceleration and braking limits.
- **LiDAR / radar / camera**: summaries derived from one shared tracked object (pedestrian, vehicle, bicycle, obstacle, road barrier), so the three sensors agree with each other. The camera sends metadata only, never video. Failed sensors report `status: FAILED` with null readings.
- **Battery**: drains with distance, load and idle use, and only rises while plugged in at one of 7 charging hubs. `LOW_BATTERY` and `CRITICAL_BATTERY` events fire once on crossing, with hysteresis.
- **Traffic**: roughly 1 km zones that change one level at a time (`LOW ↔ MODERATE ↔ HIGH ↔ CONGESTED`).
- **Status state machine**: `AVAILABLE / IN_SERVICE / IDLE / CHARGING / MAINTENANCE / EMERGENCY` with legal transitions and minimum dwell times. Passenger counts follow the status: `IN_SERVICE` means 1–6 passengers, anything else means 0.
- **Emergencies**: collision warning (from a required-deceleration check at autonomous emergency braking), sudden braking, obstacle, critical battery, sensor failure, abnormal speed, route deviation and emergency stop. Each one is created by manipulating the vehicle's state, so the telemetry around the event stays consistent.

Schemas: [`schemas/telemetry.schema.json`](schemas/telemetry.schema.json) and [`schemas/event.schema.json`](schemas/event.schema.json). Field reference: [docs/TELEMETRY_SCHEMA.md](docs/TELEMETRY_SCHEMA.md).

## Testing and results

```bash
pip install -r scripts/requirements-tools.txt
make test                                   # 35 unit tests (simulator, services, API)
make loadtest                               # 10 → 100 → 1,000 → 5,000 → 10,000 virtual vehicles
make chaos                                  # 9 software failure scenarios
```

`scripts/loadtest.py` writes `results/loadtest-<timestamp>.md/.json`, and `scripts/chaos.py` writes `results/chaos-<timestamp>.json`. **Every figure in those files is read from the running components. Nothing is estimated.**

The measurements in `results/` so far were taken in a cloud sandbox where Kafka and MongoDB could not be installed, so they cover only **simulator → Mosquitto → Node-RED**. Measured headline for that tier on 2 vCPU:
- **Up to 5,000 virtual vehicles (5,000 msg/s):** 0 messages lost, p95 edge latency 18 ms.
- **Around 7,500 msg/s:** a single Node-RED instance saturates.
- **10,000 msg/s:** overload. The broker sheds QoS 0 telemetry (21.9 % lost).
- **Scale-out:** two replicas behind an MQTT shared subscription split the load exactly 50/50.

Full-pipeline numbers (Kafka lag, MongoDB insert rate, end-to-end p50/p95/p99) have to come from running `make loadtest` on the machine used for the report. Details: [docs/TESTING.md](docs/TESTING.md).

**Measured on AWS (single EC2 t3.large, whole stack):** 100,000 *virtual* vehicles reporting every 300 s (≈ 333 msg/s) ran with no loss, an end-to-end p95 of 3.4 s and 18 % idle CPU. At 60 s and 120 s intervals the 2 vCPU box saturated. See [results/aws-100k-20261001.md](results/aws-100k-20261001.md). No physical vehicles were tested, and 100,000 vehicles at 1 msg/s was **not** tested. The target architecture is designed to scale toward 100,000 virtual vehicles. [docs/ARCHITECTURE.md §7](docs/ARCHITECTURE.md#7-scaling-toward-100000-virtual-vehicles) explains how, and which parts of that reasoning are measured and which are design.

## Repository layout

```
simulator/        virtual fleet (fleetsim package, tests, Dockerfile)
schemas/          JSON Schemas for telemetry and events
mosquitto/        broker config, password file, ACLs
nodered/          flows.json (+ readable JS sources in functions/, settings.js, Dockerfile)
services/         telemetry_service, alert_service, analytics_service (+ tests)
backend/          FastAPI API (JWT/RBAC, REST, WebSocket) (+ tests)
dashboard/        static web dashboard served by the backend
monitoring/       Prometheus scrape config, Grafana datasource + dashboard
k8s/              EKS deployment: Deployments, HPA (CPU 70 %), KEDA lag scaler, PDBs, Ingress
scripts/          loadtest.py, chaos.py, mongo-init.js, mongo-sharding.js, cert/password helpers
docs/             ARCHITECTURE, TELEMETRY_SCHEMA, TESTING, LIMITATIONS, DEMO_SCRIPT, DEPLOYMENT
results/          measured results only
```

## Documentation

- **[Evidence checklist: 60 screenshots of every component](docs/EVIDENCE_CHECKLIST.md)**
- **[AWS deployment guide (EC2 + MongoDB Atlas)](docs/AWS_DEPLOYMENT_GUIDE.md)**
- **[How to run and check every function](docs/RUN_AND_VERIFY.md)**: step-by-step guide plus `python scripts/verify_all.py` (about 60 automatic PASS/FAIL checks)
- [Architecture](docs/ARCHITECTURE.md)
- [Telemetry schema](docs/TELEMETRY_SCHEMA.md)
- [Testing and results](docs/TESTING.md)
- [Limitations](docs/LIMITATIONS.md)
- [Demonstration script](docs/DEMO_SCRIPT.md)
- [Deployment (Kubernetes/AWS)](docs/DEPLOYMENT.md)
