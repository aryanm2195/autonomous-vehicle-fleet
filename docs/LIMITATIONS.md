# Project limitations

## 1. Software simulation, not physical hardware
The prototype uses software-generated telemetry to simulate a fleet of autonomous vehicles. This lets us evaluate the architecture, event-processing pipeline, scalability mechanisms, database design, security mechanisms and monitoring infrastructure without physical autonomous vehicles or sensor hardware.

What that means in practice:
- **No physical vehicles or sensors.** No Arduino, Raspberry Pi, LiDAR, radar, camera, GPS receiver or IoT device is used. Vehicles and sensors are Python objects in `simulator/`.
- **Simplified sensor models.** LiDAR is a nearest-return distance plus an object count, not a point cloud. Radar is distance plus relative speed. The camera sends detection metadata, with no images or video. Object detection is simulated. The plan's CNN/LSTM, Kalman filter and A* routing are not implemented.
- **Simplified physics.** Kinematics are point-mass on a spherical Earth, routes are street-grid-like (no real road network), traffic is zone-based, and charging is time-compressed (0.08 %/s) so demos finish in minutes.
- **Deliberate scenarios.** Emergencies are injected (randomly at `EMERGENCY_EVENT_RATE`, by the demo script, or from the dashboard) and are not caused by real-world events. The on-board and platform detection logic that reacts to them is real code.
- **Network behaviour.** Simulated vehicles talk to the broker over localhost or a docker network. Cellular latency, packet loss and roaming are not modelled (apart from the MQTT-disconnect scenario).

## 2. Scale
- **100,000 virtual vehicles were run only at a low reporting rate.** On one EC2 t3.large (whole stack, 2 vCPU), 100,000 virtual vehicles reporting every 300 s (≈ 333 msg/s) were stable. At 60 s and 120 s intervals the box saturated (results/aws-100k-20261001.md). 100,000 vehicles at 1 msg/s each (100,000 msg/s) was not tested, and no physical vehicles were tested. Per-second scaling was measured in steps up to 10,000 msg/s (edge tier, sandbox).
- **Shared gateway sessions.** In load-test mode, vehicles share MQTT sessions (`VEHICLES_PER_CONNECTION`, default 250) instead of opening one TCP connection each, so connection-count scaling is not load-tested. Demo mode uses real per-vehicle sessions.
- **Single-node infrastructure.** Locally there is one Kafka broker (RF=1), one MongoDB (not sharded), one Redis and one Mosquitto. Replication, sharding and broker clustering are configured or scripted for production (k8s/, `mongo-sharding.js`) but were not run.
- **Kubernetes and AWS** manifests are provided but were not deployed or measured in this prototype. The HPA and KEDA behaviour described in the plan is therefore not demonstrated.

## 3. Measurement caveats
- **Shared hardware.** Simulator and pipeline run on the same machine during load tests, so they compete for CPU. The reported results are lower bounds for the pipeline on dedicated hardware.
- **Clock-based latency.** End-to-end latency compares the simulator's timestamp with the processing host's clock. That is exact on one host. Across hosts it needs NTP/PTP.
- **Sliding-window percentiles.** Percentiles come from exact sorting of the last N samples per stage (N = 20,000–50,000), so they describe the measurement window only.
- **Sandbox runs.** Results produced in the cloud sandbox during development cover only simulator → Mosquitto → Node-RED, because Kafka and MongoDB images could not be downloaded there (network policy). Full-pipeline figures must come from `make loadtest` on the report machine.

## 4. Functional scope
- **Services.** Of the ten microservices in the plan (§8), telemetry, alert (emergency + rule-based detection) and analytics are implemented. Authentication is part of the backend API. Charging, maintenance, routing, billing, fleet-registry and notification services are represented by the `vehicle.alerts` topic that they would consume, but not built.
- **No S3 archive.** The S3 archive tier and CloudWatch are described but not implemented. Prometheus and Grafana are the local equivalents.
- **TLS off by default.** TLS/mTLS is prepared (config + `scripts/gen-certs.sh`) but disabled in the local compose so the demo works out of the box. Demo credentials in `.env.example` are for local use only.
- **Dashboard data.** The dashboard shows up to 1,500 vehicle positions per snapshot so the browser stays responsive with large fleets. Aggregates always cover the whole fleet.
