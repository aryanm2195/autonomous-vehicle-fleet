# Demonstration script (about 10 minutes)

> **Opening statement (say this first):** "This prototype uses software-generated telemetry to simulate a fleet of autonomous vehicles. That lets me evaluate the architecture, event-processing pipeline, scalability mechanisms, database design, security mechanisms and monitoring infrastructure without physical autonomous vehicles or sensor hardware. The vehicles are virtual. MQTT, Node-RED, Kafka, the microservices, MongoDB, Redis, the API and the dashboard are all real and running."

## 0. Before the lecturer arrives (5 min)
```bash
cp .env.example .env
make demo                     # MODE=demo, 20 virtual vehicles, per-vehicle MQTT sessions
docker compose ps             # all services "running"/"healthy"; kafka-init "exited (0)"
```
Open these browser tabs:
1. http://localhost:8000: dashboard (log in as `operator` / `operator123`)
2. http://localhost:1880: Node-RED flow
3. http://localhost:3000: Grafana → *AV Fleet – pipeline overview* (`admin`/`admin`)
4. A terminal with `docker compose logs -f simulator alert-service`

Tip: the demo storyline starts 15 s after the simulator starts and replays roughly every 2 minutes. If you need to restart it, run `docker compose restart simulator`.

## 1. The virtual fleet (1 min)
- Dashboard map: 20 dots moving around Melbourne CBD, coloured by status. Hover over one to see its speed, battery and passengers.
- Point out: "Each dot is a stateful virtual vehicle. Its GPS moves gradually, speed changes smoothly, and battery drains with distance."
- Terminal: `curl localhost:9105/status` shows the simulator's own view (mode, vehicle count, status counts).

## 2. The real pipeline (2 min)
- The **pipeline strip** at the top of the dashboard shows live counters for Simulator → MQTT → Node-RED → Kafka → Telemetry/Alert/Analytics services → MongoDB/Redis, all green.
- Node-RED tab: walk the flow **MQTT (shared subscription) → Parse+validate → De-duplicate → Filter+enrich → Kafka producer**. The status under the Kafka node shows "connected | sent N".
- Grafana: the *Throughput by pipeline stage* lines overlap (every stage keeps up), and *End-to-end latency* shows p50/p95/p99 from vehicle timestamp to MongoDB.

## 3. Scripted emergency events (3 min)
These happen automatically. Narrate them as they appear in the **Alerts** panel (a red toast appears for CRITICAL alerts):

| Time after start | Vehicle | What you'll see |
|---|---|---|
| ~15–25 s | vehicle-00003 | `LOW_BATTERY` (MEDIUM). The vehicle then heads to a charging hub and its status turns `CHARGING` |
| ~30 s | vehicle-00007 | **`COLLISION_WARNING` (CRITICAL)** + `SUDDEN_BRAKING`. The status turns `EMERGENCY` and the dot turns red and grows |
| ~45 s | vehicle-00011 | `SENSOR_FAILURE` (HIGH). The vehicle makes a safe stop and then enters `MAINTENANCE` |
| ~60–70 s | vehicle-00015 | `ROUTE_DEVIATION` (HIGH) after a wrong turn (>150 m off the planned route) |
| ~75 s | vehicle-00018 | `ABNORMAL_SPEED` (HIGH), a controller fault at 125–150 km/h |
| ~90 s | vehicle-00005 | `OBSTACLE_DETECTED` (a road barrier). The vehicle slows and stops |

For the collision warning, point out:
- The alert card shows **`VEHICLE+PLATFORM`**. The vehicle reported it *and* the alert-service rule engine independently detected it from raw telemetry. They were merged into one alert, not duplicated.
- It shows **pipeline N ms**, the measured time from the vehicle's timestamp until the alert was stored in MongoDB and pushed to Redis.
- Click it: the map zooms to the vehicle, and the toast shows its current sensor statuses.
- Click **Acknowledge** (operator role), then log in as `viewer` to show the button is gone (RBAC).

## 4. Trigger an event on demand (1 min)
- In **Demo control**, type `vehicle-00012` and click **Collision warning**. The alert appears within about a second.
- Explain: "This button asks the simulator to create the situation. The event still travels vehicle → MQTT → Node-RED → Kafka → alert-service → MongoDB/Redis → WebSocket."

## 5. Failure simulation (2 min)
- **Malformed messages:** click *20 malformed msgs*. Node-RED's rejected counter rises by 20 in the pipeline strip, and the messages go to the `vehicle.dlq` Kafka topic.
- **Duplicates:** click *20 duplicate msgs*. The Node-RED duplicate counter rises by 20, and MongoDB stores each message once.
- **MQTT disconnect:** click *MQTT disconnect 10s*. The simulator buffers messages, reconnects and replays them (Grafana "simulator offline buffer" spikes and then drains).
- Optional, terminal: `docker compose stop telemetry-service`, wait 20 s, then `docker compose start telemetry-service`. The Grafana lag panel rises and then drains back to 0, and nothing is lost. Or run `python scripts/chaos.py kafka_consumer_failure` to print the measured lag and recovery time.

## 6. Scalability (1 min)
- Show `results/loadtest-*.md` from your own run of `make loadtest` (10 → 10,000 virtual vehicles): throughput per stage, p50/p95/p99, Kafka lag, CPU and memory.
- Say: "I have not run 100,000 vehicles. The architecture partitions everything by vehicle ID, so each tier scales horizontally. The Kubernetes manifests show the HPA at 70 % CPU and a Kafka-lag autoscaler, and the architecture document sizes each tier for 100k using the throughput I measured."

## Closing line
"Everything you saw was produced by the running system: nothing on the dashboard is hard-coded, and every performance number was measured. The only simulated part is the vehicles themselves, which is exactly what made it possible to test this architecture without a fleet of cars."
