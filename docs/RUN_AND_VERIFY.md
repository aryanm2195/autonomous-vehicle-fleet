# How to run the system and check every function

This guide assumes **Windows 10/11**; macOS and Linux commands are the same unless noted. Allow about 30 minutes the first time.

> Reminder: the vehicles are software-simulated. Everything else (MQTT, Node-RED, Kafka, the services, MongoDB, Redis, the API, the dashboard and monitoring) is real and running.

---

## Part A: Install the prerequisites (once)

1. **Docker Desktop:** download it from https://www.docker.com/products/docker-desktop, install, restart, and start it. Wait until the whale icon shows "Engine running".
   - In Settings → Resources, give Docker **at least 6 GB of memory and 4 CPUs**.
2. **Python 3.11 or newer:** install it from https://www.python.org/downloads/ and tick **"Add python.exe to PATH"**.
3. Open **PowerShell** and check both:
   ```powershell
   docker --version
   docker compose version
   python --version
   ```

## Part B: Start the system

```powershell
# 1. unzip av-fleet-sim.zip, then go into the folder
cd C:\path\to\av-fleet-sim

# 2. create your configuration file
copy .env.example .env

# 3. build and start everything (first run downloads images: 5-10 minutes)
docker compose up -d --build

# 4. install the tools used by the test scripts
python -m pip install -r scripts\requirements-tools.txt
```

Wait about 2 minutes, then check status:
```powershell
docker compose ps
```
Every service should be `running` (the ones with health checks should also say `healthy`). `kafka-init` should be `exited (0)`, because it only creates the Kafka topics and stops.

If something is not running, see [Part F: Troubleshooting](#part-f-troubleshooting).

## Part C: Run the automatic checks (one command)

```powershell
python scripts\verify_all.py
```

This command tests **about 60 functions** on the live system and prints `PASS`, `FAIL` or `SKIP` for each one. It takes about 3–4 minutes. The results are also saved to `results\verify-<timestamp>.json`, which you can cite in your report.

Useful options:
```powershell
python scripts\verify_all.py --only Kafka          # just one section (MQTT, Simulator, Node-RED, Kafka, Telemetry, Alert, Analytics, Backend, Monitoring)
python scripts\verify_all.py --quick               # skips the checks that wait for 15-second cycles
python scripts\verify_all.py --chaos               # also runs the failure tests (stops and starts containers; about 5 more minutes)
python scripts\verify_all.py -v                    # prints full error details
```

Expected result: **every check PASS**, apart from the "11 Resilience" ones, which are SKIP unless you add `--chaos`.

## Part D: Check each function by hand (what to look at and where)

Each row lists the **automatic check name** in brackets, so you can match it with the `verify_all.py` output.

### 1. Virtual vehicle simulator
| Function | How to see it yourself | Expected |
|---|---|---|
| Configurable fleet size [Control API /status] | Open http://localhost:9105/status in a browser | `"vehicles": 20`, `"mode": "demo"` |
| Change fleet size with no code change | Edit `.env`: `MODE=normal`, `VEHICLE_COUNT=100`, then `docker compose up -d simulator` | `/status` now shows 100 vehicles |
| Stateful, realistic telemetry [GPS moves gradually] [Speed changes smoothly] | `docker compose exec mosquitto mosquitto_sub -u admin -P admin-dev-password -t fleet/vehicle-00001/telemetry -C 5` | 5 messages, one per second. Latitude and longitude change slightly each time, and speed changes by only a few km/h |
| Schema-valid messages [valid vs JSON Schema] | Compare one message with `schemas\telemetry.schema.json` | Every field is present |
| Passengers follow status [Passenger count follows status] | In the messages above | `IN_SERVICE` means 1–6 passengers, otherwise 0 |
| LiDAR, radar and camera consistency [readings are consistent] | Look at a message with `"obstacle_detected": true` | LiDAR and radar distances agree, and the camera names the object |
| Traffic levels | `traffic.status` in the messages | LOW, MODERATE, HIGH or CONGESTED, and it changes slowly |
| Battery drains, alerts at 20 % and 10 % | Dashboard → Vehicles table → Battery column | Values go down slowly. `LOW_BATTERY` fires at 20 % and `CRITICAL_BATTERY` at 10 % |
| Demo storyline (demo mode) | Dashboard alerts panel during the first 2 minutes | LOW_BATTERY → COLLISION_WARNING → SENSOR_FAILURE → ROUTE_DEVIATION → ABNORMAL_SPEED → OBSTACLE |

### 2. MQTT (Mosquitto) and security
| Function | How to check | Expected |
|---|---|---|
| Anonymous access blocked [Anonymous connection is refused] | `docker compose exec mosquitto mosquitto_sub -t "fleet/#" -C 1` | `Connection error: Not authorized` |
| Username and password required [Wrong password is refused] | Same command with `-u simulator -P wrong` | Not authorized |
| Topic ACLs [nodered user cannot publish] | Automatic check only | Messages from `nodered` are dropped |

### 3. Node-RED (validation, filtering, enrichment)
| Function | How to check | Expected |
|---|---|---|
| Flow is running | Open http://localhost:1880 | Nodes: MQTT in → Parse+validate → De-duplicate → Filter+enrich → Kafka producer. The Kafka node shows green "connected" |
| Counters and latency [/stats counters increase] | Open http://localhost:1880/stats | `received` and `forwarded` go up. `edge_latency_ms` shows p50/p95/p99 |
| Rejects bad data [Malformed messages rejected] | Dashboard → Demo control → **20 malformed msgs**, then refresh /stats | `rejected` goes up by 20 and `reject_reasons` lists why |
| Removes duplicates [Duplicate messages dropped] | Dashboard → **20 duplicate msgs** | `duplicates` goes up by 20 |
| Prometheus metrics | http://localhost:1880/metrics | Text lines starting with `nodered_` |

### 4. Kafka
| Function | How to check | Expected |
|---|---|---|
| Topics created [Topics exist] | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` | vehicle.telemetry, vehicle.events, vehicle.alerts, vehicle.dlq |
| Messages flowing, enriched [carries enriched messages] | `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic vehicle.telemetry --max-messages 1` | JSON that includes an `"ingest": {...}` block |
| Dead-letter queue [land in vehicle.dlq] | Same command with `--topic vehicle.dlq --from-beginning --max-messages 3` (after sending malformed messages) | Records with a `reason` field |
| Consumer lag [Consumer groups are keeping up] | `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups` | The LAG column stays small (near 0) |

### 5. Telemetry service, MongoDB and Redis
| Function | How to check | Expected |
|---|---|---|
| Stores telemetry [Consumes and stores in MongoDB] | http://localhost:8001/stats | `inserted` goes up. `e2e_latency_ms` shows p50/p95/p99 |
| MongoDB contents | `docker compose exec mongodb mongosh -u root -p root-dev-password --eval "db.getSiblingDB('fleet').telemetry.countDocuments()"` | A number that grows |
| Indexes [MongoDB indexes] | `... --eval "db.getSiblingDB('fleet').telemetry.getIndexes()"` | message_id (unique), vehicle_id+ts, geo 2dsphere, TTL on ts |
| Latest-state snapshot [vehicle_state] | `... --eval "db.getSiblingDB('fleet').vehicle_state.countDocuments()"` | Equals the number of vehicles (updated every 15 s) |
| Redis live state [Redis latest state] | `docker compose exec redis redis-cli -a redis-dev-password HGETALL vehicle:vehicle-00001` | Status, battery, lat and lon for that vehicle |
| Redis geo index | `docker compose exec redis redis-cli -a redis-dev-password GEOSEARCH fleet:geo FROMLONLAT 144.9631 -37.8136 BYRADIUS 2 km ASC` | Vehicles within 2 km of Melbourne CBD |

### 6. Alert service (emergency events)
| Function | How to check | Expected |
|---|---|---|
| Emergency travels the whole pipeline [COLLISION_WARNING travels...] | Dashboard → Demo control → type `vehicle-00005` → **Collision warning** | Within about 1 s a red CRITICAL alert appears, with "pipeline N ms" |
| Vehicle and platform detections merged [merged into ONE alert] | Look at that alert card | Source shows `VEHICLE+PLATFORM` (can take a few seconds to update), and there is only one card |
| All emergency types [Scenario … alert] | Click each Demo control button | SENSOR_FAILURE, ABNORMAL_SPEED, ROUTE_DEVIATION (about 10–20 s), LOW_BATTERY (a few seconds), CRITICAL_BATTERY, EMERGENCY_STOP, OBSTACLE_DETECTED |
| Alert stored and published [Redis and Kafka] | http://localhost:8002/stats | `alerts_created` goes up, with latency p50/p95/p99 (CRITICAL listed separately) |

### 7. Analytics service
| Function | How to check | Expected |
|---|---|---|
| Fleet summary [Fleet summary in Redis] | http://localhost:8003/stats | `active_vehicles` equals VEHICLE_COUNT, plus status counts, average battery and msg/s |
| k-means hotspots | Dashboard → Ingest throughput panel → "Demand hotspots" line | 5 clusters with coordinates |
| History snapshots | `mongosh ... analytics_snapshots.countDocuments()` | Grows every 5 s |

### 8. Backend API and dashboard
| Function | How to check | Expected |
|---|---|---|
| Login (JWT) [Login works] | http://localhost:8000 → `operator` / `operator123` | Dashboard loads |
| Rejects bad login and no token [No token → 401] | A wrong password shows "invalid credentials". http://localhost:8000/api/fleet/summary with no token returns 401 | Rejected |
| Role-based access [RBAC] | Sign out, log in as `viewer` / `viewer123` | No Acknowledge buttons and no Demo control panel |
| Live map and KPIs [WebSocket] | Dashboard | Dots move every 2 s, and the green dot next to your username means WebSocket connected |
| Pipeline health [every stage up] | Strip at the top of the dashboard | All 9 stages green |
| API documentation | http://localhost:8000/docs | Interactive Swagger page for every endpoint |

### 9. Monitoring
| Function | How to check | Expected |
|---|---|---|
| Prometheus targets [scrape targets up] | http://localhost:9090/targets | All jobs UP (`simulator-host` is only UP during load tests, which is expected) |
| Grafana dashboard | http://localhost:3000 (`admin`/`admin`) → Dashboards → AV Fleet → *pipeline overview* | Throughput, latency, Kafka lag, errors and resources panels all have data |

### 10. Failure simulation (resilience)
Run all of them with numbers recorded:
```powershell
python scripts\chaos.py sensor_failure abnormal_behaviour malformed duplicate mqtt_disconnect kafka_consumer_failure database_unavailable redis_unavailable microservice_crash
```
Or one at a time, and watch the dashboard and Grafana while each runs:

| Scenario | What happens | What proves it worked |
|---|---|---|
| mqtt_disconnect | Vehicles go offline for 15 s | `flushed_after_reconnect` > 0 and `dropped_buffer_full` = 0 |
| kafka_consumer_failure | telemetry-service stopped for 30 s | Lag grows, then `recovery_to_lag_below_500_s` has a value |
| database_unavailable | MongoDB stopped for 20 s | `mongo_retries_during_outage` > 0, then it recovers |
| redis_unavailable | Redis stopped for 20 s | `api_summary_source_during_outage` = `mongodb-fallback` |
| microservice_crash | alert-service killed | `back_healthy_after_s` has a value |

Results are saved to `results\chaos-<timestamp>.json`.

## Part E: Performance and scalability test

```powershell
docker compose stop simulator        # the test starts its own simulators
python scripts\loadtest.py --stages 10,100,1000,5000,10000 --duration 60 --docker-stats
docker compose start simulator
```
This takes about 15 minutes and writes `results\loadtest-<timestamp>.md` (tables ready for your report) and `.json`. Close other heavy programs while it runs. If your PC struggles at 10,000, that is a finding: report the stage where latency rises, as in docs/TESTING.md §4.

For a quicker first try: `--stages 10,100,1000 --duration 30`.

## Part F: Troubleshooting

| Problem | Fix |
|---|---|
| `docker compose` not found | Update Docker Desktop (compose v2 is built in) |
| Port already in use (1883, 8000, 3000, 27017…) | Close the other program, or change the left-hand port in `docker-compose.yml` |
| A service keeps restarting | `docker compose logs <service> --tail 50`. Kafka can take 60 s on first start, so services retry automatically |
| Dashboard map is empty but KPIs work | The map tiles come from the internet. Check your connection (a plain dot map appears offline) |
| `verify_all.py` FAIL on Kafka checks with "Unable to bootstrap" | Kafka is not ready yet. Wait 1 minute and re-run `--only Kafka` |
| Changed `.env` but nothing happened | `docker compose up -d` (re-creates the changed containers) |
| Start completely fresh | `docker compose down -v` (deletes all data), then `docker compose up -d --build` |
| Stop everything | `docker compose down` |

On macOS or Linux, use `cp` instead of `copy`, `/` instead of `\`, and `python3` instead of `python`. The `make demo`, `make test`, `make loadtest` and `make chaos` shortcuts also work there.
