# Evidence checklist: screenshots of every component, taken from your own running system

Take each screenshot yourself while the system is running. Every item comes with the exact command or URL to use and what should be visible.
Suggested file names: `01-docker-ps.png`, `02-docker-desktop.png`, and so on.

> Label each screenshot with where it was taken: "local, Docker Desktop" or "AWS EC2 + MongoDB Atlas".

---

## A. Docker and infrastructure
| # | Screenshot | How to capture |
|---|---|---|
| 01 | All containers running | PowerShell: `docker compose ps` |
| 02 | Docker Desktop view | Docker Desktop → **Containers** → expand `av-fleet` |
| 03 | Resource usage per container | `docker stats --no-stream` |
| 04 | Images built | `docker images` (av-fleet-* images) |
| 05 | Config without code changes | Open `.env` in VS Code, showing MODE / VEHICLE_COUNT |

## B. Virtual vehicle simulator
| # | Screenshot | How to capture |
|---|---|---|
| 06 | Simulator status | Browser: http://localhost:9105/status |
| 07 | Simulator logs with events | `docker compose logs simulator --tail 40` (shows SCENARIO / EVENT lines) |
| 08 | Live telemetry JSON | `docker compose exec mosquitto mosquitto_sub -u admin -P admin-dev-password -t fleet/vehicle-00001/telemetry -C 3` |
| 09 | Gradual GPS / speed | Same as 08, with the changing latitude and speed values highlighted |
| 10 | 10,000-vehicle mode | Set `MODE=load_test`, `VEHICLE_COUNT=10000`, run `docker compose up -d simulator`, then capture `/status` and the dashboard |

## C. MQTT (Mosquitto) and security
| # | Screenshot | How to capture |
|---|---|---|
| 11 | Anonymous access refused | `docker compose exec mosquitto mosquitto_sub -t "fleet/#" -C 1` → "Not authorized" |
| 12 | Topic ACL file | Open `mosquitto/config/acl` |
| 13 | Events topic (QoS 1) | `docker compose exec mosquitto mosquitto_sub -u admin -P admin-dev-password -t "fleet/+/events" -v` while you click a Demo control button |

## D. Node-RED
| # | Screenshot | How to capture |
|---|---|---|
| 14 | The flow | http://localhost:1880. The whole flow, with the Kafka node showing a green "connected" |
| 15 | Validation function | Double-click **Parse + validate schema** |
| 16 | Stats / latency | http://localhost:1880/stats |
| 17 | Malformed rejected | Dashboard → *20 malformed msgs*, then refresh `/stats` (`rejected`, `reject_reasons`) |
| 18 | Duplicates dropped | Dashboard → *20 duplicate msgs*, then refresh `/stats` (`duplicates`) |
| 19 | Debug sidebar | Node-RED right sidebar (bug icon) showing CRITICAL events |

## E. Kafka
| # | Screenshot | How to capture |
|---|---|---|
| 20 | Topics | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| 21 | Partitions | `docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic vehicle.telemetry` (6 partitions) |
| 22 | Messages flowing | `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic vehicle.telemetry --max-messages 2` |
| 23 | Consumer groups + lag | `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups` |
| 24 | Dead-letter queue | `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic vehicle.dlq --from-beginning --max-messages 3` |
| 25 | Alerts topic | Same command with `--topic vehicle.alerts --from-beginning --max-messages 2` |

## F. Microservices
| # | Screenshot | How to capture |
|---|---|---|
| 26 | Telemetry service stats | http://localhost:8001/stats (inserted, e2e p50/p95/p99) |
| 27 | Alert service stats | http://localhost:8002/stats |
| 28 | Analytics service stats | http://localhost:8003/stats (summary + hotspots) |
| 29 | Alert log lines | `docker compose logs alert-service --tail 30` (ALERT … latency=…ms) |

## G. MongoDB (local, or Atlas on AWS)
| # | Screenshot | How to capture |
|---|---|---|
| 30 | Collections + counts | `docker compose exec mongodb mongosh -u root -p root-dev-password --eval "db.getSiblingDB('fleet').getCollectionNames().forEach(c=>print(c, db.getSiblingDB('fleet')[c].countDocuments()))"` |
| 31 | A telemetry document | `... --eval "printjson(db.getSiblingDB('fleet').telemetry.findOne())"` |
| 32 | Indexes (unique, 2dsphere, TTL) | `... --eval "printjson(db.getSiblingDB('fleet').telemetry.getIndexes())"` |
| 33 | An alert document | `... --eval "printjson(db.getSiblingDB('fleet').alerts.findOne({event_type:'COLLISION_WARNING'}))"` |
| 34 | (Atlas) Collections browser | Atlas → Browse Collections → `fleet.telemetry` |
| 35 | (Atlas) Metrics | Atlas → cluster → Metrics (ops/sec) |

## H. Redis
| # | Screenshot | How to capture |
|---|---|---|
| 36 | Latest vehicle state | `docker compose exec redis redis-cli -a redis-dev-password HGETALL vehicle:vehicle-00001` |
| 37 | Geo search | `docker compose exec redis redis-cli -a redis-dev-password GEOSEARCH fleet:geo FROMLONLAT 144.9631 -37.8136 BYRADIUS 2 km ASC WITHDIST` |
| 38 | Fleet summary | `docker compose exec redis redis-cli -a redis-dev-password GET analytics:summary` |

## I. Backend API, security and dashboard
| # | Screenshot | How to capture |
|---|---|---|
| 39 | Login page | http://localhost:8000 |
| 40 | Dashboard overview | After logging in as operator: pipeline strip, KPIs, map, alerts |
| 41 | Critical alert toast | Demo control → Collision warning (the red toast plus a VEHICLE+PLATFORM card) |
| 42 | RBAC | Log in as `viewer` (no Acknowledge buttons, no Demo control) |
| 43 | API docs | http://localhost:8000/docs |
| 44 | 401 without token | http://localhost:8000/api/fleet/summary |
| 45 | Pipeline health JSON | Swagger `/docs` → Authorize with a token → `GET /api/pipeline/health` |

## J. Monitoring
| # | Screenshot | How to capture |
|---|---|---|
| 46 | Prometheus targets | http://localhost:9090/targets |
| 47 | Grafana overview | http://localhost:3000 → Dashboards → AV Fleet → pipeline overview |
| 48 | Grafana throughput | Zoom in on the "Throughput by pipeline stage" panel |
| 49 | Grafana Kafka lag | During the kafka_consumer_failure chaos test (lag rises, then drains) |

## K. Testing and results
| # | Screenshot | How to capture |
|---|---|---|
| 50 | Unit tests | `cd simulator; python -m pytest -q`, and the same in `services` and `backend` |
| 51 | Automatic verification | `python scripts\verify_all.py` (the summary lines) |
| 52 | Load test running | The terminal during `python scripts\loadtest.py ...` |
| 53 | Load test results | Open `results\loadtest-*.md` |
| 54 | Chaos results | `python scripts\chaos.py mqtt_disconnect kafka_consumer_failure redis_unavailable microservice_crash` → output + JSON |

## L. AWS (after following docs/AWS_DEPLOYMENT_GUIDE.md)
| # | Screenshot | How to capture |
|---|---|---|
| 55 | EC2 instance running | EC2 → Instances (Sydney region) |
| 56 | Security group rules | EC2 → Security Groups → `av-fleet-sg` → Inbound rules |
| 57 | SSH session | `docker compose ps` inside the EC2 SSH session |
| 58 | Dashboard via public IP | Browser URL bar showing `http://<EC2-IP>:8000` |
| 59 | CloudWatch | EC2 → instance → Monitoring tab (CPU / network graphs) |
| 60 | Atlas network access | Atlas → Network Access showing the EC2 IP |

## M. Kubernetes (optional)
The `k8s/` manifests are a production design. If you want screenshots, the free way is Docker Desktop → Settings → **Kubernetes → Enable**, then:
```powershell
kubectl apply -f k8s/00-namespace-config.yaml
kubectl get all -n av-fleet
```
Deploying the rest needs container images pushed to a registry, and Kafka/MongoDB installed in the cluster (Strimzi / Helm). That's a larger task, so ask me if you want to do it. Otherwise describe Kubernetes as designed, not deployed.

---

## Design reasoning (the "thinking")
The reasoning behind each choice is written up in:
- `docs/ARCHITECTURE.md`: why each component, the data flow, security, resilience, and scaling to 100k
- `docs/TESTING.md`: what was measured and what the numbers mean
- `docs/LIMITATIONS.md`: what is simulated or out of scope

Read these until you can explain each decision in your own words. Your report and viva should reflect **your** understanding, and should acknowledge AI assistance as your unit requires.
