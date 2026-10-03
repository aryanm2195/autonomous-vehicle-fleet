# Deployment and scaling layer (Kubernetes / AWS)

> The manifests in `k8s/` show how the **same containers** used locally scale out on Amazon EKS.
> They were **not deployed or measured** as part of this prototype (see LIMITATIONS.md). They are included as the deployment design that the plan's §6.2 and §10.2 call for.

## Component → AWS mapping
| Local (docker compose) | AWS / Kubernetes |
|---|---|
| simulator | StatefulSet `simulator` (sharded by pod ordinal via `VEHICLE_ID_OFFSET`) |
| Mosquitto | Clustered MQTT (EMQX/HiveMQ on EKS) or **AWS IoT Core** (X.509 per device) |
| Node-RED | Deployment `nodered` (3–20 replicas, HPA at 70 % CPU, MQTT shared subscription) |
| Kafka (1 broker) | **Amazon MSK** or Strimzi: 3 brokers, RF=3, `min.insync.replicas=2` (plan §13 risk mitigation) |
| telemetry / alert / analytics services | Deployments + HPA (CPU 70 %) or a KEDA `ScaledObject` on consumer lag (< 1,000/partition), PDBs |
| MongoDB | **MongoDB Atlas** sharded cluster, `{vehicle_id: "hashed"}` (`scripts/mongo-sharding.js`) |
| Redis | **ElastiCache for Redis** (cluster mode, TLS) |
| backend + dashboard | Deployment `backend` + Service + **ALB Ingress** (ACM TLS) |
| Prometheus/Grafana | Amazon Managed Prometheus/Grafana, or kube-prometheus-stack. CloudWatch Container Insights |
| `.env` secrets | **AWS Secrets Manager** → External Secrets → K8s Secret `fleet-secrets` |
| (not implemented) raw archive | Kafka Connect S3 sink → **S3** (Parquet) with lifecycle rules |

## Applying
```bash
# build and push images (replace the registry)
docker build -t $REG/av-fleet-simulator:1.0.0 simulator
docker build -t $REG/av-fleet-nodered:1.0.0 nodered
docker build -t $REG/av-fleet-services:1.0.0 services
docker build -t $REG/av-fleet-backend:1.0.0 -f backend/Dockerfile .
# prerequisites: MSK/Strimzi, Atlas, ElastiCache, AWS Load Balancer Controller, (optional) KEDA
kubectl apply -f k8s/
```

## Scale-out procedure (how a progressive test would continue beyond one machine)
1. Start with `simulator` replicas=2 (10,000 virtual vehicles) and confirm per-stage throughput matches in Grafana.
2. Double the simulator replicas step by step (4 → 10 → 20). After each step, check that Kafka lag stays below 1,000 per partition, telemetry p95 below 200 ms and critical alerts p95 below 50 ms (plan §4.2), and let HPA/KEDA add Node-RED and consumer replicas.
3. When a tier saturates (the throughput lines in Grafana diverge), apply that tier's rule from ARCHITECTURE.md §7: more partitions, a broker node, or a MongoDB shard.
4. Budget guardrails (plan §13): HPA `maxReplicas` on every Deployment plus AWS Budgets alerts.
