# Deploying to AWS (EC2 + MongoDB Atlas): step-by-step

This deploys the **same containers** you ran locally to real cloud infrastructure:

```
            Your browser ──HTTP──►  AWS EC2 instance (Amazon Linux 2023, Docker Compose)
                                     ├─ Mosquitto, Node-RED, Kafka, Redis
                                     ├─ telemetry / alert / analytics services, backend + dashboard
                                     ├─ simulator (virtual vehicles)
                                     └─ Prometheus + Grafana
                                              │  TLS (mongodb+srv)
                                              ▼
                                     MongoDB Atlas (managed database, hosted on AWS Sydney)
```

**Cost.** MongoDB Atlas M0 is free. An EC2 `t3.large` in Sydney costs roughly US$0.10 per hour, only while it is running (check current prices on the EC2 pricing page). **Stop or terminate it when you finish** (step 8).
**Time.** About 45 minutes the first time.

---

## Using AWS Academy Learner Lab (Deakin)? Read this first

The steps below work in Learner Lab with these differences:

| Topic | Learner Lab |
|---|---|
| Start | Open the course → **Modules → Learner Lab** → **Start Lab**. Wait until the dot next to "AWS" turns green, then click **AWS** to open the console |
| Region | **US East (N. Virginia) us-east-1**. Pick the same region for the Atlas cluster (AWS / N. Virginia) |
| Key pair | Use the existing **`vockey`**. In Learner Lab click **AWS Details → Download PEM** to get `labsuser.pem`, and use it instead of `av-fleet-key.pem` in Step 3 |
| Instance type | `t3.large` is allowed (Learner Lab caps the size and number of instances, so don't go bigger) |
| Session timer | The lab session lasts about 4 hours. When it ends, the instance **stops** (it isn't deleted). Start the lab again, then EC2 → Start instance. The **public IP changes** after a restart, so update Atlas Network Access, `CORS_ORIGINS` and your browser URL |
| Budget | Usage comes out of your lab credit. Stop the instance when you're not using it |
| IAM | You can't create IAM users or roles. Nothing in this guide needs them |

## Step 1: MongoDB Atlas (managed database)

1. Sign up at https://www.mongodb.com/cloud/atlas/register.
2. Click **Create** → choose **M0 (Free)** → Provider **AWS** → Region **Sydney (ap-southeast-2)** → cluster name `av-fleet` → **Create Deployment**.
3. **Database user:** Security → Database Access → **Add New Database User**
   - Username: `fleet_app`, with a strong password you choose
   - Built-in role: *Read and write to any database*. For least privilege, use *Specific privileges → readWrite on `fleet`* instead.
4. **Network access:** Security → Network Access → **Add IP Address**. Add your EC2 instance's public IP after step 2. Until then you can add your own laptop IP for testing.
5. **Connection string:** Database → **Connect** → *Drivers* → copy the string. It looks like
   `mongodb+srv://fleet_app:<password>@av-fleet.xxxxx.mongodb.net/?retryWrites=true&w=majority`
   Put your password in, and add the database name `fleet` before the `?`:
   `mongodb+srv://fleet_app:YOURPASS@av-fleet.xxxxx.mongodb.net/fleet?retryWrites=true&w=majority`

> M0 limits: 512 MB storage and a limited operations rate. Use demo mode (20–100 virtual vehicles) on Atlas, and set `TELEMETRY_TTL_DAYS=1`. Do the big load tests on your local Docker setup.

## Step 2: Launch the EC2 instance

AWS Console → select region **Asia Pacific (Sydney)** (top right) → **EC2** → **Launch instance**:

| Setting | Value |
|---|---|
| Name | `av-fleet-server` |
| AMI | **Amazon Linux 2023** |
| Instance type | **t3.large** (2 vCPU, 8 GB). `t3.medium` (4 GB) is too small for Kafka plus everything else |
| Key pair | **Create new key pair** → `av-fleet-key`, RSA, `.pem` → it downloads. Keep it safe |
| Storage | **30 GiB gp3** |
| Security group | Create one named `av-fleet-sg` with the inbound rules below |
| Advanced details → User data | Paste the contents of `deploy/aws/ec2-user-data.sh` (installs Docker + Compose automatically) |

Inbound rules. Use **"My IP"** as the source for every one; never use 0.0.0.0/0 for these:

| Port | Purpose |
|---|---|
| 22 | SSH |
| 8000 | Dashboard / API |
| 3000 | Grafana |
| 1880 | Node-RED editor |
| 9090 | Prometheus (optional) |

Click **Launch instance**. When it shows *Running*, copy its **Public IPv4 address**, then go back to Atlas step 4 and add that IP.

> Optional: EC2 → Elastic IPs → Allocate → Associate with the instance, so the IP doesn't change when you stop and start it.

## Step 3: Copy the project to the server

From PowerShell on your laptop, in the folder containing `av-fleet-sim.zip` and `av-fleet-key.pem`:
```powershell
scp -i av-fleet-key.pem av-fleet-sim.zip ec2-user@<PUBLIC-IP>:~
ssh -i av-fleet-key.pem ec2-user@<PUBLIC-IP>
```
If Windows complains about key permissions, right-click the `.pem` file → Properties → Security → Advanced → disable inheritance, and leave only your own user with Read access.

On the server:
```bash
docker --version && docker compose version     # installed by the user-data script (wait 2-3 min after launch)
unzip av-fleet-sim.zip && cd av-fleet-sim
cp .env.example .env
nano .env
```

## Step 4: Configure `.env` (your connection strings and secrets)

Change at least these values:
```
MODE=demo
VEHICLE_COUNT=20
ATLAS_URI=mongodb+srv://fleet_app:YOURPASS@av-fleet.xxxxx.mongodb.net/fleet?retryWrites=true&w=majority
TELEMETRY_TTL_DAYS=1
JWT_SECRET=<a long random string>
API_USERS=admin:<strong-pass>:admin,operator:<strong-pass>:operator,viewer:<strong-pass>:viewer
CORS_ORIGINS=http://<PUBLIC-IP>:8000
REDIS_PASSWORD=<strong-pass>
REDIS_URI=redis://:<same-strong-pass>@redis:6379/0
GRAFANA_ADMIN_PASSWORD=<strong-pass>
```
Save with Ctrl+O, Enter, then Ctrl+X.

(The MQTT passwords must match `mosquitto/config/passwd`. Keep the defaults, or change them in `.env` and run `bash scripts/gen-mqtt-passwords.sh`.)

## Step 5: Start the stack on AWS

```bash
docker compose -f docker-compose.yml -f deploy/aws/docker-compose.atlas.yml up -d --build
docker compose ps
```
The override file removes the local MongoDB container and points every service at Atlas. The first build takes 5–10 minutes.

## Step 6: Verify

- Open `http://<PUBLIC-IP>:8000` in your browser. The dashboard should show vehicles and the pipeline strip should be all green.
- Atlas → **Browse Collections** → database `fleet` → `telemetry`, `alerts`, `vehicle_state`: documents appear and keep growing.
- On the server, run the automatic checks:
  ```bash
  sudo dnf install -y python3-pip
  python3 -m pip install -r scripts/requirements-tools.txt
  python3 scripts/verify_all.py
  ```
  The MongoDB checks use `ATLAS_URI` automatically. Skip `chaos.py database_unavailable` on AWS, because Atlas isn't a container you can stop.

## Step 7: Monitoring in AWS

- EC2 → your instance → **Monitoring** tab: CPU, network in/out, and status checks (CloudWatch, free basic metrics).
- Atlas → cluster → **Metrics**: operations per second, connections, and data size, generated by your telemetry service.
- Grafana at `http://<PUBLIC-IP>:3000`: the same pipeline dashboard as locally.

## Step 8: Stop paying when finished

- EC2 → Instances → **Stop instance** keeps everything, and you pay only for the 30 GB disk. **Terminate** deletes it.
- If you allocated an Elastic IP: **Release** it after terminating, because unattached IPs are billed.
- Atlas M0 is free, but you can delete the cluster when the unit ends.
- Recommended: Billing → **Budgets** → create a US$10 monthly budget with an email alert.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Services log `ServerSelectionTimeoutError` | The EC2 public IP isn't in Atlas Network Access, or there's a typo in `ATLAS_URI` |
| `docker: command not found` right after launch | The user-data script is still running. Wait 3 minutes, or check `sudo cat /var/log/cloud-init-output.log` |
| Build killed / out of memory | Use t3.large or bigger (the script also adds 2 GB swap) |
| Browser can't reach :8000 | The security group rule is missing, or your IP changed (update "My IP") |
| Map tiles blank | Normal if the network blocks the tile server. The vehicles and data still work |

## How to describe this in your report (accurately)

- **Deployed:** a single EC2 instance running the containers with Docker Compose, plus MongoDB Atlas as the managed database (both on AWS, Sydney region).
- **Designed but not deployed:** EKS, Amazon MSK, ElastiCache, the ALB and the HPA/KEDA autoscaling in `k8s/`. Keep describing those as the production design (docs/DEPLOYMENT.md), not as something you ran, unless you actually deploy them.
