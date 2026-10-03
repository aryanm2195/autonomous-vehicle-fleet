"""Print one snapshot of pipeline counters and latencies (run on the Docker host).
Used for results/aws-100k-20261001.md. Take two snapshots a few minutes apart;
the difference in counters / elapsed seconds gives the sustained rate."""
import json, re, time, urllib.request as u

g = lambda p: json.load(u.urlopen("http://localhost:" + p))
m = u.urlopen("http://localhost:9105/metrics").read().decode()
pub = re.search(r'published_total\{kind="telemetry"\} (\S+)', m).group(1)
lag = re.search(r"(?m)^sim_schedule_lag_seconds (\S+)", m).group(1)
rd = re.search(r'ROUTE_DEVIATION"[^}]*\} (\S+)', m)
n, t, a = g("1880/stats"), g("8001/stats"), g("8002/stats")
e = t["e2e_latency_ms"]
print(time.strftime("%H:%M:%S UTC", time.gmtime()))
print("pub", pub, "lag", lag, "rd", rd and rd.group(1), "NRrx", n["received"]["telemetry"], "kafka", n["kafka"].get("sent"))
print("ins", t["inserted"], "seen", t["vehicles_seen"], "e2e p50/p95/p99", e["p50"], e["p95"], e["p99"])
print("alerts", a["alerts_created"], "alert p50/p95", a["latency_ms"]["ALL"]["p50"], a["latency_ms"]["ALL"]["p95"])
