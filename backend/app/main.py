"""Backend API (FastAPI) - the only component the dashboard talks to.

Reads come from Redis (hot, latest state) with MongoDB fall-backs (history,
and a degraded mode when Redis is unavailable). Live updates are pushed over a
WebSocket fed by the Redis ``alerts:live`` channel.

Everything displayed is produced by the running pipeline:
simulator -> MQTT -> Node-RED -> Kafka -> services -> MongoDB/Redis -> here.
There is no hard-coded or fabricated dashboard data in this service.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel

from .security import (User, authenticate, check_rate_limit, current_user, decode_token, issue_token,
                       require_role)
from .store import Store

log = logging.getLogger("api")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s %(levelname)-7s api: %(message)s")

REQS = Counter("api_requests_total", "HTTP requests", ["method", "route", "code"])
REQ_LAT = Histogram("api_request_seconds", "HTTP request latency", ["route"],
                    buckets=(.002, .005, .01, .025, .05, .1, .25, .5, 1, 2.5))
WS_CLIENTS = Counter("api_ws_connections_total", "WebSocket connections accepted")

SIMULATOR_URL = os.getenv("SIMULATOR_URL", "http://simulator:9105")
NODERED_URL = os.getenv("NODERED_URL", "http://nodered:1880")
SERVICE_URLS = {
    "telemetry-service": os.getenv("TELEMETRY_SERVICE_URL", "http://telemetry-service:8001"),
    "alert-service": os.getenv("ALERT_SERVICE_URL", "http://alert-service:8002"),
    "analytics-service": os.getenv("ANALYTICS_SERVICE_URL", "http://analytics-service:8003"),
}

store = Store()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await store.connect()
    yield
    await store.close()


app = FastAPI(title="AV Fleet API (software simulation)", version="1.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:8000").split(","),
                   allow_methods=["GET", "POST"], allow_headers=["Authorization", "Content-Type"])


@app.middleware("http")
async def observe(request: Request, call_next):
    t0 = time.perf_counter()
    response = await call_next(request)
    route = request.scope.get("route")
    path = getattr(route, "path", "static")
    REQS.labels(request.method, path, response.status_code).inc()
    REQ_LAT.labels(path).observe(time.perf_counter() - t0)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"  # map tile servers require a referrer
    return response


# ------------------------------------------------------------------ public
class Login(BaseModel):
    username: str
    password: str


@app.post("/api/auth/login")
async def login(body: Login, request: Request):
    check_rate_limit(request.client.host if request.client else "?")
    user = authenticate(body.username, body.password)
    if not user:
        raise HTTPException(401, "invalid credentials")
    return {"access_token": issue_token(user), "token_type": "bearer", "role": user.role, "username": user.username}


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.get("/readyz")
async def readyz():
    h = await store.health()
    ok = h["mongodb"] or h["redis"]
    return JSONResponse(h, status_code=200 if ok else 503)


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


# ------------------------------------------------------------------ fleet
@app.get("/api/me")
async def me(user: User = Depends(current_user)):
    return {"username": user.username, "role": user.role}


@app.get("/api/fleet/summary")
async def fleet_summary(user: User = Depends(current_user)):
    return await store.summary()


@app.get("/api/vehicles")
async def vehicles(status: str | None = None, limit: int = Query(100, le=2000), offset: int = 0,
                   user: User = Depends(current_user)):
    return await store.vehicles(status, limit, offset)


@app.get("/api/vehicles/near")
async def vehicles_near(lat: float, lon: float, radius_m: float = Query(1000, le=50_000), limit: int = 50,
                        user: User = Depends(current_user)):
    return await store.near(lat, lon, radius_m, limit)


@app.get("/api/vehicles/{vehicle_id}")
async def vehicle(vehicle_id: str, user: User = Depends(current_user)):
    v = await store.vehicle(vehicle_id)
    if not v:
        raise HTTPException(404, "vehicle not found")
    return v


@app.get("/api/vehicles/{vehicle_id}/history")
async def vehicle_history(vehicle_id: str, minutes: int = Query(10, le=1440), limit: int = Query(600, le=5000),
                          user: User = Depends(current_user)):
    return await store.history(vehicle_id, minutes, limit)


# ------------------------------------------------------------------ alerts
@app.get("/api/alerts")
async def alerts(status: str | None = None, severity: str | None = None, vehicle_id: str | None = None,
                 limit: int = Query(100, le=1000), user: User = Depends(current_user)):
    return await store.alerts(status, severity, vehicle_id, limit)


@app.get("/api/alerts/stats")
async def alert_stats(user: User = Depends(current_user)):
    return await store.alert_stats()


@app.post("/api/alerts/{alert_id}/{action}")
async def alert_action(alert_id: str, action: str, user: User = Depends(require_role("operator"))):
    if action not in ("ack", "resolve"):
        raise HTTPException(400, "action must be ack or resolve")
    ok = await store.update_alert(alert_id, "ACKNOWLEDGED" if action == "ack" else "RESOLVED", user.username)
    if not ok:
        raise HTTPException(404, "alert not found")
    return {"ok": True}


# ------------------------------------------------------------------ analytics & pipeline
@app.get("/api/analytics/throughput")
async def throughput(user: User = Depends(current_user)):
    return await store.throughput()


@app.get("/api/pipeline/health")
async def pipeline_health(user: User = Depends(current_user)):
    """Live status of every stage - used by the dashboard's pipeline strip."""
    out = {"checked_at": datetime.now(timezone.utc).isoformat()}
    out.update({k: {"up": v} for k, v in (await store.health()).items()})
    async with httpx.AsyncClient(timeout=2.0) as client:
        async def probe(name, url):
            try:
                r = await client.get(url)
                return name, {"up": r.status_code == 200, "stats": r.json()}
            except Exception as exc:
                return name, {"up": False, "error": str(exc)[:120]}
        probes = [probe("simulator", f"{SIMULATOR_URL}/status"), probe("nodered", f"{NODERED_URL}/stats")]
        probes += [probe(n, f"{u}/stats") for n, u in SERVICE_URLS.items()]
        for name, res in await asyncio.gather(*probes):
            if name == "simulator" and res.get("stats"):
                res["stats"].pop("recent_events", None)
            out[name] = res
    nr = out.get("nodered", {}).get("stats", {})
    out["kafka"] = {"up": bool(nr.get("kafka", {}).get("connected")), "via": "node-red producer status"}
    # MQTT is healthy if the simulator has live connections and Node-RED is receiving
    sim = out.get("simulator", {}).get("stats", {})
    out["mqtt"] = {"up": bool(sim.get("mqtt_connections_up")) or bool(nr.get("received", {}).get("telemetry"))}
    return out


# ------------------------------------------------------------------ demo control (operator)
class Scenario(BaseModel):
    scenario: str | None = None
    failure: str | None = None
    vehicle_id: str | None = None
    seconds: float | None = None
    count: int | None = None


@app.post("/api/simulator/trigger")
async def trigger(body: Scenario, user: User = Depends(require_role("operator"))):
    """Forward a scenario to the simulator's control API so a demo can trigger events from the UI.
    The resulting event still travels vehicle -> MQTT -> Node-RED -> Kafka -> alert-service."""
    payload = {k: v for k, v in body.model_dump().items() if v is not None}
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            path = "/failure" if body.failure else "/scenario"
            r = await client.post(f"{SIMULATOR_URL}{path}", json=payload)
            return r.json()
    except Exception as exc:
        raise HTTPException(502, f"simulator not reachable: {exc}")


# ------------------------------------------------------------------ websocket
@app.websocket("/ws")
async def ws(websocket: WebSocket, token: str = ""):
    try:
        user = decode_token(token)
    except HTTPException:
        await websocket.close(code=4401)
        return
    await websocket.accept()
    WS_CLIENTS.inc()
    log.info("websocket connected: %s", user.username)
    stop = asyncio.Event()

    async def push_alerts():
        try:
            async for alert in store.alert_stream(stop):
                await websocket.send_text(json.dumps({"type": "alert", "data": alert}))
        except Exception as exc:
            log.debug("alert stream ended: %s", exc)

    async def push_state():
        while not stop.is_set():
            try:
                summary = await store.summary()
                positions = await store.positions(int(os.getenv("WS_MAX_POSITIONS", "1500")))
                await websocket.send_text(json.dumps({"type": "snapshot", "summary": summary, "positions": positions},
                                                     default=str))
            except WebSocketDisconnect:
                break
            except Exception as exc:
                log.debug("snapshot failed: %s", exc)
                if websocket.client_state.name != "CONNECTED":
                    break
            await asyncio.sleep(float(os.getenv("WS_SNAPSHOT_INTERVAL", "2")))

    tasks = [asyncio.create_task(push_alerts()), asyncio.create_task(push_state())]
    try:
        while True:
            await websocket.receive_text()   # keep-alive / ignore client messages
    except WebSocketDisconnect:
        pass
    finally:
        stop.set()
        for t in tasks:
            t.cancel()


# ------------------------------------------------------------------ dashboard (static)
_dash = os.getenv("DASHBOARD_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "dashboard"))
if os.path.isdir(_dash):
    app.mount("/", StaticFiles(directory=_dash, html=True), name="dashboard")
