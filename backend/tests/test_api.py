"""API tests: JWT/RBAC and Redis/MongoDB read paths (in-memory fakes, no external services)."""
import json
import os

os.environ.setdefault("API_USERS", "admin:a:admin,operator:o:operator,viewer:v:viewer")
os.environ["DASHBOARD_DIR"] = "/nonexistent"

import fakeredis.aioredis
import pytest
from fastapi.testclient import TestClient
from mongomock_motor import AsyncMongoMockClient

from app import main as api
from app.store import Store


@pytest.fixture
def client(monkeypatch):
    fake = Store(mongo=AsyncMongoMockClient(), redis=fakeredis.aioredis.FakeRedis(decode_responses=True))

    async def noop():
        return None
    fake.connect = noop
    monkeypatch.setattr(api, "store", fake)
    with TestClient(api.app) as c:
        c.store = fake
        yield c


def token(c, user, pwd):
    r = c.post("/api/auth/login", json={"username": user, "password": pwd})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def test_login_rejects_bad_password(client):
    assert client.post("/api/auth/login", json={"username": "admin", "password": "nope"}).status_code == 401


def test_endpoints_require_token(client):
    assert client.get("/api/fleet/summary").status_code == 401
    assert client.get("/api/fleet/summary", headers={"Authorization": "Bearer garbage"}).status_code == 401


def test_viewer_cannot_acknowledge_alerts(client):
    h = token(client, "viewer", "v")
    assert client.post("/api/alerts/alr-1/ack", headers=h).status_code == 403


def test_summary_reads_live_redis_analytics(client):
    import asyncio
    asyncio.get_event_loop().run_until_complete(
        client.store.redis.set("analytics:summary", json.dumps({"active_vehicles": 17, "status": {}})))
    h = token(client, "viewer", "v")
    r = client.get("/api/fleet/summary", headers=h)
    assert r.json()["active_vehicles"] == 17 and r.json()["source"] == "redis"


def test_operator_can_ack_alert(client):
    import asyncio
    asyncio.get_event_loop().run_until_complete(
        client.store.db.alerts.insert_one({"alert_id": "alr-1", "status": "OPEN", "severity": "CRITICAL"}))
    h = token(client, "operator", "o")
    assert client.post("/api/alerts/alr-1/ack", headers=h).status_code == 200
    doc = asyncio.get_event_loop().run_until_complete(client.store.db.alerts.find_one({"alert_id": "alr-1"}))
    assert doc["status"] == "ACKNOWLEDGED" and doc["acknowledged_by"] == "operator"


def test_vehicle_state_and_geo_search(client):
    import asyncio
    r = client.store.redis
    loop = asyncio.get_event_loop()
    loop.run_until_complete(r.hset("vehicle:vehicle-00001", mapping={"vehicle_id": "vehicle-00001", "status": "IN_SERVICE",
                                   "lat": -37.81, "lon": 144.96, "data": json.dumps({"vehicle_id": "vehicle-00001"})}))
    loop.run_until_complete(r.geoadd("fleet:geo", (144.96, -37.81, "vehicle-00001")))
    loop.run_until_complete(r.zadd("fleet:last_seen", {"vehicle-00001": 1}))
    h = token(client, "viewer", "v")
    assert client.get("/api/vehicles/vehicle-00001", headers=h).json()["vehicle_id"] == "vehicle-00001"
    near = client.get("/api/vehicles/near?lat=-37.81&lon=144.961&radius_m=500", headers=h).json()
    assert near[0]["vehicle_id"] == "vehicle-00001"
    lst = client.get("/api/vehicles?status=IN_SERVICE", headers=h).json()
    assert lst["total"] == 1


def test_websocket_rejects_missing_token(client):
    from starlette.websockets import WebSocketDisconnect
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/ws") as ws:
            ws.receive_text()
