"""Authentication (JWT) and role-based access control.

Users are configured through ``API_USERS`` ("user:password:role,...") and their
passwords are bcrypt-hashed at start-up; plaintext is never stored. In a cloud
deployment the variable comes from AWS Secrets Manager / a Kubernetes Secret.

Roles:  viewer   - read dashboards
        operator - + acknowledge/resolve alerts, trigger demo scenarios
        admin    - + everything
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass

import bcrypt
import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

ROLE_RANK = {"viewer": 0, "operator": 1, "admin": 2}
JWT_SECRET = os.getenv("JWT_SECRET", "change-me-in-production-please-32+chars")
JWT_ALG = "HS256"
JWT_TTL_S = int(os.getenv("JWT_TTL_SECONDS", str(8 * 3600)))

_bearer = HTTPBearer(auto_error=False)


@dataclass
class User:
    username: str
    role: str


def _load_users() -> dict[str, tuple[bytes, str]]:
    raw = os.getenv("API_USERS", "admin:admin123:admin,operator:operator123:operator,viewer:viewer123:viewer")
    users = {}
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        name, pwd, role = entry.split(":", 2)
        if role not in ROLE_RANK:
            raise ValueError(f"unknown role {role!r} for user {name}")
        users[name] = (bcrypt.hashpw(pwd.encode(), bcrypt.gensalt(rounds=10)), role)
    return users


USERS = _load_users()

# naive login rate limit: max 10 attempts / minute / client ip
_attempts: dict[str, list[float]] = {}


def check_rate_limit(ip: str) -> None:
    now = time.time()
    recent = [t for t in _attempts.get(ip, []) if now - t < 60]
    if len(recent) >= 10:
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "too many login attempts, try again in a minute")
    recent.append(now)
    _attempts[ip] = recent


def authenticate(username: str, password: str) -> User | None:
    rec = USERS.get(username)
    if not rec or not bcrypt.checkpw(password.encode(), rec[0]):
        return None
    return User(username, rec[1])


def issue_token(user: User) -> str:
    now = int(time.time())
    return jwt.encode({"sub": user.username, "role": user.role, "iat": now, "exp": now + JWT_TTL_S,
                       "iss": "av-fleet-api"}, JWT_SECRET, algorithm=JWT_ALG)


def decode_token(token: str) -> User:
    try:
        claims = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG], issuer="av-fleet-api")
    except jwt.ExpiredSignatureError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "token expired")
    except jwt.PyJWTError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid token")
    return User(claims["sub"], claims.get("role", "viewer"))


async def current_user(request: Request, cred: HTTPAuthorizationCredentials | None = Depends(_bearer)) -> User:
    if cred is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "missing bearer token",
                            headers={"WWW-Authenticate": "Bearer"})
    return decode_token(cred.credentials)


def require_role(min_role: str):
    async def dep(user: User = Depends(current_user)) -> User:
        if ROLE_RANK.get(user.role, -1) < ROLE_RANK[min_role]:
            raise HTTPException(status.HTTP_403_FORBIDDEN, f"requires role '{min_role}'")
        return user
    return dep
