"""근접 친구(Nearby Friends) 시뮬레이터 백엔드.

가상 면접 사례로 배우는 대규모 시스템 설계 기초 2 - 2장.

구성
- 위치 캐시   : Redis  `location:{uid}` = {"id","x","y","ts"} (TTL)
- 위치 전파   : Redis Pub/Sub  사용자마다 채널 `friend_loc:{uid}`
- 위치 갱신   : HTTP POST /api/users/{uid}/location  → SET + PUBLISH
- 근접 친구 수신: SSE GET /api/users/{uid}/stream
    SSE 핸들러(=책의 WebSocket 서버 역할)가 친구 위치를 메모리에 들고 있다가
    친구가 움직이면 반경 안일 때만 클라이언트로 내보내고,
    내가 움직이면 메모리의 친구 위치로 근접 목록을 다시 계산한다.
"""

import asyncio
import json
import math
import os
import random
import time
from contextlib import asynccontextmanager
from pathlib import Path

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
LOCATION_TTL = int(os.getenv("LOCATION_TTL", "3600"))  # 초. 비활성 사용자는 캐시에서 사라진다
MAP_W, MAP_H = 4000, 3000  # 가상 맵 크기(m)
DEFAULT_RADIUS = 500  # 근접 반경(m)

NAMES = [
    "민준", "서연", "도윤", "하윤", "시우", "지우", "주원", "서윤",
    "하준", "지유", "은우", "채원", "유준", "수아", "건우", "지아",
    "선우", "다은", "연우", "예은", "현우", "소율", "지호", "윤서",
]
USERS = [{"id": f"u{i + 1:02d}", "name": n} for i, n in enumerate(NAMES)]
USER_IDS = [u["id"] for u in USERS]

STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

rds: aioredis.Redis


def loc_key(uid: str) -> str:
    return f"location:{uid}"


def channel(uid: str) -> str:
    return f"friend_loc:{uid}"


def friends_of(uid: str) -> list[str]:
    # 모두가 서로 친구라고 가정
    return [u for u in USER_IDS if u != uid]


def distance(a: dict, b: dict) -> float:
    return math.hypot(a["x"] - b["x"], a["y"] - b["y"])


def sse(event: str, data) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def log(tag: str, msg: str) -> str:
    return sse("log", {"tag": tag, "msg": msg})


def check_user(uid: str) -> None:
    if uid not in USER_IDS:
        raise HTTPException(404, f"unknown user {uid}")


async def save_and_publish(uid: str, x: float, y: float) -> int:
    """위치 캐시 갱신 + 내 채널로 발행. 발행을 받은 구독자 수를 돌려준다."""
    payload = json.dumps({"id": uid, "x": round(x, 1), "y": round(y, 1), "ts": time.time()})
    async with rds.pipeline(transaction=False) as p:
        p.set(loc_key(uid), payload, ex=LOCATION_TTL)
        p.publish(channel(uid), payload)
        _, receivers = await p.execute()
    return receivers


async def seed(force: bool = False) -> None:
    rng = random.Random()
    for uid in USER_IDS:
        if not force and await rds.exists(loc_key(uid)):
            continue
        x = rng.uniform(200, MAP_W - 200)
        y = rng.uniform(200, MAP_H - 200)
        await save_and_publish(uid, x, y)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global rds
    rds = aioredis.from_url(REDIS_URL, decode_responses=True)
    await rds.ping()
    await seed()
    yield
    await rds.aclose()


app = FastAPI(title="Nearby Friends Simulator", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/config")
async def config():
    return {
        "users": USERS,
        "map": {"w": MAP_W, "h": MAP_H},
        "default_radius": DEFAULT_RADIUS,
        "ttl": LOCATION_TTL,
    }


class Location(BaseModel):
    x: float
    y: float


@app.post("/api/users/{uid}/location")
async def update_location(uid: str, loc: Location):
    check_user(uid)
    x = min(max(loc.x, 0), MAP_W)
    y = min(max(loc.y, 0), MAP_H)
    receivers = await save_and_publish(uid, x, y)
    return {"ok": True, "receivers": receivers}


@app.post("/api/admin/reset")
async def reset():
    await seed(force=True)
    return {"ok": True}


SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}


@app.get("/api/users/{uid}/stream")
async def user_stream(uid: str, request: Request, radius: int = DEFAULT_RADIUS):
    check_user(uid)
    radius = max(50, min(radius, 3000))
    return StreamingResponse(
        nearby_handler(uid, radius, request), media_type="text/event-stream", headers=SSE_HEADERS
    )


async def nearby_handler(uid: str, radius: int, request: Request):
    """사용자 한 명의 SSE 핸들러. 친구 위치를 메모리(friend_locs)에 보관한다."""
    pubsub = rds.pubsub()
    try:
        # [초기화 1] 클라이언트가 HTTP로 먼저 보낸 내 위치를 캐시에서 읽는다
        raw = await rds.get(loc_key(uid))
        if raw is None:
            yield sse("error", {"msg": "캐시에 내 위치가 없습니다. 먼저 POST /location 을 호출하세요."})
            return
        me = json.loads(raw)
        yield log("CACHE", f"GET {loc_key(uid)} → 내 위치 ({me['x']:.0f}, {me['y']:.0f})")

        # [초기화 2] 친구 채널 구독. MGET보다 먼저 구독해야 그 사이의 갱신을 놓치지 않는다
        friends = friends_of(uid)
        await pubsub.subscribe(channel(uid), *[channel(f) for f in friends])
        yield log("PUBSUB", f"SUBSCRIBE 친구 채널 {len(friends)}개 + 내 채널 1개")

        # [초기화 3] 모든 친구의 위치를 한 번에 가져와 핸들러 메모리에 저장
        raws = await rds.mget([loc_key(f) for f in friends])
        friend_locs: dict[str, dict] = {f: json.loads(r) for f, r in zip(friends, raws) if r}
        yield log(
            "CACHE",
            f"MGET 친구 {len(friends)}명 → {len(friend_locs)}명 위치를 핸들러 메모리에 저장",
        )

        # [초기화 4] 반경 안의 친구만 골라 SSE로 전송
        def entry(fid: str) -> dict:
            loc = friend_locs[fid]
            return {"id": fid, "x": loc["x"], "y": loc["y"], "distance": round(distance(me, loc))}

        def compute_nearby() -> set[str]:
            return {f for f, loc in friend_locs.items() if distance(me, loc) <= radius}

        def nearby_list() -> list[dict]:
            return sorted((entry(f) for f in nearby), key=lambda e: e["distance"])

        nearby = compute_nearby()
        yield log("FILTER", f"반경 {radius}m 이내 친구 {len(nearby)}명")
        yield sse(
            "init",
            {"me": me, "radius": radius, "friends_cached": len(friend_locs), "nearby": nearby_list()},
        )

        stats = {"received": 0, "forwarded": 0, "filtered": 0, "self_moves": 0, "cached": len(friend_locs)}
        sent_stats = None
        last_stats = last_ping = time.monotonic()

        while not await request.is_disconnected():
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg is not None:
                data = json.loads(msg["data"])
                fid = data["id"]
                if fid == uid:
                    # 내가 움직임 → 메모리의 친구 위치로 근접 목록 재계산
                    me = data
                    stats["self_moves"] += 1
                    new_nearby = compute_nearby()
                    entered, left = new_nearby - nearby, nearby - new_nearby
                    nearby = new_nearby
                    yield sse(
                        "nearby_list",
                        {"me": me, "nearby": nearby_list(), "entered": sorted(entered), "left": sorted(left)},
                    )
                else:
                    # 친구가 움직임 → 메모리 갱신 후 반경 안일 때만 전송
                    stats["received"] += 1
                    friend_locs[fid] = data
                    stats["cached"] = len(friend_locs)
                    d = distance(me, data)
                    if d <= radius:
                        entered = fid not in nearby
                        nearby.add(fid)
                        stats["forwarded"] += 1
                        yield sse("friend_update", {**entry(fid), "entered": entered})
                    elif fid in nearby:
                        # 반경 밖으로 나간 순간 한 번만 알려 준다
                        nearby.discard(fid)
                        stats["forwarded"] += 1
                        yield sse("friend_out", {"id": fid, "distance": round(d)})
                    else:
                        stats["filtered"] += 1

            now = time.monotonic()
            if now - last_stats >= 0.5 and stats != sent_stats:
                sent_stats = dict(stats)
                last_stats = now
                yield sse("stats", sent_stats)
            if now - last_ping >= 15:
                last_ping = now
                yield ": ping\n\n"
    finally:
        try:
            await pubsub.unsubscribe()
            await pubsub.aclose()
        except Exception:
            pass


@app.get("/api/admin/stream")
async def admin_stream(request: Request):
    """관제용 스트림: 맵에 모든 사용자를 그리기 위한 것 (근접 친구 프로토콜과는 별개)."""
    return StreamingResponse(admin_handler(request), media_type="text/event-stream", headers=SSE_HEADERS)


async def admin_handler(request: Request):
    pubsub = rds.pubsub()
    try:
        await pubsub.psubscribe(channel("*"))
        raws = await rds.mget([loc_key(u) for u in USER_IDS])
        yield sse("snapshot", {"locations": [json.loads(r) for r in raws if r]})
        last_ping = time.monotonic()
        while not await request.is_disconnected():
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg is not None:
                yield f"event: location\ndata: {msg['data']}\n\n"
            if time.monotonic() - last_ping >= 15:
                last_ping = time.monotonic()
                yield ": ping\n\n"
    finally:
        try:
            await pubsub.punsubscribe()
            await pubsub.aclose()
        except Exception:
            pass


if __name__ == "__main__":
    import uvicorn

    asyncio.run(uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8000)).serve())
