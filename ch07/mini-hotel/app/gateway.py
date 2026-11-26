"""API Gateway.

GATEWAY_MODE=public   손님용 (Public API Gateway). 정적 파일(CDN 역할), 인증, 처리율 제한, 서비스로 라우팅.
GATEWAY_MODE=internal 호텔 직원용 (Internal API). 관리자 화면과 Hotel Management Service 로 라우팅.
"""
import hashlib
import hmac
import os
import time

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse

from catalog import GUESTS
from common import DB, DEFAULT_CFG, R, call, cfg, emit, make_app, trace_id

MODE = os.environ.get("GATEWAY_MODE", "public")
SECRET = b"mini-hotel-demo"
STATIC = os.path.join(os.path.dirname(__file__), "static")
RATE_LIMIT = 40  # 사용자당 초당 요청 수

app = make_app(f"gateway-{MODE}")
db = DB()

ROUTES = [  # (접두어, 서비스, 로그인 필요)
    ("hotels", "hotel", False),
    ("rates", "rate", False),
    ("inventory", "reservation", False),
    ("reservation-orders", "reservation", True),
    ("reservations", "reservation", True),
    ("payments", "payment", True),
]


@app.on_event("startup")
async def startup():
    if MODE != "public":
        return
    await db.open()
    async with db.pool.acquire() as c:
        for name, email in GUESTS:
            await c.execute("INSERT INTO guest (name, email) VALUES ($1, $2) ON CONFLICT DO NOTHING", name, email)


def sign(gid: int) -> str:
    return f"{gid}.{hmac.new(SECRET, str(gid).encode(), hashlib.sha256).hexdigest()[:16]}"


def verify(token: str | None) -> int | None:
    if not token or "." not in token:
        return None
    gid = token.split(".")[0]
    return int(gid) if gid.isdigit() and hmac.compare_digest(sign(int(gid)), token) else None


def _resp(r):
    return Response(r.content, status_code=r.status_code, media_type=r.headers.get("content-type"))


# ── 손님용 ─────────────────────────────────────────────────────────────
if MODE == "public":

    @app.get("/")
    async def index():
        return FileResponse(os.path.join(STATIC, "index.html"), headers={"cache-control": "no-cache"})

    @app.post("/api/auth/login")
    async def login(body: dict):
        name = str(body.get("name", "")).strip()[:20]
        if not name:
            return JSONResponse({"message": "이름을 입력해 주세요"}, status_code=400)
        async with db.pool.acquire() as c:
            row = await db.fetchrow(c, """
                INSERT INTO guest (name, email) VALUES ($1, $2)
                ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                RETURNING guest_id, name, email""", name, f"{abs(hash(name)) % 100000}@example.com")
        return {"token": sign(row["guest_id"]), "guest": dict(row)}

    @app.get("/api/auth/guests")
    async def guests():
        async with db.pool.acquire() as c:
            rows = await c.fetch("SELECT guest_id, name FROM guest WHERE name NOT LIKE '봇%' ORDER BY guest_id")
        return [dict(r) for r in rows]

    @app.api_route("/api/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
    async def proxy(path: str, request: Request):
        t0 = time.perf_counter()
        head = path.split("/")[0]
        route = next((r for r in ROUTES if r[0] == head), None)
        await emit("req", f"{request.method} /v1/{path}" + (f"?{request.url.query}" if request.url.query else ""))
        if not route:
            return JSONResponse({"message": "없는 API"}, status_code=404)
        _, svc, need_auth = route
        gid = verify((request.headers.get("authorization") or "").removeprefix("Bearer "))
        if need_auth and gid is None:
            await emit("res", "401 로그인 필요", status=401)
            return JSONResponse({"error": "login", "message": "로그인이 필요합니다"}, status_code=401)
        # 처리율 제한: 사용자(없으면 IP)당 초당 RATE_LIMIT 개
        who = gid or request.client.host
        k = f"rl:{who}:{int(time.time())}"
        n = await R.incr(k)
        if n == 1:
            await R.expire(k, 2)
        if n > RATE_LIMIT:
            await emit("res", f"429 처리율 제한 ({RATE_LIMIT}/초)", status=429)
            return JSONResponse({"message": "요청이 너무 많습니다"}, status_code=429)
        headers = {"x-guest-id": str(gid)} if gid else {}
        r = await call(request.method, svc, f"/v1/{path}", params=request.query_params,
                       content=await request.body(), headers={**headers, "content-type": "application/json"})
        await emit("res", f"{r.status_code} · {(time.perf_counter() - t0) * 1000:.0f}ms", status=r.status_code)
        return _resp(r)

    # ── 개발자 패널 ────────────────────────────────────────────────────
    @app.get("/dev/events")
    async def events(request: Request):
        async def gen():
            last = "$"
            yield "retry: 2000\n\n"
            while not await request.is_disconnected():
                res = await R.xread({"trace": last}, block=10000, count=300)
                if not res:
                    yield ": ping\n\n"
                    continue
                for _, entries in res:
                    for id_, f in entries:
                        last = id_
                        yield f"data: {f['e']}\n\n"
        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers={"cache-control": "no-cache", "x-accel-buffering": "no"})

    @app.get("/dev/cfg")
    async def get_cfg():
        return await cfg()

    @app.put("/dev/cfg")
    async def put_cfg(body: dict):
        before = await cfg()
        upd = {k: str(v) for k, v in body.items() if k in DEFAULT_CFG}
        if upd:
            await R.hset("cfg", mapping=upd)
        await emit("cfg", "설정 변경: " + ", ".join(f"{k}={v}" for k, v in upd.items()))
        if "lock" in upd or "overbook_pct" in upd:
            if before["lock"] != upd.get("lock", before["lock"]) or before["overbook_pct"] != upd.get(
                    "overbook_pct", before["overbook_pct"]):
                await call("POST", "reservation", "/dev/sync-constraint")
        return await cfg()

    @app.post("/dev/bots")
    async def bots(body: dict):
        n = min(int(body.get("n", 5)), 30)
        out = []
        async with db.pool.acquire() as c:
            for i in range(1, n + 1):
                row = await c.fetchrow("""INSERT INTO guest (name, email) VALUES ($1, $2)
                    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING guest_id, name""",
                                       f"봇{i:02d}", f"bot{i:02d}@example.com")
                out.append({"token": sign(row["guest_id"]), "name": row["name"]})
        return out

    DEV_ROUTES = {
        "inventory": ("GET", "reservation", "/dev/inventory"),
        "reservations": ("GET", "reservation", "/dev/reservations"),
        "last-room": ("POST", "reservation", "/dev/last-room"),
        "payments": ("GET", "payment", "/dev/payments"),
        "cross-schema": ("POST", "payment", "/dev/cross-schema"),
    }

    @app.api_route("/dev/{name}", methods=["GET", "POST"])
    async def dev(name: str, request: Request):
        if name == "reset":
            await call("POST", "payment", "/dev/reset")
            return _resp(await call("POST", "reservation", "/dev/reset"))
        if name not in DEV_ROUTES:
            return JSONResponse({"message": "없음"}, status_code=404)
        m, svc, p = DEV_ROUTES[name]
        r = await call(m, svc, p, params=request.query_params, content=await request.body(),
                       headers={"content-type": "application/json"})
        return _resp(r)

# ── 직원용 ─────────────────────────────────────────────────────────────
else:

    @app.get("/")
    async def admin():
        return FileResponse(os.path.join(STATIC, "admin.html"), headers={"cache-control": "no-cache"})

    @app.api_route("/internal/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
    async def internal(path: str, request: Request):
        await emit("req", f"[직원] {request.method} /internal/{path}")
        r = await call(request.method, "mgmt", f"/internal/{path}", params=request.query_params,
                       content=await request.body(), headers={"content-type": "application/json"})
        await emit("res", f"{r.status_code}", status=r.status_code)
        return _resp(r)
