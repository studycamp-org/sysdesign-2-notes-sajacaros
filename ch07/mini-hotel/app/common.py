"""모든 서비스가 같이 쓰는 도구.

- 추적(trace): 요청마다 X-Trace-Id 를 서비스 사이로 넘기고, 각 서비스가 한 일(호출·SQL·캐시·상태 변화)을
  Redis 스트림 `trace` 에 남긴다. 게이트웨이가 이 스트림을 개발자 패널로 흘려보낸다.
- DB: 서비스마다 자기 계정으로 접속한다. 실행한 SQL 은 전부 추적에 남긴다.
- 설정: 개발자 패널의 실험 설정은 Redis 해시 `cfg` 에 있다.
"""
import asyncio
import contextvars
import datetime as dt
import json
import os
import re
import textwrap
import time
import uuid

import asyncpg
import httpx
import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

SERVICE = os.environ.get("SERVICE", "?")
trace_id: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default="sys")

R = aioredis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379/0"), decode_responses=True)

DEFAULT_CFG = {
    "lock": "none",            # none | pessimistic | optimistic | constraint | atomic
    "race_ms": "0",            # 재고 확인과 갱신 사이에 일부러 쉬는 시간 (경쟁 조건을 잘 보이게)
    "overbook_pct": "10",      # 초과 예약 비율
    "cache_sync_ms": "0",      # DB 변경을 캐시에 반영하기까지 걸리는 시간
    "cache_filter": "1",       # 캐시로 안 되는 요청을 미리 거르기
    "pay_mode": "success",     # success | fail | slow | lost_notify
    "pending_timeout_s": "180",
    "client_guard": "1",       # 버튼 중복 클릭 막기 (클라이언트 측)
}


async def cfg() -> dict:
    d = await R.hgetall("cfg")
    return {**DEFAULT_CFG, **d}


# ── 추적 ───────────────────────────────────────────────────────────────
async def emit(kind: str, text: str, **extra):
    ev = {"t": time.time(), "trace": trace_id.get(), "svc": SERVICE, "kind": kind, "text": text, **extra}
    try:
        await R.xadd("trace", {"e": json.dumps(ev, ensure_ascii=False, default=str)}, maxlen=4000, approximate=True)
    except Exception:
        pass


def new_trace(prefix="sys") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def make_app(title: str) -> FastAPI:
    app = FastAPI(title=title)

    @app.middleware("http")
    async def _trace(request: Request, call_next):
        trace_id.set(request.headers.get("x-trace-id") or new_trace(SERVICE))
        return await call_next(request)

    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: "ApiError"):
        return JSONResponse({"error": exc.code, "message": exc.message, **exc.extra}, status_code=exc.status)

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    return app


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, **extra):
        self.status, self.code, self.message, self.extra = status, code, message, extra


# ── DB ─────────────────────────────────────────────────────────────────
def _lit(v):
    if v is None:
        return "NULL"
    if isinstance(v, (str, dt.date)):
        return f"'{v}'"
    if isinstance(v, (list, tuple)):
        return "ARRAY[" + ",".join(_lit(x) for x in v) + "]"
    return str(v)


def show_sql(q: str, args) -> str:
    q = textwrap.dedent(q).strip("\n")
    return re.sub(r"\$(\d+)", lambda m: _lit(args[int(m.group(1)) - 1]), q)


class DB:
    """asyncpg 풀을 감싸 SQL 을 추적에 남긴다."""

    def __init__(self):
        self.pool: asyncpg.Pool | None = None

    async def open(self):
        dsn = os.environ["DB_DSN"]
        for _ in range(60):
            try:
                self.pool = await asyncpg.create_pool(dsn, min_size=2, max_size=40)
                return
            except Exception:
                await asyncio.sleep(1)
        raise RuntimeError("DB 연결 실패")

    async def _run(self, conn, how, q, args, quiet=False):
        t0 = time.perf_counter()
        try:
            res = await getattr(conn, how)(q, *args)
        except Exception as e:
            if not quiet:
                await emit("sql", show_sql(q, args), ms=round((time.perf_counter() - t0) * 1000, 1),
                           error=f"{type(e).__name__}: {e}")
            raise
        ms = round((time.perf_counter() - t0) * 1000, 1)
        if not quiet:
            if how == "execute":
                info = res
            elif how == "fetch":
                info = f"{len(res)} rows"
            else:
                info = "1 row" if res is not None else "0 rows"
            await emit("sql", show_sql(q, args), ms=ms, info=info)
        return res

    async def fetch(self, conn, q, *a, quiet=False):
        return await self._run(conn, "fetch", q, a, quiet)

    async def fetchrow(self, conn, q, *a, quiet=False):
        return await self._run(conn, "fetchrow", q, a, quiet)

    async def execute(self, conn, q, *a, quiet=False):
        return await self._run(conn, "execute", q, a, quiet)

    def tx(self, conn):
        return _Tx(conn)


class _Tx:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        self.t = self.conn.transaction()
        await self.t.start()
        await emit("tx", "BEGIN")
        return self

    async def __aexit__(self, et, e, tb):
        if et is None:
            await self.t.commit()
            await emit("tx", "COMMIT")
        else:
            await self.t.rollback()
            await emit("tx", "ROLLBACK", reason=f"{et.__name__}: {e}" if e else et.__name__)
        return False


# ── 서비스 사이 호출 ─────────────────────────────────────────────────────
_http = httpx.AsyncClient(timeout=30)


async def call(method: str, svc: str, path: str, **kw) -> httpx.Response:
    headers = {"x-trace-id": trace_id.get(), **kw.pop("headers", {})}
    await emit("call", f"{method} {path}", to=svc)
    t0 = time.perf_counter()
    r = await _http.request(method, f"http://{svc}:8000{path}", headers=headers, **kw)
    await emit("ret", f"{r.status_code} {method} {path}", to=svc, status=r.status_code,
               ms=round((time.perf_counter() - t0) * 1000, 1))
    return r


def parse_date(s: str) -> dt.date:
    try:
        return dt.date.fromisoformat(s)
    except Exception:
        raise ApiError(400, "bad_date", f"날짜 형식이 잘못됐습니다: {s}")


def nights(start: dt.date, end: dt.date) -> list[dt.date]:
    return [start + dt.timedelta(days=i) for i in range((end - start).days)]


async def bg(coro_fn, every: float):
    """주기 작업. 예외가 나도 계속 돈다."""
    while True:
        try:
            await coro_fn()
        except Exception as e:  # noqa
            print(f"[{SERVICE}] 주기 작업 오류: {e}", flush=True)
        await asyncio.sleep(every)
