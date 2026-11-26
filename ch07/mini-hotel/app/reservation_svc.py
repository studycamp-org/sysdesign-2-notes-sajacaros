"""Reservation Service — 예약을 받고 객실 재고를 관리한다.

예약(reservation)과 재고(room_type_inventory)가 같은 스키마에 있어 한 트랜잭션으로 처리한다.
이중 예약은 두 갈래로 막는다.
  ① 같은 사용자의 중복 제출 → reservation_id(멱등 키)를 기본 키로 → 유니크 제약
  ② 여러 사용자의 동시 예약 → 실험 패널에서 고른 락 방식
"""
import asyncio
import datetime as dt
import json
import math
import threading
import time

import asyncpg

from catalog import HOTELS
from common import (ApiError, DB, R, bg, call, cfg, emit, make_app, new_trace, nights, parse_date,
                    trace_id)
from fastapi import Header

app = make_app("reservation")
db = DB()

LOCKS = {
    "none": "잠금 없음",
    "pessimistic": "비관적 락 (SELECT … FOR UPDATE)",
    "optimistic": "낙관적 락 (version)",
    "constraint": "DB 제약 조건 (CHECK)",
    "atomic": "조건부 UPDATE 한 문장",
}


# ── 전역 고유 ID 생성기 (스노플레이크 흉내) ───────────────────────────────
class Snowflake:
    EPOCH = 1767225600000  # 2026-01-01 UTC

    def __init__(self, worker=1):
        self.worker, self.seq, self.last = worker, 0, -1
        self.lock = threading.Lock()

    def next(self) -> int:
        with self.lock:
            now = int(time.time() * 1000)
            if now == self.last:
                self.seq = (self.seq + 1) & 0xFFF
                if self.seq == 0:
                    while now <= self.last:
                        now = int(time.time() * 1000)
            else:
                self.seq = 0
            self.last = now
            return ((now - self.EPOCH) << 22) | (self.worker << 12) | self.seq


ids = Snowflake()


# ── 재고 행 미리 만들기 (매일 도는 배치) ──────────────────────────────────
FILL_SQL = """
INSERT INTO room_type_inventory (hotel_id, room_type_id, date, total_inventory, total_reserved)
SELECT b.hotel_id, b.room_type_id, d::date, b.inv,
       -- 처음 띄울 때 사이트가 비어 보이지 않도록 다른 채널에서 들어온 예약이 있는 것처럼 채운다
       CASE WHEN $4 THEN floor(b.inv * (0.35 + random() * 0.45))::int ELSE 0 END
FROM unnest($1::int[], $2::int[], $3::int[]) AS b(hotel_id, room_type_id, inv),
     generate_series(current_date, current_date + 729, interval '1 day') AS d
ON CONFLICT DO NOTHING
"""


def _catalog_arrays():
    hs, ts, inv = [], [], []
    for h in HOTELS:
        for t in h["types"]:
            hs.append(h["hotel_id"]); ts.append(t["room_type_id"]); inv.append(t["rooms"])
    return hs, ts, inv


async def fill(prefill=False):
    async with db.pool.acquire() as c:
        await c.execute(FILL_SQL, *_catalog_arrays(), prefill)


# ── 재고 캐시 (Redis) ─────────────────────────────────────────────────
# 키 inv:{hotel}:{room_type}:{date} → 그날 더 받을 수 있는 객실 수 (= 한도 − 예약 수)
# 지난 날짜는 쓸모가 없으므로 그 날짜가 지나면 TTL 로 저절로 사라진다.
def ckey(h, t, d):
    return f"inv:{h}:{t}:{d}"


def limit_of(total_inventory: int, overbook_pct: int) -> int:
    return math.floor(total_inventory * (100 + overbook_pct) / 100)


async def cache_put(rows, overbook):
    pipe = R.pipeline()
    for r in rows:
        k = ckey(r["hotel_id"], r["room_type_id"], r["date"])
        pipe.set(k, limit_of(r["total_inventory"], overbook) - r["total_reserved"])
        pipe.expireat(k, dt.datetime.combine(r["date"] + dt.timedelta(days=1), dt.time()))
    await pipe.execute()


async def cache_warm(days=120):
    c = await cfg()
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""SELECT * FROM room_type_inventory
                                   WHERE date >= current_date AND date < current_date + $1::int""", days)
    await cache_put(rows, int(c["overbook_pct"]))


async def cache_read(h, t, dates, conn=None) -> list[int]:
    """캐시에서 날짜별 남은 수를 읽는다. 없으면 DB 에서 읽어 채운다."""
    keys = [ckey(h, t, d) for d in dates]
    vals = await R.mget(keys)
    miss = [d for d, v in zip(dates, vals) if v is None]
    if miss:
        await emit("cache", f"MGET {len(keys)}개 → 적중 {len(keys) - len(miss)} · 없음 {len(miss)} → DB 에서 채움")
        own = conn is None
        if own:
            conn = await db.pool.acquire()
        try:
            rows = await db.fetch(conn, """SELECT * FROM room_type_inventory
                WHERE hotel_id = $1 AND room_type_id = $2 AND date = ANY($3::date[])""", h, t, miss)
        finally:
            if own:
                await db.pool.release(conn)
        await cache_put(rows, int((await cfg())["overbook_pct"]))
        vals = await R.mget(keys)
    else:
        await emit("cache", f"MGET inv:{h}:{t}:{{{dates[0]}…}} {len(keys)}개 → {', '.join(vals)}")
    return [int(v) if v is not None else 0 for v in vals]


async def cache_sync(h, t, dates, trace):
    """DB 가 바뀐 뒤 캐시에 비동기로 반영한다. 지연은 실험 패널에서 정한다."""
    trace_id.set(trace)
    delay = int((await cfg())["cache_sync_ms"])
    if delay:
        await emit("cache", f"캐시 반영 대기 {delay}ms … (그동안 캐시는 옛 값)")
        await asyncio.sleep(delay / 1000)
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""SELECT * FROM room_type_inventory
            WHERE hotel_id = $1 AND room_type_id = $2 AND date = ANY($3::date[])""", h, t, dates)
    await cache_put(rows, int((await cfg())["overbook_pct"]))
    await emit("cache", f"캐시 반영 SET inv:{h}:{t}:… {len(rows)}개 (DB → 캐시)")


def schedule_cache_sync(h, t, dates):
    asyncio.create_task(cache_sync(h, t, dates, trace_id.get()))


# ── DB 제약 조건 붙이기/떼기 ───────────────────────────────────────────
async def sync_constraint():
    c = await cfg()
    want = c["lock"] == "constraint"
    pct = int(c["overbook_pct"])
    async with db.pool.acquire() as conn:
        has = await conn.fetchval("""SELECT count(*) FROM pg_constraint
                                     WHERE conname = 'check_room_count'""")
        if has:
            await db.execute(conn, "ALTER TABLE room_type_inventory DROP CONSTRAINT check_room_count")
        if want:
            # 교재: CHECK (total_inventory - total_reserved >= 0) — 초과 예약을 허용하면 맞지 않아 한도를 반영했다
            await db.execute(conn, f"""
                ALTER TABLE room_type_inventory ADD CONSTRAINT check_room_count
                CHECK (total_reserved <= floor(total_inventory * {100 + pct} / 100.0)) NOT VALID""")
    return {"constraint": want}


# ── 시작 ───────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    await db.open()
    async with db.pool.acquire() as c:
        empty = not await c.fetchval("SELECT count(*) FROM room_type_inventory")
    await fill(prefill=empty)
    await sync_constraint()
    await cache_warm()
    asyncio.create_task(bg(lambda: fill(False), 3600))
    asyncio.create_task(bg(expire_pending, 5))


# ── 상태 바꾸기 ────────────────────────────────────────────────────────
async def set_status(conn, rid, frm, to, reason=None):
    r = await db.execute(conn, """UPDATE reservation SET status = $2, status_reason = $3, updated_at = now()
                                  WHERE reservation_id = $1 AND status = $4""", rid, to, reason, frm)
    if r.endswith(" 0"):
        return False
    await db.execute(conn, """INSERT INTO status_history (reservation_id, from_status, to_status, reason)
                              VALUES ($1, $2, $3, $4)""", rid, frm, to, reason)
    await emit("state", f"{rid} · {frm} → {to}" + (f" ({reason})" if reason else ""), rid=str(rid), frm=frm, to=to)
    return True


async def release(conn, res):
    """예약했던 재고를 돌려준다."""
    await db.execute(conn, """UPDATE room_type_inventory
        SET total_reserved = total_reserved - $4, version = version + 1
        WHERE hotel_id = $1 AND room_type_id = $2 AND date >= $3 AND date < $5""",
                     res["hotel_id"], res["room_type_id"], res["start_date"], res["room_count"], res["end_date"])


def out(r) -> dict:
    d = dict(r)
    d["reservation_id"] = str(d["reservation_id"])
    for k in ("start_date", "end_date"):
        d[k] = str(d[k])
    return d


# ── 예약 주문 만들기 (예약창 진입) ─────────────────────────────────────
@app.post("/v1/reservation-orders")
async def create_order(body: dict, x_guest_id: int = Header()):
    h, t, n = int(body["hotelID"]), int(body["roomTypeID"]), int(body.get("roomCount", 1))
    s, e = parse_date(body["startDate"]), parse_date(body["endDate"])
    if e <= s or s < dt.date.today():
        raise ApiError(400, "bad_range", "날짜를 다시 골라 주세요")
    dates = nights(s, e)
    c = await cfg()
    left = await cache_read(h, t, dates)
    if c["cache_filter"] == "1" and min(left) < n:
        await emit("info", f"캐시 기준 빈방 {min(left)}개 < 요청 {n}개 → 예약창을 열지 않음")
        raise ApiError(409, "sold_out", "선택하신 날짜에 남은 객실이 없습니다", source="cache")
    q = await call("GET", "rate", "/v1/rates", params={"hotel_id": h, "room_type_id": t, "start": str(s),
                                                       "end": str(e), "rooms": n})
    if q.status_code != 200:
        raise ApiError(q.status_code, "rate_error", q.json().get("message", "요금 조회 실패"))
    quote = q.json()
    rid = ids.next()
    await emit("id", f"reservation_id 발급 {rid} (스노플레이크: 시각 41비트 · 서버 10비트 · 순번 12비트)", rid=str(rid))
    order = {"reservationID": str(rid), "hotelID": h, "roomTypeID": t, "startDate": str(s), "endDate": str(e),
             "roomCount": n, "guestID": x_guest_id, "amount": quote["total"], "nightly": quote["nightly"],
             "expiresAt": int(time.time()) + 900}
    await R.set(f"order:{rid}", json.dumps(order), ex=900)
    await emit("cache", f"SET order:{rid} (예약창 정보, 15분 뒤 만료)")
    return order


# ── 예약 제출 ─────────────────────────────────────────────────────────
class Conflict(Exception):
    pass


async def _check(rows, n, overbook, dates):
    if len(rows) != len(dates):
        raise ApiError(404, "no_inventory", "그 기간의 재고 행이 없습니다")
    for r in rows:
        lim = limit_of(r["total_inventory"], overbook)
        if r["total_reserved"] + n > lim:
            await emit("check", f"{r['date']}: {r['total_reserved']} + {n} > {lim} (= {r['total_inventory']} × "
                                f"{100 + overbook}%) → 거절", ok=False)
            raise ApiError(409, "sold_out", "방금 다른 분이 마지막 객실을 예약했습니다", source="db")
    await emit("check", "모든 날짜 통과: " + ", ".join(
        f"{r['date'].strftime('%m-%d')} {r['total_reserved']}+{n}≤{limit_of(r['total_inventory'], overbook)}"
        for r in rows), ok=True)


async def _race_pause(c):
    ms = int(c["race_ms"])
    if ms:
        await emit("info", f"⏸ 확인과 갱신 사이 {ms}ms 쉼 (경쟁 조건 구간)")
        await asyncio.sleep(ms / 1000)


SELECT_INV = """
    SELECT date, total_inventory, total_reserved, version
    FROM room_type_inventory
    WHERE hotel_id = $1 AND room_type_id = $2
      AND date >= $3 AND date < $4
    ORDER BY date"""

UPDATE_INV = """
    UPDATE room_type_inventory
    SET total_reserved = total_reserved + $5, version = version + 1
    WHERE hotel_id = $1 AND room_type_id = $2
      AND date >= $3 AND date < $4"""


async def take_inventory(conn, c, h, t, s, e, n):
    """재고를 잡는다. 락 방식마다 다르다. 실패하면 ApiError(409) 또는 Conflict(낙관적 락 충돌)."""
    lock, ob = c["lock"], int(c["overbook_pct"])
    dates = nights(s, e)
    if lock == "atomic":
        r = await db.execute(conn, """
            UPDATE room_type_inventory
            SET total_reserved = total_reserved + $5, version = version + 1
            WHERE hotel_id = $1 AND room_type_id = $2
              AND date >= $3 AND date < $4
              AND total_reserved + $5 <= floor(total_inventory * $6 / 100.0)""", h, t, s, e, n, 100 + ob)
        changed = int(r.split()[-1])
        if changed != len(dates):
            await emit("check", f"바뀐 행 {changed} ≠ 숙박일 {len(dates)} → 한도를 넘는 날이 있음 → 롤백", ok=False)
            raise ApiError(409, "sold_out", "방금 다른 분이 마지막 객실을 예약했습니다", source="db")
        await emit("check", f"바뀐 행 {changed} = 숙박일 {len(dates)} → 성공", ok=True)
        return
    sel = SELECT_INV + (" FOR UPDATE" if lock == "pessimistic" else "")
    t0 = time.perf_counter()
    rows = await db.fetch(conn, sel, h, t, s, e)
    if lock == "pessimistic" and (w := (time.perf_counter() - t0) * 1000) > 50:
        await emit("info", f"🔒 다른 트랜잭션이 잡은 행 잠금을 {w:.0f}ms 기다렸음")
    await _check(rows, n, ob, dates)
    await _race_pause(c)
    if lock == "optimistic":
        for r in rows:
            res = await db.execute(conn, """
                UPDATE room_type_inventory
                SET total_reserved = total_reserved + $4, version = version + 1
                WHERE hotel_id = $1 AND room_type_id = $2 AND date = $3 AND version = $5""",
                                   h, t, r["date"], n, r["version"])
            if res.endswith(" 0"):
                await emit("check", f"{r['date']}: 읽었던 version {r['version']} 이 이미 바뀜 → 충돌", ok=False)
                raise Conflict()
        return
    await db.execute(conn, UPDATE_INV, h, t, s, e, n)


@app.post("/v1/reservations")
async def submit(body: dict, x_guest_id: int = Header()):
    rid = int(body["reservationID"])
    raw = await R.get(f"order:{rid}")
    if raw is None:
        # 주문이 만료됐어도 이미 예약된 건이면 같은 결과를 돌려준다(멱등)
        async with db.pool.acquire() as conn:
            ex = await db.fetchrow(conn, "SELECT * FROM reservation WHERE reservation_id = $1", rid)
        if ex:
            await emit("idem", f"이미 처리된 reservation_id {rid} → 처음 결과를 그대로 돌려줌")
            return {**out(ex), "replayed": True}
        raise ApiError(410, "order_expired", "예약 시간이 지났습니다. 처음부터 다시 진행해 주세요")
    o = json.loads(raw)
    if o["guestID"] != x_guest_id:
        raise ApiError(403, "forbidden", "다른 사용자의 예약입니다")
    h, t, n = o["hotelID"], o["roomTypeID"], o["roomCount"]
    s, e = dt.date.fromisoformat(o["startDate"]), dt.date.fromisoformat(o["endDate"])
    dates = nights(s, e)
    c = await cfg()
    await emit("info", f"락 방식: {LOCKS[c['lock']]}")

    if c["cache_filter"] == "1":
        left = await cache_read(h, t, dates)
        if min(left) < n:
            await emit("check", f"캐시 기준 빈방 {min(left)} < {n} → DB 까지 가지 않고 거절", ok=False)
            raise ApiError(409, "sold_out", "선택하신 날짜에 남은 객실이 없습니다", source="cache")

    attempts = 5 if c["lock"] == "optimistic" else 1
    for attempt in range(1, attempts + 1):
        async with db.pool.acquire() as conn:
            try:
                async with db.tx(conn):
                    await take_inventory(conn, c, h, t, s, e, n)
                    row = await db.fetchrow(conn, """
                        INSERT INTO reservation (reservation_id, hotel_id, room_type_id, start_date, end_date,
                                                 room_count, guest_id, status, amount)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8)
                        RETURNING *""", rid, h, t, s, e, n, x_guest_id, o["amount"])
                    await db.execute(conn, """INSERT INTO status_history (reservation_id, from_status, to_status)
                                              VALUES ($1, NULL, 'pending')""", rid)
                await emit("state", f"{rid} · ∅ → pending (결제 대기)", rid=str(rid), frm=None, to="pending")
                schedule_cache_sync(h, t, dates)
                return out(row)
            except asyncpg.UniqueViolationError:
                await emit("idem", f"기본 키 reservation_id={rid} 중복 → 같은 요청이 두 번 왔음. "
                                   f"앞서 잡은 재고 변경은 롤백됨, 처음 결과를 돌려줌")
                ex = await db.fetchrow(conn, "SELECT * FROM reservation WHERE reservation_id = $1", rid)
                return {**out(ex), "replayed": True}
            except asyncpg.CheckViolationError:
                await emit("check", "CHECK 제약 check_room_count 위반 → DB 가 롤백", ok=False)
                raise ApiError(409, "sold_out", "방금 다른 분이 마지막 객실을 예약했습니다", source="constraint")
            except Conflict:
                if attempt < attempts:
                    await emit("info", f"↻ 낙관적 락 재시도 {attempt + 1}/{attempts}")
                    continue
                raise ApiError(409, "conflict", "예약이 몰려 처리하지 못했습니다. 다시 시도해 주세요",
                               source="optimistic")


@app.get("/v1/reservations")
async def my_reservations(x_guest_id: int = Header()):
    async with db.pool.acquire() as conn:
        rows = await db.fetch(conn, """SELECT * FROM reservation WHERE guest_id = $1
                                       ORDER BY created_at DESC LIMIT 50""", x_guest_id)
    return [out(r) for r in rows]


@app.get("/v1/reservations/{rid}")
async def get_reservation(rid: int, x_guest_id: int = Header()):
    async with db.pool.acquire() as conn:
        r = await db.fetchrow(conn, "SELECT * FROM reservation WHERE reservation_id = $1", rid)
    if not r or r["guest_id"] != x_guest_id:
        raise ApiError(404, "not_found", "예약이 없습니다")
    return out(r)


@app.delete("/v1/reservations/{rid}")
async def cancel(rid: int, x_guest_id: int | None = Header(default=None), x_staff: str | None = Header(default=None)):
    async with db.pool.acquire() as conn:
        r = await db.fetchrow(conn, "SELECT * FROM reservation WHERE reservation_id = $1", rid)
        if not r or (r["guest_id"] != x_guest_id and not x_staff):
            raise ApiError(404, "not_found", "예약이 없습니다")
        who = "호텔 직원" if x_staff else "손님"
        if r["status"] == "pending":
            async with db.tx(conn):
                if not await set_status(conn, rid, "pending", "cancelled", f"{who} 취소"):
                    raise ApiError(409, "state_changed", "예약 상태가 바뀌었습니다. 새로고침해 주세요")
                await release(conn, r)
        elif r["status"] == "paid":
            # 결제 환불은 Payment Service 의 일이다. 다른 서비스라 한 트랜잭션으로 묶을 수 없다.
            pr = await call("POST", "payment", "/internal/payments/refund", json={"reservationID": str(rid)})
            if pr.status_code != 200:
                raise ApiError(502, "refund_failed", "환불에 실패했습니다")
            async with db.tx(conn):
                await set_status(conn, rid, "paid", "refunded", f"{who} 취소 · 환불")
                await release(conn, r)
        else:
            raise ApiError(409, "not_cancellable", "취소할 수 없는 상태입니다")
        r = await db.fetchrow(conn, "SELECT * FROM reservation WHERE reservation_id = $1", rid)
    schedule_cache_sync(r["hotel_id"], r["room_type_id"], nights(r["start_date"], r["end_date"]))
    return out(r)


@app.get("/v1/inventory")
async def availability(hotel_id: int, start: str, end: str):
    """객실 유형별로 기간 중 가장 적게 남은 날의 수. 캐시에서 읽는다(최종 판단은 예약할 때 DB)."""
    s, e = parse_date(start), parse_date(end)
    dates = nights(s, e)
    types = [t["room_type_id"] for h in HOTELS if h["hotel_id"] == hotel_id for t in h["types"]]
    outd = {}
    for t in types:
        left = await cache_read(hotel_id, t, dates)
        outd[str(t)] = max(0, min(left)) if left else 0
    return outd


@app.get("/v1/inventory/hotels")
async def availability_all(start: str, end: str):
    """목록 페이지: 호텔별로 남은 객실이 있는 유형 수."""
    s, e = parse_date(start), parse_date(end)
    dates = nights(s, e)
    keys = [(h["hotel_id"], t["room_type_id"]) for h in HOTELS for t in h["types"]]
    pipe = R.pipeline()
    for h, t in keys:
        pipe.mget([ckey(h, t, d) for d in dates])
    res = await pipe.execute()
    await emit("cache", f"MGET 호텔 {len(HOTELS)}곳 × 객실 유형 × {len(dates)}박 (목록용)")
    outd: dict = {}
    for (h, t), vals in zip(keys, res):
        left = min(int(v) if v is not None else 0 for v in vals)
        outd.setdefault(str(h), {})[str(t)] = max(0, left)
    return outd


# ── 내부 API (다른 서비스만 부른다) ─────────────────────────────────────
@app.get("/internal/reservations/{rid}")
async def internal_get(rid: int):
    async with db.pool.acquire() as conn:
        r = await db.fetchrow(conn, "SELECT * FROM reservation WHERE reservation_id = $1", rid)
    if not r:
        raise ApiError(404, "not_found", "예약이 없습니다")
    return out(r)


@app.post("/internal/reservations/{rid}/payment-result")
async def payment_result(rid: int, body: dict):
    ok = bool(body["ok"])
    async with db.pool.acquire() as conn:
        r = await db.fetchrow(conn, "SELECT * FROM reservation WHERE reservation_id = $1", rid)
        if not r:
            raise ApiError(404, "not_found", "예약이 없습니다")
        if r["status"] != "pending":
            await emit("info", f"결제 결과가 왔지만 예약은 이미 {r['status']} → 거절 (Payment Service 가 보상해야 함)")
            raise ApiError(409, "not_pending", f"예약이 이미 {r['status']} 상태입니다", current=r["status"])
        async with db.tx(conn):
            if ok:
                await set_status(conn, rid, "pending", "paid", "결제 성공")
            else:
                await set_status(conn, rid, "pending", "rejected", body.get("reason") or "결제 실패")
                await release(conn, r)
        r = await db.fetchrow(conn, "SELECT * FROM reservation WHERE reservation_id = $1", rid)
    if not ok:
        schedule_cache_sync(r["hotel_id"], r["room_type_id"], nights(r["start_date"], r["end_date"]))
    return out(r)


@app.get("/internal/hotels/{hotel_id}/reservations")
async def by_hotel(hotel_id: int):
    async with db.pool.acquire() as conn:
        rows = await db.fetch(conn, """SELECT * FROM reservation WHERE hotel_id = $1
                                       ORDER BY start_date, created_at LIMIT 200""", hotel_id)
    return [out(r) for r in rows]


@app.put("/internal/inventory")
async def set_inventory(body: dict):
    """호텔 직원이 수리 등으로 판매 객실 수를 바꾼다."""
    h, t, d, v = int(body["hotelID"]), int(body["roomTypeID"]), parse_date(body["date"]), int(body["totalInventory"])
    async with db.pool.acquire() as conn:
        try:
            await db.execute(conn, """UPDATE room_type_inventory SET total_inventory = $4, version = version + 1
                                      WHERE hotel_id = $1 AND room_type_id = $2 AND date = $3""", h, t, d, v)
        except asyncpg.CheckViolationError:
            raise ApiError(409, "constraint", "이미 예약된 수보다 적게 줄일 수 없습니다 (CHECK 제약)")
    schedule_cache_sync(h, t, [d])
    return {"ok": True}


# ── 만료: 결제하지 않은 pending 예약 풀기 ────────────────────────────────
async def expire_pending():
    secs = int((await cfg())["pending_timeout_s"])
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""SELECT * FROM reservation WHERE status = 'pending'
                                   AND created_at < now() - make_interval(secs => $1)""", secs)
        for r in rows:
            trace_id.set(new_trace("timer"))
            await emit("info", f"⏰ pending {secs}초 초과 → 재고를 풀어 줌")
            async with db.tx(conn):
                if await set_status(conn, r["reservation_id"], "pending", "cancelled", f"결제 시간 {secs}초 초과"):
                    await release(conn, r)
            schedule_cache_sync(r["hotel_id"], r["room_type_id"], nights(r["start_date"], r["end_date"]))


# ── 개발자 패널용 ──────────────────────────────────────────────────────
@app.get("/dev/inventory")
async def dev_inventory(hotel_id: int, room_type_id: int, start: str, days: int = 14):
    s = parse_date(start)
    ob = int((await cfg())["overbook_pct"])
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""SELECT * FROM room_type_inventory WHERE hotel_id = $1 AND room_type_id = $2
                                   AND date >= $3 AND date < $3 + $4::int ORDER BY date""",
                                hotel_id, room_type_id, s, days)
    cached = await R.mget([ckey(hotel_id, room_type_id, r["date"]) for r in rows]) if rows else []
    return [{"date": str(r["date"]), "total_inventory": r["total_inventory"], "total_reserved": r["total_reserved"],
             "version": r["version"], "limit": limit_of(r["total_inventory"], ob),
             "db_left": limit_of(r["total_inventory"], ob) - r["total_reserved"],
             "cache_left": None if cv is None else int(cv)} for r, cv in zip(rows, cached)]


@app.get("/dev/reservations")
async def dev_reservations(limit: int = 60):
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM reservation ORDER BY created_at DESC LIMIT $1", limit)
        hist = await conn.fetch("""SELECT * FROM status_history WHERE reservation_id = ANY($1::bigint[])
                                   ORDER BY id""", [r["reservation_id"] for r in rows])
    hmap: dict = {}
    for x in hist:
        hmap.setdefault(str(x["reservation_id"]), []).append(
            {"from": x["from_status"], "to": x["to_status"], "reason": x["reason"], "at": x["at"].isoformat()})
    return [{**out(r), "created_at": r["created_at"].isoformat(), "history": hmap.get(str(r["reservation_id"]), [])}
            for r in rows]


@app.post("/dev/last-room")
async def dev_last_room(body: dict):
    """그 날짜의 남은 객실을 left 개로 만든다 (기간 전체)."""
    h, t = int(body["hotelID"]), int(body["roomTypeID"])
    s, e = parse_date(body["startDate"]), parse_date(body["endDate"])
    left = int(body.get("left", 1))
    ob = int((await cfg())["overbook_pct"])
    async with db.pool.acquire() as conn:
        await db.execute(conn, f"""
            UPDATE room_type_inventory
            SET total_reserved = floor(total_inventory * {100 + ob} / 100.0)::int - $5, version = version + 1
            WHERE hotel_id = $1 AND room_type_id = $2 AND date >= $3 AND date < $4""", h, t, s, e, left)
    dates = nights(s, e)
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""SELECT * FROM room_type_inventory WHERE hotel_id = $1 AND room_type_id = $2
                                   AND date = ANY($3::date[])""", h, t, dates)
    await cache_put(rows, ob)
    await emit("cache", f"캐시도 바로 맞춤: 남은 객실 {left}개")
    return {"ok": True}


@app.post("/dev/sync-constraint")
async def dev_sync_constraint():
    r = await sync_constraint()
    await cache_warm()
    return r


@app.post("/dev/reset")
async def dev_reset():
    async with db.pool.acquire() as conn:
        await db.execute(conn, "TRUNCATE reservation, status_history")
        await db.execute(conn, "TRUNCATE room_type_inventory")
    await fill(prefill=True)
    keys = [k async for k in R.scan_iter("inv:*")] + [k async for k in R.scan_iter("order:*")]
    if keys:
        await R.delete(*keys)
    await sync_constraint()
    await cache_warm()
    return {"ok": True}
