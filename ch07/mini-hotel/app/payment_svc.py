"""Payment Service — 결제를 실행하고 결과를 Reservation Service 에 알린다.

결제(payment 스키마)와 예약 상태(reservation 스키마)는 다른 서비스라 한 트랜잭션으로 묶이지 않는다.
  · 결제는 됐는데 알림이 안 가면? → 알림을 될 때까지 다시 보낸다(주기 작업).
  · 다시 보냈더니 예약이 이미 만료돼 cancelled 라면? → 보상 트랜잭션으로 환불한다.
교재의 사가(saga)를 아주 작게 해 본 것이다.
"""
import asyncio

import asyncpg

from common import ApiError, DB, R, bg, call, cfg, emit, make_app, new_trace, trace_id
from fastapi import Header

app = make_app("payment")
db = DB()


@app.on_event("startup")
async def startup():
    await db.open()
    asyncio.create_task(bg(retry_notify, 5))


async def notify(conn, p) -> str:
    """결과를 Reservation Service 에 알린다. 'ok' | 'rejected_by_reservation' | 'error'"""
    try:
        r = await call("POST", "reservation", f"/internal/reservations/{p['reservation_id']}/payment-result",
                       json={"ok": p["status"] == "charged", "reason": p["note"]})
    except Exception as e:
        await emit("info", f"알림 실패: {e}")
        return "error"
    if r.status_code == 200:
        await db.execute(conn, "UPDATE payment SET notified = true, updated_at = now() WHERE payment_id = $1",
                         p["payment_id"])
        return "ok"
    if r.status_code == 409 and p["status"] == "charged":
        # 예약은 이미 다른 상태(만료 등). 돈만 빠져나간 상태이므로 되돌린다.
        await emit("info", "보상 트랜잭션: 예약이 이미 끝났으므로 결제를 환불한다")
        await db.execute(conn, """UPDATE payment SET status = 'refunded', notified = true, updated_at = now(),
                                  note = '예약 만료로 자동 환불' WHERE payment_id = $1""", p["payment_id"])
        await emit("state", f"결제 {p['payment_id']} · charged → refunded (보상)")
        return "rejected_by_reservation"
    return "error"


@app.post("/v1/payments")
async def pay(body: dict, x_guest_id: int = Header()):
    rid = int(body["reservationID"])
    card = "".join(ch for ch in str(body.get("cardNumber", "")) if ch.isdigit())
    if len(card) < 12:
        raise ApiError(400, "bad_card", "카드 번호를 확인해 주세요")
    rr = await call("GET", "reservation", f"/internal/reservations/{rid}")
    if rr.status_code != 200:
        raise ApiError(404, "not_found", "예약이 없습니다")
    res = rr.json()
    if res["guest_id"] != x_guest_id:
        raise ApiError(403, "forbidden", "다른 사용자의 예약입니다")
    mode = (await cfg())["pay_mode"]

    async with db.pool.acquire() as conn:
        ex = await db.fetchrow(conn, "SELECT * FROM payment WHERE reservation_id = $1", rid)
        if ex:
            await emit("idem", "이 예약은 이미 결제를 시도했음 → 처음 결과를 돌려줌 (결제 중복 방지)")
            return {"status": ex["status"], "notified": ex["notified"], "replayed": True}
        if res["status"] != "pending":
            raise ApiError(409, "not_pending", f"결제할 수 없는 예약입니다 ({res['status']})")

        await emit("info", f"PG 승인 요청 · {res['amount']:,}원 · 모드 {mode}")
        if mode == "slow":
            await asyncio.sleep(4)
        status = "failed" if mode == "fail" else "charged"
        note = "카드사 승인 거절" if mode == "fail" else None
        try:
            p = await db.fetchrow(conn, """
                INSERT INTO payment (reservation_id, guest_id, amount, card_last4, status, note)
                VALUES ($1, $2, $3, $4, $5, $6) RETURNING *""", rid, x_guest_id, res["amount"], card[-4:], status, note)
        except asyncpg.UniqueViolationError:
            ex = await db.fetchrow(conn, "SELECT * FROM payment WHERE reservation_id = $1", rid)
            await emit("idem", "결제 행 유니크 제약 위반 → 같은 결제가 동시에 두 번 → 처음 결과를 돌려줌")
            return {"status": ex["status"], "notified": ex["notified"], "replayed": True}

        if mode == "lost_notify":
            await emit("info", "⚡ 결제는 됐지만 Reservation Service 에 알리는 메시지가 사라짐 (예약은 아직 pending)")
            return {"status": status, "notified": False}
        n = await notify(conn, p)
        if n == "rejected_by_reservation":
            raise ApiError(409, "expired", "결제 시간이 지나 예약이 취소됐습니다. 결제는 자동으로 환불됩니다")
        if status == "failed":
            raise ApiError(402, "declined", "카드 승인이 거절됐습니다")
        return {"status": status, "notified": n == "ok"}


async def retry_notify():
    """알리지 못한 결제 결과를 다시 보낸다."""
    if (await cfg())["pay_mode"] == "lost_notify":
        return
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM payment WHERE notified = false AND status <> 'refunded'")
        for p in rows:
            trace_id.set(new_trace("retry"))
            await emit("info", f"↻ 알리지 못한 결제 결과 재전송 · 예약 {p['reservation_id']}")
            await notify(conn, p)


@app.post("/internal/payments/refund")
async def refund(body: dict):
    rid = int(body["reservationID"])
    async with db.pool.acquire() as conn:
        r = await db.execute(conn, """UPDATE payment SET status = 'refunded', updated_at = now(), note = '취소 환불'
                                      WHERE reservation_id = $1 AND status = 'charged'""", rid)
    if r.endswith(" 0"):
        raise ApiError(404, "no_payment", "환불할 결제가 없습니다")
    await emit("info", f"PG 환불 요청 · 예약 {rid}")
    return {"ok": True}


@app.get("/dev/payments")
async def dev_payments():
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM payment ORDER BY created_at DESC LIMIT 60")
    return [{**dict(r), "reservation_id": str(r["reservation_id"]), "created_at": r["created_at"].isoformat(),
             "updated_at": r["updated_at"].isoformat()} for r in rows]


@app.post("/dev/cross-schema")
async def cross_schema():
    """Payment Service 계정으로 reservation 스키마를 직접 고쳐 본다 → 권한 오류."""
    async with db.pool.acquire() as conn:
        try:
            await db.execute(conn, "UPDATE reservation.reservation SET status = 'paid' WHERE status = 'pending'")
        except Exception as e:
            return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    return {"ok": True}


@app.post("/dev/reset")
async def dev_reset():
    async with db.pool.acquire() as conn:
        await db.execute(conn, "TRUNCATE payment")
    return {"ok": True}
