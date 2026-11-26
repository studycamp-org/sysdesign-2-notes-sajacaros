"""Rate Service — 미래 날짜별 객실 요금. 요금은 날짜마다 다르다(주말·성수기)."""
import asyncio

from catalog import HOTELS
from common import ApiError, DB, bg, make_app, nights, parse_date

app = make_app("rate")
db = DB()

FILL_SQL = """
INSERT INTO room_type_rate (hotel_id, room_type_id, date, rate)
SELECT b.hotel_id, b.room_type_id, d::date,
       round(b.base
             * CASE WHEN extract(isodow FROM d) IN (5, 6) THEN 1.25 ELSE 1 END
             * CASE WHEN extract(month FROM d) IN (7, 8, 12) THEN 1.2 ELSE 1 END, -3)::int
FROM unnest($1::int[], $2::int[], $3::int[]) AS b(hotel_id, room_type_id, base),
     generate_series(current_date, current_date + 729, interval '1 day') AS d
ON CONFLICT DO NOTHING
"""


async def fill():
    """앞으로 2년치 요금 행을 채운다 (매일 도는 배치 흉내)."""
    hs, ts, bs = [], [], []
    for h in HOTELS:
        for t in h["types"]:
            hs.append(h["hotel_id"]); ts.append(t["room_type_id"]); bs.append(t["base"])
    async with db.pool.acquire() as c:
        await c.execute(FILL_SQL, hs, ts, bs)


@app.on_event("startup")
async def startup():
    await db.open()
    await fill()
    asyncio.create_task(bg(fill, 3600))


@app.get("/v1/rates")
async def quote(hotel_id: int, room_type_id: int, start: str, end: str, rooms: int = 1):
    """한 객실 유형의 숙박 요금. 체크인 날부터 체크아웃 전날까지 밤마다 더한다."""
    s, e = parse_date(start), parse_date(end)
    if e <= s:
        raise ApiError(400, "bad_range", "체크아웃은 체크인 다음 날 이후여야 합니다")
    async with db.pool.acquire() as c:
        rows = await db.fetch(c, """
            SELECT date, rate FROM room_type_rate
            WHERE hotel_id = $1 AND room_type_id = $2 AND date >= $3 AND date < $4
            ORDER BY date""", hotel_id, room_type_id, s, e)
    if len(rows) != len(nights(s, e)):
        raise ApiError(404, "no_rate", "그 기간의 요금이 아직 없습니다")
    nightly = [{"date": str(r["date"]), "rate": r["rate"]} for r in rows]
    sub = sum(r["rate"] for r in rows)
    return {"nightly": nightly, "per_room": sub, "rooms": rooms, "total": sub * rooms}


@app.get("/v1/rates/hotels")
async def hotel_totals(start: str, end: str, hotel_id: int | None = None):
    """호텔(들)의 객실 유형별 기간 합계 요금. 목록·상세 페이지용."""
    s, e = parse_date(start), parse_date(end)
    async with db.pool.acquire() as c:
        rows = await db.fetch(c, """
            SELECT hotel_id, room_type_id, sum(rate)::int AS total, count(*)::int AS n
            FROM room_type_rate
            WHERE ($3::int IS NULL OR hotel_id = $3) AND date >= $1 AND date < $2
            GROUP BY hotel_id, room_type_id""", s, e, hotel_id)
    return {f"{r['room_type_id']}": r["total"] for r in rows if r["n"] == (e - s).days}
