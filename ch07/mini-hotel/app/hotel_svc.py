"""Hotel Service — 호텔·객실 정보. 거의 바뀌지 않으므로 캐시하기 쉽다 (여기서는 프로세스 메모리 캐시)."""
import asyncio

from catalog import HOTELS
from common import ApiError, DB, emit, make_app

app = make_app("hotel")
db = DB()
_cache: dict = {}


@app.on_event("startup")
async def startup():
    await db.open()
    async with db.pool.acquire() as c:
        if await c.fetchval("SELECT count(*) FROM hotel"):
            return
        for h in HOTELS:
            await c.execute("INSERT INTO hotel VALUES ($1,$2,$3,$4,$5,$6,$7)",
                            h["hotel_id"], h["name"], h["city"], h["address"], h["stars"], h["scene"], h["description"])
            for t in h["types"]:
                await c.execute("INSERT INTO room_type VALUES ($1,$2,$3,$4,$5,$6,$7)",
                                t["room_type_id"], h["hotel_id"], t["name"], t["beds"], t["size"], t["cap"], t["amen"])
                for i in range(t["rooms"]):
                    floor = 2 + i // 10 + (t["room_type_id"] % 100 - 1) * 3
                    await c.execute("INSERT INTO room (hotel_id, room_type_id, room_number, floor) VALUES ($1,$2,$3,$4)",
                                    h["hotel_id"], t["room_type_id"], f"{floor}{i % 10 + 1:02d}", floor)


async def _load(conn, hotel_id=None):
    key = f"h:{hotel_id}"
    if key in _cache:
        await emit("cache", f"메모리 캐시 적중 · {key}")
        return _cache[key]
    where = "WHERE h.hotel_id = $1" if hotel_id else ""
    args = (hotel_id,) if hotel_id else ()
    hotels = await db.fetch(conn, f"SELECT * FROM hotel h {where} ORDER BY hotel_id", *args)
    types = await db.fetch(conn, f"""
        SELECT t.*, count(r.room_id) AS room_total
        FROM room_type t JOIN hotel h USING (hotel_id) LEFT JOIN room r USING (room_type_id)
        {where}
        GROUP BY t.room_type_id ORDER BY t.room_type_id""", *args)
    out = []
    for h in hotels:
        d = dict(h)
        d["room_types"] = [dict(t) for t in types if t["hotel_id"] == h["hotel_id"]]
        out.append(d)
    _cache[key] = out
    return out


@app.get("/v1/hotels")
async def list_hotels(city: str | None = None):
    async with db.pool.acquire() as c:
        hs = await _load(c)
    return [h for h in hs if not city or h["city"] == city]


@app.get("/v1/hotels/{hotel_id}")
async def get_hotel(hotel_id: int):
    async with db.pool.acquire() as c:
        hs = await _load(c, hotel_id)
    if not hs:
        raise ApiError(404, "not_found", "호텔이 없습니다")
    return hs[0]


@app.get("/v1/hotels/{hotel_id}/rooms/{room_id}")
async def get_room(hotel_id: int, room_id: int):
    async with db.pool.acquire() as c:
        r = await db.fetchrow(c, "SELECT * FROM room WHERE hotel_id = $1 AND room_id = $2", hotel_id, room_id)
    if not r:
        raise ApiError(404, "not_found", "객실이 없습니다")
    return dict(r)
