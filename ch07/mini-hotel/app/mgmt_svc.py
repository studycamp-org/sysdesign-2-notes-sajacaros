"""Hotel Management Service — 호텔 직원 전용. 자기 DB 없이 다른 서비스를 불러 일을 넘긴다 (그림 5)."""
from common import ApiError, call, make_app

app = make_app("mgmt")


def _pass(r):
    if r.status_code >= 400:
        j = r.json()
        raise ApiError(r.status_code, j.get("error", "error"), j.get("message", "오류"))
    return r.json()


@app.get("/internal/hotels")
async def hotels():
    return _pass(await call("GET", "hotel", "/v1/hotels"))


@app.get("/internal/hotels/{hotel_id}/reservations")
async def reservations(hotel_id: int):
    return _pass(await call("GET", "reservation", f"/internal/hotels/{hotel_id}/reservations"))


@app.get("/internal/hotels/{hotel_id}/inventory")
async def inventory(hotel_id: int, room_type_id: int, start: str, days: int = 14):
    return _pass(await call("GET", "reservation", "/dev/inventory",
                            params={"hotel_id": hotel_id, "room_type_id": room_type_id, "start": start, "days": days}))


@app.put("/internal/inventory")
async def set_inventory(body: dict):
    return _pass(await call("PUT", "reservation", "/internal/inventory", json=body))


@app.post("/internal/reservations/{rid}/cancel")
async def cancel(rid: str):
    return _pass(await call("DELETE", "reservation", f"/v1/reservations/{rid}", headers={"x-staff": "1"}))
