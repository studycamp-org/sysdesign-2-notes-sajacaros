"""서비스 탐색 (etcd/ZooKeeper 역할, 그림 9).

- POST /register  {name, role, url, meta}: 박동. TTL(6초) 안에 다시 안 오면 목록에서 지운다.
- GET  /services?role=sim&after=N: 버전이 N 보다 커질 때까지 기다렸다 준다(롱 폴링 = 변경 알림).
"""
import asyncio
import logging
import time

from aiohttp import web

from common import json_response, serve

TTL = 6.0
log = logging.getLogger("discovery")
services: dict[str, dict] = {}
version = 0
changed = asyncio.Condition()


async def bump():
    global version
    async with changed:
        version += 1
        changed.notify_all()


async def register(req: web.Request):
    body = await req.json()
    name = body["name"]
    new = name not in services or services[name]["url"] != body["url"]
    services[name] = {**body, "last_seen": time.time()}
    if new:
        log.info("등록: %s (%s)", name, body["role"])
        await bump()
    return json_response({"version": version})


def snapshot(role: str | None):
    items = [s for s in services.values() if role is None or s["role"] == role]
    return {"version": version, "services": sorted(items, key=lambda s: s["name"])}


async def list_(req: web.Request):
    role = req.query.get("role")
    after = int(req.query.get("after", "-1"))
    if version <= after:
        try:
            async with changed:
                await asyncio.wait_for(changed.wait_for(lambda: version > after), timeout=10)
        except asyncio.TimeoutError:
            pass
    return json_response(snapshot(role))


async def reaper():
    while True:
        await asyncio.sleep(1)
        now = time.time()
        dead = [n for n, s in services.items() if now - s["last_seen"] > TTL]
        for n in dead:
            log.info("TTL 만료로 제거: %s", n)
            del services[n]
        if dead:
            await bump()


async def main():
    await serve([web.post("/register", register), web.get("/services", list_)])
    log.info("discovery 시작")
    await reaper()


if __name__ == "__main__":
    asyncio.run(main())
