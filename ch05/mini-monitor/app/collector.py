"""지표 수집기. 받은(당겨 온) 지표를 Kafka 'metrics' 토픽에 넣는다 (그림 15).

- MODE=push: POST /write 로 라인 프로토콜을 받는다. 여러 대가 도커 DNS 라운드로빈(=로드밸런서) 뒤에 있다 (그림 13).
- MODE=pull: 서비스 탐색에서 pull 시뮬레이터 목록과 동료 수집기 목록을 감시하고(그림 10),
  안정 해시 링으로 자기 몫만 당겨 온다 (그림 11). 동료가 늘거나 죽으면 몫이 다시 나뉜다.
  응답이 없으면 up=0 을 직접 기록한다 (pull 의 장점: 죽은 서버를 바로 안다).
"""
import asyncio
import bisect
import hashlib
import logging
import os
import re
import time

from aiohttp import web

from common import (Rate, http, json_response, list_services, my_name, register_forever, serve,
                    start_producer)

MODE = os.environ.get("MODE", "push")
INTERVAL = float(os.environ.get("INTERVAL", "5"))
VNODES = 16  # 수집기 하나가 링 위에 찍는 가상 노드 수

log = logging.getLogger("collector")
NAME = my_name()
producer = None
received = Rate()
scrape_ok = Rate()
scrape_fail = Rate()
rejected = 0
by_partition_key: dict[str, int] = {}

# pull 전용 상태
peers: list[str] = []
targets: dict[str, str] = {}   # sim 이름 → url
owned: list[str] = []
ring: list[tuple[int, str]] = []
last_scrape: dict[str, dict] = {}


async def send(name: str, labels: dict, value: float, ts_ms: int, via: str):
    labels = {**labels, "via": via}
    await producer.send(  # 지표 이름이 키 → 같은 지표는 같은 파티션 (그림 16)
        "metrics", key=name,
        value={"name": name, "labels": labels, "value": value, "ts": ts_ms, "collector": NAME})
    by_partition_key[name] = by_partition_key.get(name, 0) + 1
    received.add()


# ---------- push ----------
LINE = re.compile(r"^([\w.]+),?(\S*) value=([-\d.eE+]+)(?: (\d+))?$")


async def write(req: web.Request):
    global rejected
    if producer is None:
        rejected += 1
        return web.Response(status=503)
    body = await req.text()
    n = 0
    for line in body.splitlines():
        m = LINE.match(line.strip())
        if not m:
            continue
        name, lbl, value, ts = m.groups()
        labels = dict(kv.split("=", 1) for kv in lbl.split(",") if "=" in kv)
        ts_ms = int(ts) // 1_000_000 if ts else int(time.time() * 1000)
        await send(name, labels, float(value), ts_ms, "push")
        n += 1
    return web.Response(status=204)


# ---------- pull ----------
def h32(s: str) -> int:
    return int(hashlib.md5(s.encode()).hexdigest()[:8], 16)


def rebuild():
    """안정 해시 링: 수집기마다 가상 노드 16개. 서버는 시계 방향으로 처음 만나는 수집기가 맡는다."""
    global ring, owned
    ring = sorted((h32(f"{p}#{i}"), p) for p in peers for i in range(VNODES))
    keys = [k for k, _ in ring]

    def owner(t):
        if not ring:
            return None
        i = bisect.bisect(keys, h32(t)) % len(ring)
        return ring[i][1]

    new = sorted(t for t in targets if owner(t) == NAME)
    if new != owned:
        log.info("내 몫 변경 (%d대): %s", len(new), new)
    owned = new


async def watch(role: str, on_change):
    """서비스 탐색 롱 폴링 = 변경 이벤트 알림 (그림 10 의 3번)."""
    version = -1
    async with http(20) as s:
        while True:
            try:
                res = await list_services(s, role, after=version)
                version = res["version"]
                on_change(res["services"])
            except Exception as e:  # noqa: BLE001
                log.warning("watch %s 실패: %s", role, e)
                await asyncio.sleep(2)


def on_sims(services):
    global targets
    targets = {s["name"]: s["url"] for s in services if s["meta"].get("mode") == "pull"}
    rebuild()


def on_peers(services):
    global peers
    peers = [s["name"] for s in services]
    if NAME not in peers:
        peers.append(NAME)
    rebuild()


PROM = re.compile(r'^(\w+)\{(.*)\} ([-\d.eE+]+)$')


async def scrape_one(s, name, url):
    ts_ms = int(time.time() * 1000)
    labels = {"host": name}
    try:
        async with s.get(f"{url}/metrics") as r:
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status}")
            text = await r.text()
        for line in text.splitlines():
            m = PROM.match(line)
            if not m:
                continue
            metric, lbl, value = m.groups()
            labels = dict(re.findall(r'(\w+)="([^"]*)"', lbl))
            await send(metric, labels, float(value), ts_ms, "pull")
        await send("up", labels, 1.0, ts_ms, "pull")
        scrape_ok.add()
        last_scrape[name] = {"ok": True, "at": time.time()}
    except Exception as e:  # noqa: BLE001
        await send("up", {"host": name, "env": "prod"}, 0.0, ts_ms, "pull")
        scrape_fail.add()
        last_scrape[name] = {"ok": False, "at": time.time(), "error": str(e) or type(e).__name__}


async def scraper():
    async with http(3) as s:
        while True:
            started = time.monotonic()
            await asyncio.gather(*(scrape_one(s, t, targets[t]) for t in owned if t in targets))
            await asyncio.sleep(max(0.0, INTERVAL - (time.monotonic() - started)))


# ---------- 공통 ----------
async def stats(_req):
    d = {"name": NAME, "mode": MODE, "rate": received.per_sec(), "total": received.total,
         "rejected": rejected, "keys": by_partition_key}
    if MODE == "pull":
        d.update(owned=owned, peers=peers, scrape_ok=scrape_ok.per_sec(), scrape_fail=scrape_fail.per_sec(),
                 last_scrape=last_scrape,
                 ring=[{"pos": k / 2**32, "owner": o} for k, o in ring],
                 targets=[{"name": t, "pos": h32(t) / 2**32} for t in sorted(targets)])
    return json_response(d)


async def main():
    global producer
    routes = [web.get("/stats", stats)]
    if MODE == "push":
        routes.append(web.post("/write", write))
    await serve(routes)
    producer = await start_producer()
    log.info("수집기 %s 시작 (mode=%s)", NAME, MODE)
    tasks = [register_forever(NAME, f"collector-{MODE}", lambda: {"mode": MODE})]
    if MODE == "pull":
        tasks += [watch("sim", on_sims), watch("collector-pull", on_peers), scraper()]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
