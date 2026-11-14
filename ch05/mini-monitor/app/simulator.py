"""지표 출처 = 가짜 서버 한 대. MODE 에 따라 두 가지로 지표를 내보낸다.

- MODE=pull: GET /metrics 로 Prometheus 텍스트 형식을 노출한다. 수집기가 당겨 간다 (그림 8, 10).
- MODE=push: 수집 에이전트가 5초마다 라인 프로토콜로 수집기에 보낸다 (그림 12).
  수집기가 거절하면 로컬 버퍼에 쌓아 두었다가 다시 보낸다.

POST /control {"action": "spike"|"down"|"leak"|"normal"} 로 장애를 흉내 낸다.
"""
import asyncio
import collections
import hashlib
import logging
import math
import os
import random
import time

from aiohttp import web

from common import http, json_response, my_name, register_forever, serve

MODE = os.environ.get("MODE", "push")
INTERVAL = float(os.environ.get("INTERVAL", "5"))
PUSH_URL = os.environ.get("PUSH_URL", "http://collector-push:8080/write")
SERVICES = ["web", "api", "db"]
REGIONS = ["us-west", "us-east", "ap-northeast"]

log = logging.getLogger("sim")
NAME = my_name()
h = int(hashlib.md5(NAME.encode()).hexdigest(), 16)
# service 는 SIM_SERVICE 로 정할 수 있다 (파이프라인 뷰의 service 별 +). 없으면 이름 해시로 고른다
LABELS = {"host": NAME, "service": os.environ.get("SIM_SERVICE") or SERVICES[h % 3], "region": REGIONS[(h // 3) % 3], "env": "prod"}

state = {"mode": "normal", "until": 0.0}
base = {"cpu": 20 + h % 40, "mem": 35 + (h // 7) % 30, "disk": 40 + (h // 11) % 40, "rps": 50 + (h // 13) % 200}
phase = (h % 360) / 57.3
mem_extra = 0.0
last: dict[str, float] = {}
buffer: collections.deque[str] = collections.deque(maxlen=5000)  # 에이전트 로컬 버퍼
pushed = 0
push_errors = 0


def current_mode() -> str:
    if state["mode"] != "normal" and time.time() > state["until"]:
        log.info("%s 상태 복귀: normal", NAME)
        state["mode"] = "normal"
    return state["mode"]


def sample() -> dict[str, float]:
    """한 번 잴 때마다 값이 조금씩 움직인다. 2분 주기 사인파 = 하루 부하 곡선의 축소판."""
    global mem_extra
    t = time.time()
    wave = math.sin(t / 120 * 2 * math.pi + phase)
    mode = current_mode()
    cpu = base["cpu"] + 15 * wave + random.gauss(0, 4)
    rps = base["rps"] * (1 + 0.4 * wave) + random.gauss(0, 10)
    lat = 80 + cpu * 1.5 + random.gauss(0, 15)
    err = max(0.0, random.gauss(0.5, 0.3))
    if mode == "spike":
        cpu = 93 + random.gauss(0, 3)
        lat = 600 + random.gauss(0, 80)
        err = 5 + random.gauss(0, 1)
    if mode == "leak":
        mem_extra = min(mem_extra + 8, 100)
    else:
        mem_extra = max(mem_extra - 8, 0)
    mem = base["mem"] + mem_extra + random.gauss(0, 1)
    disk = base["disk"] + (t % 3600) / 3600 * 5
    clamp = lambda v: round(max(0.0, min(100.0, v)), 2)  # noqa: E731
    return {
        "cpu_usage": clamp(cpu),
        "mem_usage": clamp(mem),
        "disk_usage": clamp(disk),
        "http_rps": round(max(0.0, rps), 1),
        "http_latency_p99_ms": round(max(5.0, lat), 1),
        "http_error_rate": round(err, 2),
    }


def label_str(sep: str, quote: bool) -> str:
    q = '"' if quote else ""
    return sep.join(f"{k}={q}{v}{q}" for k, v in LABELS.items())


# ---------- pull: GET /metrics ----------
async def metrics(_req):
    if current_mode() == "down":
        await asyncio.sleep(10)  # 죽은 서버처럼 응답하지 않는다 → 수집기 타임아웃 → up=0
        return web.Response(status=503)
    global last
    last = sample()
    lines = []
    for k, v in last.items():
        lines.append(f"# TYPE {k} gauge")
        lines.append(f"{k}{{{label_str(',', True)}}} {v}")
    return web.Response(text="\n".join(lines) + "\n", content_type="text/plain")


# ---------- push: 수집 에이전트 ----------
async def agent():
    global last, pushed, push_errors
    async with http(3) as s:
        while True:
            await asyncio.sleep(INTERVAL)
            if current_mode() == "down":
                continue  # 서버가 죽으면 에이전트도 같이 죽는다 → 데이터가 안 온다
            last = sample()
            ts = time.time_ns()
            for k, v in last.items():
                buffer.append(f"{k},{label_str(',', False)} value={v} {ts}")
            batch = list(buffer)
            try:
                async with s.post(PUSH_URL, data="\n".join(batch)) as r:
                    if r.status == 204:
                        for _ in batch:
                            buffer.popleft()
                        pushed += len(batch)
                    else:
                        push_errors += 1
            except Exception:  # noqa: BLE001
                push_errors += 1  # 다음 주기에 버퍼째 다시 보낸다


async def control(req: web.Request):
    body = await req.json()
    action = body.get("action", "normal")
    seconds = float(body.get("seconds", 60))
    state["mode"] = action
    state["until"] = time.time() + seconds
    log.info("%s 상태 변경: %s (%ss)", NAME, action, seconds)
    return json_response(stats_dict())


def stats_dict():
    return {"name": NAME, "mode": MODE, "labels": LABELS, "state": current_mode(),
            "state_left": max(0, round(state["until"] - time.time())), "last": last,
            "buffered": len(buffer), "pushed": pushed, "push_errors": push_errors}


async def stats(_req):
    return json_response(stats_dict())


async def main():
    routes = [web.get("/stats", stats), web.post("/control", control)]
    if MODE == "pull":
        routes.append(web.get("/metrics", metrics))
    await serve(routes)
    log.info("시뮬레이터 %s 시작 (mode=%s, %s)", NAME, MODE, LABELS)
    meta = lambda: {"mode": MODE, "service": LABELS["service"], "state": current_mode()}  # noqa: E731
    tasks = [register_forever(NAME, "sim", meta)]
    if MODE == "push":
        tasks.append(agent())
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
