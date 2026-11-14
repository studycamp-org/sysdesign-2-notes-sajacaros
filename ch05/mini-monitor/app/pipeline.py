"""파이프라인 뷰: 출처 → 수집기 → Kafka → 소비자 → TimescaleDB → 경보 를 한 화면에 보여 준다.

서비스 탐색에서 구성 요소 목록을 받아 각자의 /stats 를 모으고, Kafka 오프셋과 DB 상태를 직접 잰다.
1초마다 상태를 만들어 두고 GET /api/state 로 준다. POST /api/control 은 해당 구성 요소의 /control 로 넘긴다.
POST /api/scale {"service": "sim-push"|"sim-pull", "count": N} 은 docker.sock 으로 시뮬레이터 컨테이너 수를 바꾼다.
"""
import asyncio
import json
import logging
import os
import time

import asyncpg
import yaml
from aiohttp import ClientSession, UnixConnector, web
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient

from common import DISCOVERY_URL, KAFKA, PG_DSN, TOPICS, ensure_topics, http, json_response, serve

log = logging.getLogger("pipeline")
STATIC = os.path.join(os.path.dirname(__file__), "static")
QUERY_URL = os.environ.get("QUERY_URL", "http://query:8080")
SLO_CONF = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), "slo.yaml")))
slo_state: dict = {"ready": False}
pg_pool: asyncpg.Pool | None = None
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000")
state: dict = {"ready": False}
services: list[dict] = []

# ---------- 시뮬레이터 수 조절: Docker Engine API (소켓 마운트) ----------
DOCKER_SOCK = "/var/run/docker.sock"
PROJECT = os.environ.get("COMPOSE_PROJECT", "mini-monitor")
SCALABLE = {"sim-push": "push", "sim-pull": "pull"}
MAX_SIMS = int(os.environ.get("MAX_SIMS", "30"))
scale_lock = asyncio.Lock()


def docker() -> ClientSession:
    return ClientSession(connector=UnixConnector(path=DOCKER_SOCK), base_url="http://docker")


async def sim_containers(d: ClientSession, service: str) -> list[dict]:
    """compose 라벨로 그 서비스의 컨테이너를 찾는다. container-number 순으로 정렬."""
    flt = {"label": [f"com.docker.compose.project={PROJECT}", f"com.docker.compose.service={service}"]}
    async with d.get("/containers/json", params={"all": "1", "filters": json.dumps(flt)}) as r:
        rows = await r.json()
    num = lambda c: int(c["Labels"].get("com.docker.compose.container-number", "0"))  # noqa: E731
    return sorted(rows, key=num)


async def docker_counts() -> dict:
    if not os.path.exists(DOCKER_SOCK):
        return {"enabled": False, "reason": "docker.sock 이 마운트되지 않았다"}
    try:
        async with docker() as d:
            out = {"enabled": True, "max": MAX_SIMS}
            for svc in SCALABLE:
                every = await sim_containers(d, svc)
                nm = lambda c: f"{svc}-{c['Labels'].get('com.docker.compose.container-number', '?')}"  # noqa: E731
                cs = [c for c in every if c["State"] == "running"]
                out[svc] = len(cs)
                # 'sim-push-3' 같은 이름 (서비스 탐색에 등록되는 이름과 같다)
                out[f"{svc}:names"] = [nm(c) for c in cs]
                out[f"{svc}:stopped"] = [nm(c) for c in every if c["State"] != "running"]
            return out
    except Exception as e:  # noqa: BLE001
        return {"enabled": False, "reason": str(e)}


GROUP_LABEL = "mini-monitor.sim-service"


async def add_one(d: ClientSession, service: str, existing: list[dict], group: str | None = None):
    """같은 서비스 컨테이너 하나를 복제해 번호만 바꿔 띄운다 (docker compose --scale 과 같은 결과)."""
    used = {int(c["Labels"].get("com.docker.compose.container-number", "0")) for c in existing}
    n = next(i for i in range(1, 1000) if i not in used)
    name = f"{PROJECT}-{service}-{n}"
    if existing:
        async with d.get(f"/containers/{existing[0]['Id']}/json") as r:
            t = await r.json()
        cfg, host = t["Config"], t["HostConfig"]
        nets = list(t["NetworkSettings"]["Networks"].keys())
        image, cmd, env, labels = cfg["Image"], cfg["Cmd"], cfg["Env"], dict(cfg["Labels"])
        restart = host.get("RestartPolicy") or {"Name": "unless-stopped"}
    else:  # 0대까지 줄였을 때: 파이프라인 자신을 본떠 만든다
        async with d.get(f"/containers/{os.environ.get('HOSTNAME', '')}/json") as r:
            t = await r.json()
        cfg = t["Config"]
        nets = list(t["NetworkSettings"]["Networks"].keys())
        mode = SCALABLE[service]
        env = [e for e in cfg["Env"] if not e.startswith(("SERVICE=", "GRAFANA_URL=", "MODE=", "PUSH_URL="))]
        env += [f"SERVICE={service}", f"MODE={mode}"]
        if mode == "push":
            env.append("PUSH_URL=http://collector-push:8080/write")
        labels = {k: v for k, v in cfg["Labels"].items() if k.startswith("com.docker.compose.")
                  and k not in ("com.docker.compose.config-hash",)}
        labels["com.docker.compose.service"] = service
        image, cmd, restart = cfg["Image"], ["python", "simulator.py"], {"Name": "unless-stopped"}
    labels["com.docker.compose.container-number"] = str(n)
    env = [e for e in env if not e.startswith("SIM_SERVICE=")]
    labels.pop(GROUP_LABEL, None)
    if group:
        env.append(f"SIM_SERVICE={group}")
        labels[GROUP_LABEL] = group
    body = {
        "Image": image, "Cmd": cmd, "Env": env, "Labels": labels,
        "HostConfig": {"RestartPolicy": restart, "NetworkMode": nets[0]},
        "NetworkingConfig": {"EndpointsConfig": {nets[0]: {"Aliases": [name, service]}}},
    }
    async with d.post("/containers/create", params={"name": name}, json=body) as r:
        created = await r.json()
        if r.status >= 300:
            raise RuntimeError(created.get("message", str(created)))
    async with d.post(f"/containers/{created['Id']}/start") as r:
        if r.status >= 300:
            raise RuntimeError((await r.json()).get("message"))
    log.info("시뮬레이터 추가: %s", name)


async def api_agent(req: web.Request):
    """에이전트 하나를 강제 종료(docker kill) 하거나 다시 켠다. 서버가 통째로 죽는 상황."""
    body = await req.json()
    name, action = body.get("name", ""), body.get("action")
    if not any(name.startswith(f"{svc}-") for svc in SCALABLE) or action not in ("kill", "start"):
        return json_response({"error": "name 은 sim-push-N / sim-pull-N, action 은 kill / start"})
    async with docker() as d:
        async with d.post(f"/containers/{PROJECT}-{name}/{action}") as r:
            if r.status >= 300:
                return json_response({"error": (await r.json()).get("message")})
    log.info("에이전트 %s: %s", name, action)
    return json_response({"name": name, "action": action})


async def scale(service: str, target: int) -> dict:
    target = max(0, min(MAX_SIMS, target))
    async with scale_lock, docker() as d:
        cs = await sim_containers(d, service)
        # 멈춰 있는 것(직접 docker stop 한 것 등)은 지우고 다시 센다
        for c in [c for c in cs if c["State"] != "running"]:
            async with d.delete(f"/containers/{c['Id']}", params={"force": "1"}):
                pass
        cs = [c for c in cs if c["State"] == "running"]
        while len(cs) > target:  # 번호가 큰 것부터 지운다 (compose 와 같은 순서)
            c = cs.pop()
            async with d.delete(f"/containers/{c['Id']}", params={"force": "1"}):
                pass
            log.info("시뮬레이터 제거: %s", c["Names"][0].lstrip("/"))
        while len(cs) < target:
            await add_one(d, service, cs)
            cs = await sim_containers(d, service)
        return {"service": service, "count": len(cs)}


def group_of(c: dict, service: str) -> str | None:
    """컨테이너의 service 레이블(web/api/db). 직접 정한 건 도커 라벨에, 해시로 정해진 건 서비스 탐색 meta 에 있다."""
    if c["Labels"].get(GROUP_LABEL):
        return c["Labels"][GROUP_LABEL]
    name = f"{service}-{c['Labels'].get('com.docker.compose.container-number')}"
    return next((x.get("meta", {}).get("service") for x in services if x["name"] == name), None)


async def scale_group(service: str, group: str, delta: int) -> dict:
    async with scale_lock, docker() as d:
        cs = [c for c in await sim_containers(d, service) if c["State"] == "running"]
        if delta > 0:
            if len(cs) >= MAX_SIMS:
                return {"error": f"최대 {MAX_SIMS}대"}
            await add_one(d, service, cs, group)
        else:
            mine = [c for c in cs if group_of(c, service) == group]
            if not mine:
                return {"error": f"{group} 컨테이너가 없다"}
            c = mine[-1]  # 번호가 가장 큰 것
            async with d.delete(f"/containers/{c['Id']}", params={"force": "1"}):
                pass
            log.info("시뮬레이터 제거: %s (%s)", c["Names"][0].lstrip("/"), group)
        return {"service": service, "group": group, "delta": delta}


async def fetch_stats(s, svc):
    try:
        async with s.get(f"{svc['url']}/stats") as r:
            return {**svc, "stats": await r.json()}
    except Exception:  # noqa: BLE001
        return {**svc, "stats": None}


async def kafka_state(probe: AIOKafkaConsumer, admin: AIOKafkaAdminClient):
    out = {}
    groups = {"metrics": "tsdb-writer", "alerts": "notifier"}
    for topic, n in TOPICS.items():
        tps = [TopicPartition(topic, p) for p in range(n)]
        ends = await probe.end_offsets(tps)
        try:
            committed = await admin.list_consumer_group_offsets(groups[topic])
        except Exception:  # noqa: BLE001
            committed = {}
        parts = []
        for tp in tps:
            c = committed.get(tp)
            c = c.offset if c is not None and c.offset >= 0 else 0
            parts.append({"partition": tp.partition, "end": ends[tp], "committed": c, "lag": ends[tp] - c})
        out[topic] = {"group": groups[topic], "partitions": parts, "lag": sum(p["lag"] for p in parts)}
    return out


DB_QUERIES = {
    "ingest": """SELECT coalesce(labels->>'via','?') AS via, count(*) AS n FROM metrics
                 WHERE time > now() - interval '10 seconds' GROUP BY 1""",
    "series": "SELECT count(DISTINCT (name, host)) FROM metrics WHERE time > now() - interval '1 minute'",
    "rows": "SELECT count(*) AS approximate_row_count FROM metrics",
    "chunks": """SELECT count(*) AS total, count(*) FILTER (WHERE is_compressed) AS compressed
                 FROM timescaledb_information.chunks WHERE hypertable_name = 'metrics'""",
    "size": "SELECT hypertable_size('metrics')",
    "compression": """SELECT sum(before_compression_total_bytes) AS before, sum(after_compression_total_bytes) AS after
                      FROM chunk_compression_stats('metrics') WHERE compression_status = 'Compressed'""",
    "rollup_1m": "SELECT count(*) FROM metrics_1m WHERE bucket > now() - interval '1 hour'",
    "rollup_1h": "SELECT count(*) FROM metrics_1h",
    "alerts": """SELECT rule, host, severity, state, summary, value, updated_at, fired_at FROM alerts
                 WHERE state <> 'inactive' AND updated_at > now() - interval '30 minutes' ORDER BY
                 CASE state WHEN 'firing' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END, updated_at DESC LIMIT 30""",
    "notifications": "SELECT sent_at, host, state, channel, message FROM notifications ORDER BY id DESC LIMIT 12",
    "cpu": """SELECT DISTINCT ON (host) host, value FROM metrics
              WHERE name = 'cpu_usage' AND time > now() - interval '30 seconds' ORDER BY host, time DESC""",
}


async def db_state(pool):
    out = {}
    async with pool.acquire() as c:
        for k, q in DB_QUERIES.items():
            try:
                rows = await c.fetch(q)
                out[k] = [dict(r) for r in rows]
            except Exception as e:  # noqa: BLE001
                out[k] = {"error": str(e)}
    return out


async def refresher():
    global services
    pool = None
    probe = admin = None
    async with http(1.5) as s:
        while True:
            started = time.monotonic()
            try:
                async with s.get(f"{DISCOVERY_URL}/services") as r:
                    services = (await r.json())["services"]
                comps = await asyncio.gather(*(fetch_stats(s, x) for x in services))
                new = {"at": time.time(), "components": comps, "grafana": GRAFANA_URL}
                if probe is None:
                    await ensure_topics()
                    probe = AIOKafkaConsumer(bootstrap_servers=KAFKA)
                    await probe.start()
                    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA)
                    await admin.start()
                new["kafka"] = await kafka_state(probe, admin)
                if pool is None:
                    global pg_pool
                    pool = pg_pool = await asyncpg.create_pool(PG_DSN, min_size=1, max_size=3)
                new["db"] = await db_state(pool)
                new["docker"] = await docker_counts()
                new["ready"] = True
                state.clear()
                state.update(new)
            except Exception as e:  # noqa: BLE001
                log.warning("상태 수집 실패: %s", e)
                state["error"] = str(e)
            await asyncio.sleep(max(0.2, 1.0 - (time.monotonic() - started)))


async def api_state(_req):
    return json_response(state)


async def api_control(req: web.Request):
    body = await req.json()
    target = next((x for x in services if x["name"] == body.get("name")), None)
    if target is None:
        return json_response({"error": "그런 구성 요소가 없다"})
    async with http(3) as s:
        async with s.post(f"{target['url']}/control", json=body) as r:
            return json_response(await r.json())


async def api_scale(req: web.Request):
    body = await req.json()
    service = body.get("service")
    if service not in SCALABLE:
        return json_response({"error": f"조절할 수 있는 서비스: {list(SCALABLE)}"})
    try:
        if body.get("group"):
            return json_response(await scale_group(service, body["group"], int(body.get("delta", 1))))
        return json_response(await scale(service, int(body.get("count", 0))))
    except Exception as e:  # noqa: BLE001
        log.warning("스케일 실패: %s", e)
        return json_response({"error": str(e)})


async def index(_req):
    return web.FileResponse(os.path.join(STATIC, "index.html"))


async def api_chaos(req: web.Request):
    """장애 시나리오: 시뮬레이터 중 percent% 를 골라 같은 장애를 넣는다.
    파이프라인에 메시지를 끼워 넣는 게 아니라, 가짜 서버 자체가 나쁜 값을 만들거나(spike·leak) 멈춘다(down)."""
    import random
    body = await req.json()
    action = body.get("action", "spike")
    if action not in ("spike", "leak", "down", "normal"):
        return json_response({"error": "action 은 spike·leak·down·normal"})
    mode = body.get("mode", "all")
    sims = [x for x in services if x["role"] == "sim" and mode in ("all", x.get("meta", {}).get("mode"))]
    if action == "normal":
        chosen = sims
    else:
        n = max(1, round(len(sims) * float(body.get("percent", 100)) / 100)) if sims else 0
        chosen = random.sample(sims, n)
    seconds = float(body.get("seconds", 300)) if action != "normal" else 0

    async def one(s, x):
        try:
            async with s.post(f"{x['url']}/control", json={"action": action, "seconds": seconds}) as r:
                return r.status == 200
        except Exception:  # noqa: BLE001
            return False
    async with http(3) as s:
        ok = await asyncio.gather(*(one(s, x) for x in chosen))
    log.info("장애 시나리오 %s: %d/%d대 (%ss)", action, sum(ok), len(chosen), seconds)
    return json_response({"action": action, "seconds": seconds, "hosts": [x["name"] for x in chosen], "ok": sum(ok)})


def page(name):
    async def h(_req):
        return web.FileResponse(os.path.join(STATIC, name))
    return h


# ---------- 경보 페이지: 경보 저장소와 보낸 알림 ----------
ALERT_QUERIES = {
    "alerts": """SELECT rule, host, severity, state, summary, value, pending_since, fired_at, updated_at, notified_state
                 FROM alerts WHERE state <> 'inactive' AND updated_at > now() - interval '1 hour' ORDER BY
                 CASE state WHEN 'firing' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END, updated_at DESC LIMIT 100""",
    "notifications": "SELECT sent_at, host, state, channel, message FROM notifications ORDER BY id DESC LIMIT 60",
    "per_minute": """SELECT time_bucket('1 minute', sent_at) AS t, count(*) FILTER (WHERE state = 'firing') AS firing,
                     count(*) FILTER (WHERE state = 'resolved') AS resolved FROM notifications
                     WHERE sent_at > now() - interval '1 hour' GROUP BY 1 ORDER BY 1""",
}


async def api_alerts(_req):
    out = {}
    if pg_pool is None:
        return json_response({"error": "DB 연결 전"})
    async with pg_pool.acquire() as c:
        for k, q in ALERT_QUERIES.items():
            out[k] = [dict(r) for r in await c.fetch(q)]
    alerter = next((x for x in state.get("components", []) if x["role"] == "alerter"), None)
    out["rules"] = (alerter or {}).get("stats", {}) or {}
    return json_response(out)


# ---------- SLO: 질의 서비스로 SLI 를 재고 오류 예산을 계산한다 ----------
async def ask(s, path, **params):
    async with s.post(f"{QUERY_URL}/api/v1/{path}", data=params) as r:
        body = await r.json()
    if body.get("status") != "success":
        raise RuntimeError(body.get("error"))
    return body["data"]["result"]


def secs(w: str) -> int:
    return int(w[:-1]) * {"s": 1, "m": 60, "h": 3600}[w[-1]]


async def one_slo(s, slo, now):
    win, short = SLO_CONF["window"], SLO_CONF["short_window"]
    W = secs(win)
    if slo["kind"] == "ratio":
        val = lambda res: float(res[0]["value"][1]) if res else None  # noqa: E731
        sli = val(await ask(s, "query", query=slo["expr"].replace("{w}", win), time=now))
        sli_s = val(await ask(s, "query", query=slo["expr"].replace("{w}", short), time=now))
        res = await ask(s, "query_range", query=slo["expr"].replace("{w}", "1m"), start=now - W, end=now, step=60)
        spark = [[float(t), float(v)] for t, v in (res[0]["values"] if res else [])]
    else:
        good = (lambda v: v < slo["threshold"]) if slo["op"] == "<" else (lambda v: v > slo["threshold"])
        res = await ask(s, "query_range", query=slo["expr"], start=now - W, end=now, step=60)
        per_t: dict = {}
        for series in res:
            for t, v in series["values"]:
                g, n = per_t.get(float(t), (0, 0))
                per_t[float(t)] = (g + good(float(v)), n + 1)
        frac = lambda pts: sum(g for g, _ in pts) / max(1, sum(n for _, n in pts)) if pts else None  # noqa: E731
        sli = frac(list(per_t.values()))
        sli_s = frac([x for t, x in per_t.items() if t >= now - secs(short)])
        spark = [[t, g / n] for t, (g, n) in sorted(per_t.items())]
    allowed = 1 - slo["target"]
    burn = lambda x: None if x is None else (1 - x) / allowed  # noqa: E731
    remaining = None if sli is None else 1 - burn(sli)
    status = ("데이터 없음" if sli is None else "위반" if remaining < 0
              else "빠르게 소진" if (burn(sli_s) or 0) >= 2 else "정상")
    return {**{k: slo.get(k) for k in ("name", "desc", "kind", "target", "expr", "op", "threshold")},
            "sli": sli, "sli_short": sli_s, "budget_remaining": remaining, "burn_long": burn(sli),
            "burn_short": burn(sli_s), "status": status, "spark": spark}


async def slo_loop():
    async with http(10) as s:
        s.headers["User-Agent"] = "pipeline/slo"
        while True:
            now = time.time()
            try:
                items = [await one_slo(s, slo, now) for slo in SLO_CONF["slos"]]
                slo_state.clear()
                slo_state.update(ready=True, at=now, window=SLO_CONF["window"],
                                 short_window=SLO_CONF["short_window"], slos=items)
            except Exception as e:  # noqa: BLE001
                log.warning("SLO 계산 실패: %s", e)
                slo_state["error"] = str(e)
            await asyncio.sleep(10)


HOST_METRICS = "cpu_usage|mem_usage|disk_usage|http_rps|http_latency_p99_ms|http_error_rate|up"


async def api_hosts(_req):
    """호스트 맵: 시뮬레이터에 직접 묻지 않고 질의 서비스로 저장된 지표를 읽는다 (Grafana 와 같은 경로)."""
    try:
        async with http(5) as s:
            s.headers["User-Agent"] = "pipeline/hosts"
            now = time.time()
            age, last = await asyncio.gather(
                ask(s, "query", query="age_over_time(cpu_usage[10m])", time=now),
                ask(s, "query", query=f'{{__name__=~"{HOST_METRICS}"}}', time=now))
    except Exception as e:  # noqa: BLE001
        return json_response({"error": str(e)})
    hosts: dict = {}
    for x in age:  # 최근 10분 안에 cpu_usage 를 보낸 적 있는 호스트 = 호스트 맵의 칸
        m = x["metric"]
        hosts[m["host"]] = {"host": m["host"], "labels": {k: v for k, v in m.items() if k != "host"},
                            "age": float(x["value"][1]), "metrics": {}}
    for x in last:  # 맨 선택자 즉시 질의 = 지표마다 최근 5분 안의 마지막 값
        h = hosts.get(x["metric"].get("host"))
        if h is not None:
            h["metrics"][x["metric"]["__name__"]] = float(x["value"][1])
    return json_response({"at": now, "stale_after": 20, "hosts": sorted(hosts.values(), key=lambda h: h["host"])})


async def api_slo(_req):
    return json_response(slo_state)


async def main():
    await serve([web.get("/", index), web.get("/alerts", page("alerts.html")), web.get("/hosts", page("hosts.html")), web.get("/slo", page("slo.html")),
                 web.static("/static", STATIC),
                 web.get("/api/state", api_state), web.post("/api/control", api_control),
                 web.post("/api/scale", api_scale), web.get("/api/alerts", api_alerts), web.get("/api/slo", api_slo),
                 web.post("/api/chaos", api_chaos), web.post("/api/agent", api_agent),
                 web.get("/api/hosts", api_hosts)])
    asyncio.create_task(slo_loop())
    log.info("파이프라인 뷰 시작")
    await refresher()


if __name__ == "__main__":
    asyncio.run(main())
