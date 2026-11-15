"""모든 구성 요소가 함께 쓰는 것: 이름 짓기, 서비스 탐색 등록, /stats·/control HTTP 서버."""
import asyncio
import json
import logging
import os
import socket
import time

from aiohttp import ClientSession, ClientTimeout, web

DISCOVERY_URL = os.environ.get("DISCOVERY_URL", "http://discovery:8080")
KAFKA = os.environ.get("KAFKA", "redpanda:9092")
PG_DSN = os.environ.get("PG_DSN", "postgresql://postgres:postgres@timescaledb:5432/metrics")
PORT = 8080

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")


def my_name() -> str:
    """'sim-push-3' 같은 이름. 도커 DNS 에 컨테이너 이름(mini-monitor-sim-push-1, -2, …)을 차례로 물어
    내 IP 와 같은 것을 찾는다. (/etc/hosts 에는 컨테이너 ID 만 있어서 역조회로는 못 얻는다.)"""
    host = socket.gethostname()
    service = os.environ.get("SERVICE")
    project = os.environ.get("COMPOSE_PROJECT", "mini-monitor")
    if not service:
        return host
    try:
        me = socket.gethostbyname(host)
    except OSError:
        return host
    for i in range(1, 101):
        try:
            if socket.gethostbyname(f"{project}-{service}-{i}") == me:
                return f"{service}-{i}"
        except OSError:
            continue
    return f"{service}-{host[:6]}"


def my_url() -> str:
    return f"http://{socket.gethostbyname(socket.gethostname())}:{PORT}"


def http(timeout: float = 2.0) -> ClientSession:
    # DNS 캐시를 끄고 매번 새로 연결: 도커 DNS 라운드로빈이 로드밸런서 역할을 한다 (그림 13)
    from aiohttp import TCPConnector
    return ClientSession(timeout=ClientTimeout(total=timeout),
                         connector=TCPConnector(use_dns_cache=False, force_close=True))


async def register_forever(name: str, role: str, meta_fn=lambda: {}, interval: float = 2.0):
    """서비스 탐색에 2초마다 박동. 6초 끊기면 목록에서 빠진다 (etcd 의 lease 와 같은 생각)."""
    log = logging.getLogger("register")
    url = my_url()
    async with http() as s:
        while True:
            try:
                await s.post(f"{DISCOVERY_URL}/register",
                             json={"name": name, "role": role, "url": url, "meta": meta_fn()})
            except Exception as e:  # noqa: BLE001
                log.warning("discovery 등록 실패: %s", e)
            await asyncio.sleep(interval)


async def list_services(session: ClientSession, role: str | None = None, after: int = -1):
    """목록과 버전. after 를 주면 버전이 바뀔 때까지(최대 10초) 기다린다 = 변경 알림(watch)."""
    params = {"after": str(after)}
    if role:
        params["role"] = role
    async with session.get(f"{DISCOVERY_URL}/services", params=params,
                           timeout=ClientTimeout(total=15)) as r:
        return await r.json()


def json_response(data) -> web.Response:
    return web.Response(text=json.dumps(data, default=str), content_type="application/json")


async def serve(routes: list, port: int = PORT):
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()


class Rate:
    """최근 N초 동안 초당 몇 건인지."""

    def __init__(self, window: float = 10.0):
        self.window = window
        self.events: list[tuple[float, int]] = []
        self.total = 0

    def add(self, n: int = 1):
        now = time.monotonic()
        self.total += n
        self.events.append((now, n))
        cut = now - self.window
        while self.events and self.events[0][0] < cut:
            self.events.pop(0)

    def per_sec(self) -> float:
        cut = time.monotonic() - self.window
        return round(sum(n for t, n in self.events if t >= cut) / self.window, 1)


TOPICS = {"metrics": 6, "alerts": 1}


async def ensure_topics():
    """토픽이 없으면 만든다. 지표 토픽은 파티션 6개, 지표 이름을 키로 나눈다 (그림 16)."""
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError
    log = logging.getLogger("kafka")
    while True:
        admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA)
        try:
            await admin.start()
            existing = set(await admin.list_topics())
            new = [NewTopic(t, n, 1) for t, n in TOPICS.items() if t not in existing]
            if new:
                try:
                    await admin.create_topics(new)
                    log.info("토픽 생성: %s", [t.name for t in new])
                except TopicAlreadyExistsError:
                    pass
            return
        except Exception as e:  # noqa: BLE001
            log.warning("kafka 대기 중: %s", e)
            await asyncio.sleep(2)
        finally:
            await admin.close()


async def start_producer():
    from aiokafka import AIOKafkaProducer
    await ensure_topics()
    p = AIOKafkaProducer(bootstrap_servers=KAFKA, linger_ms=50, acks=1,
                         key_serializer=lambda k: k.encode(),
                         value_serializer=lambda v: json.dumps(v).encode())
    await p.start()
    return p
