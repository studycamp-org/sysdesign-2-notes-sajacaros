"""경보 관리자 (그림 19 의 2~5번).

5초마다 규칙의 expr 를 질의 서비스에 물어(그림 19 의 3번) 평가하고, 경보 저장소(alerts 테이블)의 상태를 바꾼다: inactive → pending → firing → resolved.
firing/resolved 가 됐는데 아직 넘기지 않은 경보는 host 별로 하나로 합쳐(그림 20) Kafka 'alerts' 에 넣는다.
넘긴 뒤에야 notified_state 를 적으므로, 도중에 죽어도 다음 주기에 다시 보낸다 = 최소 한 번.
"""
import asyncio
import logging
import os
import time
from datetime import datetime, timezone

import asyncpg
import yaml
from aiohttp import web

from common import PG_DSN, http, json_response, my_name, register_forever, serve, start_producer

EVERY = 5.0
QUERY_URL = os.environ.get("QUERY_URL", "http://query:8080")
log = logging.getLogger("alerter")
NAME = my_name()
RULES = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), "rules.yaml")))
last_eval = {"at": None, "ms": 0, "sent": 0}


def seconds(s: str) -> float:
    return float(s[:-1]) * {"s": 1, "m": 60}[s[-1]]


async def ask(session, expr: str) -> dict[str, float]:
    """질의 서비스에 즉시 질의. TSDB 가 무엇인지 경보 관리자는 모른다."""
    async with session.post(f"{QUERY_URL}/api/v1/query", data={"query": expr}) as r:
        body = await r.json()
    if body.get("status") != "success":
        raise RuntimeError(f"질의 실패 {expr}: {body.get('error')}")
    return {x["metric"].get("host", ""): float(x["value"][1]) for x in body["data"]["result"]}


async def evaluate(conn, session, rule):
    now = datetime.now(timezone.utc)
    hit = await ask(session, rule["expr"])
    cur = {r["host"]: r for r in await conn.fetch("SELECT * FROM alerts WHERE rule = $1", rule["alert"])}
    for host, value in hit.items():
        summary = rule["summary"].format(value=value)
        old = cur.get(host)
        if old is None or old["state"] in ("inactive", "resolved"):
            state = "firing" if seconds(rule["for"]) == 0 else "pending"
            await conn.execute(
                """INSERT INTO alerts (rule, host, severity, state, value, summary, pending_since, fired_at, updated_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$7)
                   ON CONFLICT (rule, host) DO UPDATE SET state=$4, value=$5, summary=$6, pending_since=$7,
                       fired_at=$8, updated_at=$7""",
                rule["alert"], host, rule["severity"], state, value, summary, now,
                now if state == "firing" else None)
        elif old["state"] == "pending" and (now - old["pending_since"]).total_seconds() >= seconds(rule["for"]):
            await conn.execute("UPDATE alerts SET state='firing', value=$3, summary=$4, fired_at=$5, updated_at=$5 "
                               "WHERE rule=$1 AND host=$2", rule["alert"], host, value, summary, now)
        else:
            await conn.execute("UPDATE alerts SET value=$3, summary=$4, updated_at=$5 WHERE rule=$1 AND host=$2",
                               rule["alert"], host, value, summary, now)
    for host, old in cur.items():
        if host in hit:
            continue
        if old["state"] == "firing":
            await conn.execute("UPDATE alerts SET state='resolved', updated_at=$3 WHERE rule=$1 AND host=$2",
                               rule["alert"], host, now)
        elif old["state"] == "pending":
            await conn.execute("UPDATE alerts SET state='inactive', updated_at=$3 WHERE rule=$1 AND host=$2",
                               rule["alert"], host, now)


async def dispatch(conn, producer) -> int:
    """아직 안 넘긴 firing/resolved 를 host 별로 합쳐서 보낸다 (병합·중복 제거)."""
    rows = await conn.fetch("SELECT * FROM alerts WHERE state IN ('firing','resolved') "
                            "AND notified_state IS DISTINCT FROM state ORDER BY host, rule")
    by_host: dict[str, list] = {}
    for r in rows:
        by_host.setdefault(r["host"], []).append(r)
    for host, items in by_host.items():
        alerts = [{"rule": r["rule"], "state": r["state"], "severity": r["severity"], "summary": r["summary"],
                   "fired_at": r["fired_at"].isoformat() if r["fired_at"] else None} for r in items]
        key = host + "|" + ",".join(f"{a['rule']}:{a['state']}:{a['fired_at']}" for a in alerts)
        await producer.send_and_wait("alerts", key=host,
                                     value={"host": host, "alerts": alerts, "key": key, "at": time.time()})
        for r in items:
            await conn.execute("UPDATE alerts SET notified_state=$3 WHERE rule=$1 AND host=$2",
                               r["rule"], host, r["state"])
    return len(by_host)


async def stats(_req):
    return json_response({"name": NAME, "rules": [{k: r[k] for k in ("alert", "severity", "for", "expr")}
                                                  for r in RULES], "last_eval": last_eval})


async def main():
    await serve([web.get("/stats", stats)])
    while True:
        try:
            pool = await asyncpg.create_pool(PG_DSN, min_size=1, max_size=2)
            break
        except Exception as e:  # noqa: BLE001
            log.warning("DB 대기 중: %s", e)
            await asyncio.sleep(2)
    producer = await start_producer()
    asyncio.create_task(register_forever(NAME, "alerter"))
    log.info("alerter 시작: 규칙 %d개", len(RULES))
    session = http(5)
    session.headers["User-Agent"] = "alerter/1.0"
    while True:
        started = time.monotonic()
        try:
            async with pool.acquire() as conn:
                for rule in RULES:
                    await evaluate(conn, session, rule)
                sent = await dispatch(conn, producer)
            last_eval.update(at=time.time(), ms=round((time.monotonic() - started) * 1000), sent=sent)
        except Exception as e:  # noqa: BLE001
            log.warning("평가 실패: %s", e)
        await asyncio.sleep(max(0.0, EVERY - (time.monotonic() - started)))


if __name__ == "__main__":
    asyncio.run(main())
