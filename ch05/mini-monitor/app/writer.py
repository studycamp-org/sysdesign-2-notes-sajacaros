"""Kafka 소비자 → TimescaleDB (그림 15 의 '소비자').

소비자 그룹 'tsdb-writer' 로 여러 대가 파티션을 나눠 갖는다. 모아서 한 번에 INSERT 하고,
DB 에 쓴 뒤에야 오프셋을 커밋한다. 그래서 DB 가 멈춰도 데이터는 Kafka 에 남아 있다가 따라잡는다.

POST /control {"action": "pause"|"resume"}: DB 장애 흉내. 쓰기를 멈추면 Kafka 에 lag 가 쌓인다.
"""
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

import asyncpg
from aiohttp import web
from aiokafka import AIOKafkaConsumer

from common import KAFKA, PG_DSN, Rate, ensure_topics, json_response, my_name, register_forever, serve

log = logging.getLogger("writer")
NAME = my_name()
BATCH = int(os.environ.get("BATCH", "2000"))
consumer: AIOKafkaConsumer | None = None
pool: asyncpg.Pool | None = None
written = Rate()
paused = False
last_batch = {"rows": 0, "ms": 0.0}
errors = 0

INSERT = """
INSERT INTO metrics (time, name, host, labels, value)
SELECT t, n, h, l::jsonb, v FROM unnest($1::timestamptz[], $2::text[], $3::text[], $4::text[], $5::float8[])
  AS u(t, n, h, l, v)
"""


async def connect_db():
    while True:
        try:
            return await asyncpg.create_pool(PG_DSN, min_size=1, max_size=2)
        except Exception as e:  # noqa: BLE001
            log.warning("DB 대기 중: %s", e)
            await asyncio.sleep(2)


async def loop():
    global errors
    while True:
        if paused:  # getmany 를 부르지 않는다. 박동은 백그라운드에서 계속 간다
            await asyncio.sleep(0.5)
            continue
        batches = await consumer.getmany(timeout_ms=1000, max_records=BATCH)
        rows = [m.value for msgs in batches.values() for m in msgs]
        if not rows:
            continue
        started = time.monotonic()
        cols = ([datetime.fromtimestamp(r["ts"] / 1000, timezone.utc) for r in rows],
                [r["name"] for r in rows],
                [r["labels"].get("host", "") for r in rows],
                [json.dumps(r["labels"]) for r in rows],
                [float(r["value"]) for r in rows])
        while True:
            try:
                async with pool.acquire() as c:
                    await c.execute(INSERT, *cols)
                break
            except Exception as e:  # noqa: BLE001
                errors += 1
                log.warning("DB 쓰기 실패, 재시도: %s", e)
                await asyncio.sleep(2)
        await consumer.commit()  # DB 에 쓴 다음에 커밋 = 최소 한 번
        written.add(len(rows))
        last_batch.update(rows=len(rows), ms=round((time.monotonic() - started) * 1000, 1))


async def control(req: web.Request):
    global paused
    action = (await req.json()).get("action")
    if action == "pause":
        consumer.pause(*consumer.assignment())
        paused = True
    elif action == "resume":
        consumer.resume(*consumer.assignment())
        paused = False
    log.info("%s: %s", NAME, action)
    return await stats(req)


async def stats(_req):
    parts = []
    if consumer:
        for tp in sorted(consumer.assignment(), key=lambda t: t.partition):
            hw = consumer.highwater(tp)
            committed = await consumer.committed(tp)
            parts.append({"partition": tp.partition, "highwater": hw, "committed": committed,
                          "lag": (hw - committed) if hw is not None and committed is not None else None})
    return json_response({"name": NAME, "rate": written.per_sec(), "total": written.total, "paused": paused,
                          "partitions": parts, "last_batch": last_batch, "errors": errors})


async def main():
    global consumer, pool
    await serve([web.get("/stats", stats), web.post("/control", control)])
    pool = await connect_db()
    await ensure_topics()
    consumer = AIOKafkaConsumer(
        "metrics", bootstrap_servers=KAFKA, group_id="tsdb-writer", enable_auto_commit=False,
        auto_offset_reset="earliest", max_poll_interval_ms=3_600_000,
        value_deserializer=lambda b: json.loads(b))
    await consumer.start()
    log.info("writer %s 시작", NAME)
    await asyncio.gather(register_forever(NAME, "writer", lambda: {"paused": paused}), loop())


if __name__ == "__main__":
    asyncio.run(main())
