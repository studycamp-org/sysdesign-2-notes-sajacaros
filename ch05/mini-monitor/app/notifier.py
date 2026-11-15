"""경보 소비자 (그림 19 의 6~7번). Kafka 'alerts' 를 읽어 채널로 보낸다.

실제로 메일·전화를 보내지는 않고 notifications 테이블과 로그에 남긴다. WEBHOOK_URL 을 주면 거기로 POST 도 한다.
같은 경보가 두 번 와도(최소 한 번의 대가) (alert_key, channel) 유일 제약으로 한 번만 남는다.
"""
import asyncio
import json
import logging
import os

import asyncpg
from aiokafka import AIOKafkaConsumer

from aiohttp import web

from common import KAFKA, PG_DSN, ensure_topics, http, json_response, my_name, register_forever, serve

log = logging.getLogger("notifier")
NAME = my_name()
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
CHANNELS = {"page": ["pagerduty", "email"], "warning": ["slack"]}
sent = {"total": 0, "duplicates": 0}


async def stats(_req):
    return json_response({"name": NAME, **sent})


async def main():
    await serve([web.get("/stats", stats)])
    while True:
        try:
            pool = await asyncpg.create_pool(PG_DSN, min_size=1, max_size=2)
            break
        except Exception as e:  # noqa: BLE001
            log.warning("DB 대기 중: %s", e)
            await asyncio.sleep(2)
    await ensure_topics()
    consumer = AIOKafkaConsumer("alerts", bootstrap_servers=KAFKA, group_id="notifier", enable_auto_commit=False,
                                auto_offset_reset="earliest", value_deserializer=lambda b: json.loads(b))
    await consumer.start()
    asyncio.create_task(register_forever(NAME, "notifier"))
    log.info("notifier 시작")
    async with http(3) as s:
        async for msg in consumer:
            ev = msg.value
            firing = [a for a in ev["alerts"] if a["state"] == "firing"]
            resolved = [a for a in ev["alerts"] if a["state"] == "resolved"]
            parts = []
            if firing:
                parts.append("🔥 " + ", ".join(f"{a['rule']}({a['summary']})" for a in firing))
            if resolved:
                parts.append("✅ 해소 " + ", ".join(a["rule"] for a in resolved))
            text = f"[{ev['host']}] " + " / ".join(parts)
            severity = "page" if any(a["severity"] == "page" for a in firing) else "warning"
            state = "firing" if firing else "resolved"
            for ch in CHANNELS[severity]:
                done = await pool.execute(
                    "INSERT INTO notifications (host, state, channel, message, alert_key) VALUES ($1,$2,$3,$4,$5) "
                    "ON CONFLICT DO NOTHING", ev["host"], state, ch, text, ev["key"])
                if done.endswith(" 1"):
                    sent["total"] += 1
                    log.info("→ %-9s %s", ch, text)
                else:
                    sent["duplicates"] += 1
            if WEBHOOK_URL:
                try:
                    await s.post(WEBHOOK_URL, json={"text": text, "event": ev})
                except Exception as e:  # noqa: BLE001
                    log.warning("웹훅 실패: %s", e)
            await consumer.commit()


if __name__ == "__main__":
    asyncio.run(main())
