"""원본 보관. raw-clicks 를 읽어 Parquet 파일로 RustFS(S3 호환)에 쌓는다.

경로: s3://raw-clicks/dt=YYYY-MM-DD/hour=HH/part-<시각>-<난수>.parquet  (이벤트 시각 기준)
3초마다 파일 하나씩 (광고 서버의 tick 은 빼고 클릭만). 파일을 올린 뒤에야 오프셋을 커밋한다 (최소 한 번).
그래서 같은 클릭이 두 파일에 들어갈 수 있는데, 재집계 검증은 click_id 로 중복을 지운다.
"""
import io
import json
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from confluent_kafka import Consumer

from common import KAFKA, RAW_TOPIC, S3_BUCKET, S3_ENDPOINT, S3_KEY, S3_SECRET, fmt_ms, log, now_ms, serve

FLUSH_SEC, FLUSH_ROWS = 3, 5000
SCHEMA = pa.schema([("click_id", pa.string()), ("ad_id", pa.string()), ("event_ts", pa.int64()),
                    ("recv_ts", pa.int64()), ("phone", pa.string())])
stats = {"files": 0, "rows": 0, "bytes": 0, "last_flush": None, "last_keys": [], "buffered": 0}

s3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET,
                  region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))


def ensure_bucket():
    while True:
        try:
            if S3_BUCKET not in [b["Name"] for b in s3.list_buckets().get("Buckets", [])]:
                s3.create_bucket(Bucket=S3_BUCKET)
                log.info("버킷 생성: %s", S3_BUCKET)
            return
        except Exception as e:  # noqa: BLE001
            log.warning("RustFS 대기 중: %s", e)
            time.sleep(2)


def flush(rows: list[dict]):
    parts = defaultdict(list)
    for r in rows:
        d = datetime.fromtimestamp(r["event_ts"] / 1000, tz=timezone.utc)
        parts[(d.strftime("%Y-%m-%d"), d.strftime("%H"))].append(r)
    keys = []
    for (dt, hour), rs in parts.items():
        table = pa.Table.from_pylist([{k: r.get(k) for k in SCHEMA.names} for r in rs], schema=SCHEMA)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd")
        key = f"dt={dt}/hour={hour}/part-{now_ms()}-{uuid.uuid4().hex[:6]}.parquet"
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
        stats["bytes"] += buf.tell()
        keys.append(key)
    stats["files"] += len(keys)
    stats["rows"] += len(rows)
    stats["last_flush"] = fmt_ms(now_ms())
    stats["last_keys"] = (stats["last_keys"] + keys)[-5:]


def main():
    ensure_bucket()
    serve(get={"/stats": lambda q: stats})
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "archiver", "enable.auto.commit": False,
                         "auto.offset.reset": "earliest"})
    consumer.subscribe([RAW_TOPIC])
    buf, last = [], time.monotonic()
    while True:
        for m in consumer.consume(1000, 1.0):
            if not m.error():
                try:
                    e = json.loads(m.value())
                except ValueError:
                    continue
                if e.get("type") == "click":
                    buf.append(e)
        stats["buffered"] = len(buf)
        if time.monotonic() - last >= FLUSH_SEC or len(buf) >= FLUSH_ROWS:
            if buf:
                flush(buf)
            try:
                consumer.commit(asynchronous=False)  # 파일이 올라간 뒤에 커밋
            except Exception:  # noqa: BLE001  (읽은 게 없으면 커밋할 오프셋도 없다)
                pass
            buf, last = [], time.monotonic()
            stats["buffered"] = 0


if __name__ == "__main__":
    main()
