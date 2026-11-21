"""재집계 검증 (교재의 reconciliation 배치). 교재는 하루에 한 번이지만 여기서는 버튼으로 돌린다.

1. DuckDB 가 RustFS 의 Parquet 원본을 S3 로 직접 읽어 (10초 윈도, 광고)별 클릭을 다시 센다.
   click_id 로 중복을 지우니 이것이 '정답'이다. 아주 늦게 온 클릭도 원본에는 다 있다.
2. ClickHouse 의 스트리밍 결과(이벤트 시각, 증가분의 합)를 Parquet 으로 받아 DuckDB 안에서 FULL JOIN 한다.
3. 다른 칸은 차이만큼 증가분 행(source='recon')을 넣어 맞춘다. 집계 DB 가 '더하기'라 보정도 더하기다.

검증 범위: 최근 30분 ~ 집계 서비스가 닫은 윈도까지 (보관 주기 3초를 넉넉히 빼고).
"""
import io
import threading
import time
import urllib.parse

import duckdb
import pyarrow.parquet as pq

from common import (S3_BUCKET, S3_ENDPOINT, S3_KEY, S3_SECRET, ch_bytes, ch_insert, fmt_ms, http_json, log,
                    now_ms, serve)

WIN = 10_000
lock = threading.Lock()
stats = {"runs": 0, "last": None}


def connect() -> duckdb.DuckDBPyConnection:
    ep = urllib.parse.urlparse(S3_ENDPOINT)
    con = duckdb.connect()
    con.execute("LOAD httpfs")
    con.execute(f"""CREATE SECRET rustfs (TYPE s3, KEY_ID '{S3_KEY}', SECRET '{S3_SECRET}', REGION 'us-east-1',
                    ENDPOINT '{ep.netloc}', URL_STYLE 'path', USE_SSL {str(ep.scheme == 'https').lower()})""")
    return con


def run(_body: dict) -> dict:
    if not lock.acquire(blocking=False):
        return {"error": "이미 실행 중"}
    try:
        return _run()
    finally:
        lock.release()


def _run() -> dict:
    t0 = time.monotonic()
    try:
        closed = http_json("http://aggregator:8080/stats", timeout=2).get("closed_upto") or 0
    except Exception:  # noqa: BLE001
        closed = 0
    hi = min(closed, now_ms() - 10_000) // WIN * WIN
    lo = (now_ms() - 30 * 60_000) // WIN * WIN
    if hi <= lo:
        return {"error": "아직 닫힌 윈도가 없다"}
    con = connect()
    try:
        con.execute(f"""
            CREATE TABLE truth AS
            WITH raw AS (  -- click_id 하나에 한 줄 (재전송·중복 보관을 지운다)
                SELECT click_id, any_value(ad_id) AS ad_id, min(event_ts) AS event_ts
                FROM read_parquet('s3://{S3_BUCKET}/**/*.parquet', hive_partitioning = true)
                GROUP BY click_id
                HAVING min(event_ts) >= {lo} AND min(event_ts) < {hi}
            )
            SELECT (event_ts // {WIN}) * {WIN // 1000} AS w, ad_id, count(*) AS clicks
            FROM raw GROUP BY ALL""")
    except duckdb.IOException as e:
        return {"error": f"원본 Parquet 을 읽지 못함 (아직 파일이 없을 수 있다): {e}"}
    ch = pq.read_table(io.BytesIO(ch_bytes(
        "SELECT toUInt32(toUnixTimestamp(window_start)) AS w, ad_id, sum(clicks) AS clicks "
        "FROM adclick.ad_clicks WHERE kind = 'event' "
        "AND window_start >= toDateTime({lo:UInt32}, 'UTC') AND window_start < toDateTime({hi:UInt32}, 'UTC') "
        "GROUP BY w, ad_id FORMAT Parquet", {"lo": lo // 1000, "hi": hi // 1000})))
    con.register("ch", ch)
    diffs = con.execute("""
        SELECT w, ad_id, coalesce(t.clicks, 0) AS truth, coalesce(c.clicks, 0) AS stream
        FROM truth t FULL OUTER JOIN ch c USING (w, ad_id)
        WHERE coalesce(t.clicks, 0) <> coalesce(c.clicks, 0)
        ORDER BY w, ad_id""").fetchall()
    windows = con.execute("SELECT count(DISTINCT w) FROM (SELECT w FROM truth UNION SELECT w FROM ch)").fetchone()[0]
    at = fmt_ms(now_ms(), millis=True)
    fix = [{"kind": "event", "window_start": fmt_ms(w * 1000), "ad_id": a, "clicks": t - s,
            "source": "recon", "emitted_at": at} for w, a, t, s in diffs]
    ch_insert("adclick.ad_clicks", fix)
    report = {
        "ran_at": fmt_ms(now_ms()), "range": f"{fmt_ms(lo)[11:]} ~ {fmt_ms(hi)[11:]}", "windows": windows,
        "mismatches": len(diffs), "fixed_clicks": sum(t - s for *_, t, s in diffs),
        "samples": [{"window": fmt_ms(w * 1000)[11:], "ad_id": a, "truth": t, "stream": s} for w, a, t, s in diffs[:12]],
        "took_ms": round((time.monotonic() - t0) * 1000),
    }
    stats["runs"] += 1
    stats["last"] = report
    log.info("재집계 검증: %s", {k: report[k] for k in ("windows", "mismatches", "fixed_clicks", "took_ms")})
    return report


def main():
    serve(get={"/stats": lambda q: stats}, post={"/run": run})
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
