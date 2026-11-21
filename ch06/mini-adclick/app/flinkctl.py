"""Flink 잡 관리: SQL Gateway 로 집계 잡을 제출하고, REST API 로 세이브포인트·상태를 본다.

집계 SQL 은 하나다 (실시간 잡과 재계산 잡이 같은 로직 = 카파).
  - 이벤트 시각 10초 텀블링 윈도, 워터마크 = event_ts - N초, 윈도별 COUNT(DISTINCT click_id)
  - 처리 시각 10초 윈도 (그림 12 비교용)
  - 버그 버전은 클릭 수에 2 를 곱한다
"""
import json
import time
import urllib.error
import urllib.request

GATEWAY = "http://sql-gateway:8083"
JM = "http://jobmanager:8081"
JOB_NAME = "adclick-aggregation"
RECALC_NAME = "adclick-recalculation"

# 전달 보장 전략 → Flink Kafka 싱크 설정
#   none          : 결과를 프로듀서 버퍼에 8초 쥐고 있다. 그 사이 체크포인트(1초)가 끝나면 '소비 위치'는 이미 앞으로 감
#                   → 죽으면 결과 유실 (교재 해법 1, 그림 18)
#   at-least-once : 결과는 바로 나가고, 체크포인트는 30초마다 → 그 사이 죽으면 마지막 체크포인트부터 다시 → 중복 (해법 2, 그림 19)
#   exactly-once  : 결과를 체크포인트와 묶은 Kafka 트랜잭션(2단계 커밋)으로 → 정확 (해법 3, 그림 20)
STRATEGY = {
    "offset_first": {"guarantee": "none", "checkpoint": "1 s", "linger": "8000"},
    "send_then_offset": {"guarantee": "at-least-once", "checkpoint": "30 s", "linger": "0"},
    "transaction": {"guarantee": "exactly-once", "checkpoint": "2 s", "linger": "0"},
}


def _req(url: str, body=None, method=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method or ("POST" if data is not None else "GET"),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{url}: {e.code} {e.read().decode(errors='replace')[:800]}") from None


def source_ddl(name: str, watermark_sec: int, startup: str, bounded: bool = False) -> str:
    bounded_opt = ",\n  'scan.bounded.mode' = 'latest-offset'" if bounded else ""
    return f"""CREATE TABLE {name} (
  type STRING,
  click_id STRING,
  ad_id STRING,
  event_ts BIGINT,
  ts AS TO_TIMESTAMP_LTZ(event_ts, 3),
  pt AS PROCTIME(),
  WATERMARK FOR ts AS ts - INTERVAL '{int(watermark_sec)}' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'raw-clicks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-aggregation',
  'properties.auto.offset.reset' = 'earliest',
  'scan.startup.mode' = '{startup}',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'{bounded_opt}
)"""


def sink_ddl(name: str, guarantee: str, linger: str, prefix: str) -> str:
    tx = f",\n  'sink.transactional-id-prefix' = '{prefix}'" if guarantee == "exactly-once" else ""
    return f"""CREATE TABLE {name} (
  kind STRING, window_start STRING, ad_id STRING, clicks BIGINT, source STRING, emitted_at STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'agg-results',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.linger.ms' = '{linger}',
  'properties.transaction.timeout.ms' = '600000',
  'format' = 'json',
  'sink.delivery-guarantee' = '{guarantee}'{tx}
)"""


def agg_query(src: str, sink: str, bug: bool, source_label: str = "stream",
              with_proc: bool = True, bound_s: int | None = None) -> str:
    mult = 2 if bug else 1
    where = f" AND window_end <= TO_TIMESTAMP_LTZ({bound_s * 1000}, 3)" if bound_s else ""
    q = f"""INSERT INTO {sink}
SELECT 'event', DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss'), ad_id, COUNT(DISTINCT click_id) * {mult},
       '{source_label}', DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS')
FROM TABLE(TUMBLE(TABLE {src}, DESCRIPTOR(ts), INTERVAL '10' SECOND))
WHERE type = 'click'{where}
GROUP BY window_start, window_end, ad_id"""
    if with_proc:
        q += f"""
UNION ALL
SELECT 'proc', DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss'), ad_id, COUNT(DISTINCT click_id),
       '{source_label}', DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss.SSS')
FROM TABLE(TUMBLE(TABLE {src}, DESCRIPTOR(pt), INTERVAL '10' SECOND))
WHERE type = 'click'
GROUP BY window_start, window_end, ad_id"""
    return q


# ---------- SQL Gateway ----------
def run_statements(props: dict, statements: list[str]) -> str | None:
    """새 세션에서 문장을 차례로 실행한다. INSERT 가 만든 잡 ID 를 돌려준다."""
    sess = _req(f"{GATEWAY}/v1/sessions", {"properties": props})["sessionHandle"]
    job_id = None
    try:
        for st in statements:
            op = _req(f"{GATEWAY}/v1/sessions/{sess}/statements", {"statement": st})["operationHandle"]
            base = f"{GATEWAY}/v1/sessions/{sess}/operations/{op}"
            for _ in range(240):
                status = _req(f"{base}/status")["status"]
                if status in ("FINISHED", "ERROR", "CANCELED", "CLOSED", "TIMEOUT"):
                    break
                time.sleep(0.25)
            if status != "FINISHED":
                try:
                    _req(f"{base}/result/0?rowFormat=JSON")
                    detail = status
                except RuntimeError as e:
                    causes = [ln.strip() for ln in str(e).replace("\\n", "\n").splitlines() if "Caused by" in ln]
                    detail = " / ".join(causes[-2:]) if causes else str(e)[-300:]
                raise RuntimeError(f"Flink SQL 실패 ({status}): {st.splitlines()[0][:60]} … {detail}")
            r = _req(f"{base}/result/0?rowFormat=JSON")
            if r.get("jobID"):
                job_id = r["jobID"]
            for row in (r.get("results") or {}).get("data", []):
                f = row.get("fields") or []
                if f and isinstance(f[0], str) and len(f[0]) == 32:
                    job_id = f[0]
    finally:
        try:
            _req(f"{GATEWAY}/v1/sessions/{sess}", method="DELETE")
        except Exception:  # noqa: BLE001
            pass
    return job_id


def deploy(cfg: dict, restore_path: str | None = None, from_earliest: bool = False) -> str:
    s = STRATEGY[cfg["strategy"]]
    use_ckpt = cfg["strategy"] == "transaction" or cfg.get("snapshot", True)
    # 한 단계 집계: 로컬·글로벌 2단계로 나누면 로컬 단계가 늦은 레코드를 메트릭 없이 버려서 '늦어서 버린 수'가 안 보인다
    props = {"pipeline.name": JOB_NAME, "table.local-time-zone": "UTC", "parallelism.default": "1",
             "table.optimizer.agg-phase-strategy": "ONE_PHASE"}
    if use_ckpt:
        props["execution.checkpointing.interval"] = s["checkpoint"]
    if restore_path:
        props["execution.state-recovery.path"] = restore_path
        props["execution.state-recovery.ignore-unclaimed-state"] = "true"
    # 체크포인트를 끄면 실패 때 되살릴 오프셋이 없다 → 처음부터 다시 읽게 둔다 (교재: 스냅숏이 없으면 처음부터 재생)
    startup = "earliest-offset" if (from_earliest or not use_ckpt) else "group-offsets"
    return run_statements(props, [
        source_ddl("clicks_src", cfg["watermark_sec"], startup),
        sink_ddl("agg_out", s["guarantee"], s["linger"], f"adclick-{int(time.time())}"),
        agg_query("clicks_src", "agg_out", cfg["bug"]),
    ])


def deploy_recalc(cfg: dict, bound_s: int) -> str:
    """재계산: raw-clicks 를 처음부터 지금 끝까지 읽고 끝나는 유한 잡. 같은 SQL, 버그 없는 버전."""
    return run_statements({"pipeline.name": RECALC_NAME, "table.local-time-zone": "UTC", "parallelism.default": "1",
                           "table.optimizer.agg-phase-strategy": "ONE_PHASE", "execution.runtime-mode": "streaming"}, [
        source_ddl("clicks_all", cfg["watermark_sec"], "earliest-offset", bounded=True),
        sink_ddl("recalc_out", "at-least-once", "0", "recalc"),
        agg_query("clicks_all", "recalc_out", bug=False, source_label="recalc", with_proc=False, bound_s=bound_s),
    ])


# ---------- REST ----------
def jobs() -> list[dict]:
    return _req(f"{JM}/jobs/overview", timeout=3).get("jobs", [])


def running_job() -> dict | None:
    js = [j for j in jobs() if j["name"] == JOB_NAME and j["state"] in ("RUNNING", "RESTARTING", "CREATED", "INITIALIZING")]
    return max(js, key=lambda j: j["start-time"]) if js else None


def stop_with_savepoint(job_id: str, timeout: float = 60) -> str:
    trig = _req(f"{JM}/jobs/{job_id}/stop", {"drain": False})["request-id"]
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = _req(f"{JM}/jobs/{job_id}/savepoints/{trig}")
        if r.get("status", {}).get("id") == "COMPLETED":
            op = r.get("operation", {})
            if "location" in op:
                return op["location"]
            raise RuntimeError(f"세이브포인트 실패: {json.dumps(op)[:300]}")
        time.sleep(0.5)
    raise RuntimeError("세이브포인트 시간 초과")


def cancel(job_id: str):
    _req(f"{JM}/jobs/{job_id}?mode=cancel", method="PATCH")


def job_detail(job_id: str) -> dict:
    return _req(f"{JM}/jobs/{job_id}", timeout=3)


def checkpoints(job_id: str) -> dict:
    return _req(f"{JM}/jobs/{job_id}/checkpoints", timeout=3)


def window_watermark(job_id: str, detail: dict | None = None) -> int | None:
    """윈도 집계 연산자의 입력 워터마크 (ms)."""
    detail = detail or job_detail(job_id)
    vs = detail.get("vertices", [])
    # 이벤트 시각 윈도는 GlobalWindowAggregate (로컬·글로벌 2단계 집계). 없으면 이름에 Window 가 든 첫 연산자
    vs = sorted(vs, key=lambda v: (not v["name"].startswith("GlobalWindowAggregate"), "Window" not in v["name"]))
    for v in vs[:1]:
        if "Window" in v["name"]:
            try:
                ws = _req(f"{JM}/jobs/{job_id}/vertices/{v['id']}/watermarks", timeout=3)
                vals = [int(float(w["value"])) for w in ws if "currentInputWatermark" in w["id"]]
                vals = [x for x in vals if x > 0]
                if vals:
                    return min(vals)
            except Exception:  # noqa: BLE001
                return None
    return None


def late_dropped(job_id: str, detail: dict | None = None) -> int | None:
    """윈도 연산자가 늦어서 버린 레코드 수 (Flink 메트릭 numLateRecordsDropped)."""
    detail = detail or job_detail(job_id)
    total, found = 0, False
    for v in detail.get("vertices", []):
        try:
            names = [m["id"] for m in _req(f"{JM}/jobs/{job_id}/vertices/{v['id']}/metrics", timeout=3)]
        except Exception:  # noqa: BLE001
            continue
        want = [n for n in names if n.endswith("numLateRecordsDropped")]
        if want:
            vals = _req(f"{JM}/jobs/{job_id}/vertices/{v['id']}/metrics?get={','.join(want)}", timeout=3)
            total += sum(int(float(m["value"])) for m in vals)
            found = True
    return total if found else None
