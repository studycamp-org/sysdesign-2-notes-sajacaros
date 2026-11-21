"""집계 서비스 (교재 6장 3단계의 문제들을 눈으로 보이게 만든 버전).

- 10초 텀블링 윈도 (교재 1분을 축소). 이벤트 시각으로 윈도를 정한다.
- 워터마크 = 지금까지 본 가장 늦은 이벤트 시각(클릭 + 광고 서버의 1초 tick) - WATERMARK.
  윈도 끝이 워터마크보다 앞서면 닫는다. 닫힌 윈도에 들어온 클릭은 '늦어서 버림'.
- 윈도마다 click_id 집합으로 클라이언트 중복을 버린다.
- 처리 시각 기준 집계도 함께 낸다 (그림 12: 이벤트 시각과 비교용).
- 결과는 배치마다 (윈도, 광고)별 증가분(delta)으로 내보낸다. 집계 DB 는 더한다.
  → 같은 결과가 두 번 나가면 DB 값이 실제로 커진다. 교재 그림 17~20 의 중복·유실이 그대로 보인다.

전달 보장 전략 (운영자가 고른다):
  offset_first     해법 1(그림 18): 오프셋 저장 → 결과 전송.  사이에서 죽으면 유실
  send_then_offset 해법 2(그림 19): 결과 전송 → 오프셋 저장.  사이에서 죽으면 중복
  transaction      해법 3(그림 20): 결과 + 스냅숏 + 오프셋을 Kafka 트랜잭션 하나로. 정확히 한 번

스냅숏 (그림 26~27): transaction 전략에서 열린 윈도 상태를 오프셋과 함께 커밋한다.
  끄면 재시작 때 raw-clicks 를 처음부터 커밋된 오프셋까지 다시 읽어 상태를 다시 만든다.

버그 배포·재계산 (그림 11): 버그 버전은 클릭을 두 번 센다. /recalc 는 고친 로직으로 raw-clicks 를
  처음부터 다시 집계해 닫힌 윈도의 결과를 갈아 끼운다 (같은 집계 코드를 쓰는 카파 방식).
"""
import json
import threading
import time
import urllib.request
from collections import Counter

from confluent_kafka import (OFFSET_BEGINNING, Consumer, KafkaException, Producer, TopicPartition)

from common import KAFKA, OUT_TOPIC, RAW_TOPIC, SNAP_TOPIC, _ch, fmt_ms, log, now_ms, serve

API = "http://api:8080"
WIN = 10_000  # 윈도 길이 10초
SNAP_KEY = b"aggregator"
RAW_TP = TopicPartition(RAW_TOPIC, 0)


def wstart(ms: int) -> int:
    return ms // WIN * WIN


def post(path: str, body: dict, timeout: float = 2):
    try:
        req = urllib.request.Request(API + path, data=json.dumps(body).encode(), method="POST",
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=timeout).read()
    except Exception as e:  # noqa: BLE001
        log.warning("api 알림 실패 %s: %s", path, e)


# ---------- 설정 (api 가 원본을 갖고 있어 재시작해도 남는다) ----------
cfg = {"watermark_sec": 3, "strategy": "transaction", "snapshot": True, "bug": False}
_cfg_at = 0.0


def refresh_cfg():
    global _cfg_at
    if time.monotonic() - _cfg_at < 1:
        return
    _cfg_at = time.monotonic()
    try:
        with urllib.request.urlopen(API + "/api/config", timeout=1) as r:
            cfg.update(json.loads(r.read()))
    except Exception:  # noqa: BLE001
        pass


# ---------- 집계 로직 (실시간과 재계산이 같이 쓴다) ----------
class Agg:
    def __init__(self):
        self.max_ts = 0          # 본 것 중 가장 늦은 이벤트 시각
        self.closed_upto = 0     # 이 시각 앞의 윈도는 닫혔다
        self.seen: dict[int, set] = {}     # 열린 윈도 → click_id 집합
        self.proc_seen: dict[str, int] = {}  # 처리 시각 집계용 중복 제거 (click_id → 본 시각)

    def watermark(self, wm_ms: int) -> int:
        return self.max_ts - wm_ms if self.max_ts else 0

    def process(self, e: dict, wm_ms: int, bug: bool, proc_now: int | None = None):
        """이벤트 하나. 반환: (판정, 이벤트 시각 증가분 [(윈도, 광고, n)], 처리 시각 증가분)"""
        ts = int(e["event_ts"])
        if e.get("type") == "tick":
            self.max_ts = max(self.max_ts, ts)
            self._close(wm_ms)
            return None, [], []
        cid, ad = e["click_id"], e["ad_id"]
        proc = []
        if proc_now is not None and cid not in self.proc_seen:
            self.proc_seen[cid] = proc_now
            proc = [(wstart(proc_now), ad, 1)]
        if ts < self.closed_upto:
            return "late", [], proc
        w = wstart(ts)
        s = self.seen.setdefault(w, set())
        if cid in s:
            return "dup", [], proc
        s.add(cid)
        self.max_ts = max(self.max_ts, ts)
        self._close(wm_ms)
        return ("counted_bug" if bug else "counted"), [(w, ad, 2 if bug else 1)], proc

    def _close(self, wm_ms: int):
        wm = self.watermark(wm_ms)
        if wm > 0:
            self.closed_upto = max(self.closed_upto, wstart(wm))
        for w in [w for w in self.seen if w + WIN <= self.closed_upto]:
            del self.seen[w]
        if len(self.proc_seen) > 5000:  # 오래된 것 정리
            cut = now_ms() - 600_000
            self.proc_seen = {k: v for k, v in self.proc_seen.items() if v >= cut}

    def dump(self) -> dict:
        return {"max_ts": self.max_ts, "closed_upto": self.closed_upto,
                "seen": {str(w): list(s) for w, s in self.seen.items()}, "proc_seen": self.proc_seen}

    def load(self, d: dict):
        self.max_ts, self.closed_upto = d["max_ts"], d["closed_upto"]
        self.seen = {int(w): set(s) for w, s in d["seen"].items()}
        self.proc_seen = d.get("proc_seen", {})


agg = Agg()
flags = {"crash": False}
runtime = {"started_at": now_ms(), "batches": 0, "last_steps": [], "last_batch_clicks": 0,
           "restore": None, "recalc": None, "committed_offset": None, "lag": None}


def result_rows(deltas: list, kind: str, source: str = "stream") -> list[dict]:
    c = Counter()
    for w, ad, n in deltas:
        c[(w, ad)] += n
    at = fmt_ms(now_ms(), millis=True)
    return [{"kind": kind, "window_start": fmt_ms(w), "ad_id": ad, "clicks": n, "source": source, "emitted_at": at}
            for (w, ad), n in c.items()]


# ---------- 재시작 복구 ----------
def committed_offset(consumer: Consumer) -> int:
    c = consumer.committed([RAW_TP], timeout=10)[0]
    return c.offset if c.offset >= 0 else 0


def read_last_snapshot() -> dict | None:
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": "aggregator-restore", "enable.auto.commit": False,
                  "isolation.level": "read_committed"})
    c.assign([TopicPartition(SNAP_TOPIC, 0, OFFSET_BEGINNING)])
    last, idle = None, 0
    while idle < 2:
        m = c.poll(1.0)
        if m is None:
            idle += 1
        elif not m.error():
            last, idle = m.value(), 0
    c.close()
    return json.loads(last) if last else None


def rebuild_by_replay(upto: int) -> int:
    """스냅숏 없이: raw-clicks 를 처음부터 upto 직전까지 다시 읽어 상태만 만든다 (결과는 내지 않는다)."""
    if upto <= 0:
        return 0
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": "aggregator-rebuild", "enable.auto.commit": False})
    c.assign([TopicPartition(RAW_TOPIC, 0, OFFSET_BEGINNING)])
    n = 0
    done = False
    while not done:
        for m in c.consume(500, 1.0):
            if m.error():
                continue
            if m.offset() >= upto:
                done = True
                break
            agg.process(json.loads(m.value()), cfg["watermark_sec"] * 1000, False, proc_now=None)
            n += 1
            if m.offset() == upto - 1:
                done = True
                break
    c.close()
    return n


def restore(consumer: Consumer):
    t0 = time.monotonic()
    committed = committed_offset(consumer)
    how, replayed = "empty", 0
    if cfg["strategy"] == "transaction" and committed > 0:
        snap = read_last_snapshot() if cfg["snapshot"] else None
        if snap and snap.get("offset") == committed:
            agg.load(snap["state"])
            how = "snapshot"
        else:
            if snap:
                log.info("스냅숏 오프셋(%s)이 커밋 오프셋(%s)과 달라 버린다", snap.get("offset"), committed)
            replayed = rebuild_by_replay(committed)
            how = "replay"
    took = round((time.monotonic() - t0) * 1000)
    runtime["restore"] = {"how": how, "committed_offset": committed, "replayed": replayed, "took_ms": took,
                          "strategy": cfg["strategy"], "snapshot": cfg["snapshot"],
                          "open_windows": sorted(fmt_ms(w)[11:] for w in agg.seen)}
    log.info("기동 복구: %s", runtime["restore"])
    post("/api/agg-event", {"type": "start", **runtime["restore"]})


# ---------- 커밋 전략 ----------
def crash_here(step: str):
    if flags["crash"]:
        log.warning("💥 강제 종료: '%s' 직후", step)
        post("/api/agg-event", {"type": "crash", "after": step, "strategy": cfg["strategy"]}, timeout=1)
        import os
        os._exit(1)


def commit(consumer, tx_producer, plain_producer, rows, decisions, next_offset):
    steps = []
    t = lambda name: steps.append({"step": name, "at": now_ms()})  # noqa: E731
    strategy = cfg["strategy"]
    offsets = [TopicPartition(RAW_TOPIC, 0, next_offset)]

    def send(producer):
        for r in rows:
            producer.produce(OUT_TOPIC, json.dumps(r).encode())

    if strategy == "offset_first":
        consumer.commit(offsets=offsets, asynchronous=False)
        t("오프셋 저장")
        if rows:
            crash_here("오프셋 저장")
        send(plain_producer)
        plain_producer.flush(10)
        t("결과 전송")
        post("/api/decisions", {"items": decisions})
    elif strategy == "send_then_offset":
        send(plain_producer)
        plain_producer.flush(10)
        t("결과 전송")
        post("/api/decisions", {"items": decisions})
        if rows:
            crash_here("결과 전송")
        consumer.commit(offsets=offsets, asynchronous=False)
        t("오프셋 저장")
    else:  # transaction
        tx_producer.begin_transaction()
        t("트랜잭션 시작")
        send(tx_producer)
        t("결과 전송")
        if cfg["snapshot"]:
            tx_producer.produce(SNAP_TOPIC, key=SNAP_KEY,
                                value=json.dumps({"offset": next_offset, "state": agg.dump()}).encode())
            t("스냅숏")
        tx_producer.send_offsets_to_transaction(offsets, consumer.consumer_group_metadata())
        t("오프셋")
        if rows:
            crash_here("결과·오프셋 전송 (커밋 전)")
        tx_producer.commit_transaction()
        t("커밋")
        post("/api/decisions", {"items": decisions})
    runtime["last_steps"] = steps
    runtime["committed_offset"] = next_offset


# ---------- 재계산 ----------
def recalc(_body):
    if runtime.get("recalc", {}) and runtime["recalc"].get("running"):
        return {"error": "이미 재계산 중"}
    threading.Thread(target=_recalc, daemon=True).start()
    return {"ok": True}


def _recalc():
    t0 = time.monotonic()
    bound = agg.closed_upto
    runtime["recalc"] = {"running": True, "bound": fmt_ms(bound)}
    post("/api/agg-event", {"type": "recalc_start", "bound": fmt_ms(bound)})
    fresh = Agg()
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"recalc-{now_ms()}", "enable.auto.commit": False})
    _, hi = c.get_watermark_offsets(RAW_TP, timeout=5)
    c.assign([TopicPartition(RAW_TOPIC, 0, OFFSET_BEGINNING)])
    deltas, n, pos = [], 0, 0
    while pos < hi:
        msgs = c.consume(500, 1.0)
        if not msgs:
            break
        for m in msgs:
            if m.error():
                continue
            pos = m.offset() + 1
            if m.offset() >= hi:
                break
            _, d, _ = fresh.process(json.loads(m.value()), cfg["watermark_sec"] * 1000, bug=False)
            deltas += [x for x in d if x[0] < bound]
            n += 1
    c.close()
    rows = result_rows(deltas, "event", source="recalc")
    # 닫힌 윈도의 이벤트 시각 결과를 지우고 새 결과로 갈아 끼운다 (교재: 두 번째 큐 → 집계 DB)
    _ch("DELETE FROM adclick.ad_clicks WHERE kind = 'event' AND window_start < toDateTime({b:UInt32}, 'UTC')",
        {"b": bound // 1000})
    p = Producer({"bootstrap.servers": KAFKA})
    for r in rows:
        p.produce(OUT_TOPIC, json.dumps(r).encode())
    p.flush(10)
    runtime["recalc"] = {"running": False, "bound": fmt_ms(bound), "events": n, "rows": len(rows),
                         "clicks": sum(r["clicks"] for r in rows), "took_ms": round((time.monotonic() - t0) * 1000)}
    post("/api/agg-event", {"type": "recalc_done", **runtime["recalc"]})
    log.info("재계산 끝: %s", runtime["recalc"])


def stats(_q):
    wm_ms = cfg["watermark_sec"] * 1000
    return {**runtime, "cfg": cfg, "max_ts": agg.max_ts, "watermark": agg.watermark(wm_ms),
            "closed_upto": agg.closed_upto, "open_windows": sorted(agg.seen), "crash_armed": flags["crash"]}


def main():
    refresh_cfg()
    tx = Producer({"bootstrap.servers": KAFKA, "transactional.id": "aggregator-1", "linger.ms": 5})
    tx.init_transactions()  # 이전 기동의 열린 트랜잭션은 여기서 중단(abort)된다
    plain = Producer({"bootstrap.servers": KAFKA, "linger.ms": 5})
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "aggregator",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest",
                         "isolation.level": "read_committed"})
    restore(consumer)
    consumer.assign([TopicPartition(RAW_TOPIC, 0, runtime["restore"]["committed_offset"])])
    serve(get={"/stats": stats},
          post={"/crash": lambda b: flags.update(crash=bool(b.get("arm", True))) or {"armed": flags["crash"]},
                "/recalc": recalc})

    next_offset = runtime["restore"]["committed_offset"]
    while True:
        refresh_cfg()
        msgs = consumer.consume(500, 1.0)
        wm_ms, bug = cfg["watermark_sec"] * 1000, cfg["bug"]
        ev, pr, decisions = [], [], []
        now = now_ms()
        for m in msgs:
            if m.error():
                continue
            next_offset = m.offset() + 1
            e = json.loads(m.value())
            verdict, d, p = agg.process(e, wm_ms, bug, proc_now=now)
            ev += d
            pr += p
            if verdict:
                decisions.append({"click_id": e["click_id"], "decision": verdict, "at": now,
                                  "window": wstart(int(e["event_ts"])), "proc_window": wstart(now)})
        if not msgs:
            continue
        rows = result_rows(ev, "event") + result_rows(pr, "proc")
        runtime["batches"] += 1
        runtime["last_batch_clicks"] = len(decisions)
        commit(consumer, tx, plain, rows, decisions, next_offset)
        try:
            _, hi = consumer.get_watermark_offsets(RAW_TP, timeout=1, cached=False)
            runtime["lag"] = hi - next_offset
        except KafkaException:
            pass


if __name__ == "__main__":
    main()
