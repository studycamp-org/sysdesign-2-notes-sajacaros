"""광고 서버 + 질의 API + 대시보드.

광고 서버 역할
  POST /api/click   휴대폰(브라우저)의 클릭을 받아 clicks.log 에 한 줄 쓴다 → Fluent Bit 이 Kafka 로.
                    받은 고유 click_id 를 이벤트 시각 윈도별로 기록한다 = '정답 장부'.
  1초마다 tick       {"type":"tick","event_ts":지금} 을 로그에 쓴다. 클릭이 드물어도 워터마크가 앞으로 가게.
  배경 클릭          운영자가 켜면 '봇'이 3초마다 한 번 누른다.

운영 설정 (집계 서비스가 1초마다 읽어 간다)
  GET/POST /api/config  {watermark_sec, strategy, snapshot, bug, bots}

화면용
  GET  /api/view       윈도별 장부·집계, 최근 클릭과 판정, 집계 서비스 상태, 기록
  POST /api/decisions  (집계 서비스 → 여기) 결과를 내보낸 클릭의 판정
  POST /api/agg-event  (집계 서비스 → 여기) 기동·강제 종료·재계산 기록
  POST /api/crash | /api/recalc | /api/recon   각 서비스로 전달
"""
import json
import os
import random
import threading
import time
import uuid
from collections import defaultdict, deque

from common import ch_rows, fmt_ms, http_json, log, now_ms, serve

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
LOG_PATH = "/logs/clicks.log"
WIN = 10_000
ADS = {"ad-1": "가벼운 러닝화", "ad-2": "싱글 오리진 원두", "ad-3": "시스템 설계 책"}

lock = threading.Lock()
os.makedirs("/logs", exist_ok=True)
out = open(LOG_PATH, "w", buffering=1)  # 켤 때마다 새 파일

config = {"watermark_sec": 3, "strategy": "transaction", "snapshot": True, "bug": False, "bots": False}
clicks: dict = {}                         # click_id → {ad_id, event_ts, phone, recv: [수신 시각...]}
ledger: dict = defaultdict(set)           # 윈도 시작 → click_id 집합 (광고 서버가 받은 고유 클릭)
decisions: dict = defaultdict(list)       # click_id → [판정...] (집계 서비스가 결과를 내보낸 뒤 알려 줌)
events: deque = deque(maxlen=40)          # 기록 (기동·강제 종료·재계산·설정 변경)
recon_last: dict = {}


def write(line: dict):
    with lock:
        out.write(json.dumps(line) + "\n")
        out.flush()


def note(kind: str, msg: str, **extra):
    events.appendleft({"at": now_ms(), "kind": kind, "msg": msg, **extra})


# ---------- 광고 서버 ----------
def click(body: dict) -> dict:
    ad = body.get("ad_id")
    if ad not in ADS:
        return {"error": f"없는 광고 {ad}"}
    cid = str(body.get("click_id") or uuid.uuid4().hex[:12])[:32]
    ev_ts = int(body.get("event_ts") or now_ms())
    recv = now_ms()
    phone = str(body.get("phone") or "?")[:20]
    write({"type": "click", "click_id": cid, "ad_id": ad, "event_ts": ev_ts, "phone": phone, "recv_ts": recv})
    with lock:
        c = clicks.setdefault(cid, {"click_id": cid, "ad_id": ad, "event_ts": ev_ts, "phone": phone, "recv": []})
        c["recv"].append(recv)
        ledger[ev_ts // WIN * WIN].add(cid)
    return {"ok": True, "click_id": cid, "recv_ts": recv, "received_times": len(c["recv"])}


def ticker():
    last_bot = 0.0
    while True:
        time.sleep(1 - (time.time() % 1))
        write({"type": "tick", "event_ts": now_ms()})
        if config["bots"] and time.time() - last_bot >= 3:
            last_bot = time.time()
            click({"ad_id": random.choice(list(ADS)), "phone": "봇"})


# ---------- 설정 ----------
LABEL = {"offset_first": "① 오프셋 먼저 저장", "send_then_offset": "② 결과 전송 후 오프셋 저장",
         "transaction": "③ 분산 트랜잭션"}


def set_config(body: dict) -> dict:
    changes = []
    for k in ("watermark_sec", "strategy", "snapshot", "bug", "bots"):
        if k in body and body[k] != config[k]:
            v = body[k]
            if k == "watermark_sec":
                v = max(0, min(int(v), 30))
            if k == "strategy" and v not in LABEL:
                continue
            config[k] = v
            changes.append(k)
    for k in changes:
        msg = {"watermark_sec": f"워터마크 {config['watermark_sec']}초", "strategy": f"전략 {LABEL[config['strategy']]}",
               "snapshot": f"스냅숏 {'켬' if config['snapshot'] else '끔'}",
               "bug": "🐞 버그 버전 배포 (클릭을 두 번 셈)" if config["bug"] else "✅ 버그 수정 버전 배포",
               "bots": f"배경 클릭 {'켬' if config['bots'] else '끔'}"}[k]
        note("config", msg)
    return config


# ---------- 집계 서비스가 보내는 것 ----------
def on_decisions(body: dict) -> dict:
    with lock:
        for d in body.get("items", []):
            decisions[d["click_id"]].append(d["decision"])
    return {"ok": True}


def on_agg_event(body: dict) -> dict:
    t = body.get("type")
    if t == "start":
        how = {"snapshot": "스냅숏에서 복구", "replay": f"처음부터 {body.get('replayed')}건 다시 읽어 복구",
               "empty": "빈 상태로 시작 (커밋된 오프셋부터)"}[body["how"]]
        note("start", f"집계 서비스 기동 · {how} · {body.get('took_ms')}ms", **body)
    elif t == "crash":
        note("crash", f"💥 집계 서비스 강제 종료 — '{body.get('after')}' 직후 ({LABEL.get(body.get('strategy'), '')})")
    elif t == "recalc_start":
        note("recalc", f"재계산 시작: {body.get('bound', '')[11:]} 앞의 닫힌 윈도를 raw-clicks 처음부터 다시 집계")
    elif t == "recalc_done":
        note("recalc", f"재계산 끝: 이벤트 {body.get('events')}건 다시 읽음, 클릭 {body.get('clicks')} · {body.get('took_ms')}ms")
    return {"ok": True}


# ---------- 화면 ----------
def agg_stats():
    try:
        return http_json("http://aggregator:8080/stats", timeout=1.5)
    except Exception as e:  # noqa: BLE001
        return {"down": True, "error": str(e)[:100]}


def view(_q):
    now = now_ms()
    a = agg_stats()
    lo = (now - 120_000) // WIN * WIN
    try:
        rows = ch_rows("""
            SELECT toUInt64(toUnixTimestamp(window_start)) * 1000 AS w,
                   sumIf(clicks, kind = 'event') AS ev, sumIf(clicks, kind = 'proc') AS pr,
                   sumIf(clicks, kind = 'event' AND source = 'recon') AS recon,
                   countIf(kind = 'event' AND source = 'recalc') > 0 AS recalc
            FROM adclick.ad_clicks
            WHERE window_start >= toDateTime({lo:UInt32}, 'UTC')
            GROUP BY w""", {"lo": lo // 1000})
        db = {int(r["w"]): r for r in rows}
        ch_ok = True
    except Exception as e:  # noqa: BLE001
        db, ch_ok = {}, False
        log.warning("ClickHouse 질의 실패: %s", e)
    closed = a.get("closed_upto") or 0
    wm = a.get("watermark") or 0
    windows = []
    with lock:
        for w in range(lo, now // WIN * WIN + WIN, WIN):
            led = len(ledger.get(w, ()))
            r = db.get(w, {})
            ev = r.get("ev", 0)
            status = "closed" if w + WIN <= closed else ("waiting" if w + WIN <= now else "open")
            windows.append({"w": w, "label": fmt_ms(w)[11:], "status": status, "ledger": led, "event": ev,
                            "proc": r.get("pr", 0), "recon": r.get("recon", 0), "recalc": bool(r.get("recalc"))})
        recent = sorted((c for c in clicks.values() if c["event_ts"] >= lo or c["recv"][-1] >= lo),
                        key=lambda c: c["recv"][0])[-150:]
        evs = [{**c, "decisions": decisions.get(c["click_id"], [])} for c in recent]
    return {"now": now, "config": config, "ads": ADS, "windows": windows, "clicks": evs, "agg": a,
            "watermark": wm, "closed_upto": closed, "max_ts": a.get("max_ts"), "events": list(events),
            "recon": recon_last, "clickhouse_ok": ch_ok}


def crash(body):
    return http_json("http://aggregator:8080/crash", {"arm": body.get("arm", True)}, timeout=3)


def recalc(_body):
    return http_json("http://aggregator:8080/recalc", {}, timeout=3)


def recon(_body):
    r = http_json("http://recon:8080/run", {}, timeout=120)
    recon_last.clear()
    recon_last.update(r)
    if "error" not in r:
        note("recon", f"재집계 검증: 윈도 {r.get('windows')}개 비교, 불일치 {r.get('mismatches')}행 → 보정 {r.get('fixed_clicks'):+d}")
    return r


def main():
    threading.Thread(target=ticker, daemon=True).start()
    serve(get={"/api/view": view, "/api/config": lambda q: config},
          post={"/api/click": click, "/api/config": set_config, "/api/decisions": on_decisions,
                "/api/agg-event": on_agg_event, "/api/crash": crash, "/api/recalc": recalc, "/api/recon": recon},
          static_dir=STATIC)
    log.info("광고 서버·대시보드 http://localhost:8006")
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
