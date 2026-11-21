"""광고 서버 + Flink 잡 관리 + 질의 API + 대시보드.

광고 서버
  POST /api/click   휴대폰(브라우저)의 클릭을 받아 clicks.log 에 한 줄 쓴다 → Fluent Bit → Kafka raw-clicks.
                    받은 고유 click_id 를 이벤트 시각 윈도별로 기록한다 = '정답 장부'.
  1초마다 tick       {"type":"tick","event_ts":지금}. 클릭이 드물어도 Flink 워터마크가 앞으로 가게.

Flink 잡 관리 (flinkctl)
  설정(워터마크·전략·체크포인트·버그)이 바뀌면 세이브포인트를 찍고 새 설정으로 다시 띄운다.
  💥 : 다음 윈도 결과가 나온 직후 TaskManager 컨테이너를 강제로 죽였다가 켠다 → Flink 가 스스로 복구.
  🔁 : 닫힌 윈도를 지우고, 같은 SQL 의 유한(bounded) 잡으로 raw-clicks 처음부터 다시 집계한다.

클릭 하나하나의 판정(늦음·중복)은 Flink 가 내보내지 않는다. 화면의 ✗·↻ 는 광고 서버가 같은 규칙
(윈도 끝 ≤ 지금까지 본 가장 늦은 시각 - 워터마크 → 늦음, 같은 click_id → 중복)으로 계산해 표시한다.
"""
import http.client
import json
import os
import random
import socket
import threading
import time
import uuid
from collections import defaultdict, deque

import flinkctl
from common import _ch, ch_rows, fmt_ms, log, now_ms, serve

STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
LOG_PATH = "/logs/clicks.log"
WIN = 10_000
ADS = {"ad-1": "가벼운 러닝화", "ad-2": "싱글 오리진 원두", "ad-3": "시스템 설계 책"}
TM_CONTAINER = os.environ.get("TM_CONTAINER", "mini-adclick-taskmanager-1")
LABEL = {"offset_first": "① 최대 한 번 (NONE)", "send_then_offset": "② 최소 한 번 (AT_LEAST_ONCE)",
         "transaction": "③ 정확히 한 번 (EXACTLY_ONCE)"}

lock = threading.Lock()
os.makedirs("/logs", exist_ok=True)
out = open(LOG_PATH, "w", buffering=1)  # 켤 때마다 새 파일

config = {"watermark_sec": 3, "strategy": "transaction", "snapshot": True, "bug": False, "bots": False}
CONFIG_PATH = "/logs/config.json"  # 광고 서버가 재시작해도 지금 잡의 설정을 기억한다
try:
    with open(CONFIG_PATH) as _f:
        config.update(json.load(_f))
except (OSError, ValueError):
    pass


def save_config():
    try:
        with open(CONFIG_PATH, "w") as f:
            json.dump(config, f)
    except OSError:
        pass
clicks: dict = {}                     # click_id → {ad_id, event_ts, phone, recv: [...], verdict}
ledger: dict = defaultdict(set)       # (윈도, 광고) → click_id 집합
max_seen = 0                          # 광고 서버가 쓴 가장 늦은 이벤트 시각 (클릭 + tick)
written: deque = deque(maxlen=20000)  # 로그에 쓴 줄의 시각 (복구 때 다시 읽은 양 계산용)
events: deque = deque(maxlen=50)
flink = {"job_id": None, "busy": "Flink 잡 준비 중", "crash_armed": False, "last_wm": 0, "recalc": None, "error": None}
recon_last: dict = {}


def note(kind: str, msg: str, **extra):
    events.appendleft({"at": now_ms(), "kind": kind, "msg": msg, **extra})
    log.info("[%s] %s", kind, msg)


def write(line: dict):
    global max_seen
    with lock:
        out.write(json.dumps(line) + "\n")
        out.flush()
        max_seen = max(max_seen, int(line["event_ts"]))
        written.append(now_ms())


# ---------- 광고 서버 ----------
def click(body: dict) -> dict:
    ad = body.get("ad_id")
    if ad not in ADS:
        return {"error": f"없는 광고 {ad}"}
    cid = str(body.get("click_id") or uuid.uuid4().hex[:12])[:32]
    ev_ts = int(body.get("event_ts") or now_ms())
    recv = now_ms()
    with lock:
        first = cid not in clicks
        if first:
            # Flink 의 실제 워터마크(약 0.5초마다 읽음) 기준. Flink 가 이 클릭을 처리할 때까지 1초쯤 더 간다
            late = bool(flink["last_wm"]) and ev_ts // WIN * WIN + WIN <= flink["last_wm"] + 1000
            clicks[cid] = {"click_id": cid, "ad_id": ad, "event_ts": ev_ts, "phone": str(body.get("phone") or "?")[:20],
                           "recv": [], "verdict": "late" if late else "ok"}
            ledger[(ev_ts // WIN * WIN, ad)].add(cid)
        clicks[cid]["recv"].append(recv)
    write({"type": "click", "click_id": cid, "ad_id": ad, "event_ts": ev_ts,
           "phone": clicks[cid]["phone"], "recv_ts": recv})
    return {"ok": True, "click_id": cid, "received_times": len(clicks[cid]["recv"])}


def ticker():
    last_bot = 0.0
    while True:
        time.sleep(1 - (time.time() % 1))
        write({"type": "tick", "event_ts": now_ms()})
        if config["bots"] and time.time() - last_bot >= 3:
            last_bot = time.time()
            click({"ad_id": random.choice(list(ADS)), "phone": "봇"})


# ---------- 도커 (💥 용) ----------
class UnixHTTP(http.client.HTTPConnection):
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect("/var/run/docker.sock")


def docker(method: str, path: str):
    c = UnixHTTP("localhost", timeout=20)
    c.request(method, path)
    r = c.getresponse()
    body = r.read()
    if r.status >= 400:
        raise RuntimeError(f"docker {path}: {r.status} {body[:200]!r}")


# ---------- Flink 잡 ----------
def flink_wm() -> int:
    jid = flink["job_id"]
    if jid:
        try:
            wm = flinkctl.window_watermark(jid)
            if wm and wm > 0:
                flink["last_wm"] = wm
        except Exception:  # noqa: BLE001
            pass
    return flink["last_wm"]


def deploy_new(reason: str, restore: str | None = None, from_earliest: bool = False):
    jid = flinkctl.deploy(config, restore_path=restore, from_earliest=from_earliest)
    flink["job_id"] = jid
    how = "세이브포인트에서 이어서" if restore else ("처음부터" if from_earliest else "Kafka 커밋 오프셋부터")
    note("deploy", f"Flink 잡 배포 ({reason}) · {how} · {LABEL[config['strategy']]} · 워터마크 {config['watermark_sec']}초"
                   f"{' · 체크포인트 끔' if config['strategy'] != 'transaction' and not config['snapshot'] else ''}"
                   f"{' · 🐞 버그 버전' if config['bug'] else ''}", job_id=jid)


def redeploy(reason: str):
    """세이브포인트를 찍고 멈춘 뒤 새 설정으로 다시 띄운다."""
    flink["busy"] = f"재배포 중: {reason}"
    try:
        old = flink["job_id"]
        sp = None
        if old:
            try:
                sp = flinkctl.stop_with_savepoint(old)
            except Exception as e:  # noqa: BLE001
                note("error", f"세이브포인트 실패 → 취소 후 커밋 오프셋부터: {str(e)[:120]}")
                try:
                    flinkctl.cancel(old)
                except Exception:  # noqa: BLE001
                    pass
        try:
            deploy_new(reason, restore=sp)
        except Exception as e:  # noqa: BLE001
            if not sp:
                raise
            note("error", f"세이브포인트로 복구 실패 → 커밋 오프셋부터: {str(e)[:160]}")
            deploy_new(reason)
        flink["error"] = None
    except Exception as e:  # noqa: BLE001
        flink["error"] = str(e)[:300]
        note("error", f"배포 실패: {str(e)[:200]}")
    finally:
        flink["busy"] = None


def wm_poller():
    while True:
        time.sleep(0.5)
        if not flink["busy"]:
            flink_wm()


def flink_boot():
    while True:
        try:
            flinkctl.jobs()
            break
        except Exception:  # noqa: BLE001
            time.sleep(2)
    time.sleep(3)
    for _ in range(30):
        try:
            js = sorted((j for j in flinkctl.jobs() if j["name"] == flinkctl.JOB_NAME
                         and j["state"] in ("RUNNING", "RESTARTING", "CREATED", "INITIALIZING")),
                        key=lambda j: j["start-time"])
            for extra in js[:-1]:  # 혹시 둘 이상 돌고 있으면 가장 최근 것만 남긴다
                flinkctl.cancel(extra["jid"])
            if js:
                flink["job_id"] = js[-1]["jid"]
                note("deploy", f"실행 중인 Flink 잡에 붙음 {js[-1]['jid'][:8]}")
            else:
                deploy_new("처음 기동")
            flink["busy"] = None
            return
        except Exception as e:  # noqa: BLE001
            log.warning("Flink 잡 기동 재시도: %s", e)
            time.sleep(3)
    flink["busy"] = None


def crash_worker():
    """다음 윈도의 결과가 나온 직후 TaskManager 를 죽인다."""
    wm_ms = config["watermark_sec"] * 1000
    strategy = config["strategy"]
    # 아직 결과가 안 나간, 클릭이 있는 가장 이른 윈도 (없으면 첫 클릭을 기다린다)
    target = None
    while flink["crash_armed"] and target is None:
        cut = max(flink_wm(), now_ms() - wm_ms - 1000)
        with lock:
            ws = sorted({w for (w, _), s in ledger.items() if s and w + WIN > cut})
        target = ws[0] if ws else None
        if target is None:
            time.sleep(0.3)
    if not flink["crash_armed"]:
        return
    # 결과가 나오는 시점 ≈ 윈도 끝 + 워터마크 + tick·Fluent Bit 지연(약 1.5초). 그 뒤 조금 더 기다린다
    #   ① : 결과는 프로듀서 버퍼(8초)에 있고 체크포인트(1초)가 한 번 이상 끝난 뒤
    #   ② : 결과는 이미 Kafka 에 나갔고 다음 체크포인트(30초)는 아직
    fire_at = target + WIN + wm_ms + 1500
    kill_at = fire_at + (2500 if strategy == "offset_first" else 1500)
    flink["crash_target"] = target
    while now_ms() < kill_at and flink["crash_armed"]:
        time.sleep(0.1)
    if not flink["crash_armed"]:
        return
    flink["crash_armed"] = False
    jid = flink["job_id"]
    last_ckpt = None
    try:
        cp = flinkctl.checkpoints(jid)
        last_ckpt = (cp.get("latest") or {}).get("completed")
    except Exception:  # noqa: BLE001
        pass
    crash_ms = now_ms()
    try:
        docker("POST", f"/containers/{TM_CONTAINER}/kill")
    except Exception as e:  # noqa: BLE001
        note("error", f"TaskManager 를 죽이지 못함: {e}")
        return
    ck = ""
    if last_ckpt:
        ck_ts = last_ckpt.get("latest_ack_timestamp") or last_ckpt.get("trigger_timestamp") or 0
        ck = f" · 마지막 체크포인트 #{last_ckpt.get('id')} ({(crash_ms - ck_ts) / 1000:.1f}초 전)"
        if strategy == "send_then_offset" and ck_ts > fire_at:
            ck += " — 결과가 나간 뒤 체크포인트가 먼저 끝나 이번엔 중복이 안 생길 수 있다"
    elif config["strategy"] != "transaction" and not config["snapshot"]:
        ck = " · 체크포인트 없음"
    note("crash", f"💥 윈도 {fmt_ms(target)[11:]} 결과 직후 TaskManager 강제 종료 ({LABEL[strategy]}){ck}")
    time.sleep(1)
    try:
        docker("POST", f"/containers/{TM_CONTAINER}/start")
    except Exception as e:  # noqa: BLE001
        note("error", f"TaskManager 를 다시 켜지 못함: {e}")
    threading.Thread(target=watch_recovery, args=(jid, crash_ms, last_ckpt), daemon=True).start()


def watch_recovery(jid: str, crash_ms: int, last_ckpt: dict | None):
    t0 = time.time()
    while time.time() - t0 < 90:
        time.sleep(1)
        try:
            d = flinkctl.job_detail(jid)
            # JobManager 가 TaskManager 를 잃은 걸 알아채고(하트비트 5초) 잡을 재시작한 뒤에야 본다
            if (d.get("timestamps") or {}).get("RESTARTING", 0) < crash_ms:
                continue
            if d.get("state") != "RUNNING" or any(v["status"] != "RUNNING" for v in d.get("vertices", [])):
                continue
            cp = flinkctl.checkpoints(jid)
            restored = (cp.get("latest") or {}).get("restored")
            took = (now_ms() - crash_ms) / 1000
            if restored and restored.get("restore_timestamp", 0) >= crash_ms - 1000 and restored.get("is_savepoint"):
                note("restore", f"Flink 복구: 체크포인트가 없어 배포 때 찍은 세이브포인트로 되돌아감 → 그 뒤 로그를 전부 "
                                f"다시 읽고 이미 낸 결과를 또 낸다 · {took:.1f}초 걸림")
            elif restored and restored.get("restore_timestamp", 0) >= crash_ms - 1000:
                ck_ts = (last_ckpt or {}).get("latest_ack_timestamp") or 0
                with lock:
                    replay = sum(1 for t in written if ck_ts <= t <= crash_ms) if ck_ts else None
                note("restore", f"Flink 복구: 체크포인트 #{restored.get('id')} 에서 상태·오프셋을 되살리고 그 뒤 로그 "
                                f"{replay if replay is not None else '?'}줄을 다시 읽음 · {took:.1f}초 걸림")
            else:
                with lock:
                    n = len(written)
                note("restore", f"Flink 복구: 체크포인트가 없어 raw-clicks 를 처음부터 다시 읽음 (로그 약 {n}줄) · "
                                f"이미 낸 결과를 또 낸다 · {took:.1f}초 걸림")
            return
        except Exception:  # noqa: BLE001
            continue
    note("error", "Flink 복구를 90초 안에 확인하지 못함")


def recalc_worker():
    flink["busy"] = "재계산 중"
    try:
        bound = flink_wm() // WIN * WIN
        if not bound:
            note("error", "재계산: 아직 닫힌 윈도가 없다")
            return
        t0 = time.time()
        note("recalc", f"재계산 시작: {fmt_ms(bound)[11:]} 앞의 닫힌 윈도를 지우고, 같은 SQL 의 유한 잡으로 "
                       f"raw-clicks 처음부터 다시 집계")
        _ch("DELETE FROM adclick.ad_clicks WHERE kind = 'event' AND window_start < toDateTime({b:UInt32}, 'UTC')",
            {"b": bound // 1000})
        jid = flinkctl.deploy_recalc(config, bound // 1000)
        while True:
            st = flinkctl.job_detail(jid).get("state")
            if st in ("FINISHED", "FAILED", "CANCELED"):
                break
            time.sleep(1)
        time.sleep(2)  # ClickHouse 반영
        flink["recalc"] = {"bound": bound, "state": st, "took": round(time.time() - t0, 1), "job_id": jid}
        note("recalc", f"재계산 끝 ({st}): 유한 잡 {jid[:8]} · {time.time() - t0:.1f}초")
    except Exception as e:  # noqa: BLE001
        note("error", f"재계산 실패: {str(e)[:200]}")
    finally:
        flink["busy"] = None


# ---------- 설정 ----------
def set_config(body: dict) -> dict:
    if flink["busy"]:
        return {**config, "error": flink["busy"]}
    changed = []
    for k in ("watermark_sec", "strategy", "snapshot", "bug", "bots"):
        if k in body and body[k] != config[k]:
            v = body[k]
            if k == "watermark_sec":
                v = max(0, min(int(v), 30))
            if k == "strategy" and v not in LABEL:
                continue
            config[k] = v
            changed.append(k)
    if changed:
        save_config()
    if "bots" in changed:
        note("config", f"배경 클릭 {'켬' if config['bots'] else '끔'}")
    job_keys = [k for k in changed if k != "bots"]
    if job_keys:
        names = {"watermark_sec": f"워터마크 {config['watermark_sec']}초", "strategy": LABEL[config["strategy"]],
                 "snapshot": f"체크포인트 {'켬' if config['snapshot'] else '끔'}",
                 "bug": "🐞 버그 버전" if config["bug"] else "✅ 버그 수정 버전"}
        reason = ", ".join(names[k] for k in job_keys)
        flink["busy"] = f"재배포 중: {reason}"
        threading.Thread(target=redeploy, args=(reason,), daemon=True).start()
    return config


def crash(body):
    arm = bool(body.get("arm", True))
    if arm and flink["busy"]:
        return {"error": flink["busy"]}
    if arm and not flink["crash_armed"]:
        flink["crash_armed"] = True
        threading.Thread(target=crash_worker, daemon=True).start()
    elif not arm:
        flink["crash_armed"] = False
    return {"armed": flink["crash_armed"]}


def recalc(_body):
    if flink["busy"]:
        return {"error": flink["busy"]}
    flink["busy"] = "재계산 중"
    threading.Thread(target=recalc_worker, daemon=True).start()
    return {"ok": True}


def recon(_body):
    from common import http_json
    r = http_json("http://recon:8080/run", {}, timeout=120)
    recon_last.clear()
    recon_last.update(r)
    if "error" not in r:
        note("recon", f"재집계 검증: 윈도 {r.get('windows')}개 비교, 불일치 {r.get('mismatches')}칸 → 보정 {r.get('fixed_clicks'):+d}")
    return r


# ---------- 화면 ----------
def flink_info() -> dict:
    jid = flink["job_id"]
    info = {"job_id": jid, "busy": flink["busy"], "error": flink["error"], "crash_armed": flink["crash_armed"],
            "recalc": flink["recalc"]}
    if not jid:
        return info
    try:
        d = flinkctl.job_detail(jid)
        info["state"] = d.get("state")
        info["tasks_running"] = all(v["status"] == "RUNNING" for v in d.get("vertices", []))
        cp = flinkctl.checkpoints(jid)
        latest = cp.get("latest") or {}
        comp = latest.get("completed") or {}
        info["checkpoints"] = {"completed": cp.get("counts", {}).get("completed"),
                               "latest_id": comp.get("id"),
                               "latest_age": round((now_ms() - (comp.get("latest_ack_timestamp") or now_ms())) / 1000, 1)
                               if comp else None,
                               "restored": (latest.get("restored") or {}).get("id")}
        info["watermark"] = flink_wm()
        info["late_dropped"] = flinkctl.late_dropped(jid, d)
    except Exception as e:  # noqa: BLE001
        info["state"] = f"조회 실패: {str(e)[:80]}"
    return info


def view(_q):
    now = now_ms()
    fi = flink_info()
    wm = fi.get("watermark") or flink["last_wm"] or 0
    lo = (now - 120_000) // WIN * WIN
    try:
        rows = ch_rows("""
            SELECT toUInt64(toUnixTimestamp(window_start)) * 1000 AS w, ad_id,
                   sumIf(clicks, kind = 'event') AS ev, sumIf(clicks, kind = 'proc') AS pr,
                   sumIf(clicks, kind = 'event' AND source = 'recon') AS recon,
                   countIf(kind = 'event' AND source = 'recalc') > 0 AS recalc
            FROM adclick.ad_clicks
            WHERE window_start >= toDateTime({lo:UInt32}, 'UTC')
            GROUP BY w, ad_id""", {"lo": lo // 1000})
        ch_ok = True
    except Exception as e:  # noqa: BLE001
        rows, ch_ok = [], False
        log.warning("ClickHouse 질의 실패: %s", e)
    db = defaultdict(dict)
    for r in rows:
        db[int(r["w"])][r["ad_id"]] = r
    windows, cells = [], {}
    with lock:
        for w in range(lo, now // WIN * WIN + WIN, WIN):
            led = {ad: len(ledger.get((w, ad), ())) for ad in ADS}
            ev = {ad: (db[w].get(ad) or {}).get("ev", 0) for ad in ADS}
            status = "closed" if w + WIN <= wm else ("waiting" if w + WIN <= now else "open")
            settled = w + WIN + 4000 <= wm  # 결과가 ClickHouse 까지 왔을 만한 때
            windows.append({"w": w, "label": fmt_ms(w)[11:], "status": status, "settled": settled,
                            "ledger": sum(led.values()), "event": sum(ev.values()),
                            "proc": sum((db[w].get(ad) or {}).get("pr", 0) for ad in ADS),
                            "recon": sum((db[w].get(ad) or {}).get("recon", 0) for ad in ADS),
                            "recalc": any((db[w].get(ad) or {}).get("recalc") for ad in ADS)})
            for ad in ADS:
                cells[f"{w}|{ad}"] = {"ledger": led[ad], "event": ev[ad], "settled": settled}
        recent = sorted((c for c in clicks.values() if c["event_ts"] >= lo or c["recv"][-1] >= lo),
                        key=lambda c: c["recv"][0])[-150:]
        evs = [dict(c) for c in recent]
    return {"now": now, "config": config, "labels": LABEL, "ads": ADS, "windows": windows, "cells": cells,
            "clicks": evs, "flink": fi, "watermark": wm, "max_seen": max_seen, "events": list(events),
            "recon": recon_last, "clickhouse_ok": ch_ok}


def closed(_q):
    return {"closed_upto": flink_wm() // WIN * WIN}


def main():
    threading.Thread(target=ticker, daemon=True).start()
    threading.Thread(target=flink_boot, daemon=True).start()
    threading.Thread(target=wm_poller, daemon=True).start()
    serve(get={"/api/view": view, "/api/config": lambda q: config, "/api/closed": closed},
          post={"/api/click": click, "/api/config": set_config, "/api/crash": crash, "/api/recalc": recalc,
                "/api/recon": recon},
          static_dir=STATIC)
    log.info("광고 서버·대시보드 http://localhost:8006")
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
