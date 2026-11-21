"""서비스들이 같이 쓰는 설정과 작은 도구 (HTTP 서버, ClickHouse HTTP 호출, 시간 형식)."""
import json
import logging
import mimetypes
import os
import threading
import time
import urllib.parse
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

KAFKA = os.environ.get("KAFKA", "kafka:9092")
CH_URL = os.environ.get("CH_URL", "http://clickhouse:8123")
CH_USER = os.environ.get("CH_USER", "default")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "demo")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://rustfs:9000")
S3_KEY = os.environ.get("S3_KEY", "rustfsadmin")
S3_SECRET = os.environ.get("S3_SECRET", "rustfsadmin")
S3_BUCKET = os.environ.get("S3_BUCKET", "raw-clicks")

RAW_TOPIC = "raw-clicks"
OUT_TOPIC = "agg-results"
SNAP_TOPIC = "agg-snapshots"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(os.environ.get("SERVICE", "app"))


# ---------- 시간 ----------
def now_ms() -> int:
    return int(time.time() * 1000)


def minute_of(ms: int) -> int:
    """밀리초 → 그 분의 시작 (밀리초)."""
    return ms // 60000 * 60000


def fmt_ms(ms: int, millis: bool = False) -> str:
    """ClickHouse 가 받는 UTC 문자열."""
    d = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return d.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if millis else d.strftime("%Y-%m-%d %H:%M:%S")


def hhmm(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M")


# ---------- HTTP 서버 (스레드) ----------
def serve(get: dict | None = None, post: dict | None = None, static_dir: str | None = None, port: int = 8080):
    """GET/POST 경로 → 함수 표. 함수는 dict 를 돌려주고, 그게 JSON 응답이 된다."""
    get, post = get or {}, post or {}

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):  # 접근 로그는 끈다
            pass

        def _send(self, code, body: bytes, ctype="application/json; charset=utf-8"):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, fn, arg):
            try:
                self._send(200, json.dumps(fn(arg), ensure_ascii=False, default=str).encode())
            except Exception as e:  # noqa: BLE001
                log.exception("handler error")
                self._send(500, json.dumps({"error": str(e)}, ensure_ascii=False).encode())

        def do_GET(self):
            u = urllib.parse.urlparse(self.path)
            q = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
            if u.path in get:
                return self._json(get[u.path], q)
            if static_dir:
                name = "index.html" if u.path in ("/", "") else u.path.lstrip("/")
                path = os.path.normpath(os.path.join(static_dir, name))
                if path.startswith(static_dir) and os.path.isfile(path):
                    with open(path, "rb") as f:
                        ctype = mimetypes.guess_type(path)[0] or "application/octet-stream"
                        return self._send(200, f.read(), ctype + ("; charset=utf-8" if ctype.startswith("text") else ""))
            self._send(404, b'{"error":"not found"}')

        def do_POST(self):
            u = urllib.parse.urlparse(self.path)
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}") if n else {}
            if u.path in post:
                return self._json(post[u.path], body)
            self._send(404, b'{"error":"not found"}')

    srv = ThreadingHTTPServer(("0.0.0.0", port), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    log.info("HTTP :%d", port)
    return srv


def http_json(url: str, body: dict | None = None, timeout: float = 5):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"},
                                 method="POST" if data is not None else "GET")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


# ---------- ClickHouse (HTTP 인터페이스) ----------
def _ch(sql: str, params: dict | None = None, data: bytes | None = None, timeout: float = 30) -> bytes:
    q = {"output_format_json_quote_64bit_integers": "0"}
    for k, v in (params or {}).items():
        q[f"param_{k}"] = str(v)
    if data is not None:  # INSERT: 질의는 URL 로, 데이터는 본문으로
        q["query"] = sql
        body = data
    else:
        body = sql.encode()
    req = urllib.request.Request(f"{CH_URL}/?{urllib.parse.urlencode(q)}", data=body, method="POST",
                                 headers={"X-ClickHouse-User": CH_USER, "X-ClickHouse-Key": CH_PASSWORD})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except urllib.error.HTTPError as e:  # ClickHouse 오류 메시지를 그대로 보이게
        raise RuntimeError(e.read().decode(errors="replace")[:500]) from None


def ch_rows(sql: str, params: dict | None = None) -> list[dict]:
    return json.loads(_ch(sql + " FORMAT JSON", params))["data"]


def ch_bytes(sql: str, params: dict | None = None) -> bytes:
    return _ch(sql, params)


def ch_insert(table: str, rows: list[dict]):
    if rows:
        _ch(f"INSERT INTO {table} FORMAT JSONEachRow",
            data="\n".join(json.dumps(r) for r in rows).encode())
