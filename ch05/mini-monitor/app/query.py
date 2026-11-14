"""질의 서비스 (교재 '질의 서비스': TSDB 와 시각화·경보 시스템 사이의 얇은 계층).

경보 관리자와 Grafana 는 TimescaleDB 를 직접 보지 않고 여기에만 묻는다. 그래서 TSDB 를 바꿔도
클라이언트는 그대로다(분리). 말하는 방식은 Prometheus HTTP API 의 일부라서 Grafana 는 기본 Prometheus
데이터소스로 붙는다. 질의어는 PromQL 의 작은 부분집합(MiniQL)이다.

  선택자     cpu_usage{service=~"web|api", host!="sim-push-1"}     {__name__=~".+"}
  범위 함수  avg_over_time / max_over_time / min_over_time / sum_over_time / count_over_time /
             last_over_time (sel[30s])     age_over_time(sel[10m]) = 마지막 점 뒤로 몇 초 (자체 함수)
  집계       sum / avg / max / min / count by (label) (식),  topk(k, 식), bottomk(k, 식)
  연산       + - * /,  비교 > < >= <= == != (조건을 만족하는 시계열만 남김),  or

범위 질의(query_range)는 step 구간마다 묶어 계산한다. 구간 = max(step, 창). 맨 선택자는 구간 평균.
step 이 1분 이상이면 연속 집계 metrics_1m, 1시간 이상이면 metrics_1h 에서 읽는다 (호출자는 모른다).
캐시 계층은 두지 않았다.
"""
import asyncio
import collections
import json
import logging
import re
import time
from dataclasses import dataclass, field

import asyncpg
from aiohttp import web

from common import PG_DSN, Rate, json_response, my_name, register_forever, serve

log = logging.getLogger("query")
NAME = my_name()
pool: asyncpg.Pool | None = None
LOOKBACK = 300.0  # 맨 선택자 즉시 질의: 최근 5분 안의 마지막 점

# ---------------- 토큰·파서 ----------------
TOKEN = re.compile(r"""
  (?P<ws>\s+) | (?P<dur>\d+(?:ms|s|m|h|d)\b) | (?P<num>\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)
| (?P<str>"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*') | (?P<id>[A-Za-z_:][\w:]*)
| (?P<op>=~|!~|!=|==|>=|<=|[{}()\[\],=+\-*/<>])""", re.X)
UNITS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}
RANGE_FN = {"avg_over_time", "max_over_time", "min_over_time", "sum_over_time", "count_over_time",
            "last_over_time", "age_over_time"}
AGG = {"sum", "avg", "max", "min", "count", "topk", "bottomk"}
CMP = {">", "<", ">=", "<=", "==", "!="}


def dur(s: str) -> float:
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(ms|s|m|h|d)?", s)
    if not m:
        raise ValueError(f"기간을 모르겠다: {s}")
    return float(m.group(1)) * UNITS[m.group(2) or "s"]


def tokenize(src: str):
    out, i = [], 0
    while i < len(src):
        m = TOKEN.match(src, i)
        if not m:
            raise ValueError(f"{i}번째 글자에서 막혔다: {src[i:i + 10]!r}")
        i = m.end()
        if m.lastgroup != "ws":
            out.append((m.lastgroup, m.group()))
    return out + [("eof", "")]


@dataclass
class Sel:
    name: str | None
    matchers: list = field(default_factory=list)  # (label, op, value)
    window: float | None = None


class Parser:
    def __init__(self, src):
        self.t, self.i = tokenize(src), 0

    def peek(self, k=0):
        return self.t[self.i + k]

    def eat(self, val=None):
        kind, v = self.t[self.i]
        if val is not None and v != val:
            raise ValueError(f"'{val}' 가 와야 하는데 '{v}'")
        self.i += 1
        return v

    def parse(self):
        e = self.expr_or()
        if self.peek()[0] != "eof":
            raise ValueError(f"남은 글자: {self.peek()[1]}")
        return e

    def expr_or(self):
        e = self.expr_cmp()
        while self.peek() == ("id", "or"):
            self.eat()
            e = ("or", e, self.expr_cmp())
        return e

    def expr_cmp(self):
        e = self.expr_add()
        while self.peek()[1] in CMP:
            op = self.eat()
            e = ("bin", op, e, self.expr_add())
        return e

    def expr_add(self):
        e = self.expr_mul()
        while self.peek()[1] in ("+", "-"):
            op = self.eat()
            e = ("bin", op, e, self.expr_mul())
        return e

    def expr_mul(self):
        e = self.unary()
        while self.peek()[1] in ("*", "/"):
            op = self.eat()
            e = ("bin", op, e, self.unary())
        return e

    def unary(self):
        if self.peek()[1] == "-":
            self.eat()
            return ("bin", "*", ("num", -1.0), self.unary())
        return self.primary()

    def by(self):
        self.eat("by")
        self.eat("(")
        labels = []
        while self.peek()[1] != ")":
            labels.append(self.eat())
            if self.peek()[1] == ",":
                self.eat()
        self.eat(")")
        return labels

    def primary(self):
        kind, v = self.peek()
        if kind == "num":
            self.eat()
            return ("num", float(v))
        if v == "(":
            self.eat()
            e = self.expr_or()
            self.eat(")")
            return e
        if kind == "id" and v in AGG and self.peek(1)[1] in ("(", "by"):
            self.eat()
            labels = self.by() if self.peek()[1] == "by" else None
            self.eat("(")
            k = None
            if v in ("topk", "bottomk"):
                k = int(float(self.eat()))
                self.eat(",")
            inner = self.expr_or()
            self.eat(")")
            if self.peek()[1] == "by":
                labels = self.by()
            return ("agg", v, labels, k, inner)
        if kind == "id" and v in RANGE_FN:
            self.eat()
            self.eat("(")
            s = self.selector()
            self.eat(")")
            if s.window is None:
                raise ValueError(f"{v} 에는 [창] 이 필요하다")
            return ("fn", v, s)
        if kind == "id" or v == "{":
            return ("sel", self.selector())
        raise ValueError(f"여기서 '{v}' 는 올 수 없다")

    def selector(self):
        name = None
        if self.peek()[0] == "id":
            name = self.eat()
        s = Sel(name)
        if self.peek()[1] == "{":
            self.eat()
            while self.peek()[1] != "}":
                label = self.eat()
                op = self.eat()
                if op not in ("=", "!=", "=~", "!~"):
                    raise ValueError(f"레이블 연산자 {op}")
                raw = self.eat()
                s.matchers.append((label, op, json.loads(raw) if raw[0] == '"' else raw[1:-1]))
                if self.peek()[1] == ",":
                    self.eat()
            self.eat("}")
        if self.peek()[1] == "[":
            self.eat()
            s.window = dur(self.eat())
            self.eat("]")
        if s.name is None and not any(m[0] == "__name__" for m in s.matchers):
            raise ValueError("지표 이름이나 {__name__=...} 가 필요하다")
        return s


# ---------------- 실행 ----------------
@dataclass
class Ctx:
    start: float
    end: float
    step: float
    instant: bool
    tiers: collections.Counter = field(default_factory=collections.Counter)


def where(sel: Sel, args: list) -> tuple[list[str], list[str]]:
    """(name·host 조건, 나머지 레이블 조건). 레이블 조건은 labels jsonb 를 본다."""
    core, lab = [], []
    ms = list(sel.matchers)
    if sel.name:
        ms.append(("__name__", "=", sel.name))
    for label, op, val in ms:
        if op == "=~" and val in (".*", ""):
            continue
        col = "name" if label == "__name__" else "host" if label == "host" else None
        if col is None:
            args.append(label)
            col = f"coalesce(labels->>${len(args)}, '')"
        if op in ("=~", "!~"):
            val = f"^(?:{val})$"
        args.append(val)
        sql_op = {"=": "=", "!=": "<>", "=~": "~", "!~": "!~"}[op]
        (core if label in ("__name__", "host") else lab).append(f"{col} {sql_op} ${len(args)}")
    return core, lab


RAW_AGG = {"avg_over_time": "avg(value)", "max_over_time": "max(value)", "min_over_time": "min(value)",
           "sum_over_time": "sum(value)", "count_over_time": "count(*)::float8", "last_over_time": "last(value, time)"}
ROLLUP_AGG = {"avg_over_time": "sum(avg * n) / sum(n)", "max_over_time": "max(max)", "min_over_time": "min(min)",
              "count_over_time": "sum(n)::float8"}


async def leaf(conn, fn: str | None, sel: Sel, ctx: Ctx) -> dict:
    """선택자 하나를 SQL 로 바꿔 시계열 묶음 {labels_key: {ts: value}} 를 만든다. 유일하게 DB 를 만지는 곳."""
    keep_name = fn is None
    if ctx.instant:
        window = sel.window or LOOKBACK
        agg = RAW_AGG.get(fn or "last_over_time") if fn != "age_over_time" else \
            "extract(epoch FROM to_timestamp($1) - max(time))"
        args: list = [ctx.end]
        core, lab = where(sel, args)
        conds = core + lab + [f"time > to_timestamp($1) - interval '{window} seconds'",
                                           "time <= to_timestamp($1)"]
        sql = (f"SELECT $1::float8 AS t, name, host, labels::text AS labels, {agg} AS v FROM metrics "
               f"WHERE {' AND '.join(conds)} GROUP BY name, host, labels")
        ctx.tiers["raw"] += 1
    else:
        bucket = max(ctx.step, sel.window or 0)
        tier = "1h" if bucket >= 3600 else "1m" if bucket >= 60 else "raw"
        rollup_ok = tier != "raw" and (fn is None or fn in ROLLUP_AGG)
        args = [bucket, ctx.start - bucket, ctx.end]
        core, lab = where(sel, args)
        if rollup_ok:
            # 롤업에는 (name, host) 만 있다. 호스트의 최신 레이블을 원본에서 찾아 다시 붙이고 그걸로 거른다
            agg = ROLLUP_AGG[fn or "avg_over_time"]
            sql = (f"WITH r AS (SELECT time_bucket(make_interval(secs => $1), bucket) AS t, name, host, {agg} AS v"
                   f" FROM metrics_{tier} WHERE bucket >= to_timestamp($2) AND bucket <= to_timestamp($3)"
                   f"{''.join(' AND ' + c for c in core)} GROUP BY 1, name, host),"
                   f" k AS (SELECT DISTINCT name, host FROM r),"
                   f" l AS (SELECT k.name, k.host, coalesce(x.labels, jsonb_build_object('host', k.host)) AS labels"
                   f"  FROM k LEFT JOIN LATERAL (SELECT labels FROM metrics m WHERE m.name = k.name AND m.host = k.host"
                   f"  ORDER BY time DESC LIMIT 1) x ON true)"
                   f" SELECT extract(epoch FROM r.t) AS t, r.name, r.host, l.labels::text AS labels, r.v"
                   f" FROM r JOIN l USING (name, host){''.join(' AND ' + c for c in lab).replace(' AND ', ' WHERE ', 1)}")
            ctx.tiers[tier] += 1
        else:
            agg = RAW_AGG.get(fn or "avg_over_time") if fn != "age_over_time" else \
                "extract(epoch FROM time_bucket(make_interval(secs => $1), time) + make_interval(secs => $1) - max(time))"
            sql = (f"SELECT extract(epoch FROM time_bucket(make_interval(secs => $1), time)) AS t, name, host,"
                   f" labels::text AS labels, {agg} AS v FROM metrics"
                   f" WHERE time >= to_timestamp($2) AND time <= to_timestamp($3)"
                   f"{''.join(' AND ' + c for c in core + lab)} GROUP BY 1, name, host, labels")
            ctx.tiers["raw"] += 1
    out: dict = collections.defaultdict(dict)
    for r in await conn.fetch(sql, *args):
        labels = json.loads(r["labels"])
        labels["host"] = r["host"]
        if keep_name:
            labels["__name__"] = r["name"]
        out[tuple(sorted(labels.items()))][float(r["t"])] = float(r["v"])
    return out


OPS = {"+": lambda a, b: a + b, "-": lambda a, b: a - b, "*": lambda a, b: a * b,
       "/": lambda a, b: a / b if b else float("nan"),
       ">": lambda a, b: a > b, "<": lambda a, b: a < b, ">=": lambda a, b: a >= b,
       "<=": lambda a, b: a <= b, "==": lambda a, b: a == b, "!=": lambda a, b: a != b}


def drop_name(vec: dict) -> dict:
    return {tuple(kv for kv in k if kv[0] != "__name__"): v for k, v in vec.items()}


async def ev(conn, node, ctx: Ctx):
    kind = node[0]
    if kind == "num":
        return node[1]
    if kind == "sel":
        return await leaf(conn, None, node[1], ctx)
    if kind == "fn":
        return await leaf(conn, node[1], node[2], ctx)
    if kind == "or":
        a, b = await ev(conn, node[1], ctx), await ev(conn, node[2], ctx)
        return {**b, **a}
    if kind == "bin":
        op, a, b = node[1], await ev(conn, node[2], ctx), await ev(conn, node[3], ctx)
        f = OPS[op]
        if not isinstance(a, dict) and not isinstance(b, dict):
            return float(f(a, b))
        out = {}
        if isinstance(a, dict) and isinstance(b, dict):  # 레이블이 똑같은 것끼리 1:1
            pairs = [(k, a[k], b[k]) for k in a if k in b]
        elif isinstance(a, dict):
            pairs = [(k, s, None) for k, s in a.items()]
        else:
            pairs = [(k, None, s) for k, s in b.items()]
        for k, sa, sb in pairs:
            pts = {}
            for t in (sa or sb):
                x = sa[t] if sa is not None else a
                y = sb.get(t) if sb is not None else b
                if y is None:
                    continue
                r = f(x, y)
                if op in CMP:
                    if r:
                        pts[t] = x if sa is not None else y  # 비교는 걸러 내기: 왼쪽 값을 그대로
                else:
                    pts[t] = float(r)
            if pts:
                out[k] = pts
        return drop_name(out) if op not in CMP else out
    if kind == "agg":
        _, fn, labels, k, inner = node
        vec = await ev(conn, inner, ctx)
        if fn in ("topk", "bottomk"):
            out: dict = collections.defaultdict(dict)
            times = {t for s in vec.values() for t in s}
            for t in times:
                ranked = sorted(((s[t], key) for key, s in vec.items() if t in s), reverse=fn == "topk")
                for v, key in ranked[:k]:
                    out[key][t] = v
            return dict(out)
        groups: dict = collections.defaultdict(lambda: collections.defaultdict(list))
        for key, s in vec.items():
            d = dict(key)
            g = tuple((l, d.get(l, "")) for l in labels) if labels else ()
            for t, v in s.items():
                groups[g][t].append(v)
        red = {"sum": sum, "avg": lambda xs: sum(xs) / len(xs), "max": max, "min": min, "count": len}[fn]
        return {g: {t: float(red(xs)) for t, xs in pts.items()} for g, pts in groups.items()}
    raise ValueError(kind)


def to_result(val, ctx: Ctx):
    if not isinstance(val, dict):
        return {"resultType": "scalar", "result": [ctx.end, str(val)]}
    items = [(dict(k), s) for k, s in val.items() if s]
    if ctx.instant:
        items.sort(key=lambda x: -max(x[1].values()))
        return {"resultType": "vector",
                "result": [{"metric": m, "value": [ctx.end, repr(round(next(iter(s.values())), 6))]} for m, s in items]}
    return {"resultType": "matrix",
            "result": [{"metric": m, "values": [[t, repr(round(v, 6))] for t, v in sorted(s.items())]}
                       for m, s in items]}


# ---------------- 통계 (파이프라인 뷰용) ----------------
rates = collections.defaultdict(Rate)
tier_total = collections.Counter()
recent: collections.deque = collections.deque(maxlen=12)
errors = 0
lat_ms: collections.deque = collections.deque(maxlen=200)


def client_of(req) -> str:
    ua = req.headers.get("User-Agent", "")
    return "grafana" if "Grafana" in ua else ua.split("/")[0] if ua.startswith(("alerter", "pipeline")) else "other"


async def params(req) -> dict:
    p = dict(req.query)
    if req.method == "POST":
        p.update(await req.post())
    return p


def ts(v, default):
    if v in (None, ""):
        return default
    try:
        return float(v)
    except ValueError:
        from datetime import datetime
        return datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp()


async def run(req, instant: bool):
    global errors
    p = await params(req)
    expr = p.get("query", "")
    now = time.time()
    if instant:
        t = ts(p.get("time"), now)
        ctx = Ctx(t, t, 0, True)
    else:
        ctx = Ctx(ts(p.get("start"), now - 900), ts(p.get("end"), now), max(1.0, dur(str(p.get("step", "15")))), False)
    client = client_of(req)
    started = time.monotonic()
    try:
        tree = Parser(expr).parse()
        async with pool.acquire() as conn:
            data = to_result(await ev(conn, tree, ctx), ctx)
    except Exception as e:  # noqa: BLE001
        errors += 1
        recent.appendleft({"at": now, "client": client, "kind": "instant" if instant else "range", "expr": expr,
                           "error": str(e)})
        return web.json_response({"status": "error", "errorType": "bad_data", "error": str(e)}, status=400)
    ms = round((time.monotonic() - started) * 1000, 1)
    lat_ms.append(ms)
    rates[client].add()
    tier_total.update(ctx.tiers)
    n = len(data["result"]) if data["resultType"] != "scalar" else 1
    recent.appendleft({"at": now, "client": client, "kind": "instant" if instant else "range", "expr": expr,
                       "ms": ms, "series": n, "tiers": dict(ctx.tiers), "step": ctx.step})
    return web.json_response({"status": "success", "data": data})


async def q_instant(req):
    return await run(req, True)


async def q_range(req):
    return await run(req, False)


# ---------------- 메타데이터 (Grafana 의 지표·레이블 목록) ----------------
def match_names(p) -> list[str]:
    raw = p.getall("match[]", []) if hasattr(p, "getall") else []
    out = []
    for m in raw:
        try:
            out.append(Parser(m).selector().name)
        except Exception:  # noqa: BLE001
            pass
    return [n for n in out if n]


async def label_values(req):
    label = req.match_info["name"]
    names = match_names(req.query)
    col = "name" if label == "__name__" else "host" if label == "host" else "labels->>$2"
    args: list = [names or None]
    if col == "labels->>$2":
        args.append(label)
    sql = (f"SELECT DISTINCT {col} AS v FROM metrics WHERE time > now() - interval '1 hour'"
           f" AND ($1::text[] IS NULL OR name = ANY($1)) AND {col} IS NOT NULL ORDER BY 1")
    async with pool.acquire() as c:
        rows = await c.fetch(sql, *args)
    return web.json_response({"status": "success", "data": [r["v"] for r in rows]})


async def labels(_req):
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT DISTINCT jsonb_object_keys(labels) AS k FROM metrics "
                             "WHERE time > now() - interval '10 minutes'")
    return web.json_response({"status": "success", "data": sorted({"__name__", "host", *[r["k"] for r in rows]})})


async def series(req):
    p = await params(req)
    names = match_names(req.query) or ([Parser(p["match[]"]).selector().name] if p.get("match[]") else [])
    async with pool.acquire() as c:
        rows = await c.fetch("SELECT DISTINCT name, host, labels::text AS l FROM metrics "
                             "WHERE time > now() - interval '10 minutes' AND ($1::text[] IS NULL OR name = ANY($1))",
                             names or None)
    return web.json_response({"status": "success", "data": [
        {"__name__": r["name"], **json.loads(r["l"]), "host": r["host"]} for r in rows]})


async def buildinfo(_req):
    return web.json_response({"status": "success", "data": {"version": "2.40.0", "application": "mini-monitor query"}})


async def empty(req):
    return web.json_response({"status": "success", "data": [] if "exemplars" in req.path else {}})


async def stats(_req):
    lat = sorted(lat_ms)
    return json_response({
        "name": NAME, "rate": round(sum(r.per_sec() for r in rates.values()), 1),
        "by_client": {k: r.per_sec() for k, r in rates.items()},
        "total": sum(r.total for r in rates.values()), "errors": errors, "tiers": dict(tier_total),
        "p50_ms": lat[len(lat) // 2] if lat else None, "p99_ms": lat[int(len(lat) * 0.99)] if lat else None,
        "recent": list(recent)})


async def main():
    global pool
    while True:
        try:
            # TimescaleDB 2.17 의 벡터화 집계가 압축 청크 + jsonb GROUP BY 에서 labels 를 NULL 로 돌려주는 버그가 있어 끈다
            pool = await asyncpg.create_pool(
                PG_DSN, min_size=1, max_size=5,
                server_settings={"timescaledb.enable_vectorized_aggregation": "off"})  # 풀의 RESET ALL 에도 남게
            break
        except Exception as e:  # noqa: BLE001
            log.warning("DB 대기 중: %s", e)
            await asyncio.sleep(2)
    await serve([
        web.route("*", "/api/v1/query", q_instant), web.route("*", "/api/v1/query_range", q_range),
        web.get("/api/v1/labels", labels), web.post("/api/v1/labels", labels),
        web.get("/api/v1/label/{name}/values", label_values), web.route("*", "/api/v1/series", series),
        web.get("/api/v1/status/buildinfo", buildinfo), web.get("/api/v1/metadata", empty),
        web.route("*", "/api/v1/query_exemplars", empty), web.get("/stats", stats)])
    log.info("질의 서비스 시작")
    await register_forever(NAME, "query")


if __name__ == "__main__":
    asyncio.run(main())
