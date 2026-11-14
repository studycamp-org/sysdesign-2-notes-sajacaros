"""Grafana 대시보드 JSON 을 만든다: python3 grafana/make_dashboard.py > grafana/dashboards/mini-monitor.json"""
import json

# 지표 패널은 질의 서비스(Prometheus 호환)로, 저장소 내부·경보 저장소 패널만 TimescaleDB 를 직접 본다
DS = {"type": "grafana-postgresql-datasource", "uid": "timescaledb"}
QDS = {"type": "prometheus", "uid": "query"}
SVC = 'service=~"$service"'
panels = []
y = 0


def q(sql, fmt="time_series", ref="A"):
    return {"refId": ref, "datasource": DS, "rawQuery": True, "editorMode": "code", "format": fmt, "rawSql": sql.strip()}


def pq(expr, legend="", instant=False, interval=None, ref="A"):
    """질의 서비스에 보내는 MiniQL 식 하나."""
    t = {"refId": ref, "datasource": QDS, "expr": expr, "legendFormat": legend, "editorMode": "code",
         "range": not instant, "instant": instant}
    if interval:
        t["interval"] = interval
    return t


def via_query(p, *targets):
    p["datasource"] = QDS
    p["targets"] = list(targets)
    return p


def add(p, w, h, x=None):
    global y
    p.setdefault("datasource", DS)
    p["gridPos"] = {"x": add.x, "y": y, "w": w, "h": h}
    add.x += w
    add.h = max(add.h, h)
    if add.x >= 24:
        y += add.h
        add.x, add.h = 0, 0
    p["id"] = len(panels) + 1
    panels.append(p)


add.x, add.h = 0, 0


def row(title):
    global y
    if add.x:
        y += add.h
        add.x, add.h = 0, 0
    panels.append({"type": "row", "title": title, "id": len(panels) + 1, "collapsed": False,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})
    y += 1


def thresholds(*steps):
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for v, c in steps]}


def stat(title, sql, unit="none", th=None, decimals=None, desc=""):
    d = {"type": "stat", "title": title, "description": desc, "targets": [q(sql, "table")],
         "options": {"reduceOptions": {"calcs": ["lastNotNull"], "values": False, "fields": ""},
                     "colorMode": "value", "graphMode": "none", "textMode": "value", "justifyMode": "center"},
         "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "thresholds"},
                                      "thresholds": th or thresholds((None, "purple"))}, "overrides": []}}
    if decimals is not None:
        d["fieldConfig"]["defaults"]["decimals"] = decimals
    return d


def ts(title, targets, unit="none", stack=False, desc="", minv=None, maxv=None, draw="line"):
    custom = {"lineWidth": 1, "fillOpacity": 25 if stack else 8, "showPoints": "never", "spanNulls": 15000,
              "drawStyle": draw, "stacking": {"mode": "normal" if stack else "none", "group": "A"}}
    d = {"type": "timeseries", "title": title, "description": desc, "targets": targets,
         "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                     "tooltip": {"mode": "multi", "sort": "desc"}},
         "fieldConfig": {"defaults": {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"}},
                         "overrides": []}}
    if minv is not None:
        d["fieldConfig"]["defaults"]["min"] = minv
    if maxv is not None:
        d["fieldConfig"]["defaults"]["max"] = maxv
    return d


def table(title, sql, desc="", overrides=None):
    return {"type": "table", "title": title, "description": desc, "targets": [q(sql, "table")],
            "options": {"showHeader": True, "cellHeight": "sm"},
            "fieldConfig": {"defaults": {"custom": {"align": "auto"}}, "overrides": overrides or []}}


# ---------------- 개요 ----------------
row("개요")
add(via_query(stat("호스트", "", desc="최근 20초 안에 cpu_usage 를 보낸 호스트 (질의 서비스)"),
              pq(f'count(last_over_time(cpu_usage{{{SVC}}}[20s]))', instant=True)), 4, 4)
add(stat("DB 적재 점/초", "SELECT count(*) / 10.0 FROM metrics WHERE time > now() - interval '10 seconds'",
         decimals=1), 4, 4)
add(stat("활성 시계열", "SELECT count(DISTINCT (name, host)) FROM metrics WHERE time > now() - interval '1 minute'",
         desc="이름 + 레이블 조합. 카디널리티"), 4, 4)
add(stat("firing 경보", "SELECT count(*) FROM alerts WHERE state = 'firing'",
         th=thresholds((None, "green"), (1, "red"))), 4, 4)
add(stat("하이퍼테이블 크기", "SELECT hypertable_size('metrics')", unit="bytes"), 4, 4)
add(stat("압축률", """SELECT coalesce(sum(before_compression_total_bytes)::float
  / nullif(sum(after_compression_total_bytes), 0), 0)
FROM chunk_compression_stats('metrics') WHERE compression_status = 'Compressed'""",
         decimals=1, desc="압축 전 / 후 바이트. 15분 지난 청크부터 압축된다"), 4, 4)
panels[-1]["fieldConfig"]["defaults"].update(unit="x", mappings=[
    {"type": "value", "options": {"0": {"text": "아직", "color": "text"}}}])

# ---------------- 호스트 맵 ----------------
row("호스트 맵")
hostmap = via_query(stat("호스트 맵 · CPU", "", unit="percent",
                         th=thresholds((None, "green"), (50, "#b8d43f"), (70, "orange"), (90, "red")),
                         desc="Datadog 호스트 맵처럼 호스트 하나가 칸 하나. 20초 넘게 소식이 없으면 DOWN (-1)"),
                    pq(f'last_over_time(cpu_usage{{{SVC}}}[20s]) or age_over_time(cpu_usage{{{SVC}}}[10m]) * 0 - 1',
                       "{{host}}", instant=True))
hostmap["options"].update(colorMode="background", textMode="value_and_name", wideLayout=False,
                          text={"titleSize": 11, "valueSize": 18}, orientation="vertical",
                          minVizWidth=88, minVizHeight=64)
hostmap["fieldConfig"]["defaults"].update(decimals=0, mappings=[
    {"type": "value", "options": {"-1": {"text": "DOWN", "color": "#444"}}}])
add(hostmap, 15, 9)
top = {"type": "bargauge", "title": "Top 10 · CPU (30초 평균)", "datasource": QDS,
       "targets": [pq(f'topk(10, avg_over_time(cpu_usage{{{SVC}}}[30s]))', "{{host}}", instant=True)],
       "options": {"orientation": "horizontal", "displayMode": "gradient", "showUnfilled": True,
                   "reduceOptions": {"calcs": ["lastNotNull"], "values": False}},
       "fieldConfig": {"defaults": {"unit": "percent", "min": 0, "max": 100, "decimals": 0,
                                    "color": {"mode": "thresholds"},
                                    "thresholds": thresholds((None, "green"), (70, "orange"), (90, "red"))},
                       "overrides": []}}
add(top, 9, 9)

# ---------------- 시스템 ----------------
row("시스템 지표")
add(via_query(ts("CPU 사용률 · host", [], "percent", minv=0, maxv=100),
              pq(f'avg by (host) (cpu_usage{{{SVC}}})', "{{host}}")), 12, 8)
add(via_query(ts("메모리 사용률 · host", [], "percent", minv=0, maxv=100),
              pq(f'avg by (host) (mem_usage{{{SVC}}})', "{{host}}")), 12, 8)
add(via_query(ts("요청 수 · service (합)", [], "reqps", stack=True),
              pq(f'sum by (service) (http_rps{{{SVC}}})', "{{service}}")), 8, 8)
add(via_query(ts("p99 지연 · service", [], "ms", desc="구간 안에서 host 들의 최댓값"),
              pq(f'max by (service) (max_over_time(http_latency_p99_ms{{{SVC}}}[$__interval]))', "{{service}}")), 8, 8)
add(via_query(ts("오류율 · service", [], "percent"),
              pq(f'avg by (service) (http_error_rate{{{SVC}}})', "{{service}}")), 8, 8)

# ---------------- 파이프라인 ----------------
row("수집 파이프라인")
add(ts("적재 점/초 · 수집 방식", [q("""
SELECT $__timeGroupAlias(time, '10s'), labels->>'via' AS metric, count(*) / 10.0 AS value
FROM metrics WHERE $__timeFilter(time) GROUP BY 1, 2 ORDER BY 1""")], "none", stack=True, draw="bars",
       desc="push 에이전트가 보낸 것과 pull 수집기가 당겨 온 것. 적재량은 저장소 내부 지표라 TimescaleDB 직접"), 8, 8)
updown = {"type": "status-history", "title": "up · pull 대상", "datasource": QDS,
          "description": "pull 수집기가 남긴 up. 0 이면 응답 없음 → instance_down",
          "targets": [pq("min by (host) (min_over_time(up[$__interval]))", "{{host}}")],
          "options": {"showValue": "never", "rowHeight": 0.85, "legend": {"showLegend": False}},
          "fieldConfig": {"defaults": {"color": {"mode": "thresholds"},
                                       "thresholds": thresholds((None, "red"), (1, "green")),
                                       "mappings": [{"type": "value", "options": {
                                           "0": {"text": "DOWN"}, "1": {"text": "UP"}}}]}, "overrides": []}}
add(updown, 16, 8)

# ---------------- 저장소 ----------------
row("저장소 (TimescaleDB)")
add(via_query(ts("다운샘플링 비교 · cpu_usage · $host", [], "percent",
                 desc="질의 서비스가 step 으로 저장 계층을 고른다: 5초 → 원본, 1분 → metrics_1m, 1시간 → metrics_1h. "
                      "평균만 남기면 스파이크가 사라지므로 max 도 남겼다"),
              pq('cpu_usage{host="$host"}', "원본 (5초)", interval="5s", ref="A"),
              pq('avg_over_time(cpu_usage{host="$host"}[1m])', "1분 avg", interval="1m", ref="B"),
              pq('max_over_time(cpu_usage{host="$host"}[1m])', "1분 max", interval="1m", ref="C"),
              pq('min_over_time(cpu_usage{host="$host"}[1m])', "1분 min", interval="1m", ref="D"),
              pq('avg_over_time(cpu_usage{host="$host"}[1h])', "1시간 avg", interval="1h", ref="E")), 14, 9)
add(table("청크", """
SELECT c.chunk_name AS "청크", c.range_start AS "시작", c.range_end AS "끝",
       CASE WHEN c.is_compressed THEN '압축됨' ELSE '원본' END AS "상태",
       s.total_bytes AS "크기"
FROM timescaledb_information.chunks c
JOIN chunks_detailed_size('metrics') s ON s.chunk_name = c.chunk_name
WHERE c.hypertable_name = 'metrics' ORDER BY c.range_start DESC""",
              desc="10분마다 새 청크. 15분 지나면 압축 정책이 컬럼형으로 바꾸고, 6시간 지나면 통째로 지운다",
              overrides=[{"matcher": {"id": "byName", "options": "크기"},
                          "properties": [{"id": "unit", "value": "bytes"}]}]), 10, 9)

# ---------------- 경보 ----------------
row("경보")
state_colors = {"type": "value", "options": {
    "firing": {"color": "red", "index": 0}, "pending": {"color": "orange", "index": 1},
    "resolved": {"color": "green", "index": 2}, "inactive": {"color": "text", "index": 3}}}
add(table("경보 저장소", """
SELECT state AS "상태", rule AS "규칙", host AS "호스트", severity AS "심각도", summary AS "내용",
       fired_at AS "발생", updated_at AS "갱신"
FROM alerts WHERE state <> 'inactive'
ORDER BY CASE state WHEN 'firing' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END, updated_at DESC LIMIT 50""",
              overrides=[{"matcher": {"id": "byName", "options": "상태"},
                          "properties": [{"id": "mappings", "value": [state_colors]},
                                         {"id": "custom.cellOptions", "value": {"type": "color-background"}}]}]),
    12, 9)
add(table("보낸 알림", """
SELECT sent_at AS "시각", channel AS "채널", message AS "메시지"
FROM notifications ORDER BY id DESC LIMIT 50"""), 12, 9)

dash = {
    "uid": "mini-monitor", "title": "mini-monitor · 지표 모니터링", "tags": ["ch05"],
    "timezone": "browser", "refresh": "5s", "schemaVersion": 39, "version": 1, "editable": True,
    "time": {"from": "now-15m", "to": "now"},
    "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m"]},
    "templating": {"list": [
        {"name": "service", "label": "service", "type": "query", "datasource": QDS, "multi": True, "includeAll": True,
         "allValue": ".*", "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "refresh": 2,
         "query": {"query": "label_values(cpu_usage, service)", "refId": "service"}},
        {"name": "host", "label": "host (다운샘플링)", "type": "query", "datasource": QDS, "multi": False,
         "includeAll": False, "refresh": 2, "sort": 1,
         "query": {"query": "label_values(cpu_usage, host)", "refId": "host"}},
    ]},
    "panels": panels,
}
print(json.dumps(dash, ensure_ascii=False, indent=1))
