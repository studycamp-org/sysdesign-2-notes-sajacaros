// 근접 친구 시뮬레이터 프론트엔드
// - 관제 스트림(/api/admin/stream): 맵에 전체 사용자 위치를 그리기 위한 용도
// - 사용자 스트림(/api/users/{id}/stream): 선택한 사용자의 근접 친구만 SSE로 수신 (노란색 표시는 이 스트림 기준)

const SVG_NS = "http://www.w3.org/2000/svg";
const $ = (id) => document.getElementById(id);

const state = {
  cfg: null,
  users: new Map(), // id -> {id, name, x, y, el, vx, vy}
  selected: null,
  es: null,
  session: 0,
  radius: 500,
  nearby: new Map(), // id -> {distance, x, y}
  dragging: null,
};

const svg = $("map");
let layerUsers, layerLinks, radiusCircle;

// ---------------------------------------------------------------- 맵 그리기
function rng(seed) {
  return () => ((seed = (seed * 16807) % 2147483647) / 2147483647);
}

function el(tag, attrs = {}, parent) {
  const e = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs)) e.setAttribute(k, v);
  if (parent) parent.appendChild(e);
  return e;
}

function drawMap() {
  const { w, h } = state.cfg.map;
  svg.setAttribute("viewBox", `0 0 ${w} ${h}`);
  const rand = rng(7);
  const bg = el("g", {}, svg);

  el("rect", { x: 0, y: 0, width: w, height: h, fill: "#ede9df" }, bg);

  // 블록(건물 덩어리)
  for (let bx = 0; bx < w; bx += 250) {
    for (let by = 0; by < h; by += 250) {
      const tone = 228 + Math.floor(rand() * 12);
      el("rect", {
        x: bx + 22, y: by + 22, width: 206, height: 206, rx: 18,
        fill: `rgb(${tone},${tone - 3},${tone - 12})`,
      }, bg);
    }
  }
  // 공원
  [[500, 250, 500, 500], [2750, 1750, 750, 500], [1750, 2250, 500, 500], [3250, 250, 500, 250]].forEach(
    ([x, y, ww, hh]) => el("rect", { x: x + 22, y: y + 22, width: ww - 44, height: hh - 44, rx: 30, fill: "#cfe3c0" }, bg)
  );
  // 강
  el("path", {
    d: `M -50 1650 C 700 1350, 1300 2050, 2100 1500 S 3400 900, ${w + 50} 1150`,
    fill: "none", stroke: "#b7d4ea", "stroke-width": 150, "stroke-linecap": "round",
  }, bg);
  // 도로
  for (let x = 0; x <= w; x += 250) {
    const major = x % 1000 === 0;
    el("line", { x1: x, y1: 0, x2: x, y2: h, stroke: "#fbfaf6", "stroke-width": major ? 40 : 18 }, bg);
  }
  for (let y = 0; y <= h; y += 250) {
    const major = y % 1000 === 0;
    el("line", { x1: 0, y1: y, x2: w, y2: y, stroke: "#fbfaf6", "stroke-width": major ? 40 : 18 }, bg);
  }
  el("line", { x1: 0, y1: h, x2: w, y2: 0, stroke: "#fbfaf6", "stroke-width": 46 }, bg);
  // 다리 표시는 생략, 축척 막대
  const sb = el("g", { transform: `translate(${w - 700}, ${h - 110})` }, bg);
  el("rect", { x: -30, y: -60, width: 620, height: 110, rx: 16, fill: "#ffffffcc" }, sb);
  el("line", { x1: 0, y1: 0, x2: 500, y2: 0, stroke: "#1d2433", "stroke-width": 10 }, sb);
  el("line", { x1: 0, y1: -20, x2: 0, y2: 20, stroke: "#1d2433", "stroke-width": 10 }, sb);
  el("line", { x1: 500, y1: -20, x2: 500, y2: 20, stroke: "#1d2433", "stroke-width": 10 }, sb);
  const t = el("text", { x: 540, y: 14, "font-size": 40, fill: "#1d2433" }, sb);
  t.textContent = "500m";

  layerLinks = el("g", {}, svg);
  radiusCircle = el("circle", { id: "radiusCircle", r: 0, cx: -9999, cy: -9999 }, svg);
  layerUsers = el("g", {}, svg);
}

function createUserNode(u) {
  const g = el("g", { class: "user", "data-id": u.id }, layerUsers);
  el("circle", { class: "ring", r: 60 }, g);
  el("circle", { class: "flash", r: 60 }, g);
  el("circle", { class: "body", r: 52 }, g);
  el("text", { class: "ini" }, g).textContent = u.name[0];
  el("text", { class: "label", y: 95 }, g).textContent = u.name;
  u.el = g;
  g.addEventListener("pointerdown", (e) => startDrag(e, u));
}

function placeUser(u, instant = false) {
  if (instant) u.el.classList.add("instant");
  u.el.style.transform = `translate(${u.x}px, ${u.y}px)`;
  if (instant) requestAnimationFrame(() => u.el.classList.remove("instant"));
  if (u.id === state.selected) updateRadiusCircle();
}

function updateRadiusCircle() {
  const u = state.users.get(state.selected);
  if (!u) return;
  radiusCircle.setAttribute("cx", u.x);
  radiusCircle.setAttribute("cy", u.y);
  radiusCircle.setAttribute("r", state.radius);
  drawLinks();
}

function drawLinks() {
  layerLinks.replaceChildren();
  const me = state.users.get(state.selected);
  if (!me) return;
  for (const id of state.nearby.keys()) {
    const f = state.users.get(id);
    if (f) el("line", { class: "link-line", x1: me.x, y1: me.y, x2: f.x, y2: f.y }, layerLinks);
  }
}

// ---------------------------------------------------------------- 로그
function addLog(tag, msg) {
  const li = document.createElement("li");
  const now = new Date();
  const ts = now.toTimeString().slice(0, 8) + "." + String(now.getMilliseconds()).padStart(3, "0").slice(0, 2);
  li.innerHTML = `<span class="t">${ts}</span><span class="tag ${tag}">${tag}</span><span class="m"></span>`;
  li.querySelector(".m").textContent = msg;
  const log = $("log");
  log.appendChild(li);
  while (log.children.length > 300) log.firstChild.remove();
  log.scrollTop = log.scrollHeight;
}

const nameOf = (id) => state.users.get(id)?.name ?? id;

// ---------------------------------------------------------------- 위치 전송 (HTTP)
// 사용자별로 요청을 하나씩만 보내고, 그 사이 들어온 위치는 최신 것만 보낸다
const senders = new Map();
function sendLocation(u, { verbose = false } = {}) {
  let s = senders.get(u.id);
  if (!s) senders.set(u.id, (s = { inflight: false, pending: false }));
  if (s.inflight) { s.pending = true; return Promise.resolve(); }
  s.inflight = true;
  return fetch(`/api/users/${u.id}/location`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ x: u.x, y: u.y }),
  })
    .then((r) => r.json())
    .then((res) => {
      if (verbose || (u.id === state.selected && state.dragging !== u)) {
        addLog("HTTP", `POST /api/users/${u.id}/location (${u.x.toFixed(0)}, ${u.y.toFixed(0)}) → SET + PUBLISH, 구독자 ${res.receivers}명`);
      }
      return res;
    })
    .finally(() => {
      s.inflight = false;
      if (s.pending) { s.pending = false; sendLocation(u); }
    });
}

// ---------------------------------------------------------------- 사용자 선택 & SSE
async function selectUser(id) {
  if (state.es) { state.es.close(); state.es = null; }
  const session = ++state.session;
  const prev = state.users.get(state.selected);
  prev?.el.classList.remove("selected");
  state.selected = id;
  state.nearby.clear();
  const u = state.users.get(id);
  u.el.classList.add("selected");
  layerUsers.appendChild(u.el); // 맨 위로
  renderMe();
  renderNearby();
  updateRadiusCircle();
  setConn("wait", "초기화 중");

  addLog("INFO", `── ${u.name}(${id}) 접속 / 초기화 시작 ──`);
  addLog("INFO", "① HTTP로 내 위치 전달");
  await sendLocation(u, { verbose: true });
  if (session !== state.session) return;

  addLog("INFO", "② SSE 연결 → 핸들러가 친구 위치 조회 · 근접 필터링");
  const es = new EventSource(`/api/users/${id}/stream?radius=${state.radius}`);
  state.es = es;
  const on = (type, fn) => es.addEventListener(type, (e) => {
    if (session === state.session) fn(JSON.parse(e.data));
  });

  on("log", (d) => addLog(d.tag, d.msg));
  on("init", (d) => {
    setConn("on", "SSE 연결됨");
    state.nearby = new Map(d.nearby.map((f) => [f.id, f]));
    addLog("SSE", `init: 근처 친구 ${d.nearby.length}명 수신 [${d.nearby.map((f) => nameOf(f.id)).join(", ")}]`);
    addLog("INFO", "③ 초기화 완료 — 이후 내 위치는 HTTP, 근처 친구 변화는 SSE");
    setStats({ cached: d.friends_cached, received: 0, forwarded: 0, filtered: 0 });
    renderNearby();
  });
  on("nearby_list", (d) => {
    state.nearby = new Map(d.nearby.map((f) => [f.id, f]));
    const parts = [];
    if (d.entered.length) parts.push(`진입 ${d.entered.map(nameOf).join(",")}`);
    if (d.left.length) parts.push(`이탈 ${d.left.map(nameOf).join(",")}`);
    if (parts.length) addLog("SSE", `내 이동 → 메모리 기반 재계산: ${parts.join(" / ")}`);
    d.entered.forEach(flash);
    renderNearby(d.entered);
  });
  on("friend_update", (d) => {
    state.nearby.set(d.id, d);
    if (d.entered) addLog("SSE", `${nameOf(d.id)} 반경 진입 (${d.distance}m)`);
    flash(d.id);
    renderNearby(d.entered ? [d.id] : []);
  });
  on("friend_out", (d) => {
    state.nearby.delete(d.id);
    addLog("SSE", `${nameOf(d.id)} 반경 이탈 (${d.distance}m)`);
    renderNearby();
  });
  on("stats", setStats);
  on("error", (d) => addLog("INFO", `오류: ${d.msg}`));
  es.onerror = () => {
    if (session === state.session) setConn("wait", "재연결 중");
  };
}

function setConn(cls, text) {
  const b = $("conn");
  b.className = "badge " + cls;
  b.textContent = text;
}

function setStats(s) {
  $("stCached").textContent = s.cached;
  $("stRecv").textContent = s.received;
  $("stFwd").textContent = s.forwarded;
  $("stDrop").textContent = s.filtered;
}

function flash(id) {
  const f = state.users.get(id);
  if (!f) return;
  f.el.classList.remove("flashing");
  void f.el.getBoundingClientRect();
  f.el.classList.add("flashing");
}

function renderMe() {
  const u = state.users.get(state.selected);
  if (!u) return;
  $("meAvatar").textContent = u.name[0];
  $("meName").textContent = `${u.name} (${u.id})`;
  $("mePos").textContent = `x ${u.x.toFixed(0)}m · y ${u.y.toFixed(0)}m`;
}

function renderNearby(popIds = []) {
  for (const u of state.users.values()) u.el.classList.toggle("near", state.nearby.has(u.id));
  $("nearCount").textContent = state.nearby.size;
  const list = $("friends");
  const items = [...state.nearby.values()].sort((a, b) => a.distance - b.distance);
  if (!items.length) {
    list.innerHTML = '<li class="empty">근처에 친구가 없습니다</li>';
  } else {
    list.replaceChildren(...items.map((f) => {
      const li = document.createElement("li");
      if (popIds.includes(f.id)) li.className = "pop";
      const pct = Math.max(4, 100 - (f.distance / state.radius) * 100);
      li.innerHTML = `<span class="fa"></span><span class="fn"></span><span class="bar"><i style="width:${pct}%"></i></span><span class="fd">${f.distance}m</span>`;
      li.querySelector(".fa").textContent = nameOf(f.id)[0];
      li.querySelector(".fn").textContent = `${nameOf(f.id)} · ${f.id}`;
      return li;
    }));
  }
  drawLinks();
}

// ---------------------------------------------------------------- 드래그
function svgPoint(e) {
  const p = svg.createSVGPoint();
  p.x = e.clientX; p.y = e.clientY;
  return p.matrixTransform(svg.getScreenCTM().inverse());
}

function startDrag(e, u) {
  e.preventDefault();
  const start = svgPoint(e);
  const off = { x: u.x - start.x, y: u.y - start.y };
  let moved = false, lastSend = 0;
  state.dragging = u;
  u.el.classList.add("dragging");
  layerUsers.appendChild(u.el);
  svg.setPointerCapture(e.pointerId);

  const { w, h } = state.cfg.map;
  const move = (ev) => {
    const p = svgPoint(ev);
    if (!moved && Math.hypot(p.x - start.x, p.y - start.y) < 15) return;
    moved = true;
    u.x = Math.min(w, Math.max(0, p.x + off.x));
    u.y = Math.min(h, Math.max(0, p.y + off.y));
    placeUser(u);
    if (u.id === state.selected) renderMe();
    else if (state.nearby.has(u.id)) drawLinks();
    const now = performance.now();
    if (now - lastSend > 60) { lastSend = now; sendLocation(u); }
  };
  const up = () => {
    svg.removeEventListener("pointermove", move);
    svg.removeEventListener("pointerup", up);
    svg.removeEventListener("pointercancel", up);
    u.el.classList.remove("dragging");
    state.dragging = null;
    if (moved) sendLocation(u);
    else if (u.id !== state.selected) selectUser(u.id);
  };
  svg.addEventListener("pointermove", move);
  svg.addEventListener("pointerup", up);
  svg.addEventListener("pointercancel", up);
}

// ---------------------------------------------------------------- 랜덤 이동
let walkTimer = null;
function walkTick() {
  const { w, h } = state.cfg.map;
  for (const u of state.users.values()) {
    if (u.id === state.selected || u === state.dragging) continue;
    if (Math.random() < 0.15) {
      const a = Math.random() * Math.PI * 2, sp = 40 + Math.random() * 90;
      u.vx = Math.cos(a) * sp; u.vy = Math.sin(a) * sp;
    }
    u.x += u.vx; u.y += u.vy;
    if (u.x < 60 || u.x > w - 60) { u.vx *= -1; u.x = Math.min(w - 60, Math.max(60, u.x)); }
    if (u.y < 60 || u.y > h - 60) { u.vy *= -1; u.y = Math.min(h - 60, Math.max(60, u.y)); }
    placeUser(u);
    sendLocation(u);
  }
  drawLinks();
}

$("walk").addEventListener("change", (e) => {
  clearInterval(walkTimer);
  if (e.target.checked) {
    for (const u of state.users.values()) {
      const a = Math.random() * Math.PI * 2;
      u.vx = Math.cos(a) * 70; u.vy = Math.sin(a) * 70;
    }
    walkTimer = setInterval(walkTick, 900);
    addLog("INFO", "다른 친구 랜덤 이동 시작 (각자 HTTP POST로 위치 전송)");
  } else {
    addLog("INFO", "랜덤 이동 정지");
  }
});

$("reset").addEventListener("click", async () => {
  state.resetting = true;
  setTimeout(() => (state.resetting = false), 1500);
  await fetch("/api/admin/reset", { method: "POST" });
  addLog("HTTP", "POST /api/admin/reset → 모든 사용자 위치 재배치");
});

$("clearLog").addEventListener("click", () => $("log").replaceChildren());

$("radius").addEventListener("input", (e) => {
  $("radiusVal").textContent = `${e.target.value}m`;
  radiusCircle.setAttribute("r", e.target.value);
});
$("radius").addEventListener("change", (e) => {
  state.radius = Number(e.target.value);
  addLog("INFO", `반경 ${state.radius}m 로 변경 → SSE 재연결`);
  if (state.selected) selectUser(state.selected);
});

// ---------------------------------------------------------------- 시작
async function main() {
  state.cfg = await (await fetch("/api/config")).json();
  state.radius = state.cfg.default_radius;
  $("radius").value = state.radius;
  $("radiusVal").textContent = `${state.radius}m`;
  drawMap();
  for (const u of state.cfg.users) {
    const user = { ...u, x: -1000, y: -1000, vx: 0, vy: 0 };
    state.users.set(u.id, user);
    createUserNode(user);
  }

  // 관제 스트림: 전체 사용자를 맵에 표시 (근접 판정과 무관)
  const admin = new EventSource("/api/admin/stream");
  let first = true;
  admin.addEventListener("snapshot", (e) => {
    for (const loc of JSON.parse(e.data).locations) {
      const u = state.users.get(loc.id);
      if (u) { u.x = loc.x; u.y = loc.y; placeUser(u, true); }
    }
    if (first) { first = false; selectUser(state.cfg.users[0].id); }
  });
  admin.addEventListener("location", (e) => {
    const loc = JSON.parse(e.data);
    const u = state.users.get(loc.id);
    if (!u || u === state.dragging) return;
    // 내가 보낸 값이 다시 돌아온 경우는 무시(랜덤 이동 중 되감기 방지)
    if (Math.abs(u.x - loc.x) < 1 && Math.abs(u.y - loc.y) < 1) return;
    if ($("walk").checked && !state.resetting && u.id !== state.selected) return;
    u.x = loc.x; u.y = loc.y;
    placeUser(u);
    if (u.id === state.selected) renderMe();
    if (state.nearby.has(u.id)) drawLinks();
  });
}

main();
