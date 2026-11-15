// mini-monitor 시연 녹화 (시연_시나리오.md 0~7). 사용법: node media/record.cjs <출력 폴더>
// 끝나면 늘린 에이전트·장애를 원래대로 돌려놓는다.
const { chromium } = require('/home/sajacaros/.npm/_npx/e41f203b7505f1fb/node_modules/playwright-core');
const OUT = process.argv[2] || '.';
const BASE = 'http://localhost:8000';
const W = 1440, H = 900;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const api = async (path, body) => (await fetch(BASE + path, body ? {
  method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) } : {})).json();

async function waitUntil(fn, timeout, every = 1000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    try { if (await fn()) return true; } catch { /* 다시 */ }
    await sleep(every);
  }
  return false;
}

async function cap(page, text) {
  await page.evaluate((t) => {
    let d = document.getElementById('demo-cap');
    if (!d) {
      d = document.createElement('div');
      d.id = 'demo-cap';
      Object.assign(d.style, {
        position: 'fixed', left: '50%', bottom: '22px', transform: 'translateX(-50%)', zIndex: 99999,
        background: 'rgba(138,92,246,.96)', color: '#fff', padding: '10px 20px', borderRadius: '8px',
        font: '600 19px system-ui, "Noto Sans KR", sans-serif', boxShadow: '0 6px 24px rgba(0,0,0,.55)',
        maxWidth: '92%', textAlign: 'center', lineHeight: '1.4',
      });
      document.body.appendChild(d);
    }
    d.textContent = t;
  }, text);
}

async function go(page, path, zoom = 0.9) {
  await page.goto(path.startsWith('http') ? path : BASE + path);
  await page.evaluate((z) => { document.body.style.zoom = String(z); }, zoom);
  await sleep(1500);
}

const stage1Card = (page, i) => page.locator('.stage').first().locator(':scope > .card').nth(i);
const groupBtn = (page, card, group, label) =>
  stage1Card(page, card).locator('.agrp', { hasText: `service:${group}` }).getByRole('button', { name: label, exact: true });

(async () => {
  await api('/api/chaos', { action: 'normal' });
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: W, height: H }, recordVideo: { dir: OUT, size: { width: W, height: H } } });
  const page = await ctx.newPage();

  // 0) 전체 흐름
  await go(page, '/');
  await cap(page, '① 지표 출처 → ② 수집기 → ③ Kafka → ④ writer → ⑤ TimescaleDB → ⑥ 질의 서비스 → ⑦ 경보 (그림 22)');
  await sleep(7000);

  // 1) push 에이전트 추가
  await cap(page, '1. push · service:web 에 에이전트 추가 → 기동 중 → 서비스 탐색에 등록');
  await groupBtn(page, 0, 'web', '+').click();
  await sleep(9000);

  // 2) pull 에이전트 추가 → 해시 링 재분배
  await cap(page, '2. pull · service:api 에 추가 → 해시 링에 올라가 수집기 담당이 다시 나뉜다 (그림 11)');
  await groupBtn(page, 1, 'api', '+').click();
  await sleep(10000);

  // 3) push-1 CPU 스파이크 → 호스트 맵
  await cap(page, '3. push-1 에 🔥 CPU 스파이크: 서버가 직접 나쁜 값을 만들어 평소 경로로 보낸다');
  await stage1Card(page, 0).locator('.agent', { hasText: 'push-1' }).first().getByTitle('spike 60초').click();
  await sleep(5000);
  await go(page, '/hosts');
  await cap(page, '호스트 맵: 질의 서비스로 읽은 저장된 지표. push-1 칸이 빨개진다');
  await page.locator('.tile', { hasText: 'push-1' }).first().click();
  await sleep(7000);

  // 4) 경보: pending → firing → 알림
  await go(page, '/alerts');
  await cap(page, '4. 경보 관리자가 5초마다 질의 서비스에 규칙을 묻는다: pending → (for 15초) → firing → 알림');
  await waitUntil(async () => (await api('/api/alerts')).alerts.some((a) => a.host === 'sim-push-1' && a.state === 'firing'), 45000);
  await cap(page, 'firing! host 별로 병합된 알림이 경보 소비자를 거쳐 채널로 나갔다 (그림 19·20)');
  await sleep(6000);

  // 5) DB 장애 → Kafka lag → 재개
  await go(page, '/');
  await cap(page, '5. writer 를 멈춘다 (DB 장애): 아직 DB 에 못 쓴 메시지가 Kafka 에 빨간 lag 로 쌓인다');
  // 화면이 1초마다 다시 그려져 버튼 클릭이 빗나가므로, 버튼이 부르는 control() 을 직접 부른다
  const writers = (await api('/api/state')).components.filter((c) => c.role === 'writer').map((c) => c.name);
  await page.evaluate((ws) => Promise.all(ws.map((w) => control(w, 'pause'))), writers);
  await waitUntil(async () => (await api('/api/state')).kafka.metrics.lag > 400, 20000);
  await sleep(4000);
  await cap(page, '재개: DB 에 쓴 뒤에만 커밋하므로 잃은 데이터 없이 따라잡는다 (그림 15)');
  await page.evaluate((ws) => Promise.all(ws.map((w) => control(w, 'resume'))), writers);
  await waitUntil(async () => (await api('/api/state')).kafka.metrics.lag < 30, 30000);
  await sleep(4000);

  // 6) SLO: 전체 장애 주입
  await go(page, '/slo');
  await cap(page, '6. SLO: 모든 서버에 3분짜리 CPU 스파이크를 넣는다');
  await page.selectOption('#cAction', 'spike');
  await page.selectOption('#cMode', 'all');
  await page.selectOption('#cPct', '100');
  await page.selectOption('#cSec', '180');
  await page.getByRole('button', { name: '장애 넣기' }).click();
  await sleep(3000);
  await cap(page, '5분 소진율이 뛰고 오류 예산이 줄어든다 → "빠르게 소진" / "위반" (10초마다 다시 계산)');
  await waitUntil(async () => ((await api('/api/slo')).slos || []).some((s) => s.status !== '정상'), 90000, 2000);
  await sleep(12000);

  // 7) Grafana
  await go(page, 'http://localhost:3000/d/mini-monitor?orgId=1&refresh=5s&kiosk', 0.8);
  await page.waitForSelector('.react-grid-layout', { timeout: 15000 }).catch(() => {});
  await sleep(2000);
  await cap(page, '7. Grafana 도 질의 서비스(Prometheus API 호환)를 거친다. TSDB 가 무엇인지 모른다');
  await sleep(9000);

  await ctx.close();
  await browser.close();

  // 원래대로
  await api('/api/chaos', { action: 'normal' });
  await api('/api/scale', { service: 'sim-push', group: 'web', delta: -1 });
  await api('/api/scale', { service: 'sim-pull', group: 'api', delta: -1 });
})().catch((e) => { console.error(e); process.exit(1); });
