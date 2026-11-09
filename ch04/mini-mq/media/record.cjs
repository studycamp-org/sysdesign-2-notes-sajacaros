// mini-mq 시연 녹화: 선출 과정 보기 → 메시지 1건 보내기(지도 없음 → 처음 받음) → 한 건 재생
const { chromium } = require('/home/sajacaros/.npm/_npx/e41f203b7505f1fb/node_modules/playwright-core');
const OUT = process.argv[2];
const URL = 'http://localhost:8081/?view=simple';
const W = 1440, H = 1000;

const sleep = ms => new Promise(r => setTimeout(r, ms));
async function waitBanner(page, re, timeout) {
  await page.waitForFunction(src => new RegExp(src).test(document.getElementById('banner').textContent),
    re.source, { timeout, polling: 200 });
}

(async () => {
  // 시작 상태: 생산자 지도·버퍼 비우기
  await fetch('http://localhost:8081/admin/producers/producer-logs/reset', { method: 'POST' }).catch(() => {});
  const browser = await chromium.launch({ args: ['--no-sandbox'] });
  const ctx = await browser.newContext({ viewport: { width: W, height: H }, recordVideo: { dir: OUT, size: { width: W, height: H } } });
  const page = await ctx.newPage();
  await page.goto(URL);
  await page.evaluate(() => { document.body.style.zoom = '0.82'; });
  await sleep(2500);

  // 1) 선출 과정 보기 (② 탭으로 바뀐다)
  await page.getByRole('button', { name: '선출 과정 보기' }).click();
  await waitBanner(page, /^끝:/, 120000);
  await sleep(3500);

  // 2) ① 기본 흐름으로 돌아와 메시지 1건 → 버퍼에서 기다림
  await page.getByRole('button', { name: '① 기본 흐름' }).click();
  await sleep(2000);
  await page.getByRole('button', { name: '1건', exact: true }).click();
  await sleep(4500);
  // 3) 덜 찬 묶음을 지금 보내기 → 라우팅 → 저장 → 복제 → ACK → 가져감 → 커밋
  await page.getByRole('button', { name: '지금 보내기' }).click();
  await waitBanner(page, /^끝:/, 150000);
  await sleep(4000);

  await ctx.close();
  await browser.close();
})().catch(e => { console.error(e); process.exit(1); });
