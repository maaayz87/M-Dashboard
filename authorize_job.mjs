import { chromium } from '/usr/local/lib/node_modules/playwright-core/index.mjs';

const sources = {
  agibot: { profile: 'zhiyuan', url: 'https://agirobot.jobs.feishu.cn/campusrecruitment/position/application' },
  inspire: { profile: 'inspire', url: 'https://arashivision.jobs.feishu.cn/campus/position/application' },
  qcraft: { profile: 'qcraft', url: 'https://qcraft.jobs.feishu.cn/campus/position/application?share_token=MzsxNzg3ODk0MzAyMDU1Ozc2Nzg5MDAxMjY5MjExNDg2OTg7MDsxLzI' },
};
const sourceId = process.argv[2];
if (!sources[sourceId]) throw new Error(`unknown source: ${sourceId}`);
const source = sources[sourceId];
const profileDir = `/home/mayizhe/service-hub/job-auth/profiles/${source.profile}`;
const context = await chromium.launchPersistentContext(profileDir, {
  executablePath: '/usr/bin/google-chrome',
  headless: false,
  viewport: { width: 1440, height: 900 },
  args: ['--no-sandbox', '--disable-dev-shm-usage', '--disable-background-networking', '--no-first-run', '--no-default-browser-check'],
});
let page = context.pages()[0];
if (!page) page = await context.newPage();
await page.goto(source.url, { waitUntil: 'domcontentloaded', timeout: 45000 });
console.log(`[job-auth] ${sourceId} browser ready: ${source.url}`);
console.log('[job-auth] complete login in the visible browser, then leave this process running');
const shutdown = async () => { try { await context.close(); } finally { process.exit(0); } };
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
await new Promise(() => {});
