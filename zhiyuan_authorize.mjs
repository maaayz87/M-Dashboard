import { chromium } from '/usr/local/lib/node_modules/playwright-core/index.mjs';

const profileDir = '/home/mayizhe/service-hub/job-auth/profiles/zhiyuan';
const targetUrl = 'https://agirobot.jobs.feishu.cn/campusrecruitment/position/application';
const executablePath = '/usr/bin/google-chrome';

const context = await chromium.launchPersistentContext(profileDir, {
  executablePath,
  headless: false,
  viewport: { width: 1440, height: 900 },
  args: [
    '--no-sandbox',
    '--disable-dev-shm-usage',
    '--disable-background-networking',
    '--no-first-run',
    '--no-default-browser-check',
  ],
});

let page = context.pages()[0];
if (!page) page = await context.newPage();
await page.goto(targetUrl, { waitUntil: 'domcontentloaded', timeout: 45000 });
console.log(`[zhiyuan] authorization browser ready: ${targetUrl}`);
console.log('[zhiyuan] finish login in the visible browser window; leave this process running');

const shutdown = async () => {
  try { await context.close(); } finally { process.exit(0); }
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
await new Promise(() => {});
