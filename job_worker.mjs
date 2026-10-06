import fs from 'node:fs/promises';
import path from 'node:path';
import { chromium } from '/usr/local/lib/node_modules/playwright-core/index.mjs';

const DATA_DIR = '/home/mayizhe/service-hub/data';
const SOURCE_FILE = path.join(DATA_DIR, 'job_applications.json');
const LIVE_FILE = path.join(DATA_DIR, 'job_live.json');
const PROFILE_ROOT = '/home/mayizhe/service-hub/job-auth/profiles';
const CHROME = '/usr/bin/google-chrome';
const INTERVAL_MS = 5 * 60 * 1000;

const FEISHU_SOURCES = {
  agibot: {
    origin: 'https://agirobot.jobs.feishu.cn',
    path: '/campusrecruitment/position/application',
    profile: 'zhiyuan',
  },
  inspire: {
    origin: 'https://arashivision.jobs.feishu.cn',
    path: '/campus/position/application',
    profile: 'inspire',
  },
  qcraft: {
    origin: 'https://qcraft.jobs.feishu.cn',
    path: '/campus/position/application?share_token=MzsxNzg3ODk0MzAyMDU1Ozc2Nzg5MDAxMjY5MjExNDg2OTg7MDsxLzI',
    profile: 'qcraft',
  },
};

const STAGES = {
  0: '已投递',
  1: '已终止',
  2: '简历筛选',
  3: '简历评估',
  4: '评估通过',
  5: '笔试',
  6: '面试',
  7: '面试通过',
  8: '待入职',
  9: '已入职',
  10: '转岗',
};

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function readJson(file, fallback) {
  try { return JSON.parse(await fs.readFile(file, 'utf8')); } catch (_) { return fallback; }
}

async function atomicWriteJson(file, data) {
  const tmp = `${file}.tmp-${process.pid}`;
  await fs.writeFile(tmp, JSON.stringify(data, null, 2), { encoding: 'utf8', mode: 0o600 });
  await fs.rename(tmp, file);
}

function isoTime(value) {
  if (!value) return null;
  const number = Number(value);
  if (!Number.isFinite(number)) return String(value);
  return new Date(number < 10_000_000_000 ? number * 1000 : number).toISOString();
}

function operationSummary(list) {
  if (!Array.isArray(list) || !list.length) return '';
  return list.map(item => {
    const code = item.operation_code ?? item.stage_id ?? '?';
    const time = isoTime(item.biz_create_time || item.created_at || item.time);
    return `event ${code}${time ? ` @ ${time}` : ''}`;
  }).join('; ');
}

function normalizeDelivery(item) {
  const stageId = item?.current_stage?.stage_id ?? null;
  const title = item?.job_post_info?.title || item?.job_post_info?.sub_title || '岗位待补充';
  return {
    id: String(item?.application_id || item?.id || `${item?.job_post_info?.id || 'unknown'}-${item?.biz_create_time || ''}`),
    role: title,
    applied_at: isoTime(item?.biz_create_time),
    status: STAGES[stageId] || `阶段 ${stageId ?? '未知'}`,
    notes: operationSummary(item?.operation_list),
    stage_id: stageId,
    job_id: item?.job_post_info?.id || null,
    application_id: item?.application_id || item?.id || null,
    portal_delivery_tag: item?.portal_delivery_tag ?? null,
  };
}

async function probeFeishu(sourceId, source) {
  const profileDir = path.join(PROFILE_ROOT, source.profile);
  await fs.mkdir(profileDir, { recursive: true, mode: 0o700 });
  let context;
  try {
    context = await chromium.launchPersistentContext(profileDir, {
      executablePath: CHROME,
      headless: true,
      args: ['--no-sandbox', '--disable-dev-shm-usage', '--no-first-run', '--no-default-browser-check'],
    });
    const page = context.pages()[0] || await context.newPage();
    let deliveries = null;
    let loginStatus = null;
    page.on('response', async response => {
      const url = response.url();
      if (url.includes('/api/v1/user/mobile/login_status')) {
        try { const body = await response.json(); loginStatus = body?.data === true; } catch (_) {}
      }
      if (response.request().method() === 'POST' && url.includes('/api/v1/search/user/applications') && response.status() === 200) {
        try {
          const body = await response.json();
          if (Array.isArray(body?.data?.delivery_list)) deliveries = body.data.delivery_list;
        } catch (_) {}
      }
    });
    await page.goto(`${source.origin}${source.path}`, { waitUntil: 'domcontentloaded', timeout: 45000 });
    for (let i = 0; i < 30 && !deliveries && loginStatus !== false; i += 1) await sleep(500);
    if (loginStatus === false) {
      return { read_status: 'auth_required', message: '登录态已失效，请重新登录', checked_at: new Date().toISOString(), applications: [] };
    }
    if (!Array.isArray(deliveries)) {
      const bodyText = await page.locator('body').innerText().catch(() => '');
      const looksLoggedIn = /应聘记录|我的投递|投递记录/.test(bodyText);
      return {
        read_status: looksLoggedIn ? 'no_data' : 'auth_required',
        message: looksLoggedIn ? '已登录，但未读取到投递列表' : '未检测到有效登录态',
        checked_at: new Date().toISOString(),
        applications: [],
      };
    }
    return {
      read_status: 'ok',
      message: `已读取 ${deliveries.length} 条投递`,
      checked_at: new Date().toISOString(),
      applications: deliveries.map(normalizeDelivery),
    };
  } catch (error) {
    return { read_status: 'unavailable', message: `读取失败：${error?.name || 'unknown error'}`, checked_at: new Date().toISOString(), applications: [] };
  } finally {
    if (context) await context.close().catch(() => {});
  }
}

async function runOnce() {
  const sources = await readJson(SOURCE_FILE, { applications: [] });
  const previous = await readJson(LIVE_FILE, { version: 1, sources: {} });
  const live = { version: 1, updated_at: new Date().toISOString(), sources: { ...(previous.sources || {}) } };
  for (const source of sources.applications || []) {
    if (FEISHU_SOURCES[source.id]) {
      live.sources[source.id] = await probeFeishu(source.id, FEISHU_SOURCES[source.id]);
    } else {
      live.sources[source.id] = {
        read_status: 'not_configured',
        message: '该平台适配器尚未配置',
        checked_at: new Date().toISOString(),
        applications: [],
      };
    }
  }
  await atomicWriteJson(LIVE_FILE, live);
  const summary = Object.entries(live.sources).map(([id, item]) => `${id}:${item.read_status}/${item.applications?.length || 0}`).join(' ');
  console.log(`[job-worker] ${live.updated_at} ${summary}`);
}

await runOnce();
while (true) {
  await sleep(INTERVAL_MS);
  await runOnce();
}
