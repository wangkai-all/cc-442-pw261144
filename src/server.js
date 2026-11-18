import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { rmSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { SystemClock } from './clock.js';
import { Scheduler } from './scheduler.js';
import { JsonStore } from './store.js';
import { attachWorker } from './worker.js';

const here = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? 3847);
const STORE_FILE = process.env.STORE_FILE ?? join(here, '..', 'data', 'scheduler-state.json');

/** Demo sources: a flaky high-priority one plus two quieter low-priority ones. */
function seedDemo(scheduler) {
  scheduler.registerSource({
    id: 'alpha',
    name: 'Alpha (flaky, high priority)',
    priority: 100,
    quota: { capacity: 6, refillMs: 30_000 },
    cooldownMs: 2_000,
    maxConcurrency: 2,
    minShare: 1,
    backoff: { baseMs: 3_000, factor: 2, maxMs: 30_000 },
    demo: { minMs: 800, maxMs: 3_000, failureRate: 0.55 },
  });
  scheduler.registerSource({
    id: 'beta',
    name: 'Beta (medium priority)',
    priority: 50,
    quota: { capacity: 4, refillMs: 30_000 },
    cooldownMs: 3_000,
    maxConcurrency: 1,
    minShare: 1,
    backoff: { baseMs: 2_000, factor: 2, maxMs: 20_000 },
    demo: { minMs: 1_000, maxMs: 4_000, failureRate: 0.1 },
  });
  scheduler.registerSource({
    id: 'gamma',
    name: 'Gamma (low priority)',
    priority: 10,
    quota: { capacity: 3, refillMs: 60_000 },
    cooldownMs: 5_000,
    maxConcurrency: 1,
    minShare: 1,
    backoff: { baseMs: 2_000, factor: 2, maxMs: 20_000 },
    demo: { minMs: 1_500, maxMs: 5_000, failureRate: 0.05 },
  });
}

function createScheduler() {
  const clock = new SystemClock();
  const store = new JsonStore(STORE_FILE);
  const scheduler = new Scheduler({ clock, store, maxConcurrency: 4 });
  attachWorker(scheduler, clock);
  if (scheduler.sources.size === 0) seedDemo(scheduler);
  scheduler.start();
  return scheduler;
}

let scheduler = createScheduler();

async function readBody(req) {
  let raw = '';
  for await (const chunk of req) raw += chunk;
  return raw ? JSON.parse(raw) : {};
}

function sendJson(res, status, body) {
  const payload = JSON.stringify(body, null, 2);
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8' });
  res.end(payload);
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, 'http://localhost');
    const path = url.pathname;

    if (req.method === 'GET' && path === '/') {
      const html = await readFile(join(here, '..', 'public', 'index.html'), 'utf8');
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      res.end(html);
      return;
    }

    if (req.method === 'GET' && path === '/api/state') {
      sendJson(res, 200, scheduler.getState());
      return;
    }

    if (req.method === 'POST' && path === '/api/sources') {
      const body = await readBody(req);
      scheduler.registerSource(body);
      sendJson(res, 200, { ok: true });
      return;
    }

    if (req.method === 'POST' && path === '/api/tasks') {
      const body = await readBody(req);
      const count = Math.max(1, Math.min(100, Number(body.count ?? 1)));
      const results = [];
      for (let i = 0; i < count; i++) {
        results.push(
          scheduler.submit({
            id: body.id && count === 1 ? body.id : undefined,
            sourceId: body.sourceId,
            payload: body.payload ?? null,
          }),
        );
      }
      sendJson(res, 200, {
        ok: true,
        submitted: results.filter((r) => !r.deduplicated).length,
        deduplicated: results.filter((r) => r.deduplicated).length,
        tasks: results.map((r) => r.task.id),
      });
      return;
    }

    const cancelMatch = path.match(/^\/api\/tasks\/([^/]+)\/cancel$/);
    if (req.method === 'POST' && cancelMatch) {
      const body = await readBody(req).catch(() => ({}));
      const task = scheduler.cancel(decodeURIComponent(cancelMatch[1]), body.reason);
      if (!task) {
        sendJson(res, 404, { ok: false, error: 'task not found' });
        return;
      }
      sendJson(res, 200, { ok: true, task });
      return;
    }

    if (req.method === 'POST' && path === '/api/reset') {
      scheduler.dispose();
      rmSync(STORE_FILE, { force: true });
      scheduler = createScheduler();
      sendJson(res, 200, { ok: true });
      return;
    }

    sendJson(res, 404, { ok: false, error: 'not found' });
  } catch (err) {
    sendJson(res, 400, { ok: false, error: String(err.message ?? err) });
  }
});

server.listen(PORT, () => {
  console.log(`scheduler dashboard: http://localhost:${PORT}`);
  console.log(`state snapshot:    ${STORE_FILE}`);
});
