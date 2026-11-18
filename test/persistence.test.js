import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { SimulatedClock } from '../src/clock.js';
import { Scheduler } from '../src/scheduler.js';
import { JsonStore } from '../src/store.js';
import { sourceState, T0 } from './helpers.js';

function bootScheduler(clock, file) {
  const scheduler = new Scheduler({ clock, store: new JsonStore(file), maxConcurrency: 2 });
  scheduler.registerSource({
    id: 'P', priority: 5, minShare: 1, maxConcurrency: 1, cooldownMs: 0,
    quota: { capacity: 10, refillMs: 10_000 },
    backoff: { baseMs: 1_000, factor: 2, maxMs: 4_000 },
  });
  return scheduler;
}

test('persistence: restart restores quota consumption, backoff and requeues in-flight tasks', () => {
  const dir = mkdtempSync(join(tmpdir(), 'scheduler-test-'));
  const file = join(dir, 'state.json');
  try {
    const clock = new SimulatedClock(T0);
    const s1 = bootScheduler(clock, file);
    // p0..p2 succeed, f1/f2 fail, 'long' stays in flight, 'queued1' never starts
    s1.on('task-started', (task) => {
      if (task.id === 'long') return; // never completes
      if (task.id.startsWith('f')) s1.complete(task.id, new Error('boom'));
      else s1.complete(task.id);
    });
    s1.start();
    for (const id of ['p0', 'p1', 'p2', 'f1']) s1.submit({ sourceId: 'P', id });
    // f1's failure pauses the source for 1000ms; f2 fails after that, pausing 2000ms
    clock.advance(1_000);
    s1.submit({ sourceId: 'P', id: 'f2' });
    clock.advance(2_000);
    s1.submit({ sourceId: 'P', id: 'long' });
    s1.submit({ sourceId: 'P', id: 'queued1' });

    assert.equal(sourceState(s1, 'P').stats.quotaSpent, 6); // p0,p1,p2,f1,f2,long
    // 10 - 6 spent + 3 refilled over 3000ms (1 token per 1000ms)
    assert.equal(sourceState(s1, 'P').quota.tokens, 7);
    assert.equal(sourceState(s1, 'P').backoff.failures, 2);
    const backoffUntil = sourceState(s1, 'P').backoff.until;
    assert.equal(backoffUntil, T0 + 3_000, 'f1 -> 1000ms, f2 -> 2000ms from T0+1000');
    assert.equal(s1.statusOf('long'), 'running');
    assert.equal(s1.statusOf('queued1'), 'queued');
    s1.dispose();

    // ---- restart: new scheduler, same clock and store file ----
    const s2 = bootScheduler(clock, file);
    const p = sourceState(s2, 'P');
    assert.equal(p.quota.tokens, 7, 'quota consumption must survive restart (not reset to capacity)');
    assert.equal(p.stats.quotaSpent, 6);
    assert.equal(p.backoff.failures, 2, 'backoff state must survive restart');
    assert.equal(p.backoff.until, backoffUntil);
    assert.equal(s2.statusOf('long'), 'queued', 'in-flight task is requeued');
    assert.equal(s2.tasks.get('long').recovered, 'requeued-after-restart');
    assert.equal(s2.statusOf('queued1'), 'queued');

    // dedupe survives restart: re-submitting a completed task costs nothing
    const dup = s2.submit({ sourceId: 'P', id: 'p0' });
    assert.equal(dup.deduplicated, true);
    assert.equal(sourceState(s2, 'P').stats.quotaSpent, 6, 'duplicate must not spend quota again');

    // recovery: after restart the scheduler resumes and drains the queue
    s2.on('task-started', (task) => s2.complete(task.id));
    clock.advance(2_000); // let the restored backoff expire
    s2.start();
    assert.equal(s2.statusOf('long'), 'succeeded');
    assert.equal(s2.statusOf('queued1'), 'succeeded');
    s2.dispose();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('persistence: corrupt snapshot starts clean instead of crashing', () => {
  const dir = mkdtempSync(join(tmpdir(), 'scheduler-test-'));
  const file = join(dir, 'state.json');
  try {
    const clock = new SimulatedClock(T0);
    const s1 = bootScheduler(clock, file);
    s1.start();
    s1.dispose();
    writeFileSync(file, '{not json'); // corrupt the snapshot
    const s2 = new Scheduler({ clock, store: new JsonStore(file), maxConcurrency: 2 });
    assert.equal(s2.sources.size, 0);
    s2.dispose();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
