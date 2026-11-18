import assert from 'node:assert/strict';
import { test } from 'node:test';
import { makeScheduler, pump, sourceState } from './helpers.js';

function cancelScheduler(maxConcurrency = 1) {
  const ctx = makeScheduler({ maxConcurrency: 3 });
  ctx.scheduler.registerSource({
    id: 'X', priority: 1, minShare: 1, maxConcurrency, cooldownMs: 0,
    quota: { capacity: 2, refillMs: 10_000 },
    backoff: { baseMs: 1_000, factor: 2, maxMs: 4_000 },
  });
  return ctx;
}

test('cancel: cancelling a running task refunds quota and stays failure-neutral', () => {
  const { clock, scheduler } = cancelScheduler();
  // worker that completes after 5s unless the task was cancelled
  scheduler.on('task-started', (task) => {
    clock.setTimeout(() => scheduler.complete(task.id), 5_000);
  });
  scheduler.start();

  scheduler.submit({ sourceId: 'X', id: 'r1' });
  assert.equal(scheduler.statusOf('r1'), 'running');
  assert.equal(sourceState(scheduler, 'X').quota.tokens, 1); // 2 - 1 spent

  scheduler.cancel('r1');
  const x = sourceState(scheduler, 'X');
  assert.equal(scheduler.statusOf('r1'), 'cancelled');
  assert.equal(x.quota.tokens, 2, 'running cancel refunds the quota token');
  assert.equal(x.stats.quotaRefunded, 1);
  assert.equal(x.stats.cancelled, 1);
  assert.equal(x.backoff.failures, 0, 'cancel must not count as a failure');
  assert.equal(x.backoff.remainingMs, 0);

  // the stale worker completion is ignored
  pump(clock, 6_000);
  assert.equal(scheduler.statusOf('r1'), 'cancelled');
  assert.equal(sourceState(scheduler, 'X').stats.succeeded, 0);

  // recovery info is visible in the recent list
  const recent = scheduler.getState().recent.find((t) => t.id === 'r1');
  assert.equal(recent.status, 'cancelled');
  assert.equal(recent.quotaRefunded, true);
});

test('cancel: cancelling a queued task removes it without touching quota', () => {
  const { scheduler } = cancelScheduler();
  scheduler.on('task-started', () => {}); // never completes: first task blocks the source
  scheduler.start();

  scheduler.submit({ sourceId: 'X', id: 'blocker' });
  scheduler.submit({ sourceId: 'X', id: 'q1' });
  assert.equal(scheduler.statusOf('q1'), 'queued');
  const tokensBefore = sourceState(scheduler, 'X').quota.tokens;

  scheduler.cancel('q1');
  assert.equal(scheduler.statusOf('q1'), 'cancelled');
  assert.equal(scheduler.getState().queuedTotal, 0);
  assert.equal(sourceState(scheduler, 'X').quota.tokens, tokensBefore, 'queued task never spent quota');
  assert.equal(sourceState(scheduler, 'X').stats.quotaRefunded, 0);
});

test('cancel: freed slot is immediately reused by the next queued task', () => {
  const { clock, scheduler } = cancelScheduler();
  scheduler.on('task-started', (task) => {
    if (task.id === 'slow') clock.setTimeout(() => scheduler.complete(task.id), 60_000);
    else scheduler.complete(task.id);
  });
  scheduler.start();

  scheduler.submit({ sourceId: 'X', id: 'slow' });
  scheduler.submit({ sourceId: 'X', id: 'next' });
  assert.equal(scheduler.statusOf('next'), 'queued');

  scheduler.cancel('slow');
  assert.equal(scheduler.statusOf('next'), 'succeeded', 'cancel frees the slot for the queue');
});
