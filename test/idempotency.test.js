import assert from 'node:assert/strict';
import { test } from 'node:test';
import { autoResolve, BIG_QUOTA, makeScheduler, pump, sourceState } from './helpers.js';

function idemScheduler() {
  const ctx = makeScheduler();
  ctx.scheduler.registerSource({
    id: 'I', priority: 1, minShare: 1, maxConcurrency: 2, cooldownMs: 0,
    quota: BIG_QUOTA, backoff: { baseMs: 100, factor: 2, maxMs: 1_000 },
  });
  return ctx;
}

test('idempotency: duplicate submissions never spend quota twice', () => {
  const { clock, scheduler } = idemScheduler();
  autoResolve(scheduler);
  scheduler.start();

  const first = scheduler.submit({ sourceId: 'I', id: 'x' });
  const dup1 = scheduler.submit({ sourceId: 'I', id: 'x' });
  assert.equal(first.deduplicated, false);
  assert.equal(dup1.deduplicated, true);
  assert.equal(dup1.task.status, 'succeeded', 'x already ran synchronously');
  assert.equal(scheduler.getState().queuedTotal, 0);

  // re-submitting after completion is still a no-op
  const dup2 = scheduler.submit({ sourceId: 'I', id: 'x' });
  assert.equal(dup2.deduplicated, true);

  const stats = sourceState(scheduler, 'I').stats;
  assert.equal(stats.quotaSpent, 1, 'quota spent exactly once for one logical task');
  assert.equal(stats.started, 1);
  assert.equal(stats.deduplicated, 2);
  pump(clock, 1_000);
  assert.equal(sourceState(scheduler, 'I').stats.quotaSpent, 1);
});

test('idempotency: duplicate of a queued task keeps a single queue entry', () => {
  const { scheduler } = idemScheduler();
  // no worker attached: tasks pile up in the queue (source cap 2, global 3,
  // but nothing completes, so the first two stay running)
  scheduler.start();
  scheduler.submit({ sourceId: 'I', id: 'y1' });
  scheduler.submit({ sourceId: 'I', id: 'y2' });
  scheduler.submit({ sourceId: 'I', id: 'y3' }); // queued behind the two running
  const dup = scheduler.submit({ sourceId: 'I', id: 'y3' });
  assert.equal(dup.deduplicated, true);
  assert.equal(scheduler.getState().queue.filter((t) => t.id === 'y3').length, 1);
  assert.equal(sourceState(scheduler, 'I').stats.submitted, 3);
});

test('idempotency: a cancelled task id can be resubmitted as a fresh task', () => {
  const { scheduler } = idemScheduler();
  scheduler.start();

  scheduler.submit({ sourceId: 'I', id: 'z' }); // no worker yet: stays running
  scheduler.cancel('z');
  assert.equal(scheduler.statusOf('z'), 'cancelled');

  autoResolve(scheduler);
  const again = scheduler.submit({ sourceId: 'I', id: 'z' });
  assert.equal(again.deduplicated, false, 'cancelled tasks are excluded from dedupe');
  assert.equal(scheduler.statusOf('z'), 'succeeded');
  const stats = sourceState(scheduler, 'I').stats;
  assert.equal(stats.cancelled, 1);
  assert.equal(stats.quotaRefunded, 1, 'cancel refunded the first attempt');
  assert.equal(stats.quotaSpent, 2, 'the fresh task spent its own token');
});
