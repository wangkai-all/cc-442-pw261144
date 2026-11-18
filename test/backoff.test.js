import assert from 'node:assert/strict';
import { test } from 'node:test';
import { BIG_QUOTA, makeScheduler, pump, sourceState, T0 } from './helpers.js';

function failingSource(scheduler) {
  scheduler.registerSource({
    id: 'F', priority: 1, minShare: 1, maxConcurrency: 1, cooldownMs: 0,
    quota: BIG_QUOTA, backoff: { baseMs: 1_000, factor: 2, maxMs: 4_000 },
  });
}

test('backoff: consecutive failures pause the source exponentially, capped at maxMs', () => {
  const { clock, scheduler } = makeScheduler();
  failingSource(scheduler);
  autoFailAll(scheduler);
  scheduler.start();

  scheduler.submit({ sourceId: 'F', id: 'f1' });
  // f1 fails immediately at T0 -> backoff 1000ms
  assert.equal(sourceState(scheduler, 'F').backoff.failures, 1);
  assert.equal(sourceState(scheduler, 'F').backoff.remainingMs, 1_000);

  scheduler.submit({ sourceId: 'F', id: 'f2' });
  pump(clock, 999);
  assert.equal(scheduler.statusOf('f2'), 'queued', 'source must stay paused during backoff');
  pump(clock, 1);
  // f2 ran at T0+1000 and failed -> backoff 2000ms from then
  assert.equal(scheduler.statusOf('f2'), 'failed');
  assert.equal(sourceState(scheduler, 'F').backoff.failures, 2);
  assert.equal(sourceState(scheduler, 'F').backoff.remainingMs, 2_000);

  scheduler.submit({ sourceId: 'F', id: 'f3' });
  pump(clock, 2_000);
  // f3 failed at T0+3000 -> backoff 4000ms
  assert.equal(sourceState(scheduler, 'F').backoff.failures, 3);
  assert.equal(sourceState(scheduler, 'F').backoff.remainingMs, 4_000);

  scheduler.submit({ sourceId: 'F', id: 'f4' });
  pump(clock, 4_000);
  // f4 failed at T0+7000 -> would be 8000ms, capped at maxMs=4000
  assert.equal(sourceState(scheduler, 'F').backoff.failures, 4);
  assert.equal(sourceState(scheduler, 'F').backoff.remainingMs, 4_000);
});

test('backoff: a success resets the failure counter and the source recovers', () => {
  const { clock, scheduler } = makeScheduler();
  failingSource(scheduler);
  let shouldFail = true;
  scheduler.on('task-started', (task) => {
    scheduler.complete(task.id, shouldFail ? new Error('boom') : undefined);
  });
  scheduler.start();

  scheduler.submit({ sourceId: 'F', id: 'f1' });
  scheduler.submit({ sourceId: 'F', id: 'f2' });
  pump(clock, 1_000); // f1 failed at T0, f2 fails at T0+1000 -> failures = 2
  assert.equal(sourceState(scheduler, 'F').backoff.failures, 2);

  shouldFail = false;
  scheduler.submit({ sourceId: 'F', id: 'ok1' });
  pump(clock, 2_000); // backoff 2000ms elapses, ok1 runs and succeeds
  assert.equal(scheduler.statusOf('ok1'), 'succeeded');
  assert.equal(sourceState(scheduler, 'F').backoff.failures, 0);
  assert.equal(sourceState(scheduler, 'F').backoff.remainingMs, 0);

  // recovered: the next task is dispatched immediately, no backoff delay
  scheduler.submit({ sourceId: 'F', id: 'ok2' });
  assert.equal(scheduler.statusOf('ok2'), 'succeeded');
});

test('backoff: backoffUntil timestamp is absolute and survives inspection', () => {
  const { scheduler } = makeScheduler();
  failingSource(scheduler);
  autoFailAll(scheduler);
  scheduler.start();
  scheduler.submit({ sourceId: 'F', id: 'f1' });
  const state = sourceState(scheduler, 'F');
  assert.equal(state.backoff.until, T0 + 1_000);
});

function autoFailAll(scheduler) {
  scheduler.on('task-started', (task) => scheduler.complete(task.id, new Error('boom')));
}
