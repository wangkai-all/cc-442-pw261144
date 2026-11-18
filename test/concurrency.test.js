import assert from 'node:assert/strict';
import { test } from 'node:test';
import { BIG_QUOTA, makeScheduler, pump, sourceState } from './helpers.js';

function trackedWorker(scheduler, clock, durationMs, counter) {
  scheduler.on('task-started', (task) => {
    counter.current++;
    counter.max = Math.max(counter.max, counter.current);
    clock.setTimeout(() => {
      counter.current--;
      scheduler.complete(task.id);
    }, durationMs);
  });
}

test('concurrency: global cap is never exceeded', () => {
  const { clock, scheduler } = makeScheduler({ maxConcurrency: 2 });
  scheduler.registerSource({
    id: 'S', priority: 1, minShare: 1, maxConcurrency: 10, cooldownMs: 0,
    quota: BIG_QUOTA, backoff: { baseMs: 100, factor: 2, maxMs: 1_000 },
  });
  const counter = { current: 0, max: 0 };
  trackedWorker(scheduler, clock, 1_000, counter);
  scheduler.start();

  for (let i = 0; i < 6; i++) scheduler.submit({ sourceId: 'S', id: `s-${i}` });
  pump(clock, 20_000);

  assert.equal(counter.max, 2, 'global concurrency cap must hold');
  assert.equal(sourceState(scheduler, 'S').stats.succeeded, 6);
});

test('concurrency: per-source cap is never exceeded even with spare global capacity', () => {
  const { clock, scheduler } = makeScheduler({ maxConcurrency: 5 });
  scheduler.registerSource({
    id: 'S', priority: 1, minShare: 1, maxConcurrency: 1, cooldownMs: 0,
    quota: BIG_QUOTA, backoff: { baseMs: 100, factor: 2, maxMs: 1_000 },
  });
  const counter = { current: 0, max: 0 };
  trackedWorker(scheduler, clock, 500, counter);
  scheduler.start();

  for (let i = 0; i < 4; i++) scheduler.submit({ sourceId: 'S', id: `s-${i}` });
  pump(clock, 10_000);

  assert.equal(counter.max, 1, 'per-source concurrency cap must hold');
  assert.equal(sourceState(scheduler, 'S').stats.succeeded, 4);
});
