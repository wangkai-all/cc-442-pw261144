import assert from 'node:assert/strict';
import { test } from 'node:test';
import { autoResolve, BIG_QUOTA, makeScheduler, pump, sourceState } from './helpers.js';

test('fairness: low-priority sources still run while a high-priority source keeps failing', () => {
  const { clock, scheduler } = makeScheduler({ maxConcurrency: 3 });
  scheduler.registerSource({
    id: 'A', priority: 100, minShare: 1, maxConcurrency: 3, cooldownMs: 0,
    quota: BIG_QUOTA, backoff: { baseMs: 1_000, factor: 2, maxMs: 8_000 },
  });
  scheduler.registerSource({
    id: 'B', priority: 10, minShare: 1, maxConcurrency: 1, cooldownMs: 0,
    quota: BIG_QUOTA, backoff: { baseMs: 1_000, factor: 2, maxMs: 8_000 },
  });
  scheduler.registerSource({
    id: 'C', priority: 1, minShare: 1, maxConcurrency: 1, cooldownMs: 0,
    quota: BIG_QUOTA, backoff: { baseMs: 1_000, factor: 2, maxMs: 8_000 },
  });
  autoResolve(scheduler, (task) => (task.sourceId === 'A' ? new Error('A is down') : null));
  scheduler.start();

  for (let i = 0; i < 30; i++) scheduler.submit({ sourceId: 'A', id: `a-${i}` });
  for (let i = 0; i < 5; i++) scheduler.submit({ sourceId: 'B', id: `b-${i}` });
  for (let i = 0; i < 5; i++) scheduler.submit({ sourceId: 'C', id: `c-${i}` });

  pump(clock, 60_000);

  const a = sourceState(scheduler, 'A');
  const b = sourceState(scheduler, 'B');
  const c = sourceState(scheduler, 'C');
  // A keeps failing and backs off...
  assert.ok(a.stats.failed >= 5, `expected A to fail repeatedly, got ${a.stats.failed}`);
  assert.ok(a.backoff.failures > 0, 'A should be in backoff');
  // ...but B and C still got their minimum execution opportunity and finished everything
  assert.equal(b.stats.succeeded, 5);
  assert.equal(c.stats.succeeded, 5);
  assert.equal(b.stats.failed, 0);
  assert.equal(c.stats.failed, 0);
});

test('fairness: minShare floor is reserved even when a high-priority source floods the queue', () => {
  const { clock, scheduler } = makeScheduler({ maxConcurrency: 3 });
  scheduler.registerSource({
    id: 'A', priority: 100, minShare: 1, maxConcurrency: 3, cooldownMs: 0,
    quota: BIG_QUOTA, backoff: { baseMs: 1_000, factor: 2, maxMs: 8_000 },
  });
  scheduler.registerSource({
    id: 'B', priority: 10, minShare: 1, maxConcurrency: 1, cooldownMs: 0,
    quota: BIG_QUOTA, backoff: { baseMs: 1_000, factor: 2, maxMs: 8_000 },
  });
  // A's tasks occupy their slot for a long time; B completes instantly.
  scheduler.on('task-started', (task) => {
    if (task.sourceId === 'A') {
      clock.setTimeout(() => scheduler.complete(task.id), 120_000);
    } else {
      scheduler.complete(task.id);
    }
  });
  scheduler.start();

  for (let i = 0; i < 20; i++) scheduler.submit({ sourceId: 'A', id: `a-${i}` });
  for (let i = 0; i < 3; i++) scheduler.submit({ sourceId: 'B', id: `b-${i}` });

  pump(clock, 5_000); // long before A's tasks finish

  const a = sourceState(scheduler, 'A');
  const b = sourceState(scheduler, 'B');
  assert.equal(b.stats.succeeded, 3, 'B must get its guaranteed slot despite A flooding');
  assert.ok(a.running >= 1, 'A should be occupying slots');
});

test('fairness: without minShare demand, a flooding priority source can use spare capacity', () => {
  const { clock, scheduler } = makeScheduler({ maxConcurrency: 2 });
  scheduler.registerSource({
    id: 'A', priority: 100, minShare: 1, maxConcurrency: 2, cooldownMs: 0,
    quota: BIG_QUOTA, backoff: { baseMs: 1_000, factor: 2, maxMs: 8_000 },
  });
  scheduler.registerSource({
    id: 'B', priority: 10, minShare: 1, maxConcurrency: 1, cooldownMs: 0,
    quota: BIG_QUOTA, backoff: { baseMs: 1_000, factor: 2, maxMs: 8_000 },
  });
  autoResolve(scheduler);
  scheduler.start();

  for (let i = 0; i < 10; i++) scheduler.submit({ sourceId: 'A', id: `a-${i}` });
  pump(clock, 1_000);
  assert.equal(sourceState(scheduler, 'A').stats.succeeded, 10);
  assert.equal(sourceState(scheduler, 'B').stats.started, 0);
});
