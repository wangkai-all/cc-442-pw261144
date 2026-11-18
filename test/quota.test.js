import assert from 'node:assert/strict';
import { test } from 'node:test';
import { autoResolve, makeScheduler, pump, sourceState } from './helpers.js';

test('quota: token bucket caps dispatches and refills over time', () => {
  const { clock, scheduler } = makeScheduler();
  scheduler.registerSource({
    id: 'Q', priority: 1, minShare: 1, maxConcurrency: 5, cooldownMs: 0,
    quota: { capacity: 2, refillMs: 10_000 }, // 1 token every 5s
    backoff: { baseMs: 100, factor: 2, maxMs: 1_000 },
  });
  autoResolve(scheduler);
  scheduler.start();

  scheduler.submit({ sourceId: 'Q', id: 'q1' });
  scheduler.submit({ sourceId: 'Q', id: 'q2' });
  scheduler.submit({ sourceId: 'Q', id: 'q3' });

  // capacity 2: q1 and q2 run immediately, q3 waits for a refill
  assert.equal(scheduler.statusOf('q1'), 'succeeded');
  assert.equal(scheduler.statusOf('q2'), 'succeeded');
  assert.equal(scheduler.statusOf('q3'), 'queued');
  assert.equal(sourceState(scheduler, 'Q').quota.tokens, 0);

  pump(clock, 4_999);
  assert.equal(scheduler.statusOf('q3'), 'queued', 'no token yet');

  pump(clock, 1); // t=5000: one token refilled
  assert.equal(scheduler.statusOf('q3'), 'succeeded');
  assert.equal(sourceState(scheduler, 'Q').stats.quotaSpent, 3);
});

test('cooldown: dispatches of one source are spaced by cooldownMs', () => {
  const { clock, scheduler } = makeScheduler();
  scheduler.registerSource({
    id: 'C', priority: 1, minShare: 1, maxConcurrency: 5, cooldownMs: 1_000,
    quota: { capacity: 10, refillMs: 60_000 },
    backoff: { baseMs: 100, factor: 2, maxMs: 1_000 },
  });
  autoResolve(scheduler);
  scheduler.start();

  scheduler.submit({ sourceId: 'C', id: 'c1' });
  scheduler.submit({ sourceId: 'C', id: 'c2' });
  scheduler.submit({ sourceId: 'C', id: 'c3' });
  pump(clock, 3_000);

  const t1 = scheduler.tasks.get('c1');
  const t2 = scheduler.tasks.get('c2');
  const t3 = scheduler.tasks.get('c3');
  assert.equal(t1.status, 'succeeded');
  assert.equal(t2.status, 'succeeded');
  assert.equal(t3.status, 'succeeded');
  assert.equal(t2.startedAt - t1.startedAt, 1_000);
  assert.equal(t3.startedAt - t2.startedAt, 1_000);
});

test('quota + cooldown combined: dispatch times follow both constraints', () => {
  const { clock, scheduler } = makeScheduler();
  scheduler.registerSource({
    id: 'QC', priority: 1, minShare: 1, maxConcurrency: 5, cooldownMs: 1_000,
    quota: { capacity: 2, refillMs: 10_000 }, // 1 token / 5s
    backoff: { baseMs: 100, factor: 2, maxMs: 1_000 },
  });
  autoResolve(scheduler);
  scheduler.start();

  scheduler.submit({ sourceId: 'QC', id: 't1' });
  scheduler.submit({ sourceId: 'QC', id: 't2' });
  scheduler.submit({ sourceId: 'QC', id: 't3' });
  pump(clock, 10_000);

  const t1 = scheduler.tasks.get('t1');
  const t2 = scheduler.tasks.get('t2');
  const t3 = scheduler.tasks.get('t3');
  assert.equal(t2.startedAt - t1.startedAt, 1_000, 't2 waits for cooldown');
  assert.equal(t3.startedAt - t1.startedAt, 5_000, 't3 waits for token refill');
});
