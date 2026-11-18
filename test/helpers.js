import { SimulatedClock } from '../src/clock.js';
import { Scheduler } from '../src/scheduler.js';

export const T0 = 1_000_000;

export function makeScheduler(opts = {}) {
  const clock = new SimulatedClock(T0);
  const scheduler = new Scheduler({ clock, maxConcurrency: 3, ...opts });
  return { clock, scheduler };
}

/** Complete every dispatched task synchronously; `fn(task)` may return an Error to fail it. */
export function autoResolve(scheduler, fn = () => null) {
  scheduler.on('task-started', (task) => {
    const err = fn(task);
    scheduler.complete(task.id, err ?? undefined);
  });
}

/** Advance the clock in steps so timers scheduled mid-flight get to run. */
export function pump(clock, ms, step = 50) {
  let remaining = ms;
  while (remaining > 0) {
    const d = Math.min(step, remaining);
    clock.advance(d);
    remaining -= d;
  }
}

export function sourceState(scheduler, id) {
  return scheduler.getState().sources.find((s) => s.id === id);
}

export const BIG_QUOTA = { capacity: 10_000, refillMs: 1_000 };
