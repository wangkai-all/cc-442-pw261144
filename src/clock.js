/**
 * Clock abstraction. The scheduler only ever reads time and schedules timers
 * through this interface, so tests can drive it with a simulated clock.
 */
export class SystemClock {
  now() {
    return Date.now();
  }
  setTimeout(fn, ms) {
    return setTimeout(fn, ms);
  }
  clearTimeout(id) {
    clearTimeout(id);
  }
}

/**
 * Deterministic clock for tests. Timers fire only when advance() moves time
 * past their scheduled instant. Timers scheduled while advancing (e.g. from
 * inside another timer's callback) fire within the same advance() call if
 * their instant is still <= the target.
 */
export class SimulatedClock {
  constructor(start = 0) {
    this.t = start;
    this.timers = new Map();
    this.nextId = 1;
  }
  now() {
    return this.t;
  }
  setTimeout(fn, ms) {
    const id = this.nextId++;
    this.timers.set(id, { at: this.t + Math.max(0, ms), fn });
    return id;
  }
  clearTimeout(id) {
    this.timers.delete(id);
  }
  advance(ms) {
    const target = this.t + ms;
    for (;;) {
      let nextId = null;
      let nextAt = Infinity;
      let nextFn = null;
      for (const [id, timer] of this.timers) {
        if (timer.at <= target && timer.at < nextAt) {
          nextAt = timer.at;
          nextId = id;
          nextFn = timer.fn;
        }
      }
      if (nextId === null) break;
      this.timers.delete(nextId);
      this.t = nextAt;
      nextFn();
    }
    this.t = target;
  }
}
