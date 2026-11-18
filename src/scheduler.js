import { EventEmitter } from 'node:events';

const TERMINAL_KEEP = 200; // terminal tasks kept in memory (dedupe + dashboard)
const EVENTS_KEEP = 120; // event log ring buffer

const DEFAULTS = {
  priority: 0,
  quota: { capacity: 10, refillMs: 60_000 }, // token bucket: `capacity` tokens per `refillMs`
  cooldownMs: 1_000, // min gap between two dispatches of the same source
  maxConcurrency: 1, // per-source in-flight cap
  minShare: 1, // fairness floor: guaranteed in-flight slots when the source has demand
  backoff: { baseMs: 1_000, factor: 2, maxMs: 60_000 },
  demo: { minMs: 500, maxMs: 2_000, failureRate: 0.2 }, // used by the demo worker only
};

/**
 * Multi-source collection scheduler.
 *
 * - Quota: per-source token bucket, one token spent per dispatch (never on
 *   submit). Restored across restarts via the store snapshot.
 * - Fairness: each source's `minShare` of the global concurrency is reserved
 *   — other sources cannot overflow into it. Sources below their floor are
 *   served first (longest-waiting task wins); spare capacity beyond the
 *   reserved slots goes to the highest priority source. A flooding / failing
 *   high-priority source therefore cannot starve the others.
 * - Cooldown: per-source minimum interval between dispatches.
 * - Backoff: consecutive failures pause a source for base * factor^(n-1),
 *   capped at max; any success resets the counter.
 * - Idempotency: task ids are unique. Re-submitting an existing id returns
 *   the existing task and never spends quota again. Cancelled tasks are
 *   excluded from dedupe so they can be resubmitted.
 * - Recovery: state is snapshotted on every mutation. On restart, quota
 *   consumption / backoff / queue are restored; tasks that were in flight
 *   are requeued and flagged `recovered`.
 */
export class Scheduler extends EventEmitter {
  constructor({ clock, store = null, maxConcurrency = 4 } = {}) {
    super();
    if (!clock) throw new Error('Scheduler requires a clock');
    this.clock = clock;
    this.store = store;
    this.maxConcurrency = maxConcurrency;

    this.sources = new Map(); // id -> { config, tokens, lastRefillAt, lastDispatchAt, consecutiveFailures, backoffUntil, running, stats }
    this.queue = []; // queued tasks, FIFO
    this.running = new Map(); // taskId -> task
    this.tasks = new Map(); // taskId -> task (queued + running + recent terminal)
    this.terminalOrder = []; // LRU of terminal task ids
    this.events = [];
    this.seq = 0;

    this._timer = null;
    this._stopped = true;
    this._dispatching = false;
    this._dispatchAgain = false;

    this._restore();
  }

  // ---------------------------------------------------------------- lifecycle

  start() {
    if (!this._stopped) return;
    this._stopped = false;
    this._requestDispatch();
  }

  dispose() {
    this._stopped = true;
    if (this._timer) {
      this.clock.clearTimeout(this._timer);
      this._timer = null;
    }
    this._persist();
  }

  // ----------------------------------------------------------------- sources

  registerSource(config) {
    if (!config || !config.id) throw new Error('source config requires an id');
    const id = config.id;
    const merged = {
      id,
      name: config.name ?? id,
      priority: config.priority ?? DEFAULTS.priority,
      quota: { ...DEFAULTS.quota, ...(config.quota ?? {}) },
      cooldownMs: config.cooldownMs ?? DEFAULTS.cooldownMs,
      maxConcurrency: config.maxConcurrency ?? DEFAULTS.maxConcurrency,
      minShare: config.minShare ?? DEFAULTS.minShare,
      backoff: { ...DEFAULTS.backoff, ...(config.backoff ?? {}) },
      demo: { ...DEFAULTS.demo, ...(config.demo ?? {}) },
    };
    if (merged.quota.capacity <= 0) throw new Error('quota.capacity must be > 0');
    const existing = this.sources.get(id);
    if (existing) {
      existing.config = merged; // dynamic state (tokens, backoff, stats) survives config updates
      this._log('source', `source "${id}" config updated`);
    } else {
      this.sources.set(id, {
        config: merged,
        tokens: merged.quota.capacity,
        lastRefillAt: this.clock.now(),
        lastDispatchAt: null,
        consecutiveFailures: 0,
        backoffUntil: 0,
        running: 0,
        stats: {
          submitted: 0, deduplicated: 0, started: 0, succeeded: 0,
          failed: 0, cancelled: 0, quotaSpent: 0, quotaRefunded: 0,
        },
      });
      this._log('source', `source "${id}" registered (priority ${merged.priority}, quota ${merged.quota.capacity}/${merged.quota.refillMs}ms)`);
    }
    this._persist();
    this._requestDispatch();
    return this.sources.get(id);
  }

  getSource(id) {
    return this.sources.get(id) ?? null;
  }

  // ------------------------------------------------------------------- tasks

  /**
   * Submit a collection task. `id` is the idempotency key: submitting an id
   * that is already queued / running / recently terminal returns the existing
   * task with `deduplicated: true` and spends no quota.
   */
  submit({ id, sourceId, payload = null }) {
    const source = this.sources.get(sourceId);
    if (!source) throw new Error(`unknown source "${sourceId}"`);
    const taskId = id ?? `t-${++this.seq}`;
    const existing = this.tasks.get(taskId);
    if (existing && existing.status !== 'cancelled') {
      source.stats.deduplicated++;
      this._log('dedupe', `duplicate submission of "${taskId}" ignored (status: ${existing.status})`);
      this._persist();
      return { task: existing, deduplicated: true };
    }
    const task = {
      id: taskId,
      sourceId,
      payload,
      status: 'queued',
      createdAt: this.clock.now(),
      startedAt: null,
      finishedAt: null,
      attempts: 0,
      lastError: null,
      quotaRefunded: false,
      recovered: null,
    };
    this.tasks.set(taskId, task);
    this.queue.push(task);
    source.stats.submitted++;
    this._log('submit', `task "${taskId}" queued for "${sourceId}"`);
    this._persist();
    this._requestDispatch();
    return { task, deduplicated: false };
  }

  /**
   * Cancel a queued or running task. Cancelling a running task refunds its
   * quota token (the fetch is abandoned before consuming the remote quota)
   * and is failure-neutral: backoff counters are untouched.
   */
  cancel(taskId, reason = 'cancelled by user') {
    const task = this.tasks.get(taskId);
    if (!task) return null;
    if (task.status !== 'queued' && task.status !== 'running') return task;

    const source = this.sources.get(task.sourceId);
    const wasRunning = task.status === 'running';
    if (wasRunning) {
      this.running.delete(taskId);
      source.running--;
      source.tokens = Math.min(source.config.quota.capacity, source.tokens + 1);
      source.stats.quotaRefunded++;
      task.quotaRefunded = true;
    } else {
      const idx = this.queue.findIndex((t) => t.id === taskId);
      if (idx >= 0) this.queue.splice(idx, 1);
    }
    task.status = 'cancelled';
    task.finishedAt = this.clock.now();
    task.lastError = reason;
    source.stats.cancelled++;
    this._markTerminal(task);
    this._log('cancel', `task "${taskId}" cancelled (${wasRunning ? 'running, quota refunded' : 'queued'})`);
    this._persist();
    this._requestDispatch();
    return task;
  }

  /** Report the outcome of a running task. Unknown / non-running ids are ignored. */
  complete(taskId, error = null) {
    const task = this.running.get(taskId);
    if (!task) return false;
    const source = this.sources.get(task.sourceId);
    this.running.delete(taskId);
    source.running--;
    task.finishedAt = this.clock.now();
    if (error) {
      task.status = 'failed';
      task.lastError = String(error.message ?? error);
      source.consecutiveFailures++;
      source.stats.failed++;
      const { baseMs, factor, maxMs } = source.config.backoff;
      const delay = Math.min(maxMs, baseMs * factor ** (source.consecutiveFailures - 1));
      source.backoffUntil = this.clock.now() + delay;
      this._log('fail', `task "${taskId}" failed (${task.lastError}); source "${source.config.id}" backing off ${delay}ms (failure #${source.consecutiveFailures})`);
    } else {
      task.status = 'succeeded';
      if (source.consecutiveFailures > 0) {
        this._log('recover', `source "${source.config.id}" recovered after ${source.consecutiveFailures} failure(s)`);
      }
      source.consecutiveFailures = 0;
      source.backoffUntil = 0;
      source.stats.succeeded++;
    }
    this._markTerminal(task);
    this._persist();
    this._requestDispatch();
    return true;
  }

  statusOf(taskId) {
    return this.tasks.get(taskId)?.status ?? null;
  }

  // ------------------------------------------------------------ introspection

  getState() {
    const now = this.clock.now();
    const taskView = (t) => ({ ...t });
    const sources = [...this.sources.values()].map((s) => ({
      id: s.config.id,
      name: s.config.name,
      priority: s.config.priority,
      minShare: s.config.minShare,
      maxConcurrency: s.config.maxConcurrency,
      cooldownMs: s.config.cooldownMs,
      config: { ...s.config, quota: { ...s.config.quota }, backoff: { ...s.config.backoff }, demo: { ...s.config.demo } },
      cooldownRemainingMs:
        s.lastDispatchAt === null ? 0 : Math.max(0, s.lastDispatchAt + s.config.cooldownMs - now),
      quota: {
        capacity: s.config.quota.capacity,
        refillMs: s.config.quota.refillMs,
        tokens: Math.floor(s.tokens * 100) / 100,
      },
      backoff: {
        failures: s.consecutiveFailures,
        until: s.backoffUntil,
        remainingMs: Math.max(0, s.backoffUntil - now),
      },
      running: s.running,
      queued: this.queue.reduce((n, t) => n + (t.sourceId === s.config.id ? 1 : 0), 0),
      stats: { ...s.stats },
      demo: { ...s.config.demo },
    }));
    return {
      now,
      maxConcurrency: this.maxConcurrency,
      runningTotal: this.running.size,
      queuedTotal: this.queue.length,
      sources,
      queue: this.queue.map(taskView),
      running: [...this.running.values()].map(taskView),
      recent: this.terminalOrder
        .slice(-30)
        .reverse()
        .map((id) => this.tasks.get(id))
        .filter(Boolean)
        .map(taskView),
      events: this.events.slice(-30).reverse(),
    };
  }

  // --------------------------------------------------------------- internals

  _log(type, message) {
    this.events.push({ t: this.clock.now(), type, message });
    if (this.events.length > EVENTS_KEEP) this.events.splice(0, this.events.length - EVENTS_KEEP);
  }

  _refill(s, now) {
    const { capacity, refillMs } = s.config.quota;
    if (refillMs <= 0) {
      s.tokens = capacity;
      s.lastRefillAt = now;
      return;
    }
    const elapsed = now - s.lastRefillAt;
    if (elapsed <= 0) return;
    s.tokens = Math.min(capacity, s.tokens + (elapsed / refillMs) * capacity);
    s.lastRefillAt = now;
  }

  _eligible(s, now) {
    return (
      s.running < s.config.maxConcurrency &&
      s.tokens >= 1 &&
      s.backoffUntil <= now &&
      (s.lastDispatchAt === null || s.lastDispatchAt + s.config.cooldownMs <= now)
    );
  }

  _requestDispatch() {
    if (this._stopped) return;
    if (this._dispatching) {
      this._dispatchAgain = true;
      return;
    }
    this._dispatching = true;
    try {
      do {
        this._dispatchAgain = false;
        this._dispatch();
      } while (this._dispatchAgain);
    } finally {
      this._dispatching = false;
    }
    // Dispatching mutates quota/queue/running — persist so a crash cannot
    // resurrect already-spent quota or lose track of in-flight tasks.
    this._persist();
    this._armTimer();
  }

  _dispatch() {
    const now = this.clock.now();
    for (const s of this.sources.values()) this._refill(s, now);
    while (this.running.size < this.maxConcurrency) {
      const source = this._pickSource(now);
      if (!source) break;
      // Fairness reservation: a source already at its minShare floor may only
      // overflow into slots that are not reserved for other sources' floors.
      if (source.running >= source.config.minShare && !this._mayOverflow(source)) break;
      this._startNext(source, now);
    }
  }

  /**
   * May `source` take a slot beyond its floor right now? Other sources'
   * minShare slots stay reserved, so a flooding source can never occupy the
   * slots that guarantee everyone else's minimum execution opportunity.
   */
  _mayOverflow(source) {
    let reserved = 0;
    for (const s of this.sources.values()) {
      if (s !== source) reserved += s.config.minShare;
    }
    return this.running.size < this.maxConcurrency - reserved;
  }

  /**
   * Pick the next source to dispatch from.
   * Pass 1 (fairness floor): sources below their minShare of in-flight slots,
   *   longest-waiting task first.
   * Pass 2 (priority): highest priority first, ties broken by longest wait.
   */
  _pickSource(now) {
    const candidates = [];
    const seen = new Set();
    for (const task of this.queue) {
      if (seen.has(task.sourceId)) continue;
      seen.add(task.sourceId);
      const s = this.sources.get(task.sourceId);
      if (!s || !this._eligible(s, now)) continue;
      candidates.push({ s, oldest: task.createdAt });
    }
    if (candidates.length === 0) return null;
    const floor = candidates.filter((c) => c.s.running < c.s.config.minShare);
    if (floor.length > 0) {
      floor.sort((a, b) => a.oldest - b.oldest);
      return floor[0].s;
    }
    candidates.sort((a, b) =>
      b.s.config.priority !== a.s.config.priority
        ? b.s.config.priority - a.s.config.priority
        : a.oldest - b.oldest,
    );
    return candidates[0].s;
  }

  _startNext(source, now) {
    const idx = this.queue.findIndex((t) => t.sourceId === source.config.id);
    if (idx < 0) return;
    const task = this.queue.splice(idx, 1)[0];
    source.tokens -= 1;
    source.stats.quotaSpent++;
    source.stats.started++;
    source.lastDispatchAt = now;
    source.running++;
    task.status = 'running';
    task.startedAt = now;
    task.attempts++;
    this.running.set(task.id, task);
    this._log('dispatch', `task "${task.id}" dispatched to "${source.config.id}" (attempt ${task.attempts})`);
    this.emit('task-started', task);
  }

  /** Arm a one-shot timer for the next moment a blocked source may become eligible. */
  _armTimer() {
    if (this._timer) {
      this.clock.clearTimeout(this._timer);
      this._timer = null;
    }
    if (this._stopped) return;
    const delay = this._nextWakeDelay();
    if (delay === null) return;
    this._timer = this.clock.setTimeout(() => {
      this._timer = null;
      this._requestDispatch();
    }, delay);
  }

  _nextWakeDelay() {
    if (this.queue.length === 0) return null;
    const now = this.clock.now();
    let next = Infinity;
    const queuedSources = new Set(this.queue.map((t) => t.sourceId));
    for (const id of queuedSources) {
      const s = this.sources.get(id);
      if (!s) continue;
      if (s.running >= s.config.maxConcurrency) continue; // a completion will wake us
      let readyAt = Infinity;
      if (s.backoffUntil > now) readyAt = Math.min(readyAt, s.backoffUntil);
      if (s.lastDispatchAt !== null && s.lastDispatchAt + s.config.cooldownMs > now) {
        readyAt = Math.min(readyAt, s.lastDispatchAt + s.config.cooldownMs);
      }
      if (s.tokens < 1) {
        const { capacity, refillMs } = s.config.quota;
        if (refillMs <= 0) continue;
        readyAt = Math.min(readyAt, s.lastRefillAt + ((1 - s.tokens) / capacity) * refillMs);
      }
      if (readyAt === Infinity) continue; // eligible now but global cap full: a completion will wake us
      next = Math.min(next, readyAt);
    }
    if (next === Infinity) return null;
    return Math.max(0, next - now);
  }

  _markTerminal(task) {
    this.terminalOrder.push(task.id);
    while (this.terminalOrder.length > TERMINAL_KEEP) {
      const oldId = this.terminalOrder.shift();
      const old = this.tasks.get(oldId);
      if (old && old.status !== 'queued' && old.status !== 'running') this.tasks.delete(oldId);
    }
  }

  // -------------------------------------------------------------- persistence

  _persist() {
    if (!this.store) return;
    this.store.save({
      version: 1,
      savedAt: this.clock.now(),
      seq: this.seq,
      maxConcurrency: this.maxConcurrency,
      sources: [...this.sources.values()].map((s) => ({
        config: s.config,
        tokens: s.tokens,
        lastRefillAt: s.lastRefillAt,
        lastDispatchAt: s.lastDispatchAt,
        consecutiveFailures: s.consecutiveFailures,
        backoffUntil: s.backoffUntil,
        stats: s.stats,
      })),
      queue: this.queue,
      running: [...this.running.values()],
      terminal: this.terminalOrder.map((id) => this.tasks.get(id)).filter(Boolean),
      events: this.events.slice(-50),
    });
  }

  _restore() {
    if (!this.store) return;
    const data = this.store.load();
    if (!data || data.version !== 1) return;
    this.seq = data.seq ?? 0;
    for (const saved of data.sources ?? []) {
      this.registerSource(saved.config);
      const s = this.sources.get(saved.config.id);
      s.tokens = saved.tokens;
      s.lastRefillAt = saved.lastRefillAt;
      s.lastDispatchAt = saved.lastDispatchAt;
      s.consecutiveFailures = saved.consecutiveFailures;
      s.backoffUntil = saved.backoffUntil;
      s.stats = { ...s.stats, ...saved.stats };
      s.running = 0;
    }
    for (const t of data.terminal ?? []) {
      this.tasks.set(t.id, t);
      this.terminalOrder.push(t.id);
    }
    for (const t of data.queue ?? []) {
      this.tasks.set(t.id, t);
      this.queue.push(t);
    }
    const recovered = (data.running ?? []).map((t) => ({
      ...t,
      status: 'queued',
      startedAt: null,
      recovered: 'requeued-after-restart',
    }));
    for (const t of recovered) {
      this.tasks.set(t.id, t);
      this.queue.unshift(t);
    }
    this.events = (data.events ?? []).slice(-EVENTS_KEEP);
    if (recovered.length > 0 || this.queue.length > 0) {
      this._log(
        'recovery',
        `state restored: ${this.queue.length} queued (${recovered.length} requeued after restart), quota/backoff preserved`,
      );
    }
  }
}
