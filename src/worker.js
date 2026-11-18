/**
 * Simulated fetch worker for the demo server. Each dispatched task completes
 * after a random duration drawn from the source's `demo` config, failing with
 * probability `demo.failureRate`. Cancelled tasks are abandoned quietly.
 */
export function attachWorker(scheduler, clock, rng = Math.random) {
  scheduler.on('task-started', (task) => {
    const source = scheduler.getSource(task.sourceId);
    const demo = source?.config.demo ?? { minMs: 500, maxMs: 2_000, failureRate: 0.2 };
    const duration = Math.round(demo.minMs + rng() * Math.max(0, demo.maxMs - demo.minMs));
    clock.setTimeout(() => {
      if (scheduler.statusOf(task.id) !== 'running') return; // cancelled meanwhile
      if (rng() < demo.failureRate) {
        scheduler.complete(task.id, new Error('simulated fetch failure'));
      } else {
        scheduler.complete(task.id);
      }
    }, duration);
  });
}
