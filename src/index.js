/**
 * Task Queue - Public API
 * Re-exports the main TaskQueue class and related utilities.
 */

export { TaskQueue, default } from './queue.js';
export { WorkerPool } from './worker.js';
export { RetryStrategy } from './retry.js';

/**
 * Creates a new TaskQueue instance.
 * @param {Object} options - Queue configuration options
 * @returns {TaskQueue} A new task queue instance
 */
export function createQueue(options) {
    return new TaskQueue(options);
}

export default { TaskQueue, createQueue, WorkerPool, RetryStrategy };