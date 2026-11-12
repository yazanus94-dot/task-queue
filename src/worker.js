/**
 * Task Queue - Worker Pool
 * Manages a pool of worker threads for parallel task processing.
 */

export class WorkerPool {
    constructor(size = navigator.hardwareConcurrency || 4) {
        this.size = size;
        this.workers = [];
        this.taskQueue = [];
        this.activeCount = 0;
    }

    /**
     * Add a worker to the pool.
     * @param {Function} workerFn - The worker function
     */
    addWorker(workerFn) {
        const worker = {
            fn: workerFn,
            busy: false,
            id: this.workers.length
        };
        this.workers.push(worker);
    }

    /**
     * Assign a task to an available worker.
     * @param {Object} task - The task to execute
     * @returns {Promise} A promise that resolves when the task is complete
     */
    async execute(task) {
        return new Promise((resolve, reject) => {
            const runTask = async () => {
                const worker = this.workers.find(w => !w.busy);
                if (!worker) {
                    this.taskQueue.push({ task, resolve, reject });
                    return;
                }

                worker.busy = true;
                this.activeCount++;

                try {
                    const result = await worker.fn(task.data);
                    resolve(result);
                } catch (error) {
                    reject(error);
                } finally {
                    worker.busy = false;
                    this.activeCount--;
                    this.processQueue();
                }
            };

            runTask();
        });
    }

    /**
     * Process queued tasks when workers become available.
     */
    processQueue() {
        while (this.taskQueue.length > 0) {
            const worker = this.workers.find(w => !w.busy);
            if (!worker) break;

            const { task, resolve, reject } = this.taskQueue.shift();
            this.execute(task).then(resolve).catch(reject);
        }
    }

    /**
     * Get pool statistics.
     * @returns {Object} Pool statistics
     */
    getStats() {
        return {
            totalWorkers: this.workers.length,
            activeWorkers: this.activeCount,
            queuedTasks: this.taskQueue.length,
            availableWorkers: this.workers.filter(w => !w.busy).length
        };
    }
}

export default WorkerPool;