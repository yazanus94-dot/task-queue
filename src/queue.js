/**
 * Task Queue - Core Queue Implementation
 * A distributed task queue with retry logic, dead letter queues, and worker pooling.
 */

export class TaskQueue {
    constructor(options = {}) {
        this.name = options.name || 'default';
        this.maxRetries = options.maxRetries ?? 3;
        this.retryDelay = options.retryDelay ?? 1000;
        this.backoff = options.backoff ?? 'exponential';
        this.processors = new Map();
        this.queue = [];
        this.deadLetterQueue = [];
        this.running = false;
        this.processing = false;
        this.stats = {
            processed: 0,
            failed: 0,
            retried: 0,
            deadLettered: 0
        };
    }

    /**
     * Register a task processor for a specific task type.
     * @param {string} type - The task type identifier
     * @param {Function} processor - Async function that processes the task
     * @param {Object} options - Processor options
     */
    register(type, processor, options = {}) {
        this.processors.set(type, { processor, options });
    }

    /**
     * Add a task to the queue.
     * @param {Object} task - The task to enqueue
     * @returns {string} The task ID
     */
    async enqueue(task) {
        const queuedTask = {
            id: task.id || this.generateId(),
            type: task.type,
            data: task.data,
            priority: task.priority || 0,
            attempts: 0,
            maxAttempts: task.maxAttempts ?? this.maxRetries,
            createdAt: Date.now(),
            scheduledAt: task.scheduledAt || Date.now(),
            status: 'pending'
        };

        if (task.scheduledAt && task.scheduledAt > Date.now()) {
            this.queue.push({ ...queuedTask, status: 'scheduled' });
        } else {
            this.queue.push(queuedTask);
        }

        this.sortQueue();
        this.process();
        return queuedTask.id;
    }

    /**
     * Start processing the queue.
     */
    start() {
        this.running = true;
        this.process();
    }

    /**
     * Stop processing the queue.
     */
    stop() {
        this.running = false;
    }

    /**
     * Process tasks in the queue.
     */
    async process() {
        if (this.processing || !this.running) return;
        this.processing = true;

        try {
            while (this.running && this.queue.length > 0) {
                const task = this.queue.find(t => 
                    t.status === 'pending' || 
                    (t.status === 'scheduled' && t.scheduledAt <= Date.now())
                );

                if (!task) break;

                await this.executeTask(task);
            }
        } finally {
            this.processing = false;
        }
    }

    /**
     * Execute a single task with retry logic.
     * @param {Object} task - The task to execute
     */
    async executeTask(task) {
        const processor = this.processors.get(task.type);
        if (!processor) {
            task.status = 'dead_letter';
            this.deadLetterQueue.push(task);
            this.stats.deadLettered++;
            this.queue = this.queue.filter(t => t.id !== task.id);
            return;
        }

        task.status = 'processing';
        
        try {
            const result = await processor.processor(task.data);
            task.status = 'completed';
            task.completedAt = Date.now();
            task.result = result;
            this.stats.processed++;
        } catch (error) {
            task.attempts++;
            task.lastError = error.message;

            if (task.attempts >= task.maxAttempts) {
                task.status = 'dead_letter';
                this.deadLetterQueue.push(task);
                this.stats.deadLettered++;
            } else {
                task.status = 'pending';
                task.scheduledAt = Date.now() + this.getRetryDelay(task.attempts);
                this.stats.retried++;
            }
        }

        this.queue = this.queue.filter(t => t.id !== task.id);
    }

    /**
     * Calculate retry delay based on backoff strategy.
     * @param {number} attempt - The current attempt number
     * @returns {number} The delay in milliseconds
     */
    getRetryDelay(attempt) {
        if (this.backoff === 'exponential') {
            return this.retryDelay * Math.pow(2, attempt - 1);
        } else if (this.backoff === 'linear') {
            return this.retryDelay * attempt;
        }
        return this.retryDelay;
    }

    /**
     * Sort queue by priority (higher first).
     */
    sortQueue() {
        this.queue.sort((a, b) => b.priority - a.priority);
    }

    /**
     * Generate a unique task ID.
     * @returns {string} A unique identifier
     */
    generateId() {
        return `task_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    }

    /**
     * Get queue statistics.
     * @returns {Object} Current queue statistics
     */
    getStats() {
        return {
            ...this.stats,
            pending: this.queue.filter(t => t.status === 'pending').length,
            processing: this.queue.filter(t => t.status === 'processing').length,
            deadLettered: this.deadLetterQueue.length
        };
    }

    /**
     * Get all dead letter tasks.
     * @returns {Array} Dead letter queue tasks
     */
    getDeadLetterQueue() {
        return [...this.deadLetterQueue];
    }

    /**
     * Retry a dead letter task.
     * @param {string} taskId - The task ID to retry
     */
    retryDeadLetter(taskId) {
        const task = this.deadLetterQueue.find(t => t.id === taskId);
        if (task) {
            task.status = 'pending';
            task.attempts = 0;
            task.scheduledAt = Date.now();
            this.queue.push(task);
            this.deadLetterQueue = this.deadLetterQueue.filter(t => t.id !== taskId);
            this.process();
            return true;
        }
        return false;
    }
}

export default TaskQueue;