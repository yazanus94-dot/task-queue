import { TaskQueue, WorkerPool, RetryStrategy } from '../src/index.js';
import assert from 'node:assert';

// --- TaskQueue Tests ---

const queue = new TaskQueue({ maxRetries: 2, retryDelay: 10 });

queue.register('success', async (data) => `processed: ${data}`);
queue.register('fail', async () => { throw new Error('always fails'); });
queue.register('retry-once', async (data) => {
    if (data.attempt < 2) throw new Error('not yet');
    return `done: ${data.attempt}`;
});

// Test 1: Successful task processing
const test1 = {
    async test() {
        const q = new TaskQueue();
        q.register('test', async (data) => `ok: ${data}`);
        const id = await q.enqueue({ type: 'test', data: 'hello' });
        assert.ok(id, 'should return task id');
        await new Promise(r => setTimeout(r, 50));
        const stats = q.getStats();
        assert.strictEqual(stats.processed, 1, 'should process 1 task');
    }
};

// Test 2: Task retry on failure
const test2 = {
    async test() {
        const q = new TaskQueue({ maxRetries: 3, retryDelay: 5 });
        let attempts = 0;
        q.register('flaky', async () => {
            attempts++;
            if (attempts < 3) throw new Error('fail');
            return 'success';
        });
        await q.enqueue({ type: 'flaky', data: null });
        await new Promise(r => setTimeout(r, 100));
        const stats = q.getStats();
        assert.strictEqual(stats.processed, 1, 'should eventually succeed');
        assert.strictEqual(stats.retried, 2, 'should retry twice');
    }
};

// Test 3: Dead letter queue on max retries exceeded
const test3 = {
    async test() {
        const q = new TaskQueue({ maxRetries: 1, retryDelay: 5 });
        q.register('doomed', async () => { throw new Error('always fails'); });
        await q.enqueue({ type: 'doomed', data: null });
        await new Promise(r => setTimeout(r, 100));
        const dll = q.getDeadLetterQueue();
        assert.strictEqual(dll.length, 1, 'should have 1 dead letter');
        assert.strictEqual(dll[0].attempts, 1, 'should have 1 attempt');
    }
};

// Test 4: Priority ordering
const test4 = {
    async test() {
        const q = new TaskQueue();
        const results = [];
        q.register('task', async (data) => { results.push(data); });
        await q.enqueue({ type: 'task', data: 'low', priority: 1 });
        await q.enqueue({ type: 'task', data: 'high', priority: 10 });
        await q.enqueue({ type: 'task', data: 'medium', priority: 5 });
        await new Promise(r => setTimeout(r, 100));
        assert.deepStrictEqual(results, ['high', 'medium', 'low']);
    }
};

// Test 5: RetryStrategy delays
const test5 = {
    test() {
        const strat = new RetryStrategy({ baseDelay: 100, strategy: 'exponential' });
        assert.strictEqual(strat.getDelay(1), 100);
        assert.strictEqual(strat.getDelay(2), 200);
        assert.strictEqual(strat.getDelay(3), 400);
    }
};

// Test 6: WorkerPool execution
const test6 = {
    async test() {
        const pool = new WorkerPool(2);
        pool.addWorker(async (data) => data * 2);
        pool.addWorker(async (data) => data + 1);
        const r1 = await pool.execute(5);
        const r2 = await pool.execute(10);
        assert.strictEqual(r1, 10);
        assert.strictEqual(r2, 20);
    }
};

// Test 7: Queue stop prevents processing
const test7 = {
    async test() {
        const q = new TaskQueue();
        q.stop();
        q.register('test', async () => 'never');
        await q.enqueue({ type: 'test', data: null });
        await new Promise(r => setTimeout(r, 50));
        const stats = q.getStats();
        assert.strictEqual(stats.processed, 0, 'should not process when stopped');
    }
};

// Test 8: Dead letter retry
const test8 = {
    async test() {
        const q = new TaskQueue({ maxRetries: 1, retryDelay: 5 });
        let attempts = 0;
        q.register('retry-test', async () => {
            attempts++;
            if (attempts < 2) throw new Error('fail');
            return 'ok';
        });
        await q.enqueue({ type: 'retry-test', data: null });
        await new Promise(r => setTimeout(r, 100));
        const dll = q.getDeadLetterQueue();
        assert.strictEqual(dll.length, 1);
        const retried = q.retryDeadLetter(dll[0].id);
        assert.ok(retried);
        await new Promise(r => setTimeout(r, 100));
        const stats = q.getStats();
        assert.strictEqual(stats.processed, 1, 'should process after retry');
    }
};

export const tests = [
    test1, test2, test3, test4, test5, test6, test7, test8
];