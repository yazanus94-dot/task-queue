/**
 * Task Queue - Retry Strategy
 * Configurable retry strategies for failed tasks.
 */

export class RetryStrategy {
    constructor(options = {}) {
        this.maxRetries = options.maxRetries ?? 3;
        this.baseDelay = options.baseDelay ?? 1000;
        this.strategy = options.strategy ?? 'exponential';
    }

    /**
     * Calculate the delay before the next retry attempt.
     * @param {number} attempt - The current attempt number (1-based)
     * @returns {number} Delay in milliseconds
     */
    getDelay(attempt) {
        switch (this.strategy) {
            case 'exponential':
                return this.baseDelay * Math.pow(2, attempt - 1);
            case 'linear':
                return this.baseDelay * attempt;
            case 'fixed':
                return this.baseDelay;
            case 'jitter':
                const expDelay = this.baseDelay * Math.pow(2, attempt - 1);
                return expDelay + Math.random() * this.baseDelay;
            default:
                return this.baseDelay;
        }
    }

    /**
     * Determine if a retry should be attempted.
     * @param {number} attempt - The current attempt number
     * @param {Error} error - The error that occurred
     * @returns {boolean} True if retry should be attempted
     */
    shouldRetry(attempt, error) {
        if (attempt >= this.maxRetries) return false;
        if (error?.retryable === false) return false;
        return true;
    }

    /**
     * Get the maximum number of retries.
     * @returns {number} Max retries
     */
    getMaxRetries() {
        return this.maxRetries;
    }
}

export default RetryStrategy;