/**
 * Exponential backoff strategy for retries
 */
export type BackoffStrategy = {
  /**
   * Initial delay in milliseconds
   */
  initialDelay: number;

  /**
   * Factor to multiply delay by on each retry
   */
  factor: number;

  /**
   * Maximum delay in milliseconds
   */
  maxDelay: number;
};

/**
 * Queue configuration options
 */
export type QueueOptions = {
  /**
   * Maximum number of concurrent jobs
   * @default -1 (unlimited)
   */
  concurrency?: number;

  /**
   * Maximum number of retries for Redis operations
   * @default 3
   */
  maxRetries?: number;

  /**
   * Backoff strategy for retries
   */
  backoffStrategy?: BackoffStrategy;
};

/**
 * Redis client configuration options
 */
export type RedisClientOptions = {
  /**
   * Maximum number of retries for Redis operations
   * @default 3
   */
  maxRetries?: number;

  /**
   * Backoff strategy for retries
   */
  backoffStrategy?: BackoffStrategy;
};
