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
 * Exponential backoff strategy for job retries.
 * Separate from Redis operation retry/backoff.
 */
export type JobBackoffStrategy = {
  /** Initial delay in milliseconds */
  initialDelay: number;
  /** Factor to multiply delay by on each retry */
  factor: number;
  /** Maximum delay in milliseconds */
  maxDelay: number;
  /** Optional jitter fraction (0-1) applied to delay */
  jitter?: number;
};

/**
 * Job retry configuration.
 */
export type JobRetryOptions = {
  /**
   * Maximum number of processing attempts (initial try counts as 1).
   * @default 3
   */
  maxAttempts?: number;

  /**
   * Backoff strategy between attempts.
   * @default { initialDelay: 1000, factor: 2, maxDelay: 30000, jitter: 0.2 }
   */
  backoffStrategy?: JobBackoffStrategy;
};

/**
 * Optional logger interface. If omitted, library stays silent.
 */
export type QueueLogger = {
  debug?: (msg: string, meta?: unknown) => void;
  info?: (msg: string, meta?: unknown) => void;
  warn?: (msg: string, meta?: unknown) => void;
  error?: (msg: string, meta?: unknown) => void;
};

/**
 * Optional metrics interface.
 */
export type QueueMetrics = {
  increment?: (name: string, labels?: Record<string, string>) => void;
  observe?: (
    name: string,
    value: number,
    labels?: Record<string, string>,
  ) => void;
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
   * Backoff strategy for Redis operation retries
   */
  backoffStrategy?: BackoffStrategy;

  /**
   * How long a job can stay active without a heartbeat before being considered stalled.
   * @default 300000 (5 minutes)
   */
  visibilityTimeoutMs?: number;

  /**
   * How often to check for stalled jobs during polling.
   * @default 60000 (1 minute)
   */
  stallCheckIntervalMs?: number;

  /**
   * Retry behavior for failed jobs.
   */
  retry?: JobRetryOptions;

  /**
   * Optional structured logger.
   */
  logger?: QueueLogger;

  /**
   * Optional metrics sink.
   */
  metrics?: QueueMetrics;
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
   * Backoff strategy for Redis operation retries
   */
  backoffStrategy?: BackoffStrategy;

  /**
   * Optional structured logger.
   */
  logger?: QueueLogger;
};
