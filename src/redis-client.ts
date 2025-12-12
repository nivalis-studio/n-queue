import { Duration, Effect, Schedule } from 'effect';
import type { RedisClientType } from 'redis';
import type { JobId } from './job-id';
import type { RedisStreamEvents } from './types/events';
import type { JobData, JobState } from './types/job';
import type { KeysMap } from './types/keys';
import type { JobNames, PayloadSchema, QueueNames } from './types/payload';
import type {
  BackoffStrategy,
  QueueLogger,
  RedisClientOptions,
} from './types/queue';

const DEFAULT_RECONNECT_DELAY = 1000;
const EXPONENTIAL_BACKOFF_BASE = 2;
const MAX_RECONNECT_DELAY = 30_000;
const DEFAULT_MAX_RETRIES = 3;

/**
 * RedisClient class to handle Redis connection and basic operations with error handling
 * @template Payload - The payload schema type
 * @template QueueName - The queue name type extending QueueNames<Payload>
 */
export class RedisClient<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
> {
  private readonly getClient: () => Promise<RedisClientType>;
  private readonly maxRetries: number;
  private readonly backoffStrategy: BackoffStrategy;
  private readonly retrySchedule: Schedule.Schedule<
    unknown,
    unknown,
    never
  > | null;
  private readonly logger?: QueueLogger;
  private consecutiveErrors = 0;

  /**
   * Creates a new RedisClient instance
   * @param {() => Promise<RedisClientType>} getClient - Function to get a Redis client
   * @param {RedisClientOptions} [options] - Optional configuration
   */
  public constructor(
    getClient: () => Promise<RedisClientType>,
    options?: RedisClientOptions,
  ) {
    this.getClient = getClient;
    this.maxRetries = options?.maxRetries ?? DEFAULT_MAX_RETRIES;
    this.backoffStrategy = options?.backoffStrategy ?? {
      initialDelay: DEFAULT_RECONNECT_DELAY,
      maxDelay: MAX_RECONNECT_DELAY,
      factor: EXPONENTIAL_BACKOFF_BASE,
    };
    this.retrySchedule = this.maxRetries > 1 ? this.buildRetrySchedule() : null;
    this.logger = options?.logger;
  }

  /**
   * Get the Redis client with error handling
   * @returns {Promise<RedisClientType>} The Redis client
   */
  public async getRedisClient(): Promise<RedisClientType> {
    return await this.getClient();
  }

  /**
   * Get a specific job by ID
   * @template JobName - The job name type extending JobNames<Payload, QueueName>
   * @param {string} id - The ID of the job to retrieve
   * @returns {Effect<JobData | null, Error>} The job data or null if not found
   */
  public getJob<JobName extends JobNames<Payload, QueueName>>(
    id?: string,
  ): Effect.Effect<JobData<Payload, QueueName, JobName> | null, Error> {
    if (!id) {
      return Effect.succeed(null);
    }

    return this.getJobData<JobName>(id).pipe(
      Effect.catchAll(error =>
        Effect.fail(
          new Error('Failed to get job from queue', { cause: error }),
        ),
      ),
    );
  }

  /**
   * Get all fields and values from a hash
   * @template JobName - The job name type
   * @param {string} key - The hash key
   * @returns {Effect<JobData | null, Error>} The hash fields and values or null if not found
   */
  public getJobData<
    JobName extends JobNames<Payload, QueueName> = JobNames<Payload, QueueName>,
  >(
    key: string,
  ): Effect.Effect<JobData<Payload, QueueName, JobName> | null, Error> {
    return this.executeWithRetry(
      async () =>
        await this.getRedisClient().then(
          async client => await client.hGetAll(key),
        ),
      `getJobData(${key})`,
    ).pipe(
      Effect.flatMap(data => {
        if (!data || Object.keys(data).length === 0) {
          return Effect.succeed(null);
        }

        const payload = data.payload;

        if (!(data.name && payload && data.queue && data.state)) {
          return Effect.fail(
            new Error(`Invalid job data structure for key ${key}`),
          );
        }

        return Effect.try({
          try: () =>
            ({
              ...data,
              payload: JSON.parse(payload),
            }) as unknown as JobData<Payload, QueueName, JobName>,
          catch: error =>
            new Error(`Failed to parse job payload for key ${key}`, {
              cause: error,
            }),
        });
      }),
      Effect.catchAll(error => {
        if (
          error instanceof Error &&
          error.message.includes('Invalid job data structure')
        ) {
          return Effect.fail(error);
        }

        return Effect.fail(
          new Error(`Failed to get job data for key ${key}`, {
            cause: error,
          }),
        );
      }),
    );
  }

  /**
   * Get the length of a list
   * @param {string} key - The list key
   * @returns {Effect<number, Error>} The length of the list
   */
  public lLen(key: string): Effect.Effect<number, Error> {
    return this.executeWithRetry(
      async () =>
        await this.getRedisClient().then(
          async client => await client.lLen(key),
        ),
      `lLen(${key})`,
    );
  }

  /**
   * Pop a value from the right of a list
   * @param {string} key - The list key
   * @param {object} jobId - The job ID to pop
   * @param {JobNames<Payload, QueueName>} [jobName] - The name of the job to pop
   * @returns {Effect<string | null, Error>} The popped value or null if the list is empty
   */
  public pop(
    key: string,
    jobId: JobId,
    jobName?: JobNames<Payload, QueueName>,
  ): Effect.Effect<string | null, Error> {
    if (jobName) {
      return this.popByName(key, jobId, jobName);
    }

    return this.executeWithRetry(
      async () =>
        await this.getRedisClient().then(
          async client => await client.rPop(key),
        ),
      `pop(${key})`,
    );
  }

  /**
   * Find a job by name in a list
   * @param {string} listKey - The list key
   * @param {JobId} jobId - The job ID to find
   * @param {string} jobName - The job name to find
   * @returns {Effect<string | null, Error>} The job ID or null if not found
   */
  public findJobByName(
    listKey: string,
    jobId: JobId,
    jobName: string,
  ): Effect.Effect<string | null, Error> {
    return this.executeWithRetry(async () => {
      const client = await this.getRedisClient();
      const ids = await client.lRange(listKey, 0, -1);

      if (ids.length === 0) {
        return null;
      }

      for (const id of ids) {
        const name = jobId?.getJobName(id);

        if (name === jobName) {
          return id;
        }
      }

      return null;
    }, `findJobByName(${listKey}, ${jobName})`);
  }

  /**
   * Pop a job by name from a list
   * @param {string} listKey - The list key
   * @param {JobId} jobId - The job ID to find
   * @param {string} jobName - The job name to find and pop
   * @returns {Effect<string | null, Error>} The job ID or null if not found
   */
  public popByName(
    listKey: string,
    jobId: JobId,
    jobName: string,
  ): Effect.Effect<string | null, Error> {
    const client = this;

    return Effect.gen(function* () {
      const id = yield* client.findJobByName(listKey, jobId, jobName);

      if (!id) {
        return null;
      }

      yield* client.executeWithRetry(
        async () =>
          await client
            .getRedisClient()
            .then(async redis => await redis.lRem(listKey, 1, id)),
        `popByName(${listKey}, ${jobName})`,
      );

      return id;
    });
  }

  /**
   * Execute multiple Redis commands atomically
   * @param {(multi: ReturnType<RedisClientType['multi']>) => void} operations - Function that defines the operations to execute
   * @returns {Effect<unknown, Error>} The result of the operations
   */
  public executeMulti(
    operations: (multi: ReturnType<RedisClientType['multi']>) => void,
  ): Effect.Effect<unknown, Error> {
    return this.executeWithRetry(async () => {
      const client = await this.getRedisClient();
      const multi = client.multi();

      try {
        operations(multi);

        return await multi.exec();
      } catch (error) {
        multi.discard();
        throw error;
      }
    }, 'executeMulti');
  }

  /**
   * Save a job and add it to the waiting queue
   * @param {string} id - The job ID
   * @param {JobData} jobData - The job data
   * @param {string} waitingKey - The waiting queue key
   * @param {string} eventsKey - The queue key for events stream
   * @returns {Effect<void, Error>}
   */
  public saveJob(
    id: string,
    jobData: JobData,
    waitingKey: string,
    eventsKey: string,
  ): Effect.Effect<void, Error> {
    return this.executeMulti(multi => {
      multi.hSet(id, jobData);
      multi.lPush(waitingKey, id);
      multi.xAdd(eventsKey, '*', {
        type: 'saved',
        id,
      } satisfies RedisStreamEvents);
    }).pipe(Effect.asVoid);
  }

  /**
   * Listen for events from Redis streams
   * @param {string} eventsKey - The stream key to listen to
   * @param {string} groupName - The consumer group name
   * @param {string} consumerName - The consumer name within the group
   * @returns {Effect<Array<{name: string; messages: Array<{id: string; message: RedisStreamEvents}>}> | null, never>} The stream messages or null if none available
   */
  public listen(
    eventsKey: string,
    groupName: string,
    consumerName: string,
  ): Effect.Effect<
    Array<{
      name: string;
      messages: Array<{
        id: string;
        message: RedisStreamEvents;
      }>;
    }> | null,
    never
  > {
    const client = this;

    return Effect.tryPromise({
      try: async () => {
        const redis = await client.getRedisClient();

        try {
          await redis.xGroupCreate(eventsKey, groupName, '0', {
            MKSTREAM: true,
          });
        } catch {
          /* Ignore error if group already exists */
        }

        const response = await redis.xReadGroup(
          redis.commandOptions({ isolated: true }),
          groupName,
          consumerName,
          [{ key: eventsKey, id: '>' }],
          {
            COUNT: 1,
            BLOCK: 5000,
          },
        );

        if (!response) {
          client.resetErrorCount();

          return null;
        }

        client.resetErrorCount();

        return response.map(stream => ({
          name: stream.name,
          messages: stream.messages.map(msg => ({
            id: msg.id,
            message: msg.message as unknown as RedisStreamEvents,
          })),
        }));
      },
      catch: error =>
        error instanceof Error
          ? error
          : new Error('Stream error', { cause: error }),
    }).pipe(
      Effect.catchAll(error =>
        Effect.gen(function* () {
          client.consecutiveErrors += 1;
          client.logger?.error?.(
            `Error reading from stream (attempt ${client.consecutiveErrors})`,
            { error },
          );

          const delay = client.calculateBackoffDelay();

          yield* Effect.sleep(Duration.millis(delay));

          return null;
        }),
      ),
    );
  }

  /**
   * Acknowledge a message in a stream
   * @param {string} streamKey - The stream key
   * @param {string} groupName - The consumer group name
   * @param {string} messageId - The message ID to acknowledge
   * @returns {Effect<void, Error>}
   */
  public ackMessage(
    streamKey: string,
    groupName: string,
    messageId: string,
  ): Effect.Effect<void, Error> {
    return this.executeWithRetry(
      async () =>
        await this.getRedisClient().then(
          async client => await client.xAck(streamKey, groupName, messageId),
        ),
      `ackMessage(${streamKey}, ${groupName}, ${messageId})`,
    ).pipe(Effect.asVoid);
  }

  /**
   * Move a job from one queue to another atomically
   * @template JobName - The job name type
   * @param {string} id - The job ID
   * @param {JobData<Payload, QueueName, JobName>} jobData - The job data
   * @param {object} options - The options
   * @param {string} options.from - The source queue key
   * @param {string} options.to - The destination queue key
   * @param {JobId} [jobId] - The job ID to move
   * @returns {Effect<void, Error>}
   */
  public moveJob<
    JobName extends JobNames<Payload, QueueName> = JobNames<Payload, QueueName>,
  >(
    id: string,
    jobData: JobData<Payload, QueueName, JobName>,
    {
      from,
      to,
    }: {
      from: `${string}:${JobState}`;
      to: `${string}:${JobState}`;
    },
    jobId: JobId,
  ): Effect.Effect<void, Error> {
    if (!jobId.isValid(id)) {
      return Effect.fail(new Error(`Invalid job ID format: ${id}`));
    }

    return this.executeWithRetry(async () => {
      const client = await this.getRedisClient();

      try {
        await client.watch([from, to]);

        const multi = client.multi();

        multi.hSet(id, jobData);
        multi.lRem(from, 0, id);
        multi.lPush(to, id);

        const result = await multi.exec();

        if (result === null) {
          throw new Error('Transaction failed, key modified');
        }
      } catch (error) {
        await client.unwatch();
        throw error;
      }
    }, `moveJob(${id}, ${from} -> ${to})`).pipe(Effect.asVoid);
  }

  /**
   * Get queue statistics
   * @param {KeysMap<Payload, QueueName>} keys - The keys map
   * @param {QueueName} queueName - The queue name
   * @param {number} concurrency - The concurrency limit
   * @returns {Effect<object, Error>} Queue statistics
   */
  public getQueueStats(
    keys: KeysMap<Payload, QueueName>,
    queueName: QueueName,
    concurrency: number,
  ): Effect.Effect<
    {
      name: QueueName;
      concurrency: number;
      waiting: number;
      active: number;
      failed: number;
      completed: number;
      total: number;
      availableSlots: number;
    },
    Error
  > {
    const client = this;

    return Effect.gen(function* () {
      const waitingCount = yield* client.lLen(keys.waiting);
      const activeCount = yield* client.lLen(keys.active);
      const failedCount = yield* client.lLen(keys.failed);
      const completedCount = yield* client.lLen(keys.completed);

      return {
        name: queueName,
        concurrency,
        waiting: waitingCount,
        active: activeCount,
        failed: failedCount,
        completed: completedCount,
        total: waitingCount + activeCount + failedCount + completedCount,
        availableSlots:
          concurrency === -1 ? -1 : Math.max(0, concurrency - activeCount),
      };
    });
  }

  /**
   * Set the progress of a job
   * @param {string} id - The job ID
   * @param {number} progress - The progress value
   * @returns {Effect<void, Error>}
   */
  public setJobProgress(
    id: string,
    progress: number,
  ): Effect.Effect<void, Error> {
    return this.executeWithRetry(async () => {
      const client = await this.getRedisClient();

      await client.hSet(id, {
        progress: progress.toString(),
        updatedAt: Date.now().toString(),
      });
    }, 'setJobProgress').pipe(Effect.asVoid);
  }

  /**
   * Atomically claim the next waiting job into active state and set its lock.
   */
  public claimJob({
    waitingKey,
    activeKey,
    locksKey,
    eventsKey,
    visibilityTimeoutMs,
  }: {
    waitingKey: string;
    activeKey: string;
    locksKey: string;
    eventsKey: string;
    visibilityTimeoutMs: number;
  }): Effect.Effect<string | null, Error> {
    const script = `
      local id = redis.call('RPOP', KEYS[1])
      if not id then return nil end
      redis.call('HSET', id, 'state', 'active', 'updatedAt', ARGV[1])
      redis.call('LPUSH', KEYS[2], id)
      redis.call('ZADD', KEYS[3], tonumber(ARGV[1]) + tonumber(ARGV[2]), id)
      redis.call('XADD', KEYS[4], '*', 'type', 'active', 'id', id)
      return id
    `;

    return this.executeWithRetry(async () => {
      const client = (await this.getRedisClient()) as unknown as {
        eval: (
          script: string,
          options: { keys: Array<string>; arguments: Array<string> },
        ) => Promise<unknown>;
      };
      const now = Date.now().toString();

      const result = await client.eval(script, {
        keys: [waitingKey, activeKey, locksKey, eventsKey],
        arguments: [now, visibilityTimeoutMs.toString()],
      });

      return (result as string | null) ?? null;
    }, `claimJob(${waitingKey})`);
  }

  /** Extend visibility lock for an active job. */
  public extendActiveLock(
    id: string,
    locksKey: string,
    visibilityTimeoutMs: number,
  ): Effect.Effect<void, Error> {
    return this.executeWithRetry(async () => {
      const client = (await this.getRedisClient()) as unknown as {
        zAdd: (
          key: string,
          members: Array<{ score: number; value: string }>,
        ) => Promise<unknown>;
      };
      const score = Date.now() + visibilityTimeoutMs;
      await client.zAdd(locksKey, [{ score, value: id }]);
    }, `extendActiveLock(${id})`).pipe(Effect.asVoid);
  }

  /** Remove visibility lock for a finished job. */
  public removeActiveLock(
    id: string,
    locksKey: string,
  ): Effect.Effect<void, Error> {
    return this.executeWithRetry(async () => {
      const client = (await this.getRedisClient()) as unknown as {
        zRem: (key: string, member: string) => Promise<unknown>;
      };
      await client.zRem(locksKey, id);
    }, `removeActiveLock(${id})`).pipe(Effect.asVoid);
  }

  /**
   * Move due delayed jobs into waiting list.
   */
  public promoteDueDelayed(
    delayedKey: string,
    waitingKey: string,
    eventsKey: string,
    limit = 100,
  ): Effect.Effect<number, Error> {
    const script = `
      local now = tonumber(ARGV[1])
      local limit = tonumber(ARGV[2])
      local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, limit)
      for _, id in ipairs(ids) do
        redis.call('ZREM', KEYS[1], id)
        redis.call('LPUSH', KEYS[2], id)
        redis.call('HSET', id, 'state', 'waiting', 'updatedAt', ARGV[1])
        redis.call('XADD', KEYS[3], '*', 'type', 'saved', 'id', id)
      end
      return #ids
    `;

    return this.executeWithRetry(async () => {
      const client = (await this.getRedisClient()) as unknown as {
        eval: (
          script: string,
          options: { keys: Array<string>; arguments: Array<string> },
        ) => Promise<unknown>;
      };
      const now = Date.now().toString();

      const result = await client.eval(script, {
        keys: [delayedKey, waitingKey, eventsKey],
        arguments: [now, limit.toString()],
      });

      return Number(result ?? 0);
    }, `promoteDueDelayed(${delayedKey})`);
  }

  /**
   * Requeue a job into the delayed set with a delay.
   */
  public requeueJobWithDelay({
    id,
    jobData,
    activeKey,
    locksKey,
    delayedKey,
    eventsKey,
    delayMs,
  }: {
    id: string;
    jobData: JobData;
    activeKey: string;
    locksKey: string;
    delayedKey: string;
    eventsKey: string;
    delayMs: number;
  }): Effect.Effect<void, Error> {
    const dueAt = Date.now() + delayMs;

    return this.executeMulti(multi => {
      const tx = multi as unknown as {
        hSet: (key: string, data: unknown) => unknown;
        lRem: (key: string, count: number, element: string) => unknown;
        zRem: (key: string, element: string) => unknown;
        zAdd: (
          key: string,
          members: Array<{ score: number; value: string }>,
        ) => unknown;
        xAdd: (key: string, id: string, message: unknown) => unknown;
      };

      tx.hSet(id, jobData);
      tx.lRem(activeKey, 0, id);
      tx.zRem(locksKey, id);
      tx.zAdd(delayedKey, [{ score: dueAt, value: id }]);
      tx.xAdd(eventsKey, '*', {
        type: 'retrying',
        id,
      } satisfies RedisStreamEvents);
      tx.xAdd(eventsKey, '*', {
        type: 'delayed',
        id,
      } satisfies RedisStreamEvents);
    }).pipe(Effect.asVoid);
  }

  /**
   * Recover stalled jobs whose visibility locks expired.
   * Moves them from active list back to waiting list.
   */
  public recoverStalled({
    activeKey,
    waitingKey,
    locksKey,
    eventsKey,
    limit = 100,
  }: {
    activeKey: string;
    waitingKey: string;
    locksKey: string;
    eventsKey: string;
    limit?: number;
  }): Effect.Effect<number, Error> {
    const script = `
      local now = tonumber(ARGV[1])
      local limit = tonumber(ARGV[2])
      local ids = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, limit)
      for _, id in ipairs(ids) do
        redis.call('ZREM', KEYS[3], id)
        redis.call('LREM', KEYS[1], 0, id)
        redis.call('LPUSH', KEYS[2], id)
        redis.call('HSET', id, 'state', 'waiting', 'updatedAt', ARGV[1])
        redis.call('XADD', KEYS[4], '*', 'type', 'stalled', 'id', id)
        redis.call('XADD', KEYS[4], '*', 'type', 'saved', 'id', id)
      end
      return #ids
    `;

    return this.executeWithRetry(async () => {
      const client = (await this.getRedisClient()) as unknown as {
        eval: (
          script: string,
          options: { keys: Array<string>; arguments: Array<string> },
        ) => Promise<unknown>;
      };
      const now = Date.now().toString();

      const result = await client.eval(script, {
        keys: [activeKey, waitingKey, locksKey, eventsKey],
        arguments: [now, limit.toString()],
      });

      return Number(result ?? 0);
    }, `recoverStalled(${activeKey})`);
  }

  private buildRetrySchedule(): Schedule.Schedule<unknown, unknown, never> {
    const base = Schedule.exponential(
      Duration.millis(this.backoffStrategy.initialDelay),
      this.backoffStrategy.factor,
    ).pipe(
      Schedule.modifyDelay((_, duration) =>
        Duration.millis(
          Math.min(Duration.toMillis(duration), this.backoffStrategy.maxDelay),
        ),
      ),
    );

    const attempts = Math.max(0, this.maxRetries - 1);

    if (attempts <= 0) {
      return base as Schedule.Schedule<unknown, unknown, never>;
    }

    return Schedule.intersect(base, Schedule.recurs(attempts));
  }

  /**
   * Calculate backoff delay based on consecutive errors
   * @returns {number} The calculated delay in milliseconds
   * @private
   */
  private calculateBackoffDelay(): number {
    return Math.min(
      this.backoffStrategy.initialDelay *
        this.backoffStrategy.factor ** (this.consecutiveErrors - 1),
      this.backoffStrategy.maxDelay,
    );
  }

  /**
   * Reset consecutive error count on successful operation
   * @returns {void}
   * @private
   */
  private resetErrorCount(): void {
    this.consecutiveErrors = 0;
  }

  /**
   * Execute a Redis operation with retries and backoff
   * @template T - The return type of the operation
   * @param {() => Promise<T>} operation - The operation to execute
   * @param {string} context - The context for error messages
   * @returns {Effect<T, Error>} The operation result
   * @private
   */
  private executeWithRetry<T>(
    operation: () => Promise<T>,
    context: string,
  ): Effect.Effect<T, Error> {
    const effect = Effect.tryPromise({
      try: operation,
      catch: error =>
        new Error(
          `Failed to execute operation (${context}) after ${this.maxRetries} attempts`,
          { cause: error },
        ),
    }).pipe(
      Effect.tap(() => Effect.sync(() => this.resetErrorCount())),
      Effect.tapError(() =>
        Effect.sync(() => {
          this.consecutiveErrors += 1;
        }),
      ),
    );

    return this.retrySchedule === null
      ? effect
      : Effect.retry(effect, { schedule: this.retrySchedule });
  }
}
