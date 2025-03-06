import { sleep } from './lib/sleep';
import type { JobId } from './job-id';
import type { RedisClientType } from 'redis';
import type { JobData, JobState } from './types/job';
import type { KeysMap } from './types/keys';
import type { JobNames, PayloadSchema, QueueNames } from './types/payload';
import type { RedisStreamEvents } from './types/events';
import type { BackoffStrategy, RedisClientOptions } from './types/queue';

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
  private readonly maxRetries: number;
  private readonly backoffStrategy: BackoffStrategy;
  private consecutiveErrors = 0;

  /**
   * Creates a new RedisClient instance
   * @param {() => Promise<RedisClientType>} getClient - Function to get a Redis client
   * @param {RedisClientOptions} [options] - Optional configuration
   */
  constructor(
    private readonly getClient: () => Promise<RedisClientType>,
    options?: RedisClientOptions,
  ) {
    this.maxRetries = options?.maxRetries ?? DEFAULT_MAX_RETRIES;
    this.backoffStrategy = options?.backoffStrategy ?? {
      initialDelay: DEFAULT_RECONNECT_DELAY,
      maxDelay: MAX_RECONNECT_DELAY,
      factor: EXPONENTIAL_BACKOFF_BASE,
    };
  }

  /**
   * Get the Redis client with error handling
   * @returns {Promise<RedisClientType>} The Redis client
   */
  async getRedisClient(): Promise<RedisClientType> {
    return await this.getClient();
  }

  /**
   * Get a specific job by ID
   * @template JobName - The job name type extending JobNames<Payload, QueueName>
   * @param {string} id - The ID of the job to retrieve
   * @returns {Promise<JobData<Payload, QueueName, JobName> | null>} The job data or null if not found
   */
  async getJob<JobName extends JobNames<Payload, QueueName>>(
    id?: string,
  ): Promise<JobData<Payload, QueueName, JobName> | null> {
    try {
      if (!id) return null;

      return await this.getJobData<JobName>(id);
    } catch (error) {
      throw new Error('Failed to get job from queue', { cause: error });
    }
  }

  /**
   * Get all fields and values from a hash
   * @template JobName - The job name type
   * @param {string} key - The hash key
   * @returns {Promise<JobData<Payload, QueueName, JobName> | null>} The hash fields and values or null if not found
   */
  async getJobData<
    JobName extends JobNames<Payload, QueueName> = JobNames<Payload, QueueName>,
  >(key: string): Promise<JobData<Payload, QueueName, JobName> | null> {
    try {
      const data = await this.executeWithRetry(
        async () =>
          await this.getRedisClient().then(
            async client => await client.hGetAll(key),
          ),
        `getJobData(${key})`,
      );

      if (!data || Object.keys(data).length === 0) {
        return null;
      }

      if (!data.name || !data.payload || !data.queue || !data.state) {
        throw new Error(`Invalid job data structure for key ${key}`);
      }

      try {
        const parsedPayload = JSON.parse(data.payload);

        return {
          ...data,
          payload: parsedPayload,
        } as unknown as JobData<Payload, QueueName, JobName>;
      } catch (error) {
        throw new Error(`Failed to parse job payload for key ${key}`, {
          cause: error,
        });
      }
    } catch (error) {
      if (
        error instanceof Error &&
        error.message.includes('Invalid job data')
      ) {
        throw error;
      }

      throw new Error(`Failed to get job data for key ${key}`, {
        cause: error,
      });
    }
  }

  /**
   * Get the length of a list
   * @param {string} key - The list key
   * @returns {Promise<number>} The length of the list
   */
  async lLen(key: string): Promise<number> {
    return await this.executeWithRetry(
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
   * @returns {Promise<string | null>} The popped value or null if the list is empty
   */
  async pop(
    key: string,
    jobId: JobId,
    jobName?: JobNames<Payload, QueueName>,
  ): Promise<string | null> {
    if (jobName) {
      return await this.popByName(key, jobId, jobName);
    }

    return await this.executeWithRetry(
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
   * @returns {Promise<string | null>} The job ID or null if not found
   */
  async findJobByName(
    listKey: string,
    jobId: JobId,
    jobName: string,
  ): Promise<string | null> {
    return await this.executeWithRetry(async () => {
      const client = await this.getRedisClient();
      const ids = await client.lRange(listKey, 0, -1);

      if (ids.length === 0) return null;

      for (const id of ids) {
        const name = jobId?.getJobName(id);

        if (name === jobName) return id;
      }

      return null;
    }, `findJobByName(${listKey}, ${jobName})`);
  }

  /**
   * Pop a job by name from a list
   * @param {string} listKey - The list key
   * @param {JobId} jobId - The job ID to find
   * @param {string} jobName - The job name to find and pop
   * @returns {Promise<string | null>} The job ID or null if not found
   */
  async popByName(
    listKey: string,
    jobId: JobId,
    jobName: string,
  ): Promise<string | null> {
    const id = await this.findJobByName(listKey, jobId, jobName);

    if (!id) return null;

    await this.executeWithRetry(
      async () =>
        await this.getRedisClient().then(
          async client => await client.lRem(listKey, 1, id),
        ),
      `popByName(${listKey}, ${jobName})`,
    );

    return id;
  }

  /**
   * Execute multiple Redis commands atomically
   * @param {(multi: ReturnType<RedisClientType['multi']>) => void} operations - Function that defines the operations to execute
   * @returns {Promise<unknown>} The result of the operations
   */
  async executeMulti(
    operations: (multi: ReturnType<RedisClientType['multi']>) => void,
  ): Promise<unknown> {
    return await this.executeWithRetry(async () => {
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
   * @returns {Promise<void>}
   */
  async saveJob(
    id: string,
    jobData: JobData,
    waitingKey: string,
    eventsKey: string,
  ): Promise<void> {
    await this.executeMulti(multi => {
      multi.hSet(id, jobData);
      multi.lPush(waitingKey, id);
      multi.xAdd(eventsKey, '*', {
        type: 'saved',
        id,
      } satisfies RedisStreamEvents);
    });
  }

  /**
   * Listen for events from Redis streams
   * @param {string} eventsKey - The stream key to listen to
   * @param {string} groupName - The consumer group name
   * @param {string} consumerName - The consumer name within the group
   * @returns {Promise<Array<{name: string; messages: Array<{id: string; message: RedisStreamEvents}>}> | null>} The stream messages or null if none available
   */
  async listen(
    eventsKey: string,
    groupName: string,
    consumerName: string,
  ): Promise<Array<{
    name: string;
    messages: Array<{
      id: string;
      message: RedisStreamEvents;
    }>;
  }> | null> {
    try {
      const client = await this.getRedisClient();

      try {
        await client.xGroupCreate(eventsKey, groupName, '0', {
          MKSTREAM: true,
        });
      } catch {
        /* Ignore error if group already exists */
      }

      const response = await client.xReadGroup(
        client.commandOptions({ isolated: true }),
        groupName,
        consumerName,
        [{ key: eventsKey, id: '>' }],
        {
          COUNT: 1,
          BLOCK: 5000,
        },
      );

      if (!response) {
        this.resetErrorCount();

        return null;
      }

      this.resetErrorCount();

      return response.map(stream => ({
        name: stream.name,
        messages: stream.messages.map(msg => ({
          id: msg.id,
          message: msg.message as unknown as RedisStreamEvents,
        })),
      }));
    } catch (error) {
      this.consecutiveErrors += 1;
      console.error(
        `Error reading from stream (attempt ${this.consecutiveErrors}):`,
        error,
      );

      const delay = this.calculateBackoffDelay();

      await sleep(delay);

      return null;
    }
  }

  /**
   * Acknowledge a message in a stream
   * @param {string} streamKey - The stream key
   * @param {string} groupName - The consumer group name
   * @param {string} messageId - The message ID to acknowledge
   * @returns {Promise<void>}
   */
  async ackMessage(
    streamKey: string,
    groupName: string,
    messageId: string,
  ): Promise<void> {
    await this.executeWithRetry(
      async () =>
        await this.getRedisClient().then(
          async client => await client.xAck(streamKey, groupName, messageId),
        ),
      `ackMessage(${streamKey}, ${groupName}, ${messageId})`,
    );
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
   * @returns {Promise<void>}
   */
  async moveJob<
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
  ): Promise<void> {
    if (jobId && !jobId?.isValid(id)) {
      throw new Error(`Invalid job ID format: ${id}`);
    }

    await this.executeWithRetry(async () => {
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
    }, `moveJob(${id}, ${from} -> ${to})`);
  }

  /**
   * Get queue statistics
   * @param {KeysMap<Payload, QueueName>} keys - The keys map
   * @param {QueueName} queueName - The queue name
   * @param {number} concurrency - The concurrency limit
   * @returns {Promise<object>} Queue statistics
   */
  async getQueueStats(
    keys: KeysMap<Payload, QueueName>,
    queueName: QueueName,
    concurrency: number,
  ): Promise<{
    name: QueueName;
    concurrency: number;
    waiting: number;
    active: number;
    failed: number;
    completed: number;
    total: number;
    availableSlots: number;
  }> {
    const [waitingCount, activeCount, failedCount, completedCount] =
      await Promise.all([
        this.lLen(keys.waiting),
        this.lLen(keys.active),
        this.lLen(keys.failed),
        this.lLen(keys.completed),
      ]);

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
  }

  /**
   * Set the progress of a job
   * @param {string} id - The job ID
   * @param {number} progress - The progress value
   * @returns {Promise<void>}
   */
  async setJobProgress(id: string, progress: number): Promise<void> {
    await this.executeWithRetry(async () => {
      const client = await this.getRedisClient();

      await client.hSet(id, {
        progress: progress.toString(),
        updatedAt: Date.now().toString(),
      });
    }, 'setJobProgress');
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
   * @returns {Promise<T>} The operation result
   * @private
   */
  private async executeWithRetry<T>(
    operation: () => Promise<T>,
    context: string,
  ): Promise<T> {
    let attempts = 0;

    while (attempts < this.maxRetries) {
      try {
        // eslint-disable-next-line no-await-in-loop
        const result = await operation();

        this.resetErrorCount();

        return result;
      } catch (error) {
        attempts += 1;
        this.consecutiveErrors += 1;

        if (attempts === this.maxRetries) {
          throw new Error(
            `Failed to execute operation (${context}) after ${this.maxRetries} attempts`,
            { cause: error },
          );
        }

        const delay = this.calculateBackoffDelay();

        // eslint-disable-next-line no-await-in-loop
        await sleep(delay);
      }
    }

    throw new Error(`Unexpected retry loop exit for operation: ${context}`);
  }
}
