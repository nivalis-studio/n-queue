/* eslint-disable no-await-in-loop */
import { JobId } from './utils/job-id';
import type { RedisClientType } from 'redis';
import type { JobData, JobState } from './types/job';
import type { KeysMap } from './types/keys';
import type { JobNames, PayloadSchema, QueueNames } from './types/payload';
import type { RedisStreamEvents } from './types/events';

const DEFAULT_MAX_CONSECUTIVE_ERRORS = 5;
const DEFAULT_RECONNECT_DELAY = 1000;
const EXPONENTIAL_BACKOFF_BASE = 2;
const MAX_RECONNECT_DELAY = 30_000;

/**
 * RedisClient class to handle Redis connection and basic operations with error handling
 * @template Payload - The payload schema type
 * @template QueueName - The queue name type extending QueueNames<Payload>
 */
export class RedisClient<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
> {
  private _maxConsecutiveErrors = DEFAULT_MAX_CONSECUTIVE_ERRORS;
  private _reconnectDelay = DEFAULT_RECONNECT_DELAY;

  /**
   * Creates a new RedisClient instance
   * @param {() => Promise<RedisClientType>} getClient - Function to get a Redis client
   * @param {KeysMap<Payload, QueueName>} keys - The keys map for Redis operations
   * @param {number} concurrency - The maximum number of concurrent jobs
   * @template Payload - The payload schema type
   * @template QueueName - The queue name type
   */
  constructor(
    private readonly getClient: () => Promise<RedisClientType>,
    private readonly keys: KeysMap<Payload, QueueName>,
    private readonly concurrency: number,
  ) {}

  private get maxConsecutiveErrors(): number {
    return this._maxConsecutiveErrors;
  }

  private get reconnectDelay(): number {
    return this._reconnectDelay;
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
   * @param {string} key - The hash key
   * @returns {Promise<JobData | null>} The hash fields and values or null if not found
   */
  async getJobData<
    JobName extends JobNames<Payload, QueueName> = JobNames<Payload, QueueName>,
  >(key: string): Promise<JobData<Payload, QueueName, JobName> | null> {
    try {
      const data = await this.executeWithErrorHandling(
        async client => await client.hGetAll(key),
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
    return await this.executeWithErrorHandling(
      async client => await client.lLen(key),
    );
  }

  /**
   * Pop a value from the right of a list
   * @param {string} key - The list key
   * @param {JobNames<any, string>} jobName The name of the job to pop
   * @returns {Promise<string | null>} The popped value or null if the list is empty
   */
  async pop(
    key: string,
    jobName?: JobNames<Payload, QueueName>,
  ): Promise<string | null> {
    if (jobName) {
      return await this.popByName(key, jobName);
    }

    return await this.executeWithErrorHandling(
      async client => await client.rPop(key),
    );
  }

  /**
   * Find a job by name in a list
   * @param {string} listKey - The list key
   * @param {string} jobName - The job name to find
   * @returns {Promise<string | null>} The job ID or null if not found
   */
  async findJobByName(
    listKey: string,
    jobName: string,
  ): Promise<string | null> {
    return await this.executeWithErrorHandling(async client => {
      const ids = await client.lRange(listKey, 0, -1);

      if (ids.length === 0) return null;

      for (const id of ids) {
        const name = JobId.getJobName(id);

        if (name === jobName) return id;
      }

      return null;
    });
  }

  /**
   * Pop a job by name from a list
   * @param {string} listKey - The list key
   * @param {string} jobName - The job name to find and pop
   * @returns {Promise<string | null>} The job ID or null if not found
   */
  async popByName(listKey: string, jobName: string): Promise<string | null> {
    const id = await this.findJobByName(listKey, jobName);

    if (!id) return null;

    await this.executeWithErrorHandling(
      async client => await client.lRem(listKey, 1, id),
    );

    return id;
  }

  /**
   * Execute multiple Redis commands atomically
   * @param {Function} operations - Function that defines the operations to execute
   * @returns {Promise<unknown>} The result of the operations
   */
  async executeMulti(
    operations: (multi: ReturnType<RedisClientType['multi']>) => void,
  ): Promise<unknown> {
    return await this.executeWithErrorHandling(async client => {
      const multi = client.multi();

      try {
        operations(multi);

        return await multi.exec();
      } catch (error) {
        multi.discard();
        throw error;
      }
    });
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

  async listen(eventsKey: string, groupName: string, consumerName: string) {
    let consecutiveErrors = 0;
    const client = await this.getRedisClient();

    try {
      await client.xGroupCreate(eventsKey, groupName, '0', {
        MKSTREAM: true,
      });
    } catch {
      /* Ignore error if group already exists */
    }

    try {
      if (consecutiveErrors >= this.maxConsecutiveErrors) {
        throw new Error('Too many consecutive errors, forcing reconnection');
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

      if (!response) return null;

      return response.map(stream => ({
        name: stream.name,
        messages: stream.messages.map(msg => ({
          id: msg.id,
          message: msg.message as unknown as RedisStreamEvents,
        })),
      }));
    } catch (error) {
      consecutiveErrors += 1;
      console.error(
        `Error reading from stream (attempt ${consecutiveErrors}/${this.maxConsecutiveErrors}):`,
        error,
      );

      const delay = Math.min(
        this.reconnectDelay *
          EXPONENTIAL_BACKOFF_BASE ** (consecutiveErrors - 1),
        MAX_RECONNECT_DELAY,
      );

      setTimeout(() => {}, delay);

      return null;
    }
  }

  /**
   * Acknowledge a message in a stream
   * @param {string} streamKey - The stream key
   * @param {string} groupName - The consumer group name
   * @param {string} messageId - The message ID to acknowledge
   */
  async ackMessage(
    streamKey: string,
    groupName: string,
    messageId: string,
  ): Promise<void> {
    await this.executeWithErrorHandling(
      async client => await client.xAck(streamKey, groupName, messageId),
    );
  }

  /**
   * Move a job from one queue to another atomically
   * @param {string} id - The job ID
   * @param {JobData} jobData - The job data
   * @param {object} options - The options
   * @param {string} options.from - The source queue key
   * @param {string} options.to - The destination queue key
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
  ): Promise<void> {
    if (!JobId.isValid(id)) {
      throw new Error(`Invalid job ID format: ${id}`);
    }

    const client = await this.getRedisClient();
    const maxRetries = 3;
    let retries = 0;

    while (retries < maxRetries) {
      try {
        await client.watch([from, to]);

        const multi = client.multi();

        multi.hSet(id, jobData);
        multi.lRem(from, 0, id);
        multi.lPush(to, id);

        const result = await multi.exec();

        if (result === null) {
          retries += 1;

          continue;
        }

        return;
      } catch (error) {
        await client.unwatch();
        throw new Error('Failed to move job atomically', { cause: error });
      }
    }

    throw new Error(`Failed to move job after ${maxRetries} attempts`);
  }

  /**
   * Get queue statistics
   * @template QueueName - The queue name type
   * @param {KeysMap<any, QueueName>} keys - The keys map
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

  async setJobProgress(id: string, progress: number) {
    await this.executeWithErrorHandling(async client => {
      await client.hSet(id, {
        progress: progress.toString(),
        updatedAt: Date.now().toString(),
      });
    });
  }

  /**
   * Execute a Redis operation with error handling
   * @template T - The return type of the operation
   * @param {Function} operation - The operation to execute
   * @returns {Promise<T>} The result of the operation
   */
  private async executeWithErrorHandling<T>(
    operation: (client: RedisClientType) => Promise<T>,
  ): Promise<T> {
    try {
      const client = await this.getRedisClient();

      return await operation(client);
    } catch (error) {
      throw new Error(`Redis operation failed`, { cause: error });
    }
  }

  /**
   * Helper method to move a job to active state
   * @template JobName - The job name type extending JobNames<Payload, QueueName>
   * @param {string} id - The ID of the job to move
   * @param {string} fromKey - The source queue key
   * @returns {Promise<JobData<Payload, QueueName, JobName> | null>} The updated job data or null if not found
   */
  private async moveToActive<JobName extends JobNames<Payload, QueueName>>(
    id: string,
    fromKey: `${string}:${JobState}`,
  ): Promise<JobData<Payload, QueueName, JobName> | null> {
    const jobData = await this.getJobData<JobName>(id);

    if (!jobData) return null;

    const activeJobData: JobData<Payload, QueueName, JobName> = {
      ...jobData,
      state: 'active',
      updatedAt: Date.now().toString(),
    };

    await this.moveJob(id, activeJobData, {
      from: fromKey,
      to: this.keys.active,
    });

    return activeJobData;
  }
}
