import type { RedisClientType } from 'redis';
import type { JobData } from './types/job';
import type { KeysMap } from './types/keys';
import type { PayloadSchema, QueueNames } from './types/payload';

/**
 * RedisClient class to handle Redis connection and basic operations with error handling
 */
export class RedisClient {
  /**
   * Creates a new RedisClient instance
   * @param {() => Promise<RedisClientType>} getClient - Function to get a Redis client
   */
  constructor(private readonly getClient: () => Promise<RedisClientType>) {}

  /**
   * Get the Redis client with error handling
   * @returns {Promise<RedisClientType>} The Redis client
   */
  async getRedisClient(): Promise<RedisClientType> {
    try {
      return await this.getClient();
    } catch (error) {
      console.error('Failed to get Redis client:', error);
      throw new Error('Redis connection failed');
    }
  }

  /**
   * Increment a key in Redis
   * @param {string} key - The key to increment
   * @returns {Promise<number>} The new value
   */
  async incr(key: string): Promise<number> {
    return await this.executeWithErrorHandling(
      async client => await client.incr(key),
      `Failed to increment key: ${key}`,
    );
  }

  /**
   * Get all fields and values from a hash
   * @param {string} key - The hash key
   * @returns {Promise<{[key: string]: string}>} The hash fields and values
   */
  async get(key: string): Promise<{ [key: string]: string }> {
    return await this.executeWithErrorHandling(
      async client => await client.hGetAll(key),
      `Failed to get hash: ${key}`,
    );
  }

  /**
   * Get the length of a list
   * @param {string} key - The list key
   * @returns {Promise<number>} The length of the list
   */
  async lLen(key: string): Promise<number> {
    return await this.executeWithErrorHandling(
      async client => await client.lLen(key),
      `Failed to get list length: ${key}`,
    );
  }

  /**
   * Push a value to the left of a list
   * @param {string} key - The list key
   * @param {string} value - The value to push
   * @returns {Promise<number>} The new length of the list
   */
  async push(key: string, value: string): Promise<number> {
    return await this.executeWithErrorHandling(
      async client => await client.lPush(key, value),
      `Failed to push to list left: ${key}`,
    );
  }

  /**
   * Pop a value from the right of a list
   * @param {string} key - The list key
   * @returns {Promise<string | null>} The popped value or null if the list is empty
   */
  async pop(key: string): Promise<string | null> {
    return await this.executeWithErrorHandling(
      async client => await client.rPop(key),
      `Failed to pop from list right: ${key}`,
    );
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

      operations(multi);

      return await multi.exec();
    }, 'Failed to execute multi operation');
  }

  /**
   * Generate a new job ID
   * @param {string} idKey - The key for job IDs
   * @returns {Promise<string>} The new job ID
   */
  async generateJobId(idKey: string): Promise<string> {
    const jobId = await this.incr(idKey);

    return jobId.toString();
  }

  /**
   * Save a job and add it to the waiting queue
   * @param {string} id - The job ID
   * @param {JobData} jobData - The job data
   * @param {string} waitingKey - The waiting queue key
   * @returns {Promise<void>}
   */
  async saveNewJob(
    id: string,
    jobData: JobData,
    waitingKey: string,
  ): Promise<void> {
    await this.executeMulti(multi => {
      multi.hSet(id, jobData as { [key: string]: string });
      multi.lPush(waitingKey, id);
    });
  }

  /**
   * Move a job from one queue to another
   * @param {string} id - The job ID
   * @param {JobData} jobData - The job data
   * @param {string} fromKey - The source queue key
   * @param {string} toKey - The destination queue key
   * @returns {Promise<void>}
   */
  async moveJob(
    id: string,
    jobData: JobData,
    fromKey: string,
    toKey: string,
  ): Promise<void> {
    await this.executeMulti(multi => {
      multi.hSet(id, jobData as { [key: string]: string });
      multi.lRem(fromKey, 0, id);
      multi.lPush(toKey, id);
    });
  }

  /**
   * Get queue statistics
   * @template Payload - The payload schema type
   * @template QueueName - The queue name type
   * @param {KeysMap<Payload, QueueName>} keys - The keys map
   * @param {QueueName} queueName - The queue name
   * @param {number} concurrency - The concurrency limit
   * @returns {Promise<object>} Queue statistics
   */
  async getQueueStats<
    Payload extends PayloadSchema,
    QueueName extends QueueNames<Payload>,
  >(
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
   * Execute a Redis operation with error handling
   * @template T - The return type of the operation
   * @param {Function} operation - The operation to execute
   * @param {string} errorMessage - Error message to display if operation fails
   * @returns {Promise<T>} The result of the operation
   */
  private async executeWithErrorHandling<T>(
    operation: (client: RedisClientType) => Promise<T>,
    errorMessage: string,
  ): Promise<T> {
    try {
      const client = await this.getRedisClient();

      return await operation(client);
    } catch (error) {
      console.error(`${errorMessage}:`, error);
      throw new Error(`Redis operation failed: ${errorMessage}`);
    }
  }
}
