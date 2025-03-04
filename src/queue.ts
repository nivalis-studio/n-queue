/* eslint-disable no-await-in-loop */
import { v4 as uuid } from 'uuid';
import { Job } from './job';
import { getKeysMap } from './types/keys';
import { RedisClient } from './redis-client';
import type { RedisClientType } from 'redis';
import type { KeysMap } from './types/keys';
import type { JobNames, PayloadSchema, QueueNames } from './types/payload';
import type { QueueOptions } from './types/queue';

export class Queue<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload> = QueueNames<Payload>,
> {
  public readonly keys: KeysMap<Payload, QueueName>;
  public readonly redisClient: RedisClient<Payload, QueueName>;
  private concurrency: number;
  private readonly groupName: string;
  private readonly consumerName: string;
  private eventHandlers = new Map<string, Set<(jobId: string) => void>>();
  private isListening = false;

  /**
   * Creates a new Queue instance
   * @param {string} name - The name of the queue
   * @param {() => Promise<RedisClientType>} getRedisClient - Function to get a Redis client
   * @param {QueueOptions} [options] - Optional configuration options
   */
  constructor(
    public readonly name: QueueName,
    getRedisClient: () => Promise<RedisClientType>,
    options?: QueueOptions,
  ) {
    this.keys = getKeysMap<Payload, QueueName>(name);
    this.concurrency = options?.concurrency ?? -1;
    this.redisClient = new RedisClient(
      getRedisClient,
      this.keys,
      this.concurrency,
    );
    this.consumerName = `${this.name}:${uuid()}`;
    this.groupName = `${this.name}:events`;
  }

  /**
   * Adds a new job to the queue
   * @template T
   * @param {string} jobName - The name of the job
   * @param {any} payload - The job payload
   * @returns {Promise<Job<any, any, any>>} The created job
   */
  add = async <JobName extends JobNames<Payload, QueueName>>(
    jobName: JobName,
    payload: Payload[QueueName][JobName],
  ): Promise<Job<Payload, QueueName, JobName>> => {
    const job = new Job<Payload, QueueName, JobName>({
      queue: this,
      name: jobName,
      payload,
    });

    return await job.save();
  };

  /**
   * Get statistics about the queue
   * @returns {Promise<object>} An object containing statistics about the queue
   */
  getStats = async (): Promise<{
    name: QueueName;
    concurrency: number;
    waiting: number;
    active: number;
    failed: number;
    completed: number;
    total: number;
    availableSlots: number;
  }> => {
    try {
      return await this.redisClient.getQueueStats(
        this.keys,
        this.name,
        this.concurrency,
      );
    } catch (error) {
      throw new Error('Failed to get queue stats', { cause: error });
    }
  };

  /**
   * Process jobs from the queue
   *
   * This method can be called in two ways:
   * 1. Without a job name - processes any job from the queue
   * 2. With a specific job name - processes only jobs with that name
   * @example
   * // Process any job
   * await queue.process(async (job) => {
   *   // Process the job
   * });
   *
   * // Process only 'jobA' jobs
   * await queue.process(async (job) => {
   *   // Process the job
   * }, 'jobA');
   */
  async process(
    fn: (
      job: Job<Payload, QueueName, JobNames<Payload, QueueName>>,
    ) => Promise<void>,
  ): Promise<void>;
  async process<JobName extends JobNames<Payload, QueueName>>(
    fn: (job: Job<Payload, QueueName, JobName>) => Promise<void>,
    jobName: JobName,
  ): Promise<void>;
  async process<JobName extends JobNames<Payload, QueueName>>(
    fn: (job: Job<Payload, QueueName, JobName>) => Promise<void>,
    jobName?: JobName,
  ): Promise<void> {
    const job = await this.take(jobName);

    if (!job) return;

    try {
      await fn(job);

      await job.move('completed');
    } catch (error) {
      await job.move('failed');
      throw new Error('Failed to process job', {
        cause: error,
      });
    }
  }

  /**
   * Listen for events from the queue
   * @template JobName - The name of the job to listen for
   * @param {JobName} [jobName] - Optional job name to filter events
   * @yields {object} An object containing the event type and job ID
   */
  // eslint-disable-next-line sonarjs/cognitive-complexity
  async *listen(jobName?: JobNames<Payload, QueueName>) {
    this.isListening = true;

    while (this.isListening) {
      try {
        const responses = await this.redisClient.listen(
          this.keys.events,
          this.groupName,
          this.consumerName,
        );

        if (!responses) continue;

        for (const response of responses) {
          const messages = response.messages;

          for (const { id, message } of messages) {
            const eventType = message.type;
            const jobId = message.id;

            // eslint-disable-next-line max-depth
            if (jobName) {
              const job = await Job.unpack<
                Payload,
                QueueName,
                JobNames<Payload, QueueName>
              >(this, jobId);

              // eslint-disable-next-line max-depth
              if (!job || job.name !== jobName) continue;
            }

            yield { eventType, jobId };

            await this.emit(eventType, jobId, id);
          }
        }
      } catch (error) {
        console.error('Error processing stream messages:', error);

        await new Promise(resolve => {
          // eslint-disable-next-line @typescript-eslint/no-magic-numbers
          setTimeout(resolve, 100);
        });
      }
    }
  }

  /**
   * Stream and process jobs from the queue
   * @template JobName - The name of the job to stream
   * @param {Function} fn - The function to process jobs
   * @param {JobName} [jobName] - Optional job name to process only specific jobs
   */
  async stream(
    fn: (
      job: Job<Payload, QueueName, JobNames<Payload, QueueName>>,
    ) => Promise<void>,
  ): Promise<void>;
  async stream<JobName extends JobNames<Payload, QueueName>>(
    fn: (job: Job<Payload, QueueName, JobName>) => Promise<void>,
    jobName: JobName,
  ): Promise<void>;
  async stream<JobName extends JobNames<Payload, QueueName>>(
    fn: (job: Job<Payload, QueueName, JobName>) => Promise<void>,
    jobName?: JobName,
  ): Promise<void> {
    for await (const { eventType, jobId } of this.listen(jobName)) {
      if (eventType === 'saved') {
        const job = await this.get<JobName>(jobId);

        if (!job) continue;

        try {
          await fn(job);
          await job.move('completed');
        } catch (error) {
          await job.move('failed');
          throw new Error('Failed to process job', {
            cause: error,
          });
        }
      }
    }
  }

  on(event: string, handler: (jobId: string) => void | Promise<void>) {
    if (!this.eventHandlers.has(event)) {
      this.eventHandlers.set(event, new Set());
    }

    const handlers = this.eventHandlers.get(event);

    handlers?.add(handler);
  }

  once(event: string, handler: (jobId: string) => void) {
    const onceHandler = (jobId: string) => {
      handler(jobId);
      this.off(event, onceHandler);
    };

    this.on(event, onceHandler);
  }

  private off(event: string, handler: (jobId: string) => void) {
    this.eventHandlers.get(event)?.delete(handler);
  }

  private async emit(event: string, jobId: string, messageId?: string) {
    const handlers = this.eventHandlers.get(event);

    if (!handlers) return;

    try {
      await Promise.all(
        // eslint-disable-next-line @typescript-eslint/require-await
        [...handlers].map(async handler => {
          // eslint-disable-next-line @typescript-eslint/no-confusing-void-expression
          return handler(jobId);
        }),
      );

      if (messageId) {
        await this.redisClient.ackMessage(
          this.keys.events,
          this.groupName,
          messageId,
        );
      }
    } catch (error) {
      console.error(`Error processing event ${event}:`, error);
      throw error;
    }
  }

  /**
   * Takes the next job from the waiting queue and moves it to the active queue
   * @param {JobNames<any, any>} jobName The name of the job to take
   * @returns {Promise<Job<any, any, any> | null>} The next job or null if no jobs are available or concurrency limit is reached
   */
  private async take<JobName extends JobNames<Payload, QueueName>>(
    jobName?: JobName,
  ): Promise<Job<Payload, QueueName, JobName> | null> {
    try {
      const activeCount = await this.redisClient.lLen(this.keys.active);

      if (this.concurrency > 0 && activeCount >= this.concurrency) {
        return null;
      }

      const id = await this.redisClient.pop(this.keys.waiting, jobName);

      if (!id) return null;

      const jobData = await this.redisClient.getJobData<JobName>(id);

      if (!jobData) return null;

      const job = new Job<Payload, QueueName, JobName>({
        queue: this,
        name: jobData.name,
        payload: jobData.payload as Payload[QueueName][JobName],
        state: 'active',
        id,
        createdAt: jobData.createdAt,
        updatedAt: Date.now().toString(),
      });

      await this.redisClient.moveJob(id, job.prepare(), {
        from: this.keys.waiting,
        to: this.keys.active,
      });

      return job;
    } catch (error) {
      throw new Error('Failed to take job from queue', {
        cause: error,
      });
    }
  }

  /**
   * Takes the next job from the waiting queue and moves it to the active queue
   * @param {string} id The name of the job to take
   * @returns {Promise<Job<any, any, any> | null>} The next job or null if no jobs are available or concurrency limit is reached
   */
  private async get<JobName extends JobNames<Payload, QueueName>>(
    id?: string,
  ): Promise<Job<Payload, QueueName, JobName> | null> {
    try {
      if (!id) return null;

      const jobData = await this.redisClient.getJobData<JobName>(id);

      if (!jobData) return null;

      const job = new Job<Payload, QueueName, JobName>({
        queue: this,
        name: jobData.name,
        payload: jobData.payload as Payload[QueueName][JobName],
        state: 'active',
        id,
        createdAt: jobData.createdAt,
        updatedAt: Date.now().toString(),
      });

      await this.redisClient.moveJob(id, job.prepare(), {
        from: this.keys.waiting,
        to: this.keys.active,
      });

      return job;
    } catch (error) {
      throw new Error('Failed to take job from queue', {
        cause: error,
      });
    }
  }
}
