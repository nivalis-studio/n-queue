/* eslint-disable no-await-in-loop */
import { v4 as uuid } from 'uuid';
import { Job } from './job';
import { getKeysMap } from './types/keys';
import { RedisClient } from './redis-client';
import { JobId } from './job-id';
import type { RedisClientType } from 'redis';
import type { KeysMap } from './types/keys';
import type { JobNames, PayloadSchema, QueueNames } from './types/payload';
import type { QueueOptions } from './types/queue';
import type { JobData } from './types/job';
import type { JobEvent, RedisStreamEvents } from './types/events';

/**
 * Queue class for managing job processing
 * @template Payload - The payload schema type
 * @template QueueName - The queue name type
 */
export class Queue<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload> = QueueNames<Payload>,
> {
  public readonly keys: KeysMap<Payload, QueueName>;
  public readonly redisClient: RedisClient<Payload, QueueName>;
  public readonly jobId: JobId;

  private readonly concurrency: number;
  private readonly groupName: string;
  private readonly consumerName: string;
  private readonly id: string;
  private isListening = false;

  /**
   * Creates a new Queue instance
   * @param {QueueName} name - The name of the queue
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
    this.redisClient = new RedisClient(getRedisClient, options);
    this.id = uuid();

    this.jobId = new JobId(':', this.name);
    this.groupName = `${this.name}:events`;
    this.consumerName = `${this.name}:${this.id}`;
  }

  /**
   * Adds a new job to the queue
   * @template JobName - The name of the job
   * @param {JobName} jobName - The name of the job
   * @param {Payload[QueueName][JobName]} payload - The job payload
   * @returns {Promise<Job>} The created job
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
   * Listen for job events from the queue
   * @param {string} [jobName] - Optional job name to filter events by
   * @yields {JobEvent} The job events from the queue
   */
  async *listen(jobName?: string): AsyncGenerator<JobEvent> {
    this.isListening = true;

    while (this.isListening) {
      const response = await this.redisClient.listen(
        this.keys.events,
        this.groupName,
        this.consumerName,
      );

      if (!response) continue;

      const events = this.processStreamMessages(response);

      for (const event of events) {
        if (jobName && !this.checkJobFilter(event.jobId, jobName)) {
          continue;
        }

        yield event;
      }
    }
  }

  /**
   * Get the current state of the queue
   * @template QueueStats - The queue name type
   * @returns {Promise<QueueStats>} The current queue statistics
   */
  async getStats(): Promise<{
    waiting: number;
    active: number;
    completed: number;
    failed: number;
  }> {
    const [waiting, active, completed, failed] = await Promise.all([
      this.redisClient.lLen(this.keys.waiting),
      this.redisClient.lLen(this.keys.active),
      this.redisClient.lLen(this.keys.completed),
      this.redisClient.lLen(this.keys.failed),
    ]);

    return {
      waiting,
      active,
      completed,
      failed,
    };
  }

  /**
   * Process jobs from the queue
   * @template JobName - The name of the job to process
   * @param {Function} fn - The function to process jobs
   * @param {JobName} [jobName] - Optional job name to process only specific jobs
   */
  async process<JobName extends JobNames<Payload, QueueName>>(
    fn: (job: Job<Payload, QueueName, JobName>) => Promise<void>,
    { jobName, jobId }: { jobName?: JobName; jobId?: string },
  ): Promise<void> {
    const job = await this.retrieveJob<JobName>({ jobId, jobName });

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
   * Stream and process jobs from the queue
   * @template JobName - The name of the job to stream
   * @param {Function} fn - The function to process jobs
   * @param {JobName} [jobName] - Optional job name to process only specific jobs
   */
  async stream<JobName extends JobNames<Payload, QueueName>>(
    fn: (job: Job<Payload, QueueName, JobName>) => Promise<void>,
    jobName?: JobName,
  ): Promise<void> {
    for await (const { eventType, jobId } of this.listen(jobName)) {
      if (eventType === 'saved') {
        await this.process(fn, { jobName, jobId });
      }
    }
  }

  private createJob<JobName extends JobNames<Payload, QueueName>>(
    jobData: JobData<Payload, QueueName, JobName>,
    state: 'waiting' | 'active' | 'completed' | 'failed',
    id?: string,
  ): Job<Payload, QueueName, JobName> {
    return new Job<Payload, QueueName, JobName>({
      queue: this,
      name: jobData.name,
      payload: jobData.payload as Payload[QueueName][JobName],
      state,
      id,
      createdAt: jobData.createdAt,
      updatedAt: Date.now().toString(),
    });
  }

  /**
   * Process stream messages from Redis
   * @param {Array<{name: string; messages: Array<{id: string; message: RedisStreamEvents}>}>} response - The response from Redis stream
   * @returns {Array<JobEvent>} The processed job events
   * @private
   */
  private processStreamMessages(
    response: Array<{
      name: string;
      messages: Array<{
        id: string;
        message: RedisStreamEvents;
      }>;
    }>,
  ): JobEvent[] {
    if (!this.isListening) {
      return [];
    }

    const events: JobEvent[] = [];

    for (const { messages } of response) {
      for (const { id, message } of messages) {
        events.push({
          eventType: message.type,
          jobId: message.id,
        });

        this.redisClient
          .ackMessage(this.keys.events, 'queue', id)
          .catch(error => {
            console.error('Failed to acknowledge message:', error);
          });
      }
    }

    return events;
  }

  private async retrieveJob<JobName extends JobNames<Payload, QueueName>>(
    { jobId, jobName }: { jobId?: string; jobName?: JobName },
    fromState: 'waiting' | 'active' = 'waiting',
  ): Promise<Job<Payload, QueueName, JobName> | null> {
    try {
      let id = jobId;

      if (fromState === 'waiting') {
        const activeCount = await this.redisClient.lLen(this.keys.active);

        if (this.concurrency > 0 && activeCount >= this.concurrency) {
          return null;
        }

        if (!id) {
          id =
            (await this.redisClient.pop(
              this.keys.waiting,
              this.jobId,
              jobName,
            )) ?? undefined;
        }
      }

      if (!id) return null;

      const jobData = await this.redisClient.getJobData<JobName>(id);

      if (!jobData) return null;

      const job = this.createJob<JobName>(jobData, 'active', id);

      await this.redisClient.moveJob(
        id,
        job.prepare(),
        {
          from: this.keys[fromState],
          to: this.keys.active,
        },
        this.jobId,
      );

      return job;
    } catch (error) {
      const idStr = jobId ?? '';

      throw new Error(
        `Failed to retrieve job${idStr} from ${fromState} state`,
        { cause: error },
      );
    }
  }

  /**
   * Check if a job matches the filter criteria
   * @param {string} jobId - The ID of the job to check
   * @param {string} jobName - The name of the job to filter by
   * @returns {boolean} Whether the job matches the filter
   * @private
   */
  private checkJobFilter = (jobId: string, jobName: string): boolean => {
    if (!jobName) return true;

    try {
      const name = this.jobId.getJobName(jobId);

      return name === jobName;
    } catch {
      return false;
    }
  };
}
