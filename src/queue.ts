import { Job } from './job';
import { getKeysMap } from './types/keys';
import type { RedisClientType } from 'redis';
import type { KeysMap } from './types/keys';
import type { JobNames, PayloadSchema, QueueNames } from './types/payload';
import type { QueueOptions } from './types/queue';

export class Queue<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
> {
  public readonly keys: KeysMap<Payload, QueueName>;
  private concurrency: number;

  /**
   * Creates a new Queue instance
   * @param {string} name - The name of the queue
   * @param {() => Promise<RedisClientType>} getRedisClient - Function to get a Redis client
   * @param {QueueOptions} [options] - Optional configuration options
   */
  constructor(
    public readonly name: QueueName,
    public readonly getRedisClient: () => Promise<RedisClientType>,
    options?: QueueOptions,
  ) {
    this.keys = getKeysMap<Payload, QueueName>(name);
    this.concurrency = options?.concurrency ?? -1;
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
   * Takes the next job from the waiting queue and moves it to the active queue
   * @returns {Promise<Job<any, any, any> | null>} The next job or null if no jobs are available or concurrency limit is reached
   */
  take = async () => {
    const client = await this.getRedisClient();

    const activeCount = await client.lLen(this.keys.active);

    if (this.concurrency > 0 && activeCount >= this.concurrency) {
      return null;
    }

    const id = await client.rPop(this.keys.waiting);

    if (!id) return null;

    const job = await Job.unpack<Payload, QueueName>(this, id);

    if (!job?.id) return null;

    const activeJob = job.withState('active');
    const jobData = activeJob.prepare();

    const multi = client.multi();

    multi.hSet(id, jobData);
    multi.lPush(this.keys.active, id);

    await multi.exec();

    return activeJob;
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
    const client = await this.getRedisClient();

    const [waitingCount, activeCount, failedCount, completedCount] =
      await Promise.all([
        client.lLen(this.keys.waiting),
        client.lLen(this.keys.active),
        client.lLen(this.keys.failed),
        client.lLen(this.keys.completed),
      ]);

    return {
      name: this.name,
      concurrency: this.concurrency,
      waiting: waitingCount,
      active: activeCount,
      failed: failedCount,
      completed: completedCount,
      total: waitingCount + activeCount + failedCount + completedCount,
      availableSlots:
        this.concurrency === -1
          ? -1
          : Math.max(0, this.concurrency - activeCount),
    };
  };
}
