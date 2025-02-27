import { v4 as uuid } from 'uuid';
import type { Queue } from './queue';
import type { RedisClient } from './redis-client';
import type { JobConfig, JobData, JobState } from './types/job';
import type { JobNames, PayloadSchema, QueueNames } from './types/payload';

export class Job<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
  JobName extends JobNames<Payload, QueueName>,
> {
  /**
   * The name of the job.
   */
  public readonly name: JobName;

  /**
   * The unique ID of the job in redis.
   */
  public readonly id: string;

  /**
   * The current state of the job.
   */
  public readonly state: JobState;

  /**
   * The time the job was created.
   */
  public readonly createdAt: string;

  /**
   * The time the job was last updated.
   */
  public readonly updatedAt: string;

  /**
   * Timestamp for when the job finished (completed or failed).
   */
  public processedAt: string | null = null;

  /**
   * The progress a job has performed so far.
   * @default 0
   */
  public progress = 0;

  /**
   * Ranges from 0 (highest priority) to 2 097 152 (lowest priority). Note that
   * using priorities has a slight impact on performance,
   * so do not use it if not required.
   * @default 0
   */
  public priority = 0;

  /**
   * Number of attempts after the job has failed.
   * @default 0
   */
  public attempts = 0;

  /**
   * Stacktrace for the error (for failed jobs).
   */
  public stacktrace: string[] = [];

  /**
   * The reason for the job failing (for failed jobs).
   */
  public failedReason: string | null = null;

  /**
   * The job data.
   */
  public readonly payload: Payload[QueueName][JobName];

  private readonly queue: Queue<Payload, QueueName>;
  private readonly redisClient: RedisClient;

  /**
   * Creates a new Job instance
   * @param {object} config - The job configuration
   */
  constructor(config: JobConfig<Payload, QueueName, JobName>) {
    this.name = config.name;
    this.state = config.state ?? 'waiting';
    this.id = config.id ?? uuid();
    this.createdAt = config.createdAt ?? Date.now().toString();
    this.updatedAt = config.updatedAt ?? Date.now().toString();
    this.queue = config.queue;
    this.payload = config.payload;
    this.redisClient = config.queue.redisClient;
  }

  /**
   * Unpacks a job from Redis by its id.
   * @template T, U
   * @param {Queue<T, U>} queue - The queue the job belongs to
   * @param {string} id - The id of the job to unpack
   * @returns {Promise<Job<T, U, JobNames<T, U>> | null>} The unpacked job or null if not found
   */
  static unpack = async <
    SPayload extends PayloadSchema,
    SQueueName extends QueueNames<SPayload>,
  >(
    queue: Queue<SPayload, SQueueName>,
    id: string,
  ) => {
    try {
      const jobData = await queue.redisClient.get(id);

      if (!jobData?.name || !jobData.payload) return null;

      const jobName = jobData.name as JobNames<SPayload, SQueueName>;
      const payload = JSON.parse(
        jobData.payload,
      ) as SPayload[SQueueName][typeof jobName];

      return new Job<SPayload, SQueueName, typeof jobName>({
        queue,
        name: jobName,
        payload,
        state: jobData.state as JobState,
        id,
        createdAt: jobData.createdAt,
        updatedAt: jobData.updatedAt,
      });
    } catch (error) {
      console.error(`Failed to unpack job ${id}:`, error);

      return null;
    }
  };

  /**
   * Create a new Job instance with a different state
   * @param {JobState} state - The new state to assign to the job
   * @returns {Job<any, any, any>} A new Job instance with the updated state
   * @throws {Error} If the job doesn't have an id
   */
  withState(state: JobState): Job<Payload, QueueName, JobName> {
    const job = new Job<Payload, QueueName, JobName>({
      queue: this.queue,
      name: this.name,
      payload: this.payload,
      state,
      id: this.id,
      createdAt: this.createdAt,
      updatedAt: Date.now().toString(),
    });

    if (state === 'failed') {
      job.processedAt = this.updatedAt;
    }

    if (state === 'completed') {
      job.processedAt = this.updatedAt;
      job.progress = 1;
    }

    return job;
  }

  /**
   * Saves the job to Redis and adds it to the waiting queue
   * @returns {Promise<Job<any, any, any>>} A new Job instance with an id and waiting state
   */
  save = async (): Promise<Job<Payload, QueueName, JobName>> => {
    try {
      const savedJob = new Job<Payload, QueueName, JobName>({
        queue: this.queue,
        name: this.name,
        payload: this.payload,
        state: 'waiting',
        id: this.id,
        createdAt: this.createdAt,
        updatedAt: Date.now().toString(),
      });

      await this.redisClient.createJob(
        this.id,
        savedJob.prepare(),
        this.queue.keys.waiting,
      );

      return savedJob;
    } catch (error) {
      console.error(error);
      throw new Error('Failed to save job');
    }
  };

  /**
   * Moves the job to a different state
   * @param {JobState} state - The new state to move the job to
   * @returns {Promise<Job<any, any, any>>} A new Job instance with the updated state
   */
  move = async (state: JobState): Promise<Job<Payload, QueueName, JobName>> => {
    try {
      if (this.state === state) return this;

      if (this.state === 'waiting' && state === 'active') {
        const activeJob = await this.queue.take();

        return (activeJob as Job<Payload, QueueName, JobName>) ?? this;
      }

      const oldState = this.state;
      const newJob = this.withState(state);

      await this.redisClient.moveJob(
        this.id,
        newJob.prepare(),
        this.queue.keys[oldState],
        this.queue.keys[state],
      );

      return newJob;
    } catch (error) {
      console.error(error);
      throw new Error(`Failed to move job to state ${state}`);
    }
  };

  /**
   * Prepares the job data for storage in Redis
   * @returns {JobData} The job data ready for storage
   */
  prepare = (): JobData => {
    return {
      name: this.name,
      payload: JSON.stringify(this.payload),
      queue: this.queue.name,
      createdAt: this.createdAt,
      updatedAt: this.updatedAt,
      state: this.state,
    };
  };
}
