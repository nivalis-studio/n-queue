import type { JobId } from './job-id';
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
  private readonly jobId: JobId;
  private readonly redisClient: RedisClient<Payload, QueueName>;

  /**
   * Creates a new Job instance
   * @param {object} config - The job configuration
   */
  constructor(config: JobConfig<Payload, QueueName, JobName>) {
    this.name = config.name;
    this.queue = config.queue;
    this.jobId = config.queue.jobId;
    this.payload = config.payload;
    this.redisClient = config.queue.redisClient;

    this.state = config.state ?? 'waiting';

    const now = Date.now().toString();

    this.createdAt = config.createdAt ?? now;
    this.updatedAt = config.updatedAt ?? now;

    this.id = config.id ?? this.jobId.generate(this.name);
  }

  /**
   * Unpacks a job from Redis by its id.
   * @template T, U
   * @param {Queue<T, U>} queue - The queue the job belongs to
   * @param {string} id - The id of the job to unpack
   * @returns {Promise<Job<T, U, JobNames<T, U>> | null>} The unpacked job or null if not found
   */
  static async unpack<
    Payload extends PayloadSchema,
    QueueName extends QueueNames<Payload>,
    JobName extends JobNames<Payload, QueueName>,
  >(
    queue: Queue<Payload, QueueName>,
    id: string,
  ): Promise<Job<Payload, QueueName, JobName> | null> {
    if (!queue.jobId.isValid(id)) {
      throw new Error(`Invalid job ID format: ${id}`);
    }

    const jobData = await queue.redisClient.getJobData<JobName>(id);

    if (!jobData) return null;

    return new Job<Payload, QueueName, JobName>({
      queue,
      name: jobData.name,
      payload: jobData.payload as Payload[QueueName][JobName],
      state: jobData.state,
      id,
      createdAt: jobData.createdAt,
      updatedAt: jobData.updatedAt,
    });
  }

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

      await this.redisClient.saveJob(
        this.id,
        savedJob.prepare(),
        this.queue.keys.waiting,
        this.queue.keys.events,
      );

      return savedJob;
    } catch (error) {
      throw new Error('Failed to save job', {
        cause: error,
      });
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
        throw new Error(
          'Cannot move job to active state from waiting state, use queue.process() instead',
        );
      }

      const oldState = this.state;
      const newJob = this.withState(state);

      await this.redisClient.moveJob(
        this.id,
        newJob.prepare(),
        {
          from: this.queue.keys[oldState],
          to: this.queue.keys[state],
        },
        this.queue.jobId,
      );

      return newJob;
    } catch (error) {
      throw new Error(`Failed to move job to state ${state}`, {
        cause: error,
      });
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
