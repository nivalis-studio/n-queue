import { Effect } from 'effect';
import { runEffect } from './lib/run-effect';
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
  public stacktrace: Array<string> = [];

  /**
   * The reason for the job failing (for failed jobs).
   */
  public failedReason: string | null = null;

  /**
   * The job data.
   */
  public readonly payload: Payload[QueueName][JobName];

  private readonly queue: Queue<Payload, QueueName>;
  private readonly redisClient: RedisClient<Payload, QueueName>;

  /**
   * Creates a new Job instance
   * @param {object} config - The job configuration
   */
  public constructor(config: JobConfig<Payload, QueueName, JobName>) {
    this.name = config.name;
    this.queue = config.queue;
    this.payload = config.payload;
    this.redisClient = config.queue.redisClient;

    this.state = config.state ?? 'waiting';

    const now = Date.now().toString();

    this.createdAt = config.createdAt ?? now;
    this.updatedAt = config.updatedAt ?? now;

    this.id = config.id ?? this.queue.jobId.generate(this.name);
  }

  /**
   * Unpacks a job from Redis by its id.
   * @template T, U
   * @param {Queue<T, U>} queue - The queue the job belongs to
   * @param {string} id - The id of the job to unpack
   * @returns {Promise<Job<T, U, JobNames<T, U>> | null>} The unpacked job or null if not found
   */
  public static unpackEffect<
    UnpackPayload extends PayloadSchema,
    UnpackQueueName extends QueueNames<UnpackPayload>,
    UnpackJobName extends JobNames<UnpackPayload, UnpackQueueName>,
  >(
    queue: Queue<UnpackPayload, UnpackQueueName>,
    id: string,
  ): Effect.Effect<
    Job<UnpackPayload, UnpackQueueName, UnpackJobName> | null,
    Error
  > {
    if (!queue.jobId.isValid(id)) {
      return Effect.fail(new Error(`Invalid job ID format: ${id}`));
    }

    return Effect.tryPromise({
      try: async () => {
        const jobData = await queue.redisClient.getJobData<UnpackJobName>(id);

        if (!jobData) {
          return null;
        }

        return new Job<UnpackPayload, UnpackQueueName, UnpackJobName>({
          queue,
          name: jobData.name,
          payload:
            jobData.payload as UnpackPayload[UnpackQueueName][UnpackJobName],
          state: jobData.state,
          id,
          createdAt: jobData.createdAt,
          updatedAt: jobData.updatedAt,
        });
      },
      catch: error =>
        new Error(`Failed to unpack job ${id}`, {
          cause: error,
        }),
    });
  }

  public static async unpack<
    UnpackPayload extends PayloadSchema,
    UnpackQueueName extends QueueNames<UnpackPayload>,
    UnpackJobName extends JobNames<UnpackPayload, UnpackQueueName>,
  >(
    queue: Queue<UnpackPayload, UnpackQueueName>,
    id: string,
  ): Promise<Job<UnpackPayload, UnpackQueueName, UnpackJobName> | null> {
    return await runEffect(
      Job.unpackEffect<UnpackPayload, UnpackQueueName, UnpackJobName>(
        queue,
        id,
      ),
    );
  }

  /**
   * Create a new Job instance with a different state
   * @param {JobState} state - The new state to assign to the job
   * @returns {Job<any, any, any>} A new Job instance with the updated state
   * @throws {Error} If the job doesn't have an id
   */
  public withState(state: JobState): Job<Payload, QueueName, JobName> {
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
   * @returns {Effect<Job<any, any, any>, Error>} Job persisted and ready for processing
   */
  public readonly saveEffect = (): Effect.Effect<
    Job<Payload, QueueName, JobName>,
    Error
  > => {
    return Effect.tryPromise({
      try: async () => {
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
      },
      catch: error =>
        new Error('Failed to save job', {
          cause: error,
        }),
    });
  };

  public save = async (): Promise<Job<Payload, QueueName, JobName>> => {
    return await runEffect(this.saveEffect());
  };

  /**
   * Moves the job to a different state
   * @param {JobState} state - The new state to move the job to
   * @returns {Effect<Job<any, any, any>, Error>} Job with the updated state
   */
  public moveEffect = (
    state: JobState,
  ): Effect.Effect<Job<Payload, QueueName, JobName>, Error> => {
    return Effect.tryPromise({
      try: async () => {
        if (this.state === state) {
          return this;
        }

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
      },
      catch: error =>
        new Error(`Failed to move job to state ${state}`, {
          cause: error,
        }),
    });
  };

  public move = async (
    state: JobState,
  ): Promise<Job<Payload, QueueName, JobName>> => {
    return await runEffect(this.moveEffect(state));
  };

  /**
   * Prepares the job data for storage in Redis
   * @returns {JobData} The job data ready for storage
   */
  public prepare = (): JobData => {
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
