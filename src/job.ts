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

    return queue.redisClient.getJobData<UnpackJobName>(id).pipe(
      Effect.map(jobData => {
        if (!jobData) {
          return null;
        }

        const job = new Job<UnpackPayload, UnpackQueueName, UnpackJobName>({
          queue,
          name: jobData.name,
          payload:
            jobData.payload as UnpackPayload[UnpackQueueName][UnpackJobName],
          state: jobData.state,
          id,
          createdAt: jobData.createdAt,
          updatedAt: jobData.updatedAt,
        });

        job.attempts = Number(jobData.attempts ?? 0);
        job.progress = Number(jobData.progress ?? 0);
        job.processedAt = jobData.processedAt ?? null;
        job.failedReason = jobData.failedReason ?? null;

        if (jobData.stacktrace) {
          try {
            job.stacktrace = JSON.parse(jobData.stacktrace) as Array<string>;
          } catch {
            job.stacktrace = [];
          }
        }

        return job;
      }),
      Effect.catchAll(error =>
        Effect.fail(
          new Error(`Failed to unpack job ${id}`, {
            cause: error,
          }),
        ),
      ),
    );
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
   * Refreshes the job visibility lock while active.
   * Call this periodically for long-running jobs.
   */
  public heartbeatEffect = (): Effect.Effect<void, Error> => {
    return this.redisClient.extendActiveLock(
      this.id,
      this.queue.keys.locks,
      this.queue.visibilityTimeoutMs,
    );
  };

  public heartbeat = async (): Promise<void> => {
    await runEffect(this.heartbeatEffect());
  };

  /**
   * Saves the job to Redis and adds it to the waiting queue
   * @returns {Effect<Job<any, any, any>, Error>} Job persisted and ready for processing
   */
  public readonly saveEffect = (): Effect.Effect<
    Job<Payload, QueueName, JobName>,
    Error
  > => {
    const job = this;

    return Effect.gen(function* () {
      const savedJob = new Job<Payload, QueueName, JobName>({
        queue: job.queue,
        name: job.name,
        payload: job.payload,
        state: 'waiting',
        id: job.id,
        createdAt: job.createdAt,
        updatedAt: Date.now().toString(),
      });

      yield* job.redisClient.saveJob(
        job.id,
        savedJob.prepare(),
        job.queue.keys.waiting,
        job.queue.keys.events,
      );

      return savedJob;
    }).pipe(
      Effect.catchAll(error =>
        Effect.fail(
          new Error('Failed to save job', {
            cause: error,
          }),
        ),
      ),
    );
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
    const job = this;

    return Effect.gen(function* () {
      if (job.state === state) {
        return job;
      }

      if (job.state === 'waiting' && state === 'active') {
        throw new Error(
          'Cannot move job to active state from waiting state, use queue.process() instead',
        );
      }

      const oldState = job.state;
      const newJob = job.withState(state);

      yield* job.redisClient.moveJob(
        job.id,
        newJob.prepare(),
        {
          from: job.queue.keys[oldState],
          to: job.queue.keys[state],
        },
        job.queue.jobId,
      );

      return newJob;
    }).pipe(
      Effect.catchAll(error =>
        Effect.fail(
          new Error(`Failed to move job to state ${state}`, {
            cause: error,
          }),
        ),
      ),
    );
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
    const data: JobData = {
      name: this.name,
      payload: JSON.stringify(this.payload),
      queue: this.queue.name,
      createdAt: this.createdAt,
      updatedAt: this.updatedAt,
      state: this.state,
    };

    // Optional fields for production features.
    if (this.attempts > 0) {
      data.attempts = this.attempts.toString();
    }

    if (this.progress !== undefined) {
      data.progress = this.progress.toString();
    }

    if (this.processedAt) {
      data.processedAt = this.processedAt;
    }

    if (this.failedReason) {
      data.failedReason = this.failedReason;
    }

    if (this.stacktrace.length > 0) {
      data.stacktrace = JSON.stringify(this.stacktrace);
    }

    return data;
  };
}
