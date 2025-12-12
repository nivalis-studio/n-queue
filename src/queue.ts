/* eslint-disable no-await-in-loop */
import { Effect } from 'effect';
import { v4 as uuid } from 'uuid';
import { Job } from './job';
import { JobId } from './job-id';
import { runEffect } from './lib/run-effect';
import { RedisClient } from './redis-client';
import { getKeysMap } from './types/keys';
import type { RedisClientType } from 'redis';
import type { JobEvent, RedisStreamEvents } from './types/events';
import type { JobData } from './types/job';
import type { KeysMap } from './types/keys';
import type { JobNames, PayloadSchema, QueueNames } from './types/payload';
import type { QueueOptions } from './types/queue';

const DEFAULT_CONCURRENCY = -1;
const DEFAULT_VISIBILITY_TIMEOUT_MS = 300_000;
const DEFAULT_STALL_CHECK_INTERVAL_MS = 60_000;

const DEFAULT_RETRY_MAX_ATTEMPTS = 3;
const DEFAULT_RETRY_INITIAL_DELAY_MS = 1000;
const DEFAULT_RETRY_BACKOFF_FACTOR = 2;
const DEFAULT_RETRY_MAX_DELAY_MS = 30_000;
const DEFAULT_RETRY_JITTER = 0.2;

/**
 * Queue class for managing job processing
 * @template Payload - The payload schema type
 * @template QueueName - The queue name type
 */
export class Queue<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload> = QueueNames<Payload>,
> {
  public readonly name: QueueName;
  public readonly keys: KeysMap<Payload, QueueName>;
  public readonly redisClient: RedisClient<Payload, QueueName>;
  public readonly jobId: JobId;

  public readonly visibilityTimeoutMs: number;

  private readonly concurrency: number;
  private readonly stallCheckIntervalMs: number;
  private readonly retryMaxAttempts: number;
  private readonly retryBackoff: {
    initialDelay: number;
    factor: number;
    maxDelay: number;
    jitter: number;
  };
  private readonly logger: QueueOptions['logger'];
  private readonly metrics: QueueOptions['metrics'];

  private readonly groupName: string;
  private readonly consumerName: string;
  private isListening = false;
  private lastMaintenanceAt = 0;

  /**
   * Creates a new Queue instance
   * @param {QueueName} name - The name of the queue
   * @param {() => Promise<RedisClientType>} getRedisClient - Function to get a Redis client
   * @param {QueueOptions} [options] - Optional configuration options
   */
  public constructor(
    name: QueueName,
    getRedisClient: () => Promise<RedisClientType>,
    options?: QueueOptions,
  ) {
    this.name = name;
    this.keys = getKeysMap<Payload, QueueName>(name);
    this.concurrency = options?.concurrency ?? DEFAULT_CONCURRENCY;
    this.visibilityTimeoutMs =
      options?.visibilityTimeoutMs ?? DEFAULT_VISIBILITY_TIMEOUT_MS;
    this.stallCheckIntervalMs =
      options?.stallCheckIntervalMs ?? DEFAULT_STALL_CHECK_INTERVAL_MS;

    this.retryMaxAttempts =
      options?.retry?.maxAttempts ?? DEFAULT_RETRY_MAX_ATTEMPTS;
    this.retryBackoff = {
      initialDelay:
        options?.retry?.backoffStrategy?.initialDelay ??
        DEFAULT_RETRY_INITIAL_DELAY_MS,
      factor:
        options?.retry?.backoffStrategy?.factor ?? DEFAULT_RETRY_BACKOFF_FACTOR,
      maxDelay:
        options?.retry?.backoffStrategy?.maxDelay ?? DEFAULT_RETRY_MAX_DELAY_MS,
      jitter: options?.retry?.backoffStrategy?.jitter ?? DEFAULT_RETRY_JITTER,
    };

    this.logger = options?.logger;
    this.metrics = options?.metrics;

    this.redisClient = new RedisClient(getRedisClient, options);
    const consumerId = uuid();

    this.jobId = new JobId(':', this.name);
    this.groupName = `${this.name}:events`;
    this.consumerName = `${this.name}:${consumerId}`;
  }

  /**
   * Adds a new job to the queue
   * @template JobName - The name of the job
   * @param {JobName} jobName - The name of the job
   * @param {Payload[QueueName][JobName]} payload - The job payload
   * @returns {Promise<Job>} The created job
   */
  public add = async <JobName extends JobNames<Payload, QueueName>>(
    jobName: JobName,
    payload: Payload[QueueName][JobName],
  ): Promise<Job<Payload, QueueName, JobName>> => {
    return await runEffect(this.createAddEffect<JobName>(jobName, payload));
  };

  private createAddEffect<JobName extends JobNames<Payload, QueueName>>(
    jobName: JobName,
    payload: Payload[QueueName][JobName],
  ): Effect.Effect<Job<Payload, QueueName, JobName>, Error> {
    const queue = this;

    return Effect.gen(function* () {
      const job = new Job<Payload, QueueName, JobName>({
        queue,
        name: jobName,
        payload,
      });

      return yield* job.saveEffect();
    });
  }

  /**
   * Get a job by id
   * @template JobName - The name of the job
   * @param {string} id - The id of the job to get
   * @returns {Promise<Job<Payload, QueueName, JobName> | null>} The job or null if not found
   */
  public get = async <
    JobName extends JobNames<Payload, QueueName> = JobNames<Payload, QueueName>,
  >(
    id: string,
  ): Promise<Job<Payload, QueueName, JobName> | null> => {
    return await runEffect(this.createGetEffect<JobName>(id));
  };

  private createGetEffect<
    JobName extends JobNames<Payload, QueueName> = JobNames<Payload, QueueName>,
  >(id: string): Effect.Effect<Job<Payload, QueueName, JobName> | null, Error> {
    return Job.unpackEffect<Payload, QueueName, JobName>(this, id);
  }

  /**
   * Listen for job events from the queue
   * @param {string} [jobName] - Optional job name to filter events by
   * @yields {JobEvent} The job events from the queue
   */
  public async *listen(jobName?: string): AsyncGenerator<JobEvent> {
    this.isListening = true;

    while (this.isListening) {
      // biome-ignore lint/performance/noAwaitInLoops: sequential polling keeps ordering predictable
      const response = await runEffect(
        this.redisClient.listen(
          this.keys.events,
          this.groupName,
          this.consumerName,
        ),
      );

      if (!response) {
        continue;
      }

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
  public async getStats(): Promise<{
    waiting: number;
    active: number;
    completed: number;
    failed: number;
  }> {
    const waiting = await runEffect(this.redisClient.lLen(this.keys.waiting));
    const active = await runEffect(this.redisClient.lLen(this.keys.active));
    const completed = await runEffect(
      this.redisClient.lLen(this.keys.completed),
    );
    const failed = await runEffect(this.redisClient.lLen(this.keys.failed));

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
  public async process<JobName extends JobNames<Payload, QueueName>>(
    fn: (job: Job<Payload, QueueName, JobName>) => void | Promise<void>,
    { jobName, jobId }: { jobName?: JobName; jobId?: string },
  ): Promise<void> {
    const job = await this.retrieveJob<JobName>({ jobId, jobName });

    if (!job) {
      return;
    }

    try {
      await fn(job);

      await job.move('completed');
      await runEffect(
        this.redisClient.removeActiveLock(job.id, this.keys.locks),
      );

      await runEffect(
        this.redisClient.executeMulti(multi => {
          multi.xAdd(this.keys.events, '*', {
            type: 'completed',
            id: job.id,
          } satisfies RedisStreamEvents);
        }),
      );

      this.metrics?.increment?.('queue_job_completed', {
        queue: this.name,
        job: job.name,
      });
    } catch (error) {
      job.attempts += 1;
      job.failedReason = error instanceof Error ? error.message : String(error);
      job.processedAt = Date.now().toString();
      job.stacktrace =
        error instanceof Error && error.stack ? error.stack.split('\n') : [];

      if (job.attempts < this.retryMaxAttempts) {
        const attemptIndex = job.attempts;
        const rawDelay =
          this.retryBackoff.initialDelay *
          this.retryBackoff.factor ** (attemptIndex - 1);
        const cappedDelay = Math.min(rawDelay, this.retryBackoff.maxDelay);
        const jitter = this.retryBackoff.jitter;
        const jitterFactor = 1 + (Math.random() * 2 - 1) * jitter;
        const delayMs = Math.max(0, Math.floor(cappedDelay * jitterFactor));

        await runEffect(
          this.redisClient.requeueJobWithDelay({
            id: job.id,
            jobData: job.prepare(),
            activeKey: this.keys.active,
            locksKey: this.keys.locks,
            delayedKey: this.keys.delayed,
            eventsKey: this.keys.events,
            delayMs,
          }),
        );

        this.logger?.warn?.('Job failed, scheduled retry', {
          id: job.id,
          attempts: job.attempts,
          delayMs,
          cause: error,
        });

        this.metrics?.increment?.('queue_job_retry', {
          queue: this.name,
          job: job.name,
        });

        return;
      }

      await job.move('failed');
      await runEffect(
        this.redisClient.removeActiveLock(job.id, this.keys.locks),
      );

      await runEffect(
        this.redisClient.executeMulti(multi => {
          multi.xAdd(this.keys.events, '*', {
            type: 'failed',
            id: job.id,
          } satisfies RedisStreamEvents);
        }),
      );

      this.metrics?.increment?.('queue_job_failed', {
        queue: this.name,
        job: job.name,
      });

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
  public async stream<JobName extends JobNames<Payload, QueueName>>(
    fn: (job: Job<Payload, QueueName, JobName>) => void | Promise<void>,
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
    const job = new Job<Payload, QueueName, JobName>({
      queue: this,
      name: jobData.name,
      payload: jobData.payload as Payload[QueueName][JobName],
      state,
      id,
      createdAt: jobData.createdAt,
      updatedAt: Date.now().toString(),
    });

    // Hydrate optional production fields.
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
  ): Array<JobEvent> {
    if (!this.isListening) {
      return [];
    }

    const events: Array<JobEvent> = [];

    for (const { messages } of response) {
      for (const { id, message } of messages) {
        events.push({
          eventType: message.type,
          jobId: message.id,
        });

        runEffect(
          this.redisClient.ackMessage(this.keys.events, this.groupName, id),
        ).catch(error => {
          this.logger?.error?.('Failed to acknowledge message', { error });
        });
      }
    }

    return events;
  }

  private async retrieveJob<JobName extends JobNames<Payload, QueueName>>(
    { jobId, jobName }: { jobId?: string; jobName?: JobName },
    fromState: 'waiting' | 'active' = 'waiting',
  ): Promise<Job<Payload, QueueName, JobName> | null> {
    return await runEffect(
      this.retrieveJobEffect<JobName>({ jobId, jobName }, fromState),
    );
  }

  private canProcessWaitingJobEffect(): Effect.Effect<boolean, Error> {
    if (this.concurrency <= 0) {
      return Effect.succeed(true);
    }

    const queue = this;

    return Effect.gen(function* () {
      const activeCount = yield* queue.redisClient.lLen(queue.keys.active);
      return activeCount < queue.concurrency;
    });
  }

  private runMaintenanceIfDueEffect(): Effect.Effect<void, Error> {
    const queue = this;

    return Effect.gen(function* () {
      const now = Date.now();

      if (now - queue.lastMaintenanceAt < queue.stallCheckIntervalMs) {
        return;
      }

      queue.lastMaintenanceAt = now;

      yield* queue.redisClient.promoteDueDelayed(
        queue.keys.delayed,
        queue.keys.waiting,
        queue.keys.events,
      );

      yield* queue.redisClient.recoverStalled({
        activeKey: queue.keys.active,
        waitingKey: queue.keys.waiting,
        locksKey: queue.keys.locks,
        eventsKey: queue.keys.events,
      });
    }).pipe(Effect.asVoid);
  }

  private claimAnyWaitingJobEffect<
    JobName extends JobNames<Payload, QueueName>,
  >(): Effect.Effect<Job<Payload, QueueName, JobName> | null, Error> {
    const queue = this;

    return Effect.gen(function* () {
      const id = yield* queue.redisClient.claimJob({
        waitingKey: queue.keys.waiting,
        activeKey: queue.keys.active,
        locksKey: queue.keys.locks,
        eventsKey: queue.keys.events,
        visibilityTimeoutMs: queue.visibilityTimeoutMs,
      });

      if (!id) {
        return null;
      }

      const claimedData = yield* queue.redisClient.getJobData<JobName>(id);

      if (!claimedData) {
        return null;
      }

      return queue.createJob<JobName>(claimedData, 'active', id);
    });
  }

  private popWaitingJobIdEffect<JobName extends JobNames<Payload, QueueName>>(
    jobName?: JobName,
  ): Effect.Effect<string | null, Error> {
    return this.redisClient.pop(this.keys.waiting, this.jobId, jobName);
  }

  private activateJobByIdEffect<JobName extends JobNames<Payload, QueueName>>(
    id: string,
    fromState: 'waiting' | 'active',
  ): Effect.Effect<Job<Payload, QueueName, JobName> | null, Error> {
    const queue = this;

    return Effect.gen(function* () {
      const jobData = yield* queue.redisClient.getJobData<JobName>(id);

      if (!jobData) {
        return null;
      }

      const job = queue.createJob<JobName>(jobData, 'active', id);

      if (fromState === 'active') {
        return job;
      }

      yield* queue.redisClient.moveJob(
        id,
        job.prepare(),
        {
          from: queue.keys[fromState],
          to: queue.keys.active,
        },
        queue.jobId,
      );

      yield* queue.redisClient.extendActiveLock(
        id,
        queue.keys.locks,
        queue.visibilityTimeoutMs,
      );

      yield* queue.redisClient.executeMulti(multi => {
        multi.xAdd(queue.keys.events, '*', {
          type: 'active',
          id,
        } satisfies RedisStreamEvents);
      });

      return job;
    });
  }

  private retrieveJobEffect<JobName extends JobNames<Payload, QueueName>>(
    { jobId, jobName }: { jobId?: string; jobName?: JobName },
    fromState: 'waiting' | 'active' = 'waiting',
  ): Effect.Effect<Job<Payload, QueueName, JobName> | null, Error> {
    const queue = this;

    return Effect.gen(function* () {
      let id = jobId;

      if (fromState === 'waiting') {
        const canStart = yield* queue.canProcessWaitingJobEffect();

        if (!canStart) {
          return null;
        }

        yield* queue.runMaintenanceIfDueEffect();

        // Fast path: atomically claim the next job if no name filter.
        if (!(id || jobName)) {
          return yield* queue.claimAnyWaitingJobEffect<JobName>();
        }

        if (!id) {
          const popped = yield* queue.popWaitingJobIdEffect(jobName);
          id = popped ?? undefined;
        }
      }

      if (!id) {
        return null;
      }

      return yield* queue.activateJobByIdEffect<JobName>(id, fromState);
    }).pipe(
      Effect.catchAll(error =>
        Effect.fail(
          new Error(
            `Failed to retrieve job${jobId ?? ''} from ${fromState} state`,
            { cause: error },
          ),
        ),
      ),
    );
  }

  /**
   * Check if a job matches the filter criteria
   * @param {string} jobId - The ID of the job to check
   * @param {string} jobName - The name of the job to filter by
   * @returns {boolean} Whether the job matches the filter
   * @private
   */
  private readonly checkJobFilter = (
    jobId: string,
    jobName: string,
  ): boolean => {
    if (!jobName) {
      return true;
    }

    try {
      const name = this.jobId.getJobName(jobId);

      return name === jobName;
    } catch {
      return false;
    }
  };
}
