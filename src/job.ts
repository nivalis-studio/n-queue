import type { Queue } from './queue';
import type { JobConfig, JobData, JobState } from './types/job';
import type { JobNames, PayloadSchema, QueueNames } from './types/payload';

export class Job<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
  JobName extends JobNames<Payload, QueueName>,
> {
  public readonly name: JobName;
  public readonly state: JobState;
  public readonly id: string | null;
  public readonly createdAt: string;
  public readonly updatedAt: string;
  private readonly queue: Queue<Payload, QueueName>;
  private readonly payload: Payload[QueueName][JobName];

  /**
   * Creates a new Job instance
   * @param {object} config - The job configuration
   */
  constructor(config: JobConfig<Payload, QueueName, JobName>) {
    this.name = config.name;
    this.state = config.state ?? 'waiting';
    this.id = config.id ?? null;
    this.createdAt = config.createdAt ?? Date.now().toString();
    this.updatedAt = config.updatedAt ?? Date.now().toString();
    this.queue = config.queue;
    this.payload = config.payload;
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
    const redisClient = await queue.getRedisClient();
    const jobData = await redisClient.hGetAll(id);

    if (!jobData.name || !jobData.payload) return null;

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
  };

  /**
   * Create a new Job instance with a different state
   * @param {JobState} state - The new state to assign to the job
   * @returns {Job<any, any, any>} A new Job instance with the updated state
   * @throws {Error} If the job doesn't have an id
   */
  withState(state: JobState): Job<Payload, QueueName, JobName> {
    if (!this.id) {
      throw new Error('Cannot change state of a job without an id');
    }

    return new Job<Payload, QueueName, JobName>({
      queue: this.queue,
      name: this.name,
      payload: this.payload,
      state,
      id: this.id,
      createdAt: this.createdAt,
      updatedAt: Date.now().toString(),
    });
  }

  /**
   * Saves the job to Redis and adds it to the waiting queue
   * @returns {Promise<Job<any, any, any>>} A new Job instance with an id and waiting state
   */
  save = async (): Promise<Job<Payload, QueueName, JobName>> => {
    const client = await this.queue.getRedisClient();

    const jobId = await client.incr(this.queue.keys.id);
    const id = jobId.toString();

    const savedJob = new Job<Payload, QueueName, JobName>({
      queue: this.queue,
      name: this.name,
      payload: this.payload,
      state: 'waiting',
      id,
      createdAt: this.createdAt,
      updatedAt: Date.now().toString(),
    });

    const multi = client.multi();

    multi.hSet(id, savedJob.prepare());
    multi.lPush(this.queue.keys.waiting, id);

    await multi.exec();

    return savedJob;
  };

  /**
   * Moves the job to a different state
   * @param {JobState} state - The new state to move the job to
   * @returns {Promise<Job<any, any, any>>} A new Job instance with the updated state
   */
  move = async (state: JobState): Promise<Job<Payload, QueueName, JobName>> => {
    if (!this.id) return this;

    if (this.state === state) return this;

    if (this.state === 'waiting' && state === 'active') {
      const activeJob = await this.queue.take();

      return (activeJob as Job<Payload, QueueName, JobName>) ?? this;
    }

    const client = await this.queue.getRedisClient();

    const oldState = this.state;
    const newJob = this.withState(state);

    const multi = client.multi();

    multi.hSet(this.id, newJob.prepare());
    multi.lRem(this.queue.keys[oldState], 0, this.id);
    multi.lPush(this.queue.keys[state], this.id);

    await multi.exec();

    return newJob;
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
