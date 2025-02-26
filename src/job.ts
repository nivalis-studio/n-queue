import type { Queue } from './queue';
import type { JobData, JobOptions, JobState } from './types/job';
import type { JobNames, PayloadSchema, QueueNames } from './types/payload';

export class Job<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
  JobName extends JobNames<Payload, QueueName>,
> {
  public id: string | null = null;
  public state: JobState;
  public createdAt: string;
  public updatedAt: string;

  constructor(
    private queue: Queue<Payload, QueueName>,
    public name: JobName,
    private payload: Payload[QueueName][JobName],
    options?: JobOptions,
  ) {
    this.state = options?.state ?? 'waiting';
    this.createdAt = options?.createdAt ?? Date.now().toString();
    this.updatedAt = options?.updatedAt ?? Date.now().toString();
  }

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

    return new Job<SPayload, SQueueName, typeof jobName>(
      queue,
      jobName,
      payload,
      {
        state: jobData.state as JobState,
        createdAt: jobData.createdAt,
        updatedAt: jobData.updatedAt,
      },
    );
  };

  save = async () => {
    const client = await this.queue.getRedisClient();

    const jobId = await client.incr(this.queue.keys.id);

    this.id = jobId.toString();
    this.state = 'waiting';
    this.updatedAt = Date.now().toString();

    const multi = client.multi();

    multi.hSet(this.id, this.prepare());
    multi.lPush(this.queue.keys.waiting, this.id);

    await multi.exec();

    return this.id;
  };

  move = async (state: JobState) => {
    if (!this.id) return;

    if (this.state === state) return;

    if (this.state === 'waiting' && state === 'active') {
      return await this.queue.take();
    }

    const client = await this.queue.getRedisClient();

    const oldState = this.state;

    this.state = state;
    this.updatedAt = Date.now().toString();

    const multi = client.multi();

    multi.hSet(this.id, this.prepare());
    multi.lRem(this.queue.keys[oldState], 0, this.id);
    multi.lPush(this.queue.keys[state], this.id);

    await multi.exec();

    return this;
  };

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
