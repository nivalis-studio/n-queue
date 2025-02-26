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

  constructor(
    public readonly name: QueueName,
    public readonly getRedisClient: () => Promise<RedisClientType>,
    options?: QueueOptions,
  ) {
    this.keys = getKeysMap<Payload, QueueName>(name);
    this.concurrency = options?.concurrency ?? -1;
  }

  add = async <JobName extends JobNames<Payload, QueueName>>(
    jobName: JobName,
    payload: Payload[QueueName][JobName],
  ): Promise<Job<Payload, QueueName, JobName>> => {
    const job = new Job<Payload, QueueName, JobName>(this, jobName, payload);

    await job.save();

    return job;
  };

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

    job.state = 'active';
    job.updatedAt = Date.now().toString();

    const jobData = job.prepare();

    const multi = client.multi();

    multi.hSet(job.id, jobData);
    multi.lPush(this.keys.active, job.id);

    await multi.exec();

    return job;
  };
}
