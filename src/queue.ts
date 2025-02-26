import { Job } from './job';
import { getKeysMap } from './types/keys';
import type { KeysMap } from './types/keys';
import type { JobNames, PayloadSchema, QueueNames } from './types/payload';
import type { RedisClientType } from 'redis';

export class Queue<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
> {
  public readonly keys: KeysMap<Payload, QueueName>;

  constructor(
    public readonly name: QueueName,
    public readonly getRedisClient: () => Promise<RedisClientType>,
  ) {
    this.keys = getKeysMap<Payload, QueueName>(name);
  }

  add = async <JobName extends JobNames<Payload, QueueName>>(
    jobName: JobName,
    payload: Payload[QueueName][JobName],
  ) => {
    const job = new Job<Payload, QueueName, JobName>(this, jobName, payload);

    await job.save();

    return job;
  };

  take = async () => {
    const client = await this.getRedisClient();
    const id = await client.rPop(this.keys.waiting);

    if (!id) return null;

    const job = await Job.unpack<Payload, QueueName>(this, id);

    if (!job) return null;

    await job.move('active');

    return job;
  };
}
