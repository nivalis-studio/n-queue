import type { Queue } from '../queue';
import type { JobNames, PayloadSchema, QueueNames } from './payload';

export type JobState = 'waiting' | 'active' | 'failed' | 'completed';

export type JobData<
  Payload extends PayloadSchema = PayloadSchema,
  QueueName extends QueueNames<Payload> = QueueNames<Payload>,
  JobName extends JobNames<Payload, QueueName> = JobNames<Payload, QueueName>,
> = {
  name: JobName;
  payload: string;
  queue: string;
  state: JobState;
  createdAt: string;
  updatedAt: string;
};

export type JobConfig<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
  JobName extends JobNames<Payload, QueueName>,
> = {
  queue: Queue<Payload, QueueName>;
  name: JobName;
  payload: Payload[QueueName][JobName];
  state?: JobState;
  id?: string | null;
  createdAt?: string;
  updatedAt?: string;
};
