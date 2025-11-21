type SerializableValue =
  | string
  | number
  | boolean
  | null
  | Array<SerializableValue>
  | { [key: string]: SerializableValue };

export type PayloadSchema = {
  [queueName: string]: { [jobName: string]: SerializableValue };
};

export type QueueNames<Payload extends PayloadSchema> = keyof Payload & string;

export type JobNames<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
> = keyof Payload[QueueName] & string;

export type JobPayloads<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
> = Payload[QueueName][JobNames<Payload, QueueName>];
