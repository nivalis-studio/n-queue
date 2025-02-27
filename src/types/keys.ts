import type { JobState } from './job';
import type { PayloadSchema, QueueNames } from './payload';

export type Keys = 'id' | JobState;

export type QueueKeys<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
  Key extends Keys,
> = `${QueueName}:${Key}`;

export type KeysMap<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
> = { [Key in Keys]: QueueKeys<Payload, QueueName, Key> };

export const getKeysMap = <
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
>(
  name: QueueName,
): KeysMap<Payload, QueueName> => ({
  id: `${name}:id`,
  waiting: `${name}:waiting`,
  active: `${name}:active`,
  failed: `${name}:failed`,
  completed: `${name}:completed`,
});
