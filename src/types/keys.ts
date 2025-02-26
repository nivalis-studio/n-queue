import type { JobState } from './job';
import type { PayloadSchema, QueueNames } from './payload';

export type Keys = 'id' | JobState;
export type KeysMap<
  Payload extends PayloadSchema,
  QueueName extends QueueNames<Payload>,
> = { [index in Keys]: `${QueueName}:${Keys}` };

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
