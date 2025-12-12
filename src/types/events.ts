export type RedisStreamEventType =
  | 'saved'
  | 'active'
  | 'completed'
  | 'failed'
  | 'retrying'
  | 'delayed'
  | 'stalled'
  | 'progress';

/**
 * Events that can be emitted by the Redis stream
 */
export type RedisStreamEvents = {
  type: RedisStreamEventType;
  id: string;
};

/**
 * Public event type union.
 */
export type JobEventType = RedisStreamEventType;

/**
 * Events that can be emitted by the queue
 */
export type JobEvent = {
  eventType: string;
  jobId: string;
};
