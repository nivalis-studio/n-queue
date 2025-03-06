type RedisStreamEventType = 'saved' | 'failed' | 'completed' | 'progress';

/**
 * Events that can be emitted by the Redis stream
 */
export type RedisStreamEvents = {
  type: RedisStreamEventType;
  id: string;
};

/**
 * Events that can be emitted by the queue
 */
export type JobEvent = {
  eventType: string;
  jobId: string;
};
