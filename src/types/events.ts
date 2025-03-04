type RedisStreamEventType = 'saved' | 'failed' | 'completed' | 'progress';

export type RedisStreamEvents = {
  type: RedisStreamEventType;
  id: string;
};
