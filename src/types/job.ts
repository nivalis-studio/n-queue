export type JobState = 'waiting' | 'active' | 'failed' | 'completed';

export type JobData = {
  name: string;
  payload: string;
  queue: string;
  state: JobState;
  createdAt: string;
  updatedAt: string;
};

export type JobOptions = {
  state?: JobState;
  createdAt?: string;
  updatedAt?: string;
};
