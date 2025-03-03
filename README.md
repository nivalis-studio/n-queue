# @nivalis/n-queue

A lightweight, Redis-backed job queue system for Next.js applications.

## Features

- Simple, type-safe API for job queue management
- Redis-backed for persistence and reliability
- Support for multiple queues with different job types
- Concurrency control for parallel job processing
- Comprehensive error handling
- TypeScript support with full type safety

## Architecture

The system consists of three main components:

1. **Queue**: Manages a collection of jobs with the same queue name
2. **Job**: Represents a unit of work with payload data
3. **RedisClient**: Handles Redis operations with error handling

## Installation

```bash
npm install @nivalis/n-queue
```

## Usage

### Basic Example

```typescript
import { createClient } from 'redis';
import { Queue } from '@nivalis/n-queue';

// Define your payload schema
type MyPayload = {
  emailQueue: {
    sendEmail: {
      to: string;
      subject: string;
      body: string;
    };
    sendNotification: {
      userId: string;
      message: string;
    };
  };
};

// Create a Redis client factory
const getRedisClient = async () => {
  const client = createClient({
    url: 'redis://localhost:6379',
  });

  if (!client.isOpen) {
    await client.connect();
  }

  return client;
};

// Create a queue
const emailQueue = new Queue<MyPayload, 'emailQueue'>(
  'emailQueue',
  getRedisClient,
  { concurrency: 5 }
);

// Add a job to the queue
await emailQueue.add('sendEmail', {
  to: 'user@example.com',
  subject: 'Hello',
  body: 'This is a test email'
});

// Process any job from the queue
await emailQueue.process(async (job) => {
  // Process the job
  const { payload } = job;
  console.log(`Processing job: ${job.name}`);

  // No need to manually mark as completed or failed
  // The process method handles this automatically
});

// Process only specific job types
await emailQueue.process(async (job) => {
  // Process the sendEmail job
  const { to, subject, body } = job.payload;
  console.log(`Sending email to ${to}`);
}, 'sendEmail');

// Get queue statistics
const stats = await emailQueue.getStats();
console.log(stats);
```

## API Reference

### Queue

```typescript
class Queue<
  Payload extends PayloadSchema,
  QueueName extends keyof Payload & string = keyof Payload & string
> {
  constructor(
    name: QueueName,
    getRedisClient: () => Promise<RedisClientType>,
    options?: QueueOptions
  );

  // Add a job to the queue
  add<JobName extends JobNames<Payload, QueueName>>(
    jobName: JobName,
    payload: Payload[QueueName][JobName]
  ): Promise<Job<Payload, QueueName, JobName>>;

  // Process any job from the queue
  process(
    fn: (job: Job<Payload, QueueName, JobNames<Payload, QueueName>>) => Promise<void>
  ): Promise<void>;

  // Process only jobs with a specific name
  process<JobName extends JobNames<Payload, QueueName>>(
    fn: (job: Job<Payload, QueueName, JobName>) => Promise<void>,
    jobName: JobName
  ): Promise<void>;

  // Get queue statistics
  getStats(): Promise<QueueStats>;
}
```

### Job

```typescript
class Job<Payload, QueueName, JobName> {
  id: string | null;
  name: JobName;
  state: 'waiting' | 'active' | 'completed' | 'failed';
  payload: Payload[QueueName][JobName];

  save(): Promise<Job>;
  move(state: JobState): Promise<Job>;
}
```

## License

MIT
