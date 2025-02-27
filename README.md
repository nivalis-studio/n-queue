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

// Process jobs
const job = await emailQueue.take();
if (job) {
  try {
    // Process the job
    const { payload } = job;
    console.log(`Sending email to ${payload.to}`);

    // Mark job as completed
    await job.move('completed');
  } catch (error) {
    // Mark job as failed
    await job.move('failed');
  }
}

// Get queue statistics
const stats = await emailQueue.getStats();
console.log(stats);
```

## API Reference

### Queue

```typescript
class Queue<Payload, QueueName> {
  constructor(
    name: QueueName,
    getRedisClient: () => Promise<RedisClientType>,
    options?: QueueOptions
  );

  add<JobName>(jobName: JobName, payload: Payload[QueueName][JobName]): Promise<Job>;
  take(): Promise<Job | null>;
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
