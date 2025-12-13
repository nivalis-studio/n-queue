# @nivalis/n-queue

[![CI](https://github.com/nivalis-studio/n-queue/actions/workflows/ci.yaml/badge.svg)](https://github.com/nivalis-studio/n-queue/actions/workflows/ci.yaml) [![npm](https://img.shields.io/npm/v/%40nivalis%2Fn-queue)](https://www.npmjs.com/package/@nivalis/n-queue)

A robust, Redis-backed job queue system with type safety, event handling, and streaming capabilities.

## Features

- 🎯 **Type-safe API**: Full TypeScript support with generic types for payloads and job names
- ⚡ **Effect-powered core**: All queue orchestration now runs through Effect while keeping the familiar Promise-based facade
- 💾 **Redis-backed**: Reliable persistence and atomic operations
- 🔄 **Event Streaming**: Real-time job status updates and event handling
- 🎚️ **Concurrency Control**: Fine-grained control over parallel job processing
- 🔍 **Job Tracking**: Comprehensive job lifecycle management and progress tracking
- 🛡️ **Error Handling**: Robust error handling with automatic retries
- 🔄 **Transaction Support**: Atomic operations for job state changes
- 📊 **Queue Statistics**: Real-time queue metrics and monitoring

## Compatibility

- Node.js: 18, 20, 22 (CI)
- Redis: 7.x (CI Docker)
- OS: Linux (CI), macOS/Windows should work (no native deps)

## Installation

- Requires Node.js 18+.
- Requires a Redis server (tested with Redis 7).

```bash
npm install @nivalis/n-queue redis
```

## 2-minute Quickstart (One Command)

This repo includes a runnable demo in `examples/quickstart` that starts Redis + a worker and enqueues/executes one job.

Prerequisites:
- Docker (Docker Desktop is fine)

From a fresh clone:

```bash
pnpm quickstart
```

Or, without pnpm:

```bash
docker compose -f examples/quickstart/docker-compose.yml up --build --abort-on-container-exit
```

## Demo (10 seconds)

TODO: add a 10-second GIF showing `pnpm quickstart` (enqueue -> process -> stats).

## Quick Start

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
    url: process.env.REDIS_URL ?? 'redis://localhost:6379',
  });

  if (!client.isOpen) {
    await client.connect();
  }

  return client;
};

// Create a queue with concurrency limit
const emailQueue = new Queue<MyPayload, 'emailQueue'>(
  'emailQueue',
  getRedisClient,
  { concurrency: 5 }
);

// Add jobs to the queue
const emailJob = await emailQueue.add('sendEmail', {
  to: 'user@example.com',
  subject: 'Welcome!',
  body: 'Welcome to our platform!'
});

// Process a single job (automatic completion/failure handling)
await emailQueue.process(async (job) => {
  console.log(`Processing ${job.name} job ${job.id}`);
  await sendEmail(job.payload);
}, {});

// Or process a specific job type
await emailQueue.process(async (job) => {
  await sendNotification(job.payload);
}, { jobName: 'sendNotification' });

// Stream jobs in real-time (runs until you stop the process)
await emailQueue.stream(async (job) => {
  console.log(`Processing streamed job ${job.id}`);
  await processJob(job);
});

// Listen for events
emailQueue.on('saved', (jobId) => {
  console.log(`Job ${jobId} was saved`);
});

emailQueue.on('completed', (jobId) => {
  console.log(`Job ${jobId} completed successfully`);
});
```

## Effect Integration

Internally every Redis call now runs through [Effect](https://effect.website/) so retries, backoff, and orchestration remain deterministic. Consumers can keep the familiar Promise-based API, but if you are already inside an `Effect` pipeline you can compose directly with the exposed helpers:

```typescript
import { Effect } from 'effect';
import { Job, Queue } from '@nivalis/n-queue';

const enqueueEmail = (queue: Queue<MyPayload, 'emailQueue'>) =>
  Effect.gen(function* () {
    const job = new Job({
      queue,
      name: 'sendEmail',
      payload: {
        to: 'user@example.com',
        subject: 'Welcome',
        body: '👋',
      },
    });

    return yield* job.saveEffect();
  });
```

## Architecture

The system consists of three main components:

### 1. Queue
- Manages job lifecycle and processing
- Handles concurrency and job distribution
- Provides event streaming and real-time updates
- Maintains queue statistics and monitoring

### 2. Job
- Represents a unit of work with typed payload
- Tracks job state and progress
- Manages job transitions and updates
- Handles job-specific operations

### 3. RedisClient
- Provides atomic operations for job state changes
- Manages Redis connections and error handling
- Implements retry mechanisms and transaction support
- Handles stream operations and event publishing

## API Reference

### Queue

```typescript
class Queue<Payload, QueueName> {
  constructor(
    name: QueueName,
    getRedisClient: () => Promise<RedisClientType>,
    options?: QueueOptions
  );

  // Job Management
  add<JobName>(jobName: JobName, payload: Payload[QueueName][JobName]): Promise<Job>;
  process(
    fn: (job: Job) => Promise<void>,
    options: { jobName?: JobName; jobId?: string }
  ): Promise<void>;
  stream(fn: (job: Job) => Promise<void>, jobName?: JobName): Promise<void>;

  // Event Handling
  on(event: string, handler: (jobId: string) => void): void;
  once(event: string, handler: (jobId: string) => void): void;

  // Queue Information
  getStats(): Promise<{
    waiting: number;
    active: number;
    failed: number;
    completed: number;
  }>;
}
```

### Job

```typescript
class Job<Payload, QueueName, JobName> {
  readonly id: string;
  readonly name: JobName;
  readonly state: JobState;
  readonly payload: Payload[QueueName][JobName];
  readonly createdAt: string;
  readonly updatedAt: string;

  progress: number;
  processedAt: string | null;
  attempts: number;
  failedReason: string | null;
  stacktrace: string[];

  // Job Operations
  save(): Promise<Job>;
  move(state: JobState): Promise<Job>;
  withState(state: JobState): Job;
}
```

### Queue Options

```typescript
type QueueOptions = {
  // Max concurrent jobs (-1 for unlimited)
  concurrency?: number;

  // Job visibility / stall recovery
  visibilityTimeoutMs?: number;
  stallCheckIntervalMs?: number;

  // Retry for failed jobs
  retry?: {
    maxAttempts?: number;
    backoffStrategy?: {
      initialDelay: number;
      factor: number;
      maxDelay: number;
      jitter?: number;
    };
  };

  // Optional integrations
  logger?: {
    debug?: (msg: string, meta?: unknown) => void;
    info?: (msg: string, meta?: unknown) => void;
    warn?: (msg: string, meta?: unknown) => void;
    error?: (msg: string, meta?: unknown) => void;
  };
  metrics?: {
    increment?: (name: string, labels?: Record<string, string>) => void;
    observe?: (name: string, value: number, labels?: Record<string, string>) => void;
  };
};
```

### Job States

```typescript
type JobState = 'waiting' | 'active' | 'completed' | 'failed';
```

### Events

The queue emits the following events:

- `saved`: When a job is added to the queue
- `active`: When a job starts processing
- `completed`: When a job completes successfully
- `failed`: When a job fails
- `retrying`: When a job is scheduled for retry
- `delayed`: When a job is re-queued with a delay
- `stalled`: When an active job is recovered back to waiting
- `progress`: When job progress is updated

## Best Practices

1. **Error Handling**
   ```typescript
   queue.process(async (job) => {
     try {
       await processJob(job);
       // Job automatically marked as completed
     } catch (error) {
       // Job automatically marked as failed
       console.error(`Job ${job.id} failed:`, error);
       throw error; // Rethrow to trigger failure handling
     }
   });
   ```

2. **Progress Tracking**
   ```typescript
   queue.process(async (job) => {
     for (let i = 0; i < items.length; i++) {
       await processItem(items[i]);
       job.progress = (i + 1) / items.length;
     }
   });
   ```

3. **Event Handling**
   ```typescript
   queue.on('failed', async (jobId) => {
     const job = await Job.unpack(queue, jobId);
     await notifyFailure(job);
   });
   ```

## Trust Signals

- CI runs lint, typecheck, unit tests, and Docker integration tests on PRs.
- Packages are published from version tags (`v*`) with npm provenance enabled.
- Dependencies are pinned via `pnpm-lock.yaml` for reproducible installs.
- Security reporting: see `SECURITY.md`.

## License

MIT
