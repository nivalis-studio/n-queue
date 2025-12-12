import { afterAll, beforeEach, describe, expect, test } from 'bun:test';
import { createClient, type RedisClientType } from 'redis';
import { Queue } from './queue';

type TestPayload = {
  emailQueue: {
    sendEmail: {
      to: string;
    };
  };
};

const REDIS_URL = process.env.REDIS_URL;

const WAIT_FOR_RETRY_PROMOTION_MS = 80;

describe('Queue (integration)', () => {
  if (!REDIS_URL) {
    test.skip('requires REDIS_URL', () => {
      /* requires REDIS_URL */
    });
    return;
  }

  const redis = createClient({ url: REDIS_URL }) as unknown as RedisClientType;

  const getRedisClient = async () => {
    if (!redis.isOpen) {
      await redis.connect();
    }

    return redis;
  };

  const queue = new Queue<TestPayload, 'emailQueue'>(
    'emailQueue',
    getRedisClient,
    {
      // keep integration tests quick
      visibilityTimeoutMs: 10_000,
      // Run maintenance frequently so retry promotion is deterministic.
      stallCheckIntervalMs: 10,
      retry: {
        maxAttempts: 2,
        backoffStrategy: {
          initialDelay: 50,
          factor: 2,
          maxDelay: 200,
          jitter: 0,
        },
      },
    },
  );

  beforeEach(async () => {
    await getRedisClient();
    await redis.flushDb();
  });

  afterAll(async () => {
    if (redis.isOpen) {
      await redis.quit();
    }
  });

  test('processes a job end-to-end and releases lock', async () => {
    const job = await queue.add('sendEmail', { to: 'user@example.com' });

    await queue.process(j => {
      expect(j.id).toBe(job.id);
      expect(j.payload).toEqual({ to: 'user@example.com' });
    }, {});

    const stats = await queue.getStats();
    expect(stats.waiting).toBe(0);
    expect(stats.active).toBe(0);
    expect(stats.completed).toBe(1);

    // lock should be released after completion
    const lockCount = await redis.zCard(queue.keys.locks);
    expect(lockCount).toBe(0);
  });

  test('retries once, then fails on second attempt', async () => {
    await queue.add('sendEmail', { to: 'fail@example.com' });

    // attempt 1: fails, schedules retry
    await queue
      .process(() => {
        throw new Error('boom');
      }, {})
      .catch(() => {
        /* expected */
      });

    // allow delay to elapse + promotion to run on next poll
    await new Promise(r => setTimeout(r, WAIT_FOR_RETRY_PROMOTION_MS));

    // attempt 2: fails again -> moves to failed
    await queue
      .process(() => {
        throw new Error('boom-again');
      }, {})
      .catch(() => {
        /* expected */
      });

    const stats = await queue.getStats();
    expect(stats.failed).toBe(1);

    const lockCount = await redis.zCard(queue.keys.locks);
    expect(lockCount).toBe(0);
  });
});
