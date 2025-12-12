import { describe, expect, test } from 'bun:test';
import { Effect } from 'effect';
import { RedisClient } from './redis-client';
import type { RedisClientType } from 'redis';
import type { JobData } from './types/job';
import type { QueueNames } from './types/payload';

type TestPayload = {
  emailQueue: {
    sendEmail: {
      to: string;
    };
  };
};

class MockRedisClient {
  public hGetAllResponse: Record<string, string> | null = null;
  public readonly hGetAllCalls: Array<string> = [];
  public readonly multiHistory: Array<
    Array<{ command: string; args: Array<unknown> }>
  > = [];

  public hGetAll(key: string): Promise<Record<string, string>> {
    this.hGetAllCalls.push(key);

    return Promise.resolve(this.hGetAllResponse ?? {});
  }

  public multi(): unknown {
    const commands: Array<{ command: string; args: Array<unknown> }> = [];
    const multi: any = {
      hSet: (key: string, data: unknown) => {
        commands.push({ command: 'hSet', args: [key, data] });

        return 1;
      },
      lPush: (key: string, value: unknown) => {
        commands.push({ command: 'lPush', args: [key, value] });

        return 1;
      },
      xAdd: (key: string, id: string, message: unknown) => {
        commands.push({ command: 'xAdd', args: [key, id, message] });

        return 'ok';
      },
      exec: () => {
        this.multiHistory.push([...commands]);

        return Promise.resolve(commands);
      },
      discard: () => {
        /* no-op */
      },
    };

    return multi;
  }
}

const createRedisClient = () => {
  const mock = new MockRedisClient();
  const client = new RedisClient<TestPayload, QueueNames<TestPayload>>(
    async () => mock as unknown as RedisClientType,
    { maxRetries: 1 },
  );

  return { mock, client };
};

describe('RedisClient', () => {
  test('getJobData returns parsed payload when hash exists', async () => {
    const { mock, client } = createRedisClient();

    mock.hGetAllResponse = {
      name: 'sendEmail',
      payload: JSON.stringify({ to: 'user@example.com' }),
      queue: 'emailQueue',
      state: 'waiting',
      createdAt: '1',
      updatedAt: '2',
    };

    const jobData = await Effect.runPromise(
      client.getJobData<'sendEmail'>('job:test'),
    );

    expect(jobData).not.toBeNull();
    expect(jobData?.name).toBe('sendEmail');
    expect(jobData?.queue).toBe('emailQueue');
    expect(jobData?.state).toBe('waiting');
    expect(jobData?.createdAt).toBe('1');
    expect(jobData?.updatedAt).toBe('2');
    expect((jobData?.payload as unknown as { to: string }) ?? null).toEqual({
      to: 'user@example.com',
    });
    expect(mock.hGetAllCalls).toEqual(['job:test']);
  });

  test('getJobData returns null for missing hash', async () => {
    const { mock, client } = createRedisClient();

    mock.hGetAllResponse = {};

    const result = await Effect.runPromise(
      client.getJobData<'sendEmail'>('job:missing'),
    );

    expect(result).toBeNull();
  });

  test('saveJob writes job payload, queue pointers, and enqueue event', async () => {
    const { mock, client } = createRedisClient();

    const jobData: JobData<TestPayload, 'emailQueue', 'sendEmail'> = {
      name: 'sendEmail',
      payload: JSON.stringify({ to: 'user@example.com' }),
      queue: 'emailQueue',
      state: 'waiting',
      createdAt: '1',
      updatedAt: '1',
    };

    await Effect.runPromise(
      client.saveJob('job:test', jobData, 'queue:waiting', 'queue:events'),
    );

    expect(mock.multiHistory).toHaveLength(1);
    expect(mock.multiHistory[0]).toEqual([
      { command: 'hSet', args: ['job:test', jobData] },
      { command: 'lPush', args: ['queue:waiting', 'job:test'] },
      {
        command: 'xAdd',
        args: ['queue:events', '*', { type: 'saved', id: 'job:test' }],
      },
    ]);
  });
});
