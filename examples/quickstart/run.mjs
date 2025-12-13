import { Queue } from '@nivalis/n-queue';
import { createClient } from 'redis';

/** @typedef {{ emailQueue: { sendEmail: { to: string, subject: string, body: string } } }} Payload */

const redisUrl = process.env.REDIS_URL ?? 'redis://localhost:6379';

let clientPromise;
const getRedisClient = async () => {
  if (!clientPromise) {
    clientPromise = (async () => {
      const client = createClient({ url: redisUrl });
      if (!client.isOpen) {
        await client.connect();
      }
      return client;
    })();
  }

  return await clientPromise;
};

const queue = new Queue('emailQueue', getRedisClient, {
  concurrency: 5,
  logger: console,
});

const job = await queue.add('sendEmail', {
  to: 'user@example.com',
  subject: 'Welcome!',
  body: 'Thanks for trying @nivalis/n-queue.',
});

console.log(`Enqueued job: ${job.id}`);

await queue.process(activeJob => {
  console.log(
    `Processing ${activeJob.name} (${activeJob.id}) -> ${activeJob.payload.to}`,
  );
  // Your real integration goes here.
}, {});

const stats = await queue.getStats();
console.log('Queue stats:', stats);

const client = await getRedisClient();
await client.quit();
