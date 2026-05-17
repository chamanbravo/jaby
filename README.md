# jaby

A Redis-backed job queue for Node.js. Like BullMQ, but smaller and lighter.

## Install

```bash
npm install jaby
```

## Usage

```ts
import Redis from "ioredis";
import { Queue, Worker } from "jaby";

const client = new Redis();

const queue = new Queue("emails", client);
await queue.add({
  data: { to: "coder@himaligoat.com", subject: "Add me in your codebase" },
});

// Create a worker
const worker = new Worker(
  queue,
  async (job) => {
    await sendEmail(job.data);
  },
  client,
);
```

## Delays

```ts
// Run this in 10 seconds.
await queue.add({
  data: { message: "think about it" },
  opts: { delay: 10_000 },
});
```

## Retries

```ts
await queue.add({
  data: { url: "https://flaky-api.example.com" },
  opts: {
    attempts: 5,
    backoff: { type: "exponential", delay: 1000 },
    // will retry at 2s, 4s, 8s, 16s before giving up and moving on with its life
  },
});
```

## Events

```ts
queue
  .on("completed", ({ jobId }) => console.log(`${jobId} done`))
  .on("failed", ({ jobId, reason }) =>
    console.error(`${jobId} exploded: ${reason}`),
  )
  .on("retrying", ({ jobId, attempt }) =>
    console.warn(`${jobId} trying again (attempt ${attempt})`),
  );
```

Events are published over Redis pub/sub, so multiple processes all see them.

## Concurrency

```ts
// Process multiple jobs at once
const worker = new Worker(queue, processor, client, { concurrency: 5 });
```

## Redis keys

| Key                      | What's in it                   |
| ------------------------ | ------------------------------ |
| `queue:<name>:<id>`      | Job hash                       |
| `queue:<name>:waiting`   | Jobs ready to go               |
| `queue:<name>:delayed`   | Jobs waiting for their moment  |
| `queue:<name>:active`    | Jobs currently being worked on |
| `queue:<name>:completed` | Jobs that made it              |
| `queue:<name>:failed`    | Jobs that did not              |
