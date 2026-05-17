import Redis from "ioredis";
import { Job } from "./Job";
import { Queue } from "./Queue";
import { RedisConnection } from "./EventEmitter";

type Processor = (job: Job) => Promise<void>;
export type WorkerOptions = { concurrency?: number };

// Atomically moves all jobs whose processAt <= now from delayed to waiting.
// Runs as a single Redis transaction — safe across multiple workers.
const MOVE_TO_WAITING_SCRIPT = `
local delayed = KEYS[1]
local waiting = KEYS[2]
local now = tonumber(ARGV[1])
local jobs = redis.call('ZRANGEBYSCORE', delayed, '-inf', now)
for i = 1, #jobs do
  redis.call('ZREM', delayed, jobs[i])
  redis.call('LPUSH', waiting, jobs[i])
end
return #jobs
`;

export class Worker {
  private queue: Queue;
  private processor: Processor;
  private redis: Redis;
  private concurrency: number;
  private activeCount = 0;
  private ticking = false;
  private closing = false;
  private ownedClient: boolean;
  private pollInterval: ReturnType<typeof setInterval>;

  constructor(queue: Queue, processor: Processor, connection: RedisConnection, opts: WorkerOptions = {}) {
    this.queue = queue;
    this.processor = processor;
    if (connection instanceof Redis) {
      this.redis = connection;
      this.ownedClient = false;
    } else {
      this.redis = new Redis(connection as any);
      this.ownedClient = true;
    }
    this.concurrency = opts.concurrency ?? 1;
    this.pollInterval = setInterval(() => this._tick(), 500);
  }

  private async _tick(): Promise<void> {
    if (this.ticking || this.closing) return;
    this.ticking = true;
    try {
      await this._moveToWaiting();
      while (this.activeCount < this.concurrency) {
        const job = await this._pickJob();
        if (!job) break;
        this.activeCount++;
        this._processJob(job).finally(() => { this.activeCount--; });
      }
    } finally {
      this.ticking = false;
    }
  }

  private async _moveToWaiting(): Promise<void> {
    await this.redis.eval(
      MOVE_TO_WAITING_SCRIPT,
      2,
      `queue:${this.queue.name}:delayed`,
      `queue:${this.queue.name}:waiting`,
      Date.now(),
    );
  }

  // RPOPLPUSH atomically moves a job ID from waiting → active list,
  // so a crash before completion leaves it recoverable in active.
  private async _pickJob(): Promise<Job | null> {
    const jobId = await this.redis.rpoplpush(
      `queue:${this.queue.name}:waiting`,
      `queue:${this.queue.name}:active`,
    );
    if (!jobId) return null;

    const hash = await this.redis.hgetall(`queue:${this.queue.name}:${jobId}`);
    if (!hash?.id) {
      await this.redis.lrem(`queue:${this.queue.name}:active`, 1, jobId);
      return null;
    }

    return Job.fromHash(hash);
  }

  private async _processJob(job: Job): Promise<void> {
    const prefix = `queue:${this.queue.name}`;
    const { emitter } = this.queue;

    await this.redis.hset(`${prefix}:${job.id}`, "status", "active");
    await emitter.emit("active", { jobId: job.id });

    try {
      await this.processor(job);

      await this.redis.hset(`${prefix}:${job.id}`, { status: "completed", completedAt: Date.now() });
      await this.redis.lrem(`${prefix}:active`, 1, job.id);
      await this.redis.zadd(`${prefix}:completed`, Date.now(), job.id);
      await emitter.emit("completed", { jobId: job.id, data: job.data });

    } catch (err: unknown) {
      const reason = err instanceof Error ? err.message : String(err);
      job.attemptsMade++;
      const maxAttempts = job.opts.attempts ?? 1;

      if (job.attemptsMade < maxAttempts) {
        const delay = this._backoffDelay(job);
        job.processAt = Date.now() + delay;

        await this.redis.hset(`${prefix}:${job.id}`, {
          status: "delayed",
          attemptsMade: job.attemptsMade,
          failedReason: reason,
          processAt: job.processAt,
        });
        await this.redis.lrem(`${prefix}:active`, 1, job.id);
        await this.redis.zadd(`${prefix}:delayed`, job.processAt, job.id);
        await emitter.emit("retrying", { jobId: job.id, attempt: job.attemptsMade, reason });

      } else {
        await this.redis.hset(`${prefix}:${job.id}`, {
          status: "failed",
          failedReason: reason,
          failedAt: Date.now(),
        });
        await this.redis.lrem(`${prefix}:active`, 1, job.id);
        await this.redis.zadd(`${prefix}:failed`, Date.now(), job.id);
        await emitter.emit("failed", { jobId: job.id, reason });
      }
    }
  }

  private _backoffDelay(job: Job): number {
    const { backoff } = job.opts;
    if (!backoff) return 1000;
    if (backoff.type === "exponential") {
      return Math.pow(2, job.attemptsMade) * (backoff.delay ?? 1000);
    }
    return backoff.delay ?? 1000;
  }

  async close(): Promise<void> {
    this.closing = true;
    clearInterval(this.pollInterval);
    await new Promise<void>((resolve) => {
      if (this.activeCount === 0 && !this.ticking) return resolve();
      const check = setInterval(() => {
        if (this.activeCount === 0 && !this.ticking) {
          clearInterval(check);
          resolve();
        }
      }, 100);
    });
    if (this.ownedClient) this.redis.disconnect();
  }
}
