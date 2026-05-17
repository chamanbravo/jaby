import { Job, JobParams } from "./Job";
import { EventEmitter, RedisConnection } from "./EventEmitter";
import Redis from "ioredis";

export class Queue {
  name: string;
  redis: Redis;
  emitter: EventEmitter;
  private ownedClient: boolean;

  constructor(name: string, connection: RedisConnection) {
    this.name = name;
    if (connection instanceof Redis) {
      this.redis = connection;
      this.ownedClient = false;
    } else {
      this.redis = new Redis(connection as any);
      this.ownedClient = true;
    }
    this.emitter = new EventEmitter(`queue:${name}:events`, connection);
  }

  on(event: string, handler: (data: unknown) => void): this {
    this.emitter.on(event, handler);
    return this;
  }

  async add({ data, opts }: JobParams): Promise<Job> {
    const job = new Job({ data, opts });
    const isDelayed = (opts?.delay ?? 0) > 0;
    job.status = isDelayed ? "delayed" : "waiting";

    await this.redis.hset(`queue:${this.name}:${job.id}`, job.asJSON());

    if (isDelayed) {
      await this.redis.zadd(`queue:${this.name}:delayed`, job.processAt, job.id);
      await this.emitter.emit("delayed", { jobId: job.id });
    } else {
      await this.redis.lpush(`queue:${this.name}:waiting`, job.id);
      await this.emitter.emit("waiting", { jobId: job.id });
    }

    return job;
  }

  async close(): Promise<void> {
    await this.emitter.close();
    if (this.ownedClient) this.redis.disconnect();
  }
}
