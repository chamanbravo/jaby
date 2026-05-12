import { Job, JobParams } from "./Job";
import Redis from "ioredis";

export class Queue {
  name: string;
  redis;

  constructor(name: string, connection) {
    this.name = name;
    this.redis = new Redis(connection);
  }

  async add({ data, opts }: JobParams) {
    const job = new Job({ data, opts });

    await this.redis.hset(`queue:${this.name}:${job.id}`, job.asJSON());
    await this.redis.zadd(`queue:${this.name}:delayed`, job.processAt, job.id);
    await this.redis.publish(`queue:${this.name}:events`, "delayed");

    return job;
  }
}
