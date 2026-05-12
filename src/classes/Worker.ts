import Redis from "ioredis";

export class Worker {
  private queue;
  private processor;
  private redisClient;
  private pollActiveInterval: ReturnType<typeof setInterval>;
  private pollWaitingInterval: ReturnType<typeof setInterval>;

  constructor(queue, processor, connection) {
    this.queue = queue;
    this.processor = processor;
    this.redisClient = new Redis(connection);
    this.pollActiveInterval = setInterval(() => this._processActiveTick(), 500);
    this.pollWaitingInterval = setInterval(
      () => this._moveToWaitingTick(),
      500,
    );
  }

  async _processActiveTick() {
    const jobId = await this.redisClient.rpop(
      `queue:${this.queue.name}:waiting`,
      1,
    );

    if (!jobId) return;

    const jobDetails = await this.redisClient.hgetall(
      `queue:${this.queue.name}:${jobId?.[0]}`,
    );

    this.processor(jobDetails.data);
    this.redisClient.del(`queue:${this.queue.name}:${jobId?.[0]}`);
  }

  async _moveToWaitingTick() {
    const [job, score] = await this.redisClient.zrange(
      `queue:${this.queue.name}:delayed`,
      0,
      0,
      "WITHSCORES",
    );

    if (Number(score) <= Date.now()) {
      const [job, score] = await this.redisClient.zpopmin(
        `queue:${this.queue.name}:delayed`,
      );

      console.log("Popped: ", job, score);

      await this.redisClient.lpush(`queue:${this.queue.name}:waiting`, job);
    }
  }
}
