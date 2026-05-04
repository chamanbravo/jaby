"use strict";

import EventEmitter from "node:events";

type JobData = Record<string, any>;
type JobOptions = { delay?: number; attempts?: number; jobId?: string };
type JobStatus = "waiting" | "active" | "completed" | "failed";

interface JobParams {
  jobId?: string | number;
  data: JobData;
  opts?: JobOptions;
}

class Job {
  id: string;
  data: Record<any, any>;
  opts: Record<any, any>;
  status: JobStatus;
  createdAt: number;
  processAt: number;

  constructor({ jobId, data, opts }: JobParams) {
    this.id = String(jobId) ?? String(Math.floor(Math.random() * 100_000_000));
    this.data = data;
    this.opts = opts ?? {
      delay: 0,
    };
    this.status = "waiting";
    this.createdAt = Date.now();
    this.processAt = Date.now() + (opts?.delay ?? 0);
  }

  _isReady(): boolean {
    return this.processAt <= Date.now();
  }
}

class Queue extends EventEmitter {
  name: string;
  private jobs: Job[] = [];

  constructor(name: string) {
    super();
    this.name = name;
  }

  add({ jobId, data, opts }: JobParams) {
    const job = new Job({
      jobId,
      data,
      opts,
    });

    this.jobs.push(job);
    this.emit("waiting", job);
  }

  _poll(): Job | undefined {
    return this.jobs.find((j) => j.status === "waiting" && j._isReady());
  }

  _complete(jobId: string) {
    this.jobs = this.jobs.filter((j) => j.id !== jobId);
  }

  getJobs(): Job[] {
    return this.jobs;
  }
}

type Processor = (job: Job) => Promise<void>;

class Worker extends EventEmitter {
  private queue: Queue;
  private processor: Processor;
  private isPaused: boolean;
  private interval: ReturnType<typeof setInterval>;

  constructor(queue: Queue, processor: Processor) {
    super();
    this.queue = queue;
    this.processor = processor;
    this.isPaused = false;
    this.interval = setInterval(() => this._tick(), 500);
  }

  private async _tick() {
    if (this.isPaused) return;

    const job = this.queue._poll();
    if (!job) return;

    this.processor(job)
      .then(() => {
        job.status = "completed";
        this.queue._complete(job.id);
        this.emit("completed", job);
      })
      .catch((err: Error) => {
        job.status = "failed";
        this.queue._complete(job.id);
        this.emit("failed", job, err);
      })
      .finally(() => {});
  }

  pause() {
    this.isPaused = true;
    this.emit("paused");
  }

  resume() {
    this.isPaused = false;
    this.emit("resumed");
  }

  close() {
    clearInterval(this.interval);
    this.emit("closed");
  }
}

const queues: Record<string, Queue> = {};

const createQueue = (name: string): Queue => {
  if (!queues[name]) queues[name] = new Queue(name);
  return queues[name];
};

// ---
const mailQueue = createQueue("mail");

mailQueue.on("waiting", (job: Job) => {
  console.log(
    `[Queue] Job ${job.id} waiting, starts at ${new Date(job.processAt).toISOString()}`,
  );
});

const mailWorker = new Worker(mailQueue, async (job: Job) => {
  console.log(`[Worker] Processing job ${job.id}:`, job.data);
  await new Promise((res) => setTimeout(res, 200));
});

mailWorker
  .on("active", (job: Job) => console.log(`[Worker] Active:    job ${job.id}`))
  .on("completed", (job: Job) =>
    console.log(`[Worker] Completed: job ${job.id}`),
  )
  .on("failed", (job: Job, err: Error) =>
    console.error(`[Worker] Failed:    job ${job.id}`, err.message),
  )
  .on("retrying", (job: Job, err: Error) =>
    console.warn(`[Worker] Retrying:  job ${job.id}`, err.message),
  );

mailQueue.add({
  jobId: 123,
  data: { name: "chad", to: "chad@m.com" },
  opts: {
    delay: 1000 * 30,
  },
});

mailQueue.add({
  jobId: 12345,
  data: { name: "chad2", to: "chad2@m.com" },
  opts: {
    delay: 1000,
  },
});
