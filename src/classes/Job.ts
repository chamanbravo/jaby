type JobData = Record<string, unknown>;
type BackoffOpts = { type?: "fixed" | "exponential"; delay?: number };
export type JobOptions = { delay?: number; attempts?: number; backoff?: BackoffOpts };
export type JobStatus = "waiting" | "active" | "completed" | "failed" | "delayed";

export interface JobParams {
  data: JobData;
  opts?: JobOptions;
}

export class Job {
  id: string;
  data: JobData;
  opts: JobOptions;
  status: JobStatus;
  createdAt: number;
  processAt: number;
  attemptsMade: number;
  failedReason?: string;
  completedAt?: number;
  failedAt?: number;

  constructor({ data, opts }: JobParams) {
    this.id = crypto.randomUUID();
    this.data = data;
    this.opts = opts ?? {};
    this.status = "waiting";
    this.createdAt = Date.now();
    this.processAt = Date.now() + (opts?.delay ?? 0);
    this.attemptsMade = 0;
  }

  asJSON(): Record<string, string | number> {
    return {
      id: this.id,
      data: JSON.stringify(this.data),
      opts: JSON.stringify(this.opts),
      status: this.status,
      createdAt: this.createdAt,
      processAt: this.processAt,
      attemptsMade: this.attemptsMade,
      ...(this.failedReason !== undefined ? { failedReason: this.failedReason } : {}),
      ...(this.completedAt !== undefined ? { completedAt: this.completedAt } : {}),
      ...(this.failedAt !== undefined ? { failedAt: this.failedAt } : {}),
    };
  }

  static fromHash(hash: Record<string, string>): Job {
    const job = new Job({
      data: JSON.parse(hash.data ?? "{}"),
      opts: JSON.parse(hash.opts ?? "{}"),
    });
    job.id = hash.id;
    job.status = hash.status as JobStatus;
    job.createdAt = Number(hash.createdAt);
    job.processAt = Number(hash.processAt);
    job.attemptsMade = Number(hash.attemptsMade ?? 0);
    if (hash.failedReason) job.failedReason = hash.failedReason;
    if (hash.completedAt) job.completedAt = Number(hash.completedAt);
    if (hash.failedAt) job.failedAt = Number(hash.failedAt);
    return job;
  }
}
