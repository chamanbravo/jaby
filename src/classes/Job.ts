type JobData = Record<string, any>;
type JobOptions = { delay?: number; attempts?: number };
type JobStatus = "waiting" | "active" | "completed" | "failed" | "delayed";

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

  constructor({ data, opts }: JobParams) {
    this.id = String(data?.id || Math.floor(Math.random() * 100_000_000));
    this.data = data;
    this.opts = opts ?? { delay: 0 };
    this.status = "waiting";
    this.createdAt = Date.now();
    this.processAt = Date.now() + (opts?.delay || 0);
  }

  asJSON() {
    return {
      id: this.id,
      data: JSON.stringify(typeof this.data === "undefined" ? {} : this.data),
      opts: JSON.stringify(this.opts),
      status: this.status,
      createdAt: this.createdAt,
      processAt: this.processAt,
    };
  }
}
