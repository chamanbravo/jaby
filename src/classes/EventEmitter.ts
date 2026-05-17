import Redis, { RedisOptions } from "ioredis";

type EventHandler = (data: unknown) => void;
export type ConnectionOptions = RedisOptions | string | number;
export type RedisConnection = Redis | ConnectionOptions;

export class EventEmitter {
  private listeners = new Map<string, Set<EventHandler>>();
  private subscriber: Redis;
  private publisher: Redis;
  private channel: string;
  private ownedPublisher: boolean;

  constructor(channel: string, connection: RedisConnection) {
    this.channel = channel;

    if (connection instanceof Redis) {
      // duplicate() creates a fresh connection with the same config so the
      // passed client is never put into subscriber-only mode.
      this.subscriber = connection.duplicate();
      this.publisher = connection;
      this.ownedPublisher = false;
    } else {
      this.subscriber = new Redis(connection as RedisOptions);
      this.publisher = new Redis(connection as RedisOptions);
      this.ownedPublisher = true;
    }

    this.subscriber.subscribe(channel);
    this.subscriber.on("message", (_ch, message) => {
      try {
        const { event, data } = JSON.parse(message) as { event: string; data: unknown };
        this.listeners.get(event)?.forEach((h) => h(data));
      } catch {}
    });
  }

  on(event: string, handler: EventHandler): this {
    if (!this.listeners.has(event)) this.listeners.set(event, new Set());
    this.listeners.get(event)!.add(handler);
    return this;
  }

  off(event: string, handler: EventHandler): this {
    this.listeners.get(event)?.delete(handler);
    return this;
  }

  async emit(event: string, data?: unknown): Promise<void> {
    await this.publisher.publish(this.channel, JSON.stringify({ event, data }));
  }

  async close(): Promise<void> {
    await this.subscriber.unsubscribe(this.channel);
    this.subscriber.disconnect();
    if (this.ownedPublisher) this.publisher.disconnect();
  }
}
