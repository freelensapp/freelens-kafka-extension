import type { KafkaConnection } from "./kafka-connection";

export interface KafkaSessionManagerOptions {
  maxSessions?: number;
  idleTimeoutMs?: number;
  now?: () => number;
  setTimer?: (callback: () => void, delayMs: number) => ReturnType<typeof setTimeout>;
  clearTimer?: (timer: ReturnType<typeof setTimeout>) => void;
}

interface SessionEntry {
  connection: KafkaConnection;
  leases: number;
  lastUsedAt: number;
  idleTimer?: ReturnType<typeof setTimeout>;
}

export interface KafkaSessionLease {
  connection: KafkaConnection;
  release(): Promise<void>;
}

export class KafkaSessionManager {
  private generation = 0;
  private readonly keyGenerations = new Map<string, number>();
  private readonly sessions = new Map<string, SessionEntry>();
  private readonly maxSessions: number;
  private readonly idleTimeoutMs: number;
  private readonly now: () => number;
  private readonly setTimer: NonNullable<KafkaSessionManagerOptions["setTimer"]>;
  private readonly clearTimer: NonNullable<KafkaSessionManagerOptions["clearTimer"]>;
  private readonly pending = new Map<string, Promise<SessionEntry>>();

  constructor(options: KafkaSessionManagerOptions = {}) {
    this.maxSessions = Math.max(1, options.maxSessions ?? 3);
    this.idleTimeoutMs = Math.max(1, options.idleTimeoutMs ?? 5 * 60_000);
    this.now = options.now ?? Date.now;
    this.setTimer = options.setTimer ?? ((callback, delayMs) => setTimeout(callback, delayMs));
    this.clearTimer = options.clearTimer ?? ((timer) => clearTimeout(timer));
  }

  async acquire(key: string, connect: () => Promise<KafkaConnection>): Promise<KafkaSessionLease> {
    let entry = this.sessions.get(key);
    let acquiredFromPending = false;
    if (!entry) {
      let pending = this.pending.get(key);
      if (!pending) {
        const generation = this.generation;
        const keyGeneration = this.keyGenerations.get(key) ?? 0;
        pending = connect().then(async (connection) => {
          if (generation !== this.generation || keyGeneration !== (this.keyGenerations.get(key) ?? 0)) {
            await connection.disconnect();
            throw new Error("Kafka session was invalidated while connecting");
          }
          return { connection, leases: 0, lastUsedAt: this.now() };
        });
        this.pending.set(key, pending);
      }
      try {
        entry = await pending;
        acquiredFromPending = true;
      } finally {
        if (this.pending.get(key) === pending) this.pending.delete(key);
      }
    }
    if (entry.idleTimer) {
      this.clearTimer(entry.idleTimer);
      entry.idleTimer = undefined;
    }
    entry.leases += 1;
    entry.lastUsedAt = this.now();
    if (acquiredFromPending) {
      this.sessions.set(key, entry);
      await this.evictIfNeeded(key);
    }
    let released = false;
    return {
      connection: entry.connection,
      release: async () => {
        if (released) return;
        released = true;
        entry.leases = Math.max(0, entry.leases - 1);
        entry.lastUsedAt = this.now();
        if (entry.leases === 0) {
          await this.evictIfNeeded();
          if (this.sessions.get(key) === entry) this.scheduleIdleCleanup(key, entry);
        }
      },
    };
  }

  async clear(key?: string): Promise<void> {
    if (key) this.keyGenerations.set(key, (this.keyGenerations.get(key) ?? 0) + 1);
    else this.generation++;
    const pending = key ? [this.pending.get(key)] : [...this.pending.values()];
    const entries = key ? [[key, this.sessions.get(key)] as const] : [...this.sessions.entries()];
    for (const [entryKey, entry] of entries) {
      if (!entry) continue;
      if (entry.idleTimer) this.clearTimer(entry.idleTimer);
      this.sessions.delete(entryKey);
      await entry.connection.disconnect();
    }
    await Promise.allSettled(pending.filter((value): value is Promise<SessionEntry> => Boolean(value)));
  }

  async release(connection: KafkaConnection): Promise<void> {
    const entry = [...this.sessions.values()].find((candidate) => candidate.connection === connection);
    if (!entry) return;
    entry.leases = Math.max(0, entry.leases - 1);
    entry.lastUsedAt = this.now();
    if (entry.leases === 0) {
      const key = [...this.sessions.entries()].find(([, candidate]) => candidate === entry)?.[0];
      await this.evictIfNeeded();
      if (key && this.sessions.get(key) === entry) this.scheduleIdleCleanup(key, entry);
    }
  }

  size(): number {
    return this.sessions.size;
  }

  private scheduleIdleCleanup(key: string, entry: SessionEntry): void {
    entry.idleTimer = this.setTimer(() => {
      if (entry.leases === 0 && this.sessions.get(key) === entry) {
        this.sessions.delete(key);
        void entry.connection.disconnect();
      }
    }, this.idleTimeoutMs);
  }

  private async evictIfNeeded(protectedKey?: string): Promise<void> {
    while (this.sessions.size > this.maxSessions) {
      const candidate = [...this.sessions.entries()]
        .filter(([key, entry]) => key !== protectedKey && entry.leases === 0)
        .sort(([, left], [, right]) => left.lastUsedAt - right.lastUsedAt)[0];
      if (!candidate) return;
      const [key, entry] = candidate;
      if (entry.idleTimer) this.clearTimer(entry.idleTimer);
      this.sessions.delete(key);
      await entry.connection.disconnect();
    }
  }
}
