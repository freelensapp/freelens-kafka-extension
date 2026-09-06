export interface KafkaSnapshot<T> {
  data?: T;
  fresh: boolean;
  stale: boolean;
  loading: boolean;
  error?: string;
  updatedAt?: number;
}

interface SnapshotEntry<T> {
  data?: T;
  error?: string;
  expiresAt?: number;
  generation: number;
  inFlight?: Promise<T>;
  lastAccessedAt: number;
  updatedAt?: number;
}

export interface KafkaSnapshotCacheOptions {
  now?: () => number;
  ttlMs: number;
  maxEntries?: number;
}

export class KafkaSnapshotCache<T> {
  private readonly entries = new Map<string, SnapshotEntry<T>>();
  private readonly now: () => number;
  private readonly ttlMs: number;
  private readonly maxEntries: number;
  private generation = 0;

  constructor(options: KafkaSnapshotCacheOptions) {
    this.now = options.now ?? Date.now;
    this.ttlMs = Math.max(1, options.ttlMs);
    this.maxEntries = Math.max(1, options.maxEntries ?? 100);
  }

  read(key: string): KafkaSnapshot<T> {
    const entry = this.entries.get(key);
    if (!entry) return { fresh: false, stale: false, loading: false };
    entry.lastAccessedAt = this.now();
    const fresh = entry.data !== undefined && entry.expiresAt !== undefined && entry.expiresAt > this.now();
    return {
      data: entry.data,
      fresh,
      stale: entry.data !== undefined && !fresh,
      loading: Boolean(entry.inFlight),
      error: entry.error,
      updatedAt: entry.updatedAt,
    };
  }

  load(key: string, loader: () => Promise<T>): Promise<T> {
    const existing = this.entries.get(key);
    if (existing) {
      existing.lastAccessedAt = this.now();
      if (existing.data !== undefined && existing.expiresAt !== undefined && existing.expiresAt > this.now()) {
        return Promise.resolve(existing.data);
      }
      if (existing.inFlight) return existing.inFlight;
    }

    const generation = ++this.generation;
    const inFlight = Promise.resolve()
      .then(loader)
      .then((data) => {
        const current = this.entries.get(key);
        if (current?.generation === generation) {
          const updatedAt = this.now();
          this.entries.set(key, {
            data,
            expiresAt: updatedAt + this.ttlMs,
            generation,
            lastAccessedAt: updatedAt,
            updatedAt,
          });
          this.evictIfNeeded();
        }
        return data;
      })
      .catch((error: unknown) => {
        const current = this.entries.get(key);
        if (current?.generation === generation) {
          this.entries.set(key, {
            ...current,
            error: error instanceof Error ? error.message : String(error),
            generation,
            inFlight: undefined,
            lastAccessedAt: this.now(),
          });
        }
        throw error;
      });

    this.entries.set(key, {
      data: existing?.data,
      error: undefined,
      expiresAt: existing?.expiresAt,
      generation,
      inFlight,
      lastAccessedAt: this.now(),
      updatedAt: existing?.updatedAt,
    });
    this.evictIfNeeded(key);
    return inFlight;
  }

  invalidate(key: string): void {
    const current = this.entries.get(key);
    if (!current) return;
    this.entries.set(key, {
      ...current,
      expiresAt: 0,
      generation: ++this.generation,
      inFlight: undefined,
      lastAccessedAt: this.now(),
    });
  }

  invalidatePrefix(prefix: string): void {
    for (const key of this.entries.keys()) {
      if (key.startsWith(prefix)) this.invalidate(key);
    }
  }

  delete(key: string): void {
    this.entries.delete(key);
  }

  clear(): void {
    this.entries.clear();
  }

  size(): number {
    return this.entries.size;
  }

  private evictIfNeeded(protectedKey?: string): void {
    while (this.entries.size > this.maxEntries) {
      const candidate = [...this.entries.entries()]
        .filter(([key, entry]) => key !== protectedKey && !entry.inFlight)
        .sort(([, left], [, right]) => left.lastAccessedAt - right.lastAccessedAt)[0];
      if (!candidate) return;
      this.entries.delete(candidate[0]);
    }
  }
}
