import { isExactAggregateHealth } from "../../common/aggregate-health-snapshot";
import { KAFKA_CLUSTER_HEALTH_CACHE_TTL_MS } from "../../common/constants";

import type { ClusterOverviewHealthDto } from "../../common/ipc";
import type { KafkaProgressUpdate } from "./progress";

export interface AggregateHealthSnapshot {
  data?: ClusterOverviewHealthDto;
  error?: string;
  fresh: boolean;
  lastComplete?: ClusterOverviewHealthDto;
  loading: boolean;
  updatedAt?: number;
}

export interface RestoredAggregateHealthSnapshot {
  data: ClusterOverviewHealthDto;
  lastComplete?: ClusterOverviewHealthDto;
  updatedAt: number;
}

export type AggregateHealthLoader = (
  report: (progress: KafkaProgressUpdate) => void,
  signal: AbortSignal,
) => Promise<ClusterOverviewHealthDto>;

interface AggregateHealthEntry {
  abortController?: AbortController;
  data?: ClusterOverviewHealthDto;
  error?: string;
  expiresAt?: number;
  generation: number;
  inFlight?: Promise<ClusterOverviewHealthDto>;
  lastUsedAt: number;
  lastComplete?: ClusterOverviewHealthDto;
  latestProgress?: KafkaProgressUpdate;
  listeners: Set<(progress: KafkaProgressUpdate) => void>;
  progressHealth?: Partial<ClusterOverviewHealthDto>;
  updatedAt?: number;
}

interface AggregateHealthManagerOptions {
  maxEntries?: number;
  now?: () => number;
  ttlMs?: number;
}

export function aggregateHealthWorkerKey(contextId: string, targetId: string, securityGeneration: string): string {
  return JSON.stringify({ contextId, targetId, securityGeneration });
}

export class KafkaAggregateHealthManager {
  private readonly entries = new Map<string, AggregateHealthEntry>();
  private generation = 0;
  private readonly maxEntries: number;
  private readonly now: () => number;
  private readonly ttlMs: number;

  constructor({
    maxEntries = 10,
    now = Date.now,
    ttlMs = KAFKA_CLUSTER_HEALTH_CACHE_TTL_MS,
  }: AggregateHealthManagerOptions = {}) {
    this.maxEntries = Math.max(1, maxEntries);
    this.now = now;
    this.ttlMs = Math.max(1, ttlMs);
  }

  read(key: string): AggregateHealthSnapshot {
    const entry = this.entries.get(key);
    if (!entry) return { fresh: false, loading: false };
    entry.lastUsedAt = this.now();
    return {
      data: entry.data,
      error: entry.error,
      fresh: entry.data !== undefined && (entry.expiresAt ?? 0) > this.now(),
      lastComplete: entry.lastComplete,
      loading: Boolean(entry.inFlight),
      updatedAt: entry.updatedAt,
    };
  }

  load(
    key: string,
    loader: AggregateHealthLoader,
    report?: (progress: KafkaProgressUpdate) => void,
    force = false,
  ): Promise<ClusterOverviewHealthDto> {
    const entry = this.entry(key);
    const listener = report;
    if (listener) {
      entry.listeners.add(listener);
      if (entry.latestProgress) this.notify(listener, entry.latestProgress);
      else if (entry.data) {
        this.notify(listener, {
          value: 0,
          phase: "cache",
          label: "Previous cluster health",
          detail: "Showing the previous non-secret aggregate while refreshing.",
          healthSnapshot: entry.data,
          healthSource: "cache",
        });
      }
    }

    const finishSubscription = <T>(promise: Promise<T>): Promise<T> =>
      promise.finally(() => {
        if (listener) entry.listeners.delete(listener);
      });

    if (!force && entry.data !== undefined && (entry.expiresAt ?? 0) > this.now()) {
      return finishSubscription(Promise.resolve(entry.data));
    }
    if (entry.inFlight) {
      const inFlight = entry.inFlight;
      if (entry.abortController?.signal.aborted) {
        return finishSubscription(
          inFlight
            .catch(() => undefined)
            .then(() => {
              if (entry.inFlight === inFlight) {
                entry.abortController = undefined;
                entry.inFlight = undefined;
              }
              return this.load(key, loader, report, force);
            }),
        );
      }
      return finishSubscription(inFlight);
    }

    const generation = ++this.generation;
    const abortController = new AbortController();
    entry.abortController = abortController;
    entry.error = undefined;
    entry.generation = generation;
    entry.lastUsedAt = this.now();
    entry.progressHealth = undefined;
    const publish = (rawProgress: KafkaProgressUpdate) => {
      if (entry.generation !== generation || abortController.signal.aborted) return;
      const healthSnapshot = rawProgress.healthSnapshot
        ? this.monotonicHealth(entry.progressHealth, rawProgress.healthSnapshot)
        : undefined;
      if (healthSnapshot) entry.progressHealth = healthSnapshot;
      const progress = healthSnapshot ? { ...rawProgress, healthSnapshot } : rawProgress;
      entry.latestProgress = progress;
      for (const subscriber of entry.listeners) this.notify(subscriber, progress);
    };

    let pending!: Promise<ClusterOverviewHealthDto>;
    pending = Promise.resolve()
      .then(() => loader(publish, abortController.signal))
      .then((data) => {
        if (entry.generation !== generation || abortController.signal.aborted) {
          throw new Error("Aggregate health worker was cancelled");
        }
        const updatedAt = this.now();
        entry.data = data;
        if (this.isComplete(data)) entry.lastComplete = data;
        entry.error = undefined;
        entry.expiresAt = updatedAt + this.ttlMs;
        entry.updatedAt = updatedAt;
        return data;
      })
      .catch((error: unknown) => {
        if (entry.generation === generation && !abortController.signal.aborted) {
          entry.error = error instanceof Error ? error.message : String(error);
          abortController.abort();
        }
        throw error;
      })
      .finally(() => {
        if (entry.generation === generation && entry.inFlight === pending) {
          entry.abortController = undefined;
          entry.inFlight = undefined;
          entry.latestProgress = undefined;
          entry.listeners.clear();
          entry.progressHealth = undefined;
        }
      });
    entry.inFlight = pending;
    this.evictIfNeeded(key);
    return finishSubscription(pending);
  }

  invalidate(key?: string): void {
    if (key !== undefined) {
      this.invalidateEntry(key);
      return;
    }
    for (const entryKey of [...this.entries.keys()]) this.invalidateEntry(entryKey);
  }

  remove(key: string): void {
    this.invalidateEntry(key);
    this.entries.delete(key);
  }

  clear(): void {
    for (const key of [...this.entries.keys()]) this.remove(key);
  }

  size(): number {
    return this.entries.size;
  }

  restore(key: string, snapshot: RestoredAggregateHealthSnapshot): void {
    const entry = this.entry(key);
    if (entry.data !== undefined || entry.inFlight) return;
    entry.data = snapshot.data;
    entry.lastComplete = snapshot.lastComplete ?? (this.isComplete(snapshot.data) ? snapshot.data : undefined);
    entry.updatedAt = snapshot.updatedAt;
    entry.expiresAt = undefined;
    entry.lastUsedAt = this.now();
  }

  private entry(key: string): AggregateHealthEntry {
    let entry = this.entries.get(key);
    if (!entry) {
      entry = {
        generation: ++this.generation,
        lastUsedAt: this.now(),
        listeners: new Set(),
      };
      this.entries.set(key, entry);
    }
    return entry;
  }

  private evictIfNeeded(protectedKey: string): void {
    while (this.entries.size > this.maxEntries) {
      const candidate = [...this.entries.entries()]
        .filter(([key]) => key !== protectedKey)
        .sort(([, left], [, right]) => left.lastUsedAt - right.lastUsedAt)[0];
      if (!candidate) return;
      this.invalidateEntry(candidate[0]);
      this.entries.delete(candidate[0]);
    }
  }

  private invalidateEntry(key: string): void {
    const entry = this.entries.get(key);
    if (!entry) return;
    const pending = entry.inFlight;
    entry.abortController?.abort();
    entry.error = undefined;
    entry.expiresAt = undefined;
    entry.generation = ++this.generation;
    entry.latestProgress = undefined;
    entry.listeners.clear();
    entry.progressHealth = undefined;
    if (pending) {
      void pending
        .catch(() => undefined)
        .finally(() => {
          if (entry.inFlight === pending) {
            entry.abortController = undefined;
            entry.inFlight = undefined;
          }
        });
    } else {
      entry.abortController = undefined;
    }
  }

  private notify(listener: (progress: KafkaProgressUpdate) => void, progress: KafkaProgressUpdate): void {
    try {
      listener(progress);
    } catch {
      // A subscriber cannot cancel or fail the shared worker.
    }
  }

  private isComplete(data: ClusterOverviewHealthDto): boolean {
    return isExactAggregateHealth(data);
  }

  private monotonicHealth(
    previous: Partial<ClusterOverviewHealthDto> | undefined,
    current: Partial<ClusterOverviewHealthDto>,
  ): Partial<ClusterOverviewHealthDto> {
    const merged = { ...previous, ...current };
    const previousLag = this.knownLag(previous?.consumerGroupLag);
    const currentLag = this.knownLag(current.consumerGroupLag);
    if (previousLag !== undefined && (currentLag === undefined || currentLag < previousLag)) {
      merged.consumerGroupLag = `≥${previousLag}`;
    }
    return merged;
  }

  private knownLag(value: string | undefined): bigint | undefined {
    const match = /^(?:≥)?(\d+)$/.exec(value ?? "");
    return match ? BigInt(match[1]) : undefined;
  }
}
