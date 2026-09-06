import { KAFKA_CLUSTER_HEALTH_CACHE_TTL_MS, KAFKA_RESOURCE_CACHE_TTL_MS } from "../common/constants";
import { createKafkaTargetId } from "../common/kafka-target";
import { type KafkaSnapshot, KafkaSnapshotCache } from "./kafka-snapshot-cache";

import type {
  ClusterOverviewDto,
  ClusterOverviewHealthDto,
  ConsumerGroupsDto,
  DiscoveredKafkaInfo,
  TopicDetailDto,
} from "../common/ipc";

interface CacheEntry<T> {
  data?: T;
  expiresAt?: number;
  generation: number;
  inFlight?: Promise<T>;
  operationId?: string;
  requestCount: number;
  updatedAt?: number;
}

export interface KafkaCacheSnapshot<T> {
  data?: T;
  fresh: boolean;
  loading: boolean;
  operationId?: string;
  requestCount: number;
  updatedAt?: number;
}

export interface KafkaResourceCacheStats {
  discoveryRequests: number;
  healthRequests: number;
  overviewRequests: number;
  reachabilityRequests: number;
}

interface KafkaResourceCacheOptions {
  healthTtlMs?: number;
  now?: () => number;
  ttlMs?: number;
}

function emptySnapshot<T>(): KafkaCacheSnapshot<T> {
  return { fresh: false, loading: false, requestCount: 0 };
}

export function formatKafkaCacheAge(updatedAt: number, now = Date.now()): string {
  const elapsed = Math.max(0, now - updatedAt);
  if (elapsed < 5_000) return "just now";
  if (elapsed < 60_000) return `${Math.floor(elapsed / 1_000)}s ago`;
  if (elapsed < 3_600_000) return `${Math.floor(elapsed / 60_000)}m ago`;
  return `${Math.floor(elapsed / 3_600_000)}h ago`;
}

export class KafkaResourceCache {
  private activeKubernetesClusterId?: string;
  private readonly discovery = new Map<string, CacheEntry<DiscoveredKafkaInfo[]>>();
  private generation = 0;
  private readonly health = new Map<string, CacheEntry<ClusterOverviewHealthDto>>();
  private readonly healthTtlMs: number;
  private readonly now: () => number;
  private readonly overviews = new Map<string, CacheEntry<ClusterOverviewDto>>();
  private readonly reachability = new Map<string, CacheEntry<boolean>>();
  private readonly reachabilityRequestCounts = new Map<string, number>();
  private readonly topicDetails: KafkaSnapshotCache<TopicDetailDto>;
  private readonly groupLists: KafkaSnapshotCache<ConsumerGroupsDto>;
  private readonly ttlMs: number;

  constructor({
    healthTtlMs = KAFKA_CLUSTER_HEALTH_CACHE_TTL_MS,
    now = Date.now,
    ttlMs = KAFKA_RESOURCE_CACHE_TTL_MS,
  }: KafkaResourceCacheOptions = {}) {
    this.healthTtlMs = Math.max(1, healthTtlMs);
    this.now = now;
    this.ttlMs = ttlMs;
    this.topicDetails = new KafkaSnapshotCache({ now, ttlMs, maxEntries: 100 });
    this.groupLists = new KafkaSnapshotCache({ now, ttlMs, maxEntries: 10 });
  }

  activateKubernetesCluster(kubernetesClusterId: string): void {
    if (this.activeKubernetesClusterId === kubernetesClusterId) return;
    this.clear();
    this.activeKubernetesClusterId = kubernetesClusterId;
  }

  readDiscovery(kubernetesClusterId: string): KafkaCacheSnapshot<DiscoveredKafkaInfo[]> {
    this.activateKubernetesCluster(kubernetesClusterId);
    return this.snapshot(this.discovery.get(kubernetesClusterId));
  }

  loadDiscovery(
    kubernetesClusterId: string,
    operationId: string,
    loader: () => Promise<DiscoveredKafkaInfo[]>,
  ): Promise<DiscoveredKafkaInfo[]> {
    this.activateKubernetesCluster(kubernetesClusterId);
    return this.load(this.discovery, kubernetesClusterId, operationId, loader);
  }

  readOverview(kubernetesClusterId: string, targetId: string): KafkaCacheSnapshot<ClusterOverviewDto> {
    this.activateKubernetesCluster(kubernetesClusterId);
    return this.snapshot(this.overviews.get(this.targetKey(kubernetesClusterId, targetId)));
  }

  loadOverview(
    kubernetesClusterId: string,
    targetId: string,
    operationId: string,
    loader: () => Promise<ClusterOverviewDto>,
  ): Promise<ClusterOverviewDto> {
    this.activateKubernetesCluster(kubernetesClusterId);
    return this.load(this.overviews, this.targetKey(kubernetesClusterId, targetId), operationId, loader);
  }

  readHealth(kubernetesClusterId: string, targetId: string): KafkaCacheSnapshot<ClusterOverviewHealthDto> {
    this.activateKubernetesCluster(kubernetesClusterId);
    return this.snapshot(this.health.get(this.targetKey(kubernetesClusterId, targetId)));
  }

  loadHealth(
    kubernetesClusterId: string,
    targetId: string,
    operationId: string,
    loader: () => Promise<ClusterOverviewHealthDto>,
  ): Promise<ClusterOverviewHealthDto> {
    this.activateKubernetesCluster(kubernetesClusterId);
    return this.load(this.health, this.targetKey(kubernetesClusterId, targetId), operationId, loader, this.healthTtlMs);
  }

  readTopic(kubernetesClusterId: string, targetId: string, topic: string): KafkaSnapshot<TopicDetailDto> {
    this.activateKubernetesCluster(kubernetesClusterId);
    return this.topicDetails.read(this.topicKey(kubernetesClusterId, targetId, topic));
  }

  loadTopic(
    kubernetesClusterId: string,
    targetId: string,
    topic: string,
    loader: () => Promise<TopicDetailDto>,
  ): Promise<TopicDetailDto> {
    this.activateKubernetesCluster(kubernetesClusterId);
    return this.topicDetails.load(this.topicKey(kubernetesClusterId, targetId, topic), loader);
  }

  readGroups(kubernetesClusterId: string, targetId: string): KafkaSnapshot<ConsumerGroupsDto> {
    this.activateKubernetesCluster(kubernetesClusterId);
    return this.groupLists.read(this.targetKey(kubernetesClusterId, targetId));
  }

  loadGroups(
    kubernetesClusterId: string,
    targetId: string,
    loader: () => Promise<ConsumerGroupsDto>,
  ): Promise<ConsumerGroupsDto> {
    this.activateKubernetesCluster(kubernetesClusterId);
    return this.groupLists.load(this.targetKey(kubernetesClusterId, targetId), loader);
  }

  readReachability(kubernetesClusterId: string, targetId: string): KafkaCacheSnapshot<boolean> {
    this.activateKubernetesCluster(kubernetesClusterId);
    return this.snapshot(this.reachability.get(this.targetKey(kubernetesClusterId, targetId)));
  }

  async loadReachability(
    kubernetesClusterId: string,
    bootstraps: string[],
    loader: (missingBootstraps: string[]) => Promise<Record<string, boolean>>,
  ): Promise<Record<string, boolean>> {
    this.activateKubernetesCluster(kubernetesClusterId);
    const uniqueBootstraps = [...new Set(bootstraps)];
    const values = new Map<string, Promise<boolean>>();
    const missing: { bootstrap: string; key: string; generation: number }[] = [];

    for (const bootstrap of uniqueBootstraps) {
      const key = this.targetKey(kubernetesClusterId, createKafkaTargetId(bootstrap));
      const entry = this.reachability.get(key);
      if (entry?.data !== undefined && this.isFresh(entry)) {
        values.set(bootstrap, Promise.resolve(entry.data));
      } else if (entry?.inFlight) {
        values.set(bootstrap, entry.inFlight);
      } else {
        const generation = this.nextGeneration();
        missing.push({ bootstrap, key, generation });
      }
    }

    if (missing.length > 0) {
      this.reachabilityRequestCounts.set(
        kubernetesClusterId,
        (this.reachabilityRequestCounts.get(kubernetesClusterId) ?? 0) + 1,
      );
      const batch = this.callLoader(() => loader(missing.map(({ bootstrap }) => bootstrap)));

      for (const { bootstrap, key, generation } of missing) {
        const previous = this.reachability.get(key);
        const inFlight = batch
          .then((result) => {
            const data = result[bootstrap];
            if (data === undefined) throw new Error(`reachability result missing for ${bootstrap}`);
            const current = this.reachability.get(key);
            if (current?.generation === generation) {
              const updatedAt = this.now();
              this.reachability.set(key, {
                data,
                expiresAt: updatedAt + this.ttlMs,
                generation,
                requestCount: current.requestCount,
                updatedAt,
              });
            }
            return data;
          })
          .catch((error: unknown) => {
            const current = this.reachability.get(key);
            if (current?.generation === generation) {
              this.reachability.set(key, {
                data: previous?.data,
                expiresAt: previous?.expiresAt,
                generation,
                requestCount: current.requestCount,
                updatedAt: previous?.updatedAt,
              });
            }
            throw error;
          });

        this.reachability.set(key, {
          data: previous?.data,
          expiresAt: previous?.expiresAt,
          generation,
          inFlight,
          requestCount: (previous?.requestCount ?? 0) + 1,
          updatedAt: previous?.updatedAt,
        });
        values.set(bootstrap, inFlight);
      }
    }

    const resolved = await Promise.all(
      uniqueBootstraps.map(async (bootstrap) => [bootstrap, await values.get(bootstrap)] as const),
    );
    return Object.fromEntries(resolved) as Record<string, boolean>;
  }

  invalidateDiscovery(kubernetesClusterId: string): void {
    this.activateKubernetesCluster(kubernetesClusterId);
    this.invalidate(this.discovery, kubernetesClusterId);
  }

  invalidateTarget(kubernetesClusterId: string, targetId: string): void {
    this.activateKubernetesCluster(kubernetesClusterId);
    const key = this.targetKey(kubernetesClusterId, targetId);
    this.invalidate(this.health, key);
    this.invalidate(this.overviews, key);
    this.invalidate(this.reachability, key);
    this.topicDetails.invalidatePrefix(`${key}:`);
    this.groupLists.invalidate(key);
  }

  invalidateHealth(kubernetesClusterId: string, targetId: string): void {
    this.activateKubernetesCluster(kubernetesClusterId);
    this.invalidate(this.health, this.targetKey(kubernetesClusterId, targetId));
  }

  stats(kubernetesClusterId: string, targetId?: string): KafkaResourceCacheStats {
    this.activateKubernetesCluster(kubernetesClusterId);
    return {
      discoveryRequests: this.discovery.get(kubernetesClusterId)?.requestCount ?? 0,
      healthRequests: targetId
        ? (this.health.get(this.targetKey(kubernetesClusterId, targetId))?.requestCount ?? 0)
        : 0,
      overviewRequests: targetId
        ? (this.overviews.get(this.targetKey(kubernetesClusterId, targetId))?.requestCount ?? 0)
        : 0,
      reachabilityRequests: this.reachabilityRequestCounts.get(kubernetesClusterId) ?? 0,
    };
  }

  clear(): void {
    this.discovery.clear();
    this.health.clear();
    this.overviews.clear();
    this.reachability.clear();
    this.topicDetails.clear();
    this.groupLists.clear();
    this.reachabilityRequestCounts.clear();
  }

  private callLoader<T>(loader: () => Promise<T>): Promise<T> {
    try {
      return Promise.resolve(loader());
    } catch (error) {
      return Promise.reject(error);
    }
  }

  private invalidate<T>(entries: Map<string, CacheEntry<T>>, key: string): void {
    const current = entries.get(key);
    if (!current) return;
    entries.set(key, {
      ...current,
      expiresAt: 0,
      generation: this.nextGeneration(),
      inFlight: undefined,
      operationId: undefined,
      requestCount: current.requestCount,
    });
  }

  private isFresh<T>(entry: CacheEntry<T>): boolean {
    return entry.expiresAt !== undefined && entry.expiresAt > this.now();
  }

  private load<T>(
    entries: Map<string, CacheEntry<T>>,
    key: string,
    operationId: string,
    loader: () => Promise<T>,
    ttlMs = this.ttlMs,
  ): Promise<T> {
    const existing = entries.get(key);
    if (existing?.data !== undefined && this.isFresh(existing)) return Promise.resolve(existing.data);
    if (existing?.inFlight) return existing.inFlight;

    const generation = this.nextGeneration();
    const requestCount = (existing?.requestCount ?? 0) + 1;
    const inFlight = this.callLoader(loader)
      .then((data) => {
        const current = entries.get(key);
        if (current?.generation === generation) {
          const updatedAt = this.now();
          entries.set(key, {
            data,
            expiresAt: updatedAt + ttlMs,
            generation,
            requestCount: current.requestCount,
            updatedAt,
          });
        }
        return data;
      })
      .catch((error: unknown) => {
        const current = entries.get(key);
        if (current?.generation === generation) {
          entries.set(key, {
            data: existing?.data,
            expiresAt: existing?.expiresAt,
            generation,
            requestCount: current.requestCount,
            updatedAt: existing?.updatedAt,
          });
        }
        throw error;
      });

    entries.set(key, {
      data: existing?.data,
      expiresAt: existing?.expiresAt,
      generation,
      inFlight,
      operationId,
      requestCount,
      updatedAt: existing?.updatedAt,
    });
    return inFlight;
  }

  private snapshot<T>(entry?: CacheEntry<T>): KafkaCacheSnapshot<T> {
    if (!entry) return emptySnapshot();
    return {
      data: entry.data,
      fresh: entry.data !== undefined && this.isFresh(entry),
      loading: Boolean(entry.inFlight),
      operationId: entry.operationId,
      requestCount: entry.requestCount,
      updatedAt: entry.updatedAt,
    };
  }

  private nextGeneration(): number {
    this.generation += 1;
    return this.generation;
  }

  private targetKey(kubernetesClusterId: string, targetId: string): string {
    return `${kubernetesClusterId}:${targetId}`;
  }

  private topicKey(kubernetesClusterId: string, targetId: string, topic: string): string {
    return `${this.targetKey(kubernetesClusterId, targetId)}:${topic}`;
  }
}
