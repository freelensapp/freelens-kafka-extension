import { describe, expect, it, vi } from "vitest";
import { formatKafkaCacheAge, KafkaResourceCache } from "./kafka-resource-cache";

import type {
  ClusterOverviewDto,
  ClusterOverviewHealthDto,
  ConsumerGroupsDto,
  DiscoveredKafkaInfo,
} from "../common/ipc";

const target: DiscoveredKafkaInfo = {
  targetId: "kafka-target",
  source: "manual",
  name: "local",
  namespace: "",
  bootstrap: "127.0.0.1:9092",
  tls: false,
  port: 9092,
  listeners: [],
  brokerPods: [],
};

const overview: ClusterOverviewDto = {
  brokers: [{ nodeId: 1, host: "127.0.0.1", port: 9092 }],
  controller: 1,
  topics: ["orders"],
};

const health: ClusterOverviewHealthDto = {
  onlineBrokers: 1,
  unavailablePartitions: 0,
  underReplicatedPartitions: 0,
  consumerGroupLag: "3",
};

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

describe("KafkaResourceCache", () => {
  it("formats bounded refresh ages for the page status", () => {
    expect(formatKafkaCacheAge(10_000, 12_000)).toBe("just now");
    expect(formatKafkaCacheAge(10_000, 25_000)).toBe("15s ago");
    expect(formatKafkaCacheAge(10_000, 130_000)).toBe("2m ago");
  });

  it("deduplicates concurrent discovery and serves a fresh warm result", async () => {
    let now = 1_000;
    const cache = new KafkaResourceCache({ now: () => now, ttlMs: 60_000 });
    const pending = deferred<DiscoveredKafkaInfo[]>();
    const loader = vi.fn(() => pending.promise);

    const first = cache.loadDiscovery("kube-a", "discovery-1", loader);
    const second = cache.loadDiscovery("kube-a", "discovery-2", loader);

    expect(loader).toHaveBeenCalledTimes(1);
    expect(cache.readDiscovery("kube-a").operationId).toBe("discovery-1");
    pending.resolve([target]);
    await expect(Promise.all([first, second])).resolves.toEqual([[target], [target]]);

    now += 10_000;
    await expect(cache.loadDiscovery("kube-a", "discovery-3", loader)).resolves.toEqual([target]);
    expect(loader).toHaveBeenCalledTimes(1);
    expect(cache.readDiscovery("kube-a")).toMatchObject({ fresh: true, loading: false, requestCount: 1 });
  });

  it("reloads expired entries and explicit invalidation prevents a stale response from winning", async () => {
    let now = 1_000;
    const cache = new KafkaResourceCache({ now: () => now, ttlMs: 100 });
    const first = vi.fn(async () => [target]);

    await cache.loadDiscovery("kube-a", "discovery-1", first);
    now += 101;
    await cache.loadDiscovery("kube-a", "discovery-2", first);
    expect(first).toHaveBeenCalledTimes(2);

    const stale = deferred<DiscoveredKafkaInfo[]>();
    cache.invalidateDiscovery("kube-a");
    const staleLoad = cache.loadDiscovery("kube-a", "discovery-3", () => stale.promise);
    cache.invalidateDiscovery("kube-a");
    const currentTarget = { ...target, name: "current" };
    await cache.loadDiscovery("kube-a", "discovery-4", async () => [currentTarget]);
    stale.resolve([{ ...target, name: "stale" }]);
    await staleLoad;

    expect(cache.readDiscovery("kube-a").data).toEqual([currentTarget]);
    expect(cache.stats("kube-a").discoveryRequests).toBe(4);
  });

  it("isolates overview entries by target and clears them when the Kubernetes cluster changes", async () => {
    const cache = new KafkaResourceCache();
    const otherOverview = { ...overview, controller: 2 };

    await cache.loadOverview("kube-a", "target-a", "overview-1", async () => overview);
    await cache.loadOverview("kube-a", "target-b", "overview-2", async () => otherOverview);
    expect(cache.readOverview("kube-a", "target-a").data).toEqual(overview);
    expect(cache.readOverview("kube-a", "target-b").data).toEqual(otherOverview);

    cache.activateKubernetesCluster("kube-b");
    expect(cache.readOverview("kube-b", "target-a")).toMatchObject({ fresh: false, requestCount: 0 });
  });

  it("deduplicates in-flight health and reuses the result for five minutes across page mounts", async () => {
    let now = 1_000;
    const cache = new KafkaResourceCache({ now: () => now, ttlMs: 100, healthTtlMs: 5 * 60_000 });
    const pending = deferred<ClusterOverviewHealthDto>();
    const loader = vi.fn(() => pending.promise);

    const first = cache.loadHealth("kube-a", "target-a", "health-1", loader);
    const second = cache.loadHealth("kube-a", "target-a", "health-2", loader);

    expect(loader).toHaveBeenCalledTimes(1);
    expect(cache.readHealth("kube-a", "target-a")).toMatchObject({
      loading: true,
      operationId: "health-1",
      requestCount: 1,
    });

    pending.resolve(health);
    await expect(Promise.all([first, second])).resolves.toEqual([health, health]);
    now += 4 * 60_000;
    await expect(cache.loadHealth("kube-a", "target-a", "health-3", loader)).resolves.toEqual(health);
    expect(loader).toHaveBeenCalledTimes(1);
    expect(cache.readHealth("kube-a", "target-a")).toMatchObject({ data: health, fresh: true, loading: false });
    expect(cache.stats("kube-a", "target-a").healthRequests).toBe(1);
  });

  it("rejects a stale response across an A to B to A Kubernetes cluster switch", async () => {
    const cache = new KafkaResourceCache();
    const stale = deferred<DiscoveredKafkaInfo[]>();
    const staleLoad = cache.loadDiscovery("kube-a", "discovery-stale", () => stale.promise);

    cache.activateKubernetesCluster("kube-b");
    cache.activateKubernetesCluster("kube-a");
    const currentTarget = { ...target, name: "current" };
    await cache.loadDiscovery("kube-a", "discovery-current", async () => [currentTarget]);
    stale.resolve([{ ...target, name: "stale" }]);
    await staleLoad;

    expect(cache.readDiscovery("kube-a").data).toEqual([currentTarget]);
  });

  it("invalidates only the selected target metadata", async () => {
    const cache = new KafkaResourceCache();
    await cache.loadOverview("kube-a", "target-a", "overview-1", async () => overview);
    await cache.loadOverview("kube-a", "target-b", "overview-2", async () => overview);
    await cache.loadHealth("kube-a", "target-a", "health-1", async () => health);
    await cache.loadHealth("kube-a", "target-b", "health-2", async () => health);

    cache.invalidateTarget("kube-a", "target-a");

    expect(cache.readOverview("kube-a", "target-a").fresh).toBe(false);
    expect(cache.readOverview("kube-a", "target-a").data).toEqual(overview);
    expect(cache.readOverview("kube-a", "target-b").fresh).toBe(true);
    expect(cache.readHealth("kube-a", "target-a")).toMatchObject({ data: health, fresh: false });
    expect(cache.readHealth("kube-a", "target-b")).toMatchObject({ data: health, fresh: true });
  });

  it("invalidates target topic and group snapshots after security or explicit refresh", async () => {
    const cache = new KafkaResourceCache();
    const detail = {
      name: "orders",
      internal: false,
      partitions: [],
      partitionCount: 0,
      replicationFactor: 0,
      underReplicatedPartitions: 0,
      unavailablePartitions: 0,
    };
    await cache.loadTopic("kube-a", "target-a", "orders", async () => detail);
    await cache.loadGroups("kube-a", "target-a", async () => ({ groups: [] }));

    cache.invalidateTarget("kube-a", "target-a");

    expect(cache.readTopic("kube-a", "target-a", "orders")).toMatchObject({ data: detail, fresh: false, stale: true });
    expect(cache.readGroups("kube-a", "target-a")).toMatchObject({ data: { groups: [] }, fresh: false, stale: true });
  });

  it("shares batched reachability results with subset requests", async () => {
    const cache = new KafkaResourceCache();
    const loader = vi.fn(async (bootstraps: string[]) =>
      Object.fromEntries(bootstraps.map((bootstrap) => [bootstrap, true])),
    );

    await cache.loadReachability("kube-a", ["127.0.0.1:9092", "127.0.0.1:9093"], loader);
    await expect(cache.loadReachability("kube-a", ["127.0.0.1:9092"], loader)).resolves.toEqual({
      "127.0.0.1:9092": true,
    });

    expect(loader).toHaveBeenCalledTimes(1);
    expect(cache.stats("kube-a", target.targetId).reachabilityRequests).toBe(1);
  });

  it("serves stale topic detail while one refresh is in flight", async () => {
    let now = 1_000;
    const cache = new KafkaResourceCache({ now: () => now, ttlMs: 100 });
    const detail = {
      name: "orders",
      internal: false,
      partitions: [],
      partitionCount: 0,
      replicationFactor: 0,
      underReplicatedPartitions: 0,
      unavailablePartitions: 0,
    };
    await cache.loadTopic("kube-a", "target-a", "orders", async () => detail);
    now += 101;
    const pending = deferred<typeof detail>();
    const refresh = cache.loadTopic("kube-a", "target-a", "orders", () => pending.promise);
    expect(cache.readTopic("kube-a", "target-a", "orders")).toMatchObject({ data: detail, stale: true, loading: true });
    pending.resolve({ ...detail, partitionCount: 1 });
    await refresh;
    expect(cache.readTopic("kube-a", "target-a", "orders")).toMatchObject({
      data: { partitionCount: 1 },
      fresh: true,
      stale: false,
    });
  });

  it("serves stale consumer-group lists while one refresh is in flight", async () => {
    let now = 1_000;
    const cache = new KafkaResourceCache({ now: () => now, ttlMs: 100 });
    const groups: ConsumerGroupsDto = { groups: [] };
    await cache.loadGroups("kube-a", "target-a", async () => groups);
    now += 101;
    const pending = deferred<typeof groups>();
    const refresh = cache.loadGroups("kube-a", "target-a", () => pending.promise);
    expect(cache.readGroups("kube-a", "target-a")).toMatchObject({ data: groups, stale: true, loading: true });
    pending.resolve({ groups: [{ groupId: "orders", state: "Stable", protocolType: "consumer", memberCount: 1 }] });
    await refresh;
    expect(cache.readGroups("kube-a", "target-a")).toMatchObject({
      data: { groups: [{ groupId: "orders" }] },
      fresh: true,
    });
  });
});
