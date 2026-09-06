import { describe, expect, it, vi } from "vitest";
import { aggregateHealthWorkerKey, KafkaAggregateHealthManager } from "./aggregate-health-manager";

import type { ClusterOverviewHealthDto } from "../../common/ipc";
import type { KafkaProgressUpdate } from "./progress";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

const health: ClusterOverviewHealthDto = {
  onlineBrokers: 3,
  unavailablePartitions: 0,
  underReplicatedPartitions: 0,
  consumerGroupLag: "42",
  consumerGroupLagCoverage: {
    complete: true,
    completedAt: 2_000,
    resolvedGroups: 10,
    startedAt: 1_000,
    totalGroups: 10,
    unavailableGroups: 0,
  },
};

describe("KafkaAggregateHealthManager", () => {
  it("coalesces one worker per key and multicasts progress to concurrent subscribers", async () => {
    const pending = deferred<ClusterOverviewHealthDto>();
    let publish!: (progress: KafkaProgressUpdate) => void;
    const loader = vi.fn((report: (progress: KafkaProgressUpdate) => void) => {
      publish = report;
      return pending.promise;
    });
    const firstProgress = vi.fn();
    const secondProgress = vi.fn();
    const manager = new KafkaAggregateHealthManager();

    const first = manager.load("target-a", loader, firstProgress);
    const second = manager.load("target-a", loader, secondProgress);
    await Promise.resolve();
    publish({ value: 75, phase: "groups", label: "Reading groups", completed: 5, total: 10 });
    pending.resolve(health);

    await expect(Promise.all([first, second])).resolves.toEqual([health, health]);
    expect(loader).toHaveBeenCalledTimes(1);
    expect(firstProgress).toHaveBeenCalledOnce();
    expect(secondProgress).toHaveBeenCalledOnce();
    expect(manager.read("target-a")).toMatchObject({ data: health, fresh: true, loading: false });
  });

  it("reuses a warm result and preserves stale data when refresh fails", async () => {
    let now = 1_000;
    const manager = new KafkaAggregateHealthManager({ now: () => now, ttlMs: 100 });
    const loader = vi.fn(async () => health);

    await manager.load("target", loader);
    await manager.load("target", loader);
    expect(loader).toHaveBeenCalledTimes(1);
    now += 101;
    await expect(manager.load("target", async () => Promise.reject(new Error("refresh failed")))).rejects.toThrow(
      "refresh failed",
    );

    expect(manager.read("target")).toMatchObject({
      data: health,
      error: "refresh failed",
      fresh: false,
      lastComplete: health,
      loading: false,
    });
  });

  it("restores a persisted snapshot as stale and publishes it before revalidation", async () => {
    const manager = new KafkaAggregateHealthManager({ now: () => 2_000 });
    manager.restore("target", { data: health, lastComplete: health, updatedAt: 1_000 });
    const progress = vi.fn();
    const pending = deferred<ClusterOverviewHealthDto>();

    const refresh = manager.load("target", () => pending.promise, progress);
    expect(progress).toHaveBeenCalledWith(expect.objectContaining({ phase: "cache", healthSnapshot: health }));
    expect(manager.read("target")).toMatchObject({ data: health, fresh: false, loading: true, updatedAt: 1_000 });
    pending.resolve(health);
    await refresh;
  });

  it("aborts invalidated work and ignores its late result", async () => {
    const manager = new KafkaAggregateHealthManager();
    let workerSignal!: AbortSignal;
    const pending = deferred<ClusterOverviewHealthDto>();
    const load = manager.load("target", async (_report, signal) => {
      workerSignal = signal;
      await pending.promise;
      if (signal.aborted) throw new Error("cancelled");
      return health;
    });
    await Promise.resolve();

    manager.invalidate("target");
    expect(workerSignal.aborted).toBe(true);
    pending.resolve(health);
    await expect(load).rejects.toThrow("cancelled");
    expect(manager.read("target")).toMatchObject({ data: undefined, fresh: false, loading: false });
  });

  it("rejects a late non-cooperative loader result after invalidation", async () => {
    const manager = new KafkaAggregateHealthManager();
    const pending = deferred<ClusterOverviewHealthDto>();
    const load = manager.load("target", () => pending.promise);
    await Promise.resolve();

    manager.invalidate("target");
    pending.resolve(health);

    await expect(load).rejects.toThrow("cancelled");
    expect(manager.read("target").data).toBeUndefined();
  });

  it("waits for cancelled work to settle before starting a replacement worker", async () => {
    const manager = new KafkaAggregateHealthManager();
    const firstGate = deferred<ClusterOverviewHealthDto>();
    let active = 0;
    let maximumActive = 0;
    let calls = 0;
    const loader = async (_report: (progress: KafkaProgressUpdate) => void, signal: AbortSignal) => {
      calls++;
      active++;
      maximumActive = Math.max(maximumActive, active);
      try {
        if (calls === 1) await firstGate.promise;
        if (signal.aborted) throw new Error("cancelled");
        return health;
      } finally {
        active--;
      }
    };
    const first = manager.load("target", loader);
    await Promise.resolve();
    manager.invalidate("target");
    const second = manager.load("target", loader);

    expect(calls).toBe(1);
    firstGate.resolve(health);
    await expect(first).rejects.toThrow("cancelled");
    await expect(second).resolves.toEqual(health);
    expect(calls).toBe(2);
    expect(maximumActive).toBe(1);
  });

  it("keeps the least-recently-used entry count bounded", async () => {
    const manager = new KafkaAggregateHealthManager({ maxEntries: 3 });
    for (let index = 0; index < 200; index++) {
      await manager.load(`target-${index}`, async () => ({ ...health, consumerGroupLag: String(index) }));
    }

    expect(manager.size()).toBe(3);
    expect(manager.read("target-0").data).toBeUndefined();
    expect(manager.read("target-199").data?.consumerGroupLag).toBe("199");
  });

  it("forces one revalidation without dropping the previous result", async () => {
    const manager = new KafkaAggregateHealthManager();
    await manager.load("target", async () => health);
    const pending = deferred<ClusterOverviewHealthDto>();
    const refresh = manager.load("target", () => pending.promise, undefined, true);

    expect(manager.read("target")).toMatchObject({ data: health, loading: true, lastComplete: health });
    pending.resolve({ ...health, consumerGroupLag: "43" });
    await expect(refresh).resolves.toMatchObject({ consumerGroupLag: "43" });
  });

  it("isolates context and security generations and removes cancelled targets", async () => {
    const first = aggregateHealthWorkerKey("context-a", "target", "security-1");
    const second = aggregateHealthWorkerKey("context-b", "target", "security-1");
    const third = aggregateHealthWorkerKey("context-a", "target", "security-2");
    expect(new Set([first, second, third])).toHaveLength(3);

    const manager = new KafkaAggregateHealthManager();
    await manager.load(first, async () => health);
    manager.remove(first);
    expect(manager.size()).toBe(0);
    expect(manager.read(first).data).toBeUndefined();
  });

  it("keeps the known lag lower bound monotonic during one refresh", async () => {
    const manager = new KafkaAggregateHealthManager();
    const progress = vi.fn();

    await manager.load(
      "target",
      async (report) => {
        report({
          value: 80,
          phase: "groups",
          label: "Groups",
          healthSnapshot: { consumerGroupLag: "≥10" },
        });
        report({
          value: 90,
          phase: "watermarks",
          label: "Watermarks",
          healthSnapshot: { consumerGroupLag: "7" },
        });
        return { ...health, consumerGroupLag: "12" };
      },
      progress,
    );

    expect(progress.mock.calls.map(([event]) => event.healthSnapshot?.consumerGroupLag)).toEqual(["≥10", "≥10"]);
    expect(manager.read("target").data?.consumerGroupLag).toBe("12");
  });

  it("never promotes incomplete or unproven data to lastComplete", async () => {
    const manager = new KafkaAggregateHealthManager();
    await manager.load("target", async () => ({
      ...health,
      consumerGroupLag: "≥42",
      consumerGroupLagUnavailableGroups: 1,
      consumerGroupLagCoverage: { ...health.consumerGroupLagCoverage!, complete: false, unavailableGroups: 1 },
    }));
    expect(manager.read("target").lastComplete).toBeUndefined();

    manager.remove("target");
    await manager.load("target", async () => {
      const { consumerGroupLagCoverage: _coverage, ...withoutCoverage } = health;
      return withoutCoverage;
    });
    expect(manager.read("target").lastComplete).toBeUndefined();
  });
});
