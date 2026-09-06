import { describe, expect, it, vi } from "vitest";
import { type ClusterHealthProgress, fetchClusterHealth } from "./cluster-health";
import { ConsumerGroupOffsetIndex } from "./group-fetch";
import { fetchHighWatermarks } from "./offset-batch";

import type { Admin } from "kafkajs";

function makeAdmin(overrides: Partial<Admin> = {}): Admin {
  return {
    describeCluster: async () => ({ brokers: [], controller: null, clusterId: "test" }),
    fetchTopicMetadata: async () => ({ topics: [] }),
    listGroups: async () => ({ groups: [] }),
    fetchOffsets: async () => [],
    fetchTopicOffsets: async () => [],
    ...overrides,
  } as unknown as Admin;
}

describe("fetchClusterHealth", () => {
  it("reports topology, consumer-group and watermark progress without hiding measured values", async () => {
    const progress: ClusterHealthProgress[] = [];
    const admin = makeAdmin({
      describeCluster: async () => ({
        brokers: [{ nodeId: 1, host: "broker-1", port: 9092 }],
        controller: 1,
        clusterId: "test",
      }),
      fetchTopicMetadata: async () => ({
        topics: [
          {
            name: "orders",
            partitions: [{ partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1] }],
          },
        ],
      }),
      listGroups: async () => ({ groups: [{ groupId: "app", protocolType: "consumer" }] }),
      fetchOffsets: async () => [{ topic: "orders", partitions: [{ partition: 0, offset: "5", metadata: null }] }],
      fetchTopicOffsets: async () => [{ partition: 0, offset: "8", high: "8", low: "0" }],
    });

    await expect(
      fetchClusterHealth(admin, new ConsumerGroupOffsetIndex(), (event) => progress.push(event)),
    ).resolves.toMatchObject({ consumerGroupLag: "3", onlineBrokers: 1 });

    expect(progress[0]).toMatchObject({
      phase: "topology",
      health: { onlineBrokers: 1, unavailablePartitions: 0, underReplicatedPartitions: 0 },
    });
    expect(progress).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ phase: "groups", completed: 0, total: 1 }),
        expect.objectContaining({ phase: "groups", completed: 1, total: 1 }),
        expect.objectContaining({ phase: "watermarks", completed: 1, total: 1 }),
      ]),
    );
    expect(progress.at(-1)).toMatchObject({
      phase: "complete",
      health: { consumerGroupLag: "3", onlineBrokers: 1 },
    });
  });

  it("keeps topology health available when consumer lag cannot be measured", async () => {
    const progress: ClusterHealthProgress[] = [];
    const admin = makeAdmin({
      describeCluster: async () => ({
        brokers: [{ nodeId: 1, host: "broker-1", port: 9092 }],
        controller: 1,
        clusterId: "test",
      }),
      fetchTopicMetadata: async () => ({ topics: [] }),
      listGroups: async () => {
        throw new Error("group scan unavailable");
      },
    });

    await expect(
      fetchClusterHealth(
        admin,
        new ConsumerGroupOffsetIndex(),
        (event) => progress.push(event),
        undefined,
        undefined,
        () => 1_234,
      ),
    ).resolves.toEqual({
      onlineBrokers: 1,
      unavailablePartitions: 0,
      underReplicatedPartitions: 0,
      topologyMeasuredAt: 1_234,
      consumerGroupLag: "Unavailable",
    });
    expect(progress.at(-1)).toMatchObject({
      phase: "complete",
      detail: "Consumer lag is unavailable; broker and partition health is ready.",
      health: { onlineBrokers: 1 },
    });
  });

  it("uses the high-watermark batch reader without calling the topic fallback", async () => {
    const fetchTopicOffsets = vi.fn();
    const progress: ClusterHealthProgress[] = [];
    const admin = makeAdmin({
      describeCluster: async () => ({
        brokers: [{ nodeId: 1, host: "broker-1", port: 9092 }],
        controller: 1,
        clusterId: "test",
      }),
      fetchTopicMetadata: async () => ({
        topics: [
          {
            name: "orders",
            partitions: [{ partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1] }],
          },
        ],
      }),
      listGroups: async () => ({ groups: [{ groupId: "app", protocolType: "consumer" }] }),
      fetchOffsets: async () => [{ topic: "orders", partitions: [{ partition: 0, offset: "5", metadata: null }] }],
      fetchTopicOffsets,
    });

    await expect(
      fetchClusterHealth(
        admin,
        new ConsumerGroupOffsetIndex(),
        (event) => progress.push(event),
        async (partitions) => new Map([[partitions[0].topic, new Map([[0, "8"]])]]),
      ),
    ).resolves.toMatchObject({ consumerGroupLag: "3" });
    expect(fetchTopicOffsets).not.toHaveBeenCalled();
    expect(progress.filter((event) => event.phase === "watermarks").at(-1)).toMatchObject({ completed: 1, total: 1 });
    expect(progress.filter((event) => event.phase === "watermarks").every((event) => (event.completed ?? 0) <= 1)).toBe(
      true,
    );
  });

  it("falls back to public topic offsets when the batch reader fails", async () => {
    const fetchTopicOffsets = vi.fn(async () => [{ partition: 0, offset: "8", high: "8", low: "0" }]);
    const admin = makeAdmin({
      describeCluster: async () => ({
        brokers: [{ nodeId: 1, host: "broker-1", port: 9092 }],
        controller: 1,
        clusterId: "test",
      }),
      fetchTopicMetadata: async () => ({
        topics: [
          {
            name: "orders",
            partitions: [{ partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1] }],
          },
        ],
      }),
      listGroups: async () => ({ groups: [{ groupId: "app", protocolType: "consumer" }] }),
      fetchOffsets: async () => [{ topic: "orders", partitions: [{ partition: 0, offset: "5", metadata: null }] }],
      fetchTopicOffsets,
    });

    await expect(
      fetchClusterHealth(admin, new ConsumerGroupOffsetIndex(), undefined, async () => {
        throw new TypeError("private adapter unavailable");
      }),
    ).resolves.toMatchObject({ consumerGroupLag: "3" });
    expect(fetchTopicOffsets).toHaveBeenCalledWith("orders");
  });

  it("does not start the public watermark fallback after cancellation", async () => {
    const fetchTopicOffsets = vi.fn();
    const abortController = new AbortController();
    const admin = makeAdmin({
      describeCluster: async () => ({
        brokers: [{ nodeId: 1, host: "broker-1", port: 9092 }],
        controller: 1,
        clusterId: "test",
      }),
      fetchTopicMetadata: async () => ({
        topics: [
          {
            name: "orders",
            partitions: [{ partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1] }],
          },
        ],
      }),
      listGroups: async () => ({ groups: [{ groupId: "app", protocolType: "consumer" }] }),
      fetchOffsets: async () => [{ topic: "orders", partitions: [{ partition: 0, offset: "5", metadata: null }] }],
      fetchTopicOffsets,
    });

    await expect(
      fetchClusterHealth(
        admin,
        new ConsumerGroupOffsetIndex(),
        undefined,
        async () => {
          abortController.abort();
          throw new Error("cancelled batch");
        },
        abortController.signal,
      ),
    ).rejects.toThrow("cancelled batch");
    expect(fetchTopicOffsets).not.toHaveBeenCalled();
  });

  it("falls back exactly after a later broker batch fails", async () => {
    const fetchTopicOffsets = vi.fn(async (topic: string) => [
      { partition: 0, offset: topic === "orders" ? "8" : "10", high: topic === "orders" ? "8" : "10", low: "0" },
    ]);
    const admin = makeAdmin({
      describeCluster: async () => ({
        brokers: [
          { nodeId: 1, host: "broker-1", port: 9092 },
          { nodeId: 2, host: "broker-2", port: 9092 },
        ],
        controller: 1,
        clusterId: "test",
      }),
      fetchTopicMetadata: async () => ({
        topics: [
          {
            name: "orders",
            partitions: [{ partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1] }],
          },
          {
            name: "payments",
            partitions: [{ partitionErrorCode: 0, partitionId: 0, leader: 2, replicas: [2], isr: [2] }],
          },
        ],
      }),
      listGroups: async () => ({ groups: [{ groupId: "app", protocolType: "consumer" }] }),
      fetchOffsets: async () => [
        { topic: "orders", partitions: [{ partition: 0, offset: "5", metadata: null }] },
        { topic: "payments", partitions: [{ partition: 0, offset: "8", metadata: null }] },
      ],
      fetchTopicOffsets,
    });
    let batchCalls = 0;
    const cluster = {
      addMultipleTargetTopics: async () => undefined,
      fetchTopicsOffset: async (
        topics: Array<{ topic: string; partitions: Array<{ partition: number }>; fromBeginning?: boolean }>,
      ) => {
        batchCalls++;
        if (batchCalls === 2) throw new Error("broker 2 unavailable");
        return topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: "8" })),
        }));
      },
    };

    await expect(
      fetchClusterHealth(admin, new ConsumerGroupOffsetIndex(), undefined, (partitions) =>
        fetchHighWatermarks(cluster, partitions),
      ),
    ).resolves.toMatchObject({ consumerGroupLag: "5" });
    expect(batchCalls).toBe(2);
    expect(fetchTopicOffsets.mock.calls.map(([topic]) => topic).sort()).toEqual(["orders", "payments"]);
  });

  it("measures broker, partition and aggregate consumer lag health", async () => {
    const admin = makeAdmin({
      describeCluster: async () => ({
        brokers: [
          { nodeId: 1, host: "broker-1", port: 9092 },
          { nodeId: 2, host: "broker-2", port: 9092 },
        ],
        controller: 1,
        clusterId: "test",
      }),
      fetchTopicMetadata: async () => ({
        topics: [
          {
            name: "orders",
            partitions: [
              { partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1, 2], isr: [1, 2] },
              { partitionErrorCode: 5, partitionId: 1, leader: -1, replicas: [1, 2], isr: [1] },
            ],
          },
          {
            name: "payments",
            partitions: [{ partitionErrorCode: 0, partitionId: 0, leader: 2, replicas: [1, 2], isr: [2] }],
          },
        ],
      }),
      listGroups: async () => ({ groups: [{ groupId: "app", protocolType: "consumer" }] }),
      fetchOffsets: async () => [
        {
          topic: "orders",
          partitions: [
            { partition: 0, offset: "5", metadata: null },
            { partition: 1, offset: "10", metadata: null },
          ],
        },
        { topic: "payments", partitions: [{ partition: 0, offset: "8", metadata: null }] },
      ],
      fetchTopicOffsets: async (topic) =>
        topic === "orders"
          ? [
              { partition: 0, offset: "8", high: "8", low: "0" },
              { partition: 1, offset: "12", high: "12", low: "0" },
            ]
          : [{ partition: 0, offset: "10", high: "10", low: "0" }],
    });

    await expect(fetchClusterHealth(admin, new ConsumerGroupOffsetIndex())).resolves.toMatchObject({
      onlineBrokers: 2,
      unavailablePartitions: 1,
      underReplicatedPartitions: 2,
      consumerGroupLag: "7",
      consumerGroupLagCoverage: {
        complete: true,
        resolvedGroups: 1,
        totalGroups: 1,
        unavailableGroups: 0,
      },
    });
  });

  it("preserves known lag when one public fallback group fails", async () => {
    const admin = makeAdmin({
      describeCluster: async () => ({
        brokers: [{ nodeId: 1, host: "broker-1", port: 9092 }],
        controller: 1,
        clusterId: "test",
      }),
      fetchTopicMetadata: async () => ({
        topics: [
          {
            name: "orders",
            partitions: [{ partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1] }],
          },
        ],
      }),
      listGroups: async () => ({
        groups: [
          { groupId: "good", protocolType: "consumer" },
          { groupId: "unavailable", protocolType: "consumer" },
        ],
      }),
      fetchOffsets: async ({ groupId }) => {
        if (groupId === "unavailable") throw new Error("coordinator unavailable");
        return [{ topic: "orders", partitions: [{ partition: 0, offset: "5", metadata: null }] }];
      },
      fetchTopicOffsets: async () => [{ partition: 0, offset: "8", high: "8", low: "0" }],
    });
    const index = new ConsumerGroupOffsetIndex(undefined, () => 100);

    await expect(fetchClusterHealth(admin, index)).resolves.toMatchObject({
      consumerGroupLag: "≥3",
      consumerGroupLagUnavailableGroups: 1,
      consumerGroupLagCoverage: {
        complete: false,
        completedAt: 100,
        resolvedGroups: 1,
        startedAt: 100,
        totalGroups: 2,
        unavailableGroups: 1,
      },
    });
  });

  it("reports measured zero lag without watermark reads when no groups have offsets", async () => {
    const fetchTopicOffsets = vi.fn();
    const admin = makeAdmin({ fetchTopicOffsets });

    await expect(fetchClusterHealth(admin, new ConsumerGroupOffsetIndex())).resolves.toMatchObject({
      consumerGroupLag: "0",
    });
    expect(fetchTopicOffsets).not.toHaveBeenCalled();
  });

  it("reports a truthful minimum lag when a committed partition has no watermark", async () => {
    const admin = makeAdmin({
      listGroups: async () => ({ groups: [{ groupId: "app", protocolType: "consumer" }] }),
      fetchOffsets: async () => [
        {
          topic: "orders",
          partitions: [
            { partition: 0, offset: "5", metadata: null },
            { partition: 1, offset: "5", metadata: null },
          ],
        },
      ],
      fetchTopicOffsets: async () => [{ partition: 0, offset: "8", high: "8", low: "0" }],
    });

    await expect(fetchClusterHealth(admin, new ConsumerGroupOffsetIndex())).resolves.toMatchObject({
      consumerGroupLag: "≥3",
      consumerGroupLagUnavailableTopics: 1,
    });
  });

  it("bounds concurrent topic watermark reads", async () => {
    let active = 0;
    let maximumActive = 0;
    const groupIds = Array.from({ length: 40 }, (_, index) => `group-${index}`);
    const admin = makeAdmin({
      listGroups: async () => ({ groups: groupIds.map((groupId) => ({ groupId, protocolType: "consumer" })) }),
      fetchOffsets: async ({ groupId }) => [
        { topic: groupId, partitions: [{ partition: 0, offset: "0", metadata: null }] },
      ],
      fetchTopicOffsets: async () => {
        active++;
        maximumActive = Math.max(maximumActive, active);
        await new Promise((resolve) => setTimeout(resolve, 1));
        active--;
        return [{ partition: 0, offset: "1", high: "1", low: "0" }];
      },
    });

    await expect(fetchClusterHealth(admin, new ConsumerGroupOffsetIndex())).resolves.toMatchObject({
      consumerGroupLag: "40",
    });
    expect(maximumActive).toBe(16);
  });
});
