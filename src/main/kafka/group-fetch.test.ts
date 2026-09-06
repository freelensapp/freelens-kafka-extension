import { describe, expect, it, vi } from "vitest";
import {
  ConsumerGroupOffsetIndex,
  fetchConsumerGroupDetail,
  fetchTopicConsumers,
  listConsumerGroups,
} from "./group-fetch";

import type { Admin } from "kafkajs";

function makeAdmin(overrides: Partial<Admin> = {}): Admin {
  return {
    connect: async () => undefined,
    disconnect: async () => undefined,
    listGroups: async () => ({ groups: [] }),
    describeGroups: async () => ({ groups: [] }),
    fetchOffsets: async () => [],
    fetchTopicOffsets: async () => [],
    ...overrides,
  } as unknown as Admin;
}

describe("listConsumerGroups", () => {
  it("returns empty array when no groups exist", async () => {
    const admin = makeAdmin();
    expect(await listConsumerGroups(admin)).toEqual([]);
  });

  it("maps group state and member count from describeGroups", async () => {
    const admin = makeAdmin({
      listGroups: async () => ({
        groups: [
          { groupId: "app-consumer", protocolType: "consumer" },
          { groupId: "empty-group", protocolType: "consumer" },
        ],
      }),
      describeGroups: async () => ({
        groups: [
          {
            groupId: "app-consumer",
            state: "Stable",
            protocol: "range",
            protocolType: "consumer",
            members: [
              {
                memberId: "m1",
                clientId: "c1",
                clientHost: "/127.0.0.1",
                memberAssignment: Buffer.alloc(0),
                memberMetadata: Buffer.alloc(0),
              },
            ],
          },
          {
            groupId: "empty-group",
            state: "Empty",
            protocol: "",
            protocolType: "consumer",
            members: [],
          },
        ],
      }),
    });
    const result = await listConsumerGroups(admin);
    expect(result).toEqual([
      { groupId: "app-consumer", state: "Stable", protocolType: "consumer", memberCount: 1 },
      { groupId: "empty-group", state: "Empty", protocolType: "consumer", memberCount: 0 },
    ]);
  });

  it("sorts groups alphabetically by group ID", async () => {
    const admin = makeAdmin({
      listGroups: async () => ({
        groups: [
          { groupId: "zebra-group", protocolType: "consumer" },
          { groupId: "alpha-group", protocolType: "consumer" },
        ],
      }),
      describeGroups: async () => ({
        groups: [
          { groupId: "zebra-group", state: "Stable", protocol: "range", protocolType: "consumer", members: [] },
          { groupId: "alpha-group", state: "Stable", protocol: "range", protocolType: "consumer", members: [] },
        ],
      }),
    });
    const result = await listConsumerGroups(admin);
    expect(result.map((g) => g.groupId)).toEqual(["alpha-group", "zebra-group"]);
  });

  it("shares one in-flight group enumeration with the aggregate offset index", async () => {
    let releaseGroups!: () => void;
    const groupGate = new Promise<void>((resolve) => {
      releaseGroups = resolve;
    });
    const listGroups = vi.fn(async () => {
      await groupGate;
      return { groups: [{ groupId: "shared", protocolType: "consumer" }] };
    });
    const admin = makeAdmin({
      listGroups,
      describeGroups: async () => ({
        groups: [{ groupId: "shared", state: "Stable", protocol: "range", protocolType: "consumer", members: [] }],
      }),
      fetchOffsets: async () => [],
    });
    const index = new ConsumerGroupOffsetIndex();

    const scan = index.all(admin);
    const list = listConsumerGroups(admin, index);
    releaseGroups();
    await Promise.all([scan, list]);

    expect(listGroups).toHaveBeenCalledOnce();
  });

  it("does not seed the aggregate group inventory from a cold list request", async () => {
    const listGroups = vi
      .fn()
      .mockResolvedValueOnce({ groups: [{ groupId: "before", protocolType: "consumer" }] })
      .mockResolvedValueOnce({ groups: [{ groupId: "after", protocolType: "consumer" }] });
    const fetchOffsets = vi.fn(async ({ groupId }: { groupId: string }) => [
      { topic: "orders", partitions: [{ partition: 0, offset: groupId === "after" ? "2" : "1", metadata: null }] },
    ]);
    const admin = makeAdmin({
      listGroups,
      describeGroups: async () => ({
        groups: [{ groupId: "before", state: "Stable", protocol: "range", protocolType: "consumer", members: [] }],
      }),
      fetchOffsets: fetchOffsets as unknown as Admin["fetchOffsets"],
    });
    const index = new ConsumerGroupOffsetIndex();

    await listConsumerGroups(admin, index);
    await expect(index.all(admin)).resolves.toEqual(
      new Map([["orders", [{ groupId: "after", committed: [{ partition: 0, offset: "2" }] }]]]),
    );

    expect(listGroups).toHaveBeenCalledTimes(2);
    expect(fetchOffsets).toHaveBeenCalledWith({ groupId: "after", resolveOffsets: false });
  });

  it("retries Groups with its own Admin after shared enumeration is cancelled", async () => {
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const listGroups = vi
      .fn()
      .mockImplementationOnce(async () => {
        await firstGate;
        return { groups: [{ groupId: "stale", protocolType: "consumer" }] };
      })
      .mockResolvedValueOnce({ groups: [{ groupId: "fresh", protocolType: "consumer" }] });
    const admin = makeAdmin({
      listGroups,
      describeGroups: async () => ({
        groups: [{ groupId: "fresh", state: "Stable", protocol: "range", protocolType: "consumer", members: [] }],
      }),
      fetchOffsets: async () => [],
    });
    const index = new ConsumerGroupOffsetIndex();
    const cancelled = new AbortController();
    const scan = index.all(admin, "", undefined, cancelled.signal);
    const groups = listConsumerGroups(admin, index);
    cancelled.abort();
    releaseFirst();

    await expect(scan).rejects.toThrow("cancelled");
    await expect(groups).resolves.toEqual([
      { groupId: "fresh", state: "Stable", protocolType: "consumer", memberCount: 0 },
    ]);
    expect(listGroups).toHaveBeenCalledTimes(2);
  });
});

describe("fetchConsumerGroupDetail — lag computation", () => {
  it("computes lag as highWatermark minus committedOffset", async () => {
    const admin = makeAdmin({
      describeGroups: async () => ({
        groups: [{ groupId: "g1", state: "Stable", protocol: "range", protocolType: "consumer", members: [] }],
      }),
      fetchOffsets: async () => [{ topic: "orders", partitions: [{ partition: 0, offset: "5", metadata: null }] }],
      fetchTopicOffsets: async () => [{ partition: 0, offset: "5", high: "10", low: "0" }],
    });
    const detail = await fetchConsumerGroupDetail(admin, "g1");
    const p = detail.topicOffsets[0].partitions[0];
    expect(p.committedOffset).toBe("5");
    expect(p.highWatermark).toBe("10");
    expect(p.lag).toBe("5");
  });

  it("reuses an in-flight aggregate offset result instead of fetching the group again", async () => {
    let releaseBatch!: () => void;
    const batchGate = new Promise<void>((resolve) => {
      releaseBatch = resolve;
    });
    const readBatch = vi.fn(async () => {
      await batchGate;
      return {
        supported: true,
        offsetsByTopic: new Map([["orders", [{ groupId: "g1", committed: [{ partition: 0, offset: "5" }] }]]]),
        resolvedGroupIds: new Set(["g1"]),
        unresolvedGroupIds: new Set<string>(),
      };
    });
    const fetchOffsets = vi.fn();
    const admin = makeAdmin({
      listGroups: async () => ({ groups: [{ groupId: "g1", protocolType: "consumer" }] }),
      describeGroups: async () => ({
        groups: [{ groupId: "g1", state: "Stable", protocol: "range", protocolType: "consumer", members: [] }],
      }),
      fetchOffsets,
      fetchTopicOffsets: async () => [{ partition: 0, offset: "5", high: "10", low: "0" }],
    });
    const index = new ConsumerGroupOffsetIndex(readBatch);
    const scan = index.all(admin);
    await Promise.resolve();
    const detail = fetchConsumerGroupDetail(admin, "g1", index);
    releaseBatch();

    await scan;
    await expect(detail).resolves.toMatchObject({
      topicOffsets: [{ topic: "orders", totalLag: "5" }],
    });
    expect(fetchOffsets).not.toHaveBeenCalled();
  });

  it("uses public offsets when the shared index has only partial unavailable group data", async () => {
    let fallbackCalls = 0;
    const readBatch = vi.fn(async () => ({
      supported: true,
      offsetsByTopic: new Map([["orders", [{ groupId: "g1", committed: [{ partition: 0, offset: "5" }] }]]]),
      resolvedGroupIds: new Set<string>(),
      unresolvedGroupIds: new Set(["g1"]),
    }));
    const fetchOffsets = vi.fn(async () => {
      fallbackCalls++;
      if (fallbackCalls === 1) throw new Error("initial fallback failed");
      return [
        {
          topic: "orders",
          partitions: [
            { partition: 0, offset: "5", metadata: null },
            { partition: 1, offset: "6", metadata: null },
          ],
        },
      ];
    });
    const admin = makeAdmin({
      listGroups: async () => ({ groups: [{ groupId: "g1", protocolType: "consumer" }] }),
      describeGroups: async () => ({
        groups: [{ groupId: "g1", state: "Stable", protocol: "range", protocolType: "consumer", members: [] }],
      }),
      fetchOffsets: fetchOffsets as unknown as Admin["fetchOffsets"],
      fetchTopicOffsets: async () => [
        { partition: 0, offset: "5", high: "10", low: "0" },
        { partition: 1, offset: "6", high: "10", low: "0" },
      ],
    });
    const index = new ConsumerGroupOffsetIndex(readBatch);
    await index.all(admin);

    await expect(fetchConsumerGroupDetail(admin, "g1", index)).resolves.toMatchObject({
      topicOffsets: [{ topic: "orders", totalLag: "9", partitions: [{ partition: 0 }, { partition: 1 }] }],
    });
    expect(fetchOffsets).toHaveBeenCalledTimes(2);
  });

  it("clamps negative lag to 0", async () => {
    const admin = makeAdmin({
      describeGroups: async () => ({
        groups: [{ groupId: "g1", state: "Stable", protocol: "range", protocolType: "consumer", members: [] }],
      }),
      fetchOffsets: async () => [{ topic: "t", partitions: [{ partition: 0, offset: "20", metadata: null }] }],
      fetchTopicOffsets: async () => [{ partition: 0, offset: "15", high: "15", low: "0" }],
    });
    const detail = await fetchConsumerGroupDetail(admin, "g1");
    expect(detail.topicOffsets[0].partitions[0].lag).toBe("0");
  });

  it("shows — for uncommitted partitions (offset -1)", async () => {
    const admin = makeAdmin({
      describeGroups: async () => ({
        groups: [{ groupId: "g1", state: "Empty", protocol: "", protocolType: "consumer", members: [] }],
      }),
      fetchOffsets: async () => [{ topic: "t", partitions: [{ partition: 0, offset: "-1", metadata: null }] }],
      fetchTopicOffsets: async () => [{ partition: 0, offset: "0", high: "10", low: "0" }],
    });
    const detail = await fetchConsumerGroupDetail(admin, "g1");
    const p = detail.topicOffsets[0].partitions[0];
    expect(p.lag).toBe("—");
    expect(p.committedOffset).toBe("-1");
  });

  it("sums partition lags for totalLag", async () => {
    const admin = makeAdmin({
      describeGroups: async () => ({
        groups: [{ groupId: "g1", state: "Stable", protocol: "range", protocolType: "consumer", members: [] }],
      }),
      fetchOffsets: async () => [
        {
          topic: "t",
          partitions: [
            { partition: 0, offset: "5", metadata: null },
            { partition: 1, offset: "10", metadata: null },
          ],
        },
      ],
      fetchTopicOffsets: async () => [
        { partition: 0, offset: "5", high: "8", low: "0" },
        { partition: 1, offset: "10", high: "15", low: "0" },
      ],
    });
    const detail = await fetchConsumerGroupDetail(admin, "g1");
    expect(detail.topicOffsets[0].totalLag).toBe("8"); // (8-5) + (15-10)
  });

  it("shows — totalLag when all partitions are uncommitted", async () => {
    const admin = makeAdmin({
      describeGroups: async () => ({
        groups: [{ groupId: "g1", state: "Empty", protocol: "", protocolType: "consumer", members: [] }],
      }),
      fetchOffsets: async () => [{ topic: "t", partitions: [{ partition: 0, offset: "-1", metadata: null }] }],
      fetchTopicOffsets: async () => [{ partition: 0, offset: "0", high: "5", low: "0" }],
    });
    const detail = await fetchConsumerGroupDetail(admin, "g1");
    expect(detail.topicOffsets[0].totalLag).toBe("—");
  });

  it("includes member plaintext fields in detail", async () => {
    const admin = makeAdmin({
      describeGroups: async () => ({
        groups: [
          {
            groupId: "g1",
            state: "Stable",
            protocol: "range",
            protocolType: "consumer",
            members: [
              {
                memberId: "member-1",
                clientId: "my-app-1",
                clientHost: "/10.0.0.1",
                memberAssignment: Buffer.alloc(0),
                memberMetadata: Buffer.alloc(0),
              },
            ],
          },
        ],
      }),
      fetchOffsets: async () => [],
      fetchTopicOffsets: async () => [],
    });
    const detail = await fetchConsumerGroupDetail(admin, "g1");
    expect(detail.members).toEqual([{ memberId: "member-1", clientId: "my-app-1", clientHost: "/10.0.0.1" }]);
  });
});

describe("fetchTopicConsumers", () => {
  it("returns only consumer groups with committed offsets and aggregates topic lag", async () => {
    const fetchOffsets = vi.fn(async ({ groupId }: { groupId: string }) => [
      {
        topic: "orders",
        partitions:
          groupId === "orders-app"
            ? [
                { partition: 0, offset: "5", metadata: null },
                { partition: 1, offset: "10", metadata: null },
              ]
            : [{ partition: 0, offset: "-1", metadata: null }],
      },
    ]);
    const describeGroups = vi.fn(async (groupIds: string[]) => ({
      groups: groupIds.map((groupId) => ({
        groupId,
        state: "Stable" as const,
        protocol: "range",
        protocolType: "consumer",
        members: [],
      })),
    }));
    const admin = makeAdmin({
      listGroups: async () => ({
        groups: [
          { groupId: "orders-app", protocolType: "consumer" },
          { groupId: "uncommitted-app", protocolType: "consumer" },
          { groupId: "transactional-app", protocolType: "connect" },
        ],
      }),
      describeGroups,
      fetchOffsets: fetchOffsets as unknown as Admin["fetchOffsets"],
      fetchTopicOffsets: async () => [
        { partition: 0, offset: "5", high: "8", low: "0" },
        { partition: 1, offset: "10", high: "15", low: "0" },
      ],
    });

    await expect(fetchTopicConsumers(admin, "orders")).resolves.toEqual({
      topic: "orders",
      groups: [{ groupId: "orders-app", state: "Stable", memberCount: 0, totalLag: "8" }],
    });
    expect(fetchOffsets).toHaveBeenCalledTimes(2);
    expect(fetchOffsets).toHaveBeenCalledWith({
      groupId: "orders-app",
      resolveOffsets: false,
    });
    expect(describeGroups).toHaveBeenCalledOnce();
    expect(describeGroups).toHaveBeenCalledWith(["orders-app"]);
  });

  it("reports bounded scan progress for every consumer group", async () => {
    const progress = vi.fn();
    const admin = makeAdmin({
      listGroups: async () => ({
        groups: [
          { groupId: "first", protocolType: "consumer" },
          { groupId: "second", protocolType: "consumer" },
        ],
      }),
      fetchOffsets: async () => [{ topic: "orders", partitions: [{ partition: 0, offset: "-1", metadata: null }] }],
    });

    await fetchTopicConsumers(admin, "orders", progress);
    expect(progress.mock.calls.map(([value]) => value)).toEqual([
      { completed: 0, total: 2 },
      { completed: 1, total: 2 },
      { completed: 2, total: 2 },
    ]);
  });

  it("emits a partial row when the index finds a matching group", async () => {
    const progress = vi.fn();
    const admin = makeAdmin({
      listGroups: async () => ({ groups: [{ groupId: "orders-app", protocolType: "consumer" }] }),
      fetchOffsets: async () => [{ topic: "orders", partitions: [{ partition: 0, offset: "2", metadata: null }] }],
      describeGroups: async () => ({
        groups: [
          {
            groupId: "orders-app",
            state: "Stable",
            protocol: "range",
            protocolType: "consumer",
            members: [],
          },
        ],
      }),
      fetchTopicOffsets: async () => [{ partition: 0, offset: "5", high: "5", low: "0" }],
    });

    await fetchTopicConsumers(admin, "orders", progress);

    expect(progress).toHaveBeenCalledWith({
      completed: 1,
      total: 1,
      topicConsumerGroup: { groupId: "orders-app", state: "Loading", memberCount: 0, totalLag: "—" },
    });
  });

  it("returns an empty result without fetching topic offsets when no consumer groups exist", async () => {
    const fetchTopicOffsets = vi.fn();
    const admin = makeAdmin({ fetchTopicOffsets });

    await expect(fetchTopicConsumers(admin, "orders")).resolves.toEqual({ topic: "orders", groups: [] });
    expect(fetchTopicOffsets).not.toHaveBeenCalled();
  });

  it("does not describe any group when none has committed offsets on the topic", async () => {
    const describeGroups = vi.fn();
    const fetchTopicOffsets = vi.fn();
    const admin = makeAdmin({
      listGroups: async () => ({ groups: [{ groupId: "uncommitted", protocolType: "consumer" }] }),
      fetchOffsets: async () => [{ topic: "orders", partitions: [{ partition: 0, offset: "-1", metadata: null }] }],
      describeGroups,
      fetchTopicOffsets,
    });

    await expect(fetchTopicConsumers(admin, "orders")).resolves.toEqual({ topic: "orders", groups: [] });
    expect(describeGroups).not.toHaveBeenCalled();
    expect(fetchTopicOffsets).not.toHaveBeenCalled();
  });

  it("bounds concurrent offset reads when scanning many groups", async () => {
    let active = 0;
    let maximumActive = 0;
    const groupIds = Array.from({ length: 40 }, (_, index) => `group-${index}`);
    const admin = makeAdmin({
      listGroups: async () => ({ groups: groupIds.map((groupId) => ({ groupId, protocolType: "consumer" })) }),
      fetchOffsets: async () => {
        active++;
        maximumActive = Math.max(maximumActive, active);
        await new Promise((resolve) => setTimeout(resolve, 1));
        active--;
        return [{ topic: "orders", partitions: [{ partition: 0, offset: "-1", metadata: null }] }];
      },
    });

    await expect(fetchTopicConsumers(admin, "orders")).resolves.toEqual({ topic: "orders", groups: [] });
    expect(maximumActive).toBe(16);
  });

  it("reuses one all-topic offset index across later topic lookups", async () => {
    const listGroups = vi.fn(async () => ({ groups: [{ groupId: "shared", protocolType: "consumer" }] }));
    const fetchOffsets = vi.fn(async () => [
      { topic: "orders", partitions: [{ partition: 0, offset: "2", metadata: null }] },
      { topic: "payments", partitions: [{ partition: 0, offset: "3", metadata: null }] },
    ]);
    const admin = makeAdmin({
      listGroups,
      fetchOffsets: fetchOffsets as unknown as Admin["fetchOffsets"],
      describeGroups: async (groupIds) => ({
        groups: groupIds.map((groupId) => ({
          groupId,
          state: "Stable",
          protocol: "range",
          protocolType: "consumer",
          members: [],
        })),
      }),
      fetchTopicOffsets: async () => [{ partition: 0, offset: "5", high: "5", low: "0" }],
    });
    const index = new ConsumerGroupOffsetIndex();

    await fetchTopicConsumers(admin, "orders", undefined, index);
    await fetchTopicConsumers(admin, "payments", undefined, index);

    expect(listGroups).toHaveBeenCalledTimes(1);
    expect(fetchOffsets).toHaveBeenCalledTimes(1);
  });

  it("uses a complete batch result without public per-group offset calls", async () => {
    const fetchOffsets = vi.fn();
    const readBatch = vi.fn(async () => ({
      supported: true,
      offsetsByTopic: new Map([["orders", [{ groupId: "g1", committed: [{ partition: 0, offset: "2" }] }]]]),
      resolvedGroupIds: new Set(["g1", "g2"]),
      unresolvedGroupIds: new Set<string>(),
    }));
    const admin = makeAdmin({
      listGroups: async () => ({
        groups: [
          { groupId: "g1", protocolType: "consumer" },
          { groupId: "g2", protocolType: "consumer" },
        ],
      }),
      fetchOffsets,
    });

    await expect(new ConsumerGroupOffsetIndex(readBatch).all(admin)).resolves.toEqual(
      new Map([["orders", [{ groupId: "g1", committed: [{ partition: 0, offset: "2" }] }]]]),
    );
    expect(readBatch).toHaveBeenCalledWith(["g1", "g2"], expect.any(AbortSignal));
    expect(fetchOffsets).not.toHaveBeenCalled();
  });

  it("preserves batch successes and falls back only unresolved groups", async () => {
    const fetchOffsets = vi.fn(async ({ groupId }: { groupId: string }) => [
      { topic: "payments", partitions: [{ partition: 0, offset: groupId === "fallback" ? "3" : "99" }] },
    ]);
    const readBatch = vi.fn(async () => ({
      supported: true,
      offsetsByTopic: new Map([["orders", [{ groupId: "batched", committed: [{ partition: 0, offset: "2" }] }]]]),
      resolvedGroupIds: new Set(["batched"]),
      unresolvedGroupIds: new Set(["fallback"]),
    }));
    const admin = makeAdmin({
      listGroups: async () => ({
        groups: [
          { groupId: "batched", protocolType: "consumer" },
          { groupId: "fallback", protocolType: "consumer" },
        ],
      }),
      fetchOffsets: fetchOffsets as unknown as Admin["fetchOffsets"],
    });

    await expect(new ConsumerGroupOffsetIndex(readBatch).all(admin)).resolves.toEqual(
      new Map([
        ["orders", [{ groupId: "batched", committed: [{ partition: 0, offset: "2" }] }]],
        ["payments", [{ groupId: "fallback", committed: [{ partition: 0, offset: "3" }] }]],
      ]),
    );
    expect(fetchOffsets).toHaveBeenCalledOnce();
    expect(fetchOffsets).toHaveBeenCalledWith({ groupId: "fallback", resolveOffsets: false });
  });

  it("keeps partial batch offsets as a lower bound when public fallback fails", async () => {
    const readBatch = vi.fn(async () => ({
      supported: true,
      offsetsByTopic: new Map([["orders", [{ groupId: "partial", committed: [{ partition: 0, offset: "2" }] }]]]),
      resolvedGroupIds: new Set<string>(),
      unresolvedGroupIds: new Set(["partial"]),
    }));
    const admin = makeAdmin({
      listGroups: async () => ({ groups: [{ groupId: "partial", protocolType: "consumer" }] }),
      fetchOffsets: async () => {
        throw new Error("fallback unavailable");
      },
      describeGroups: async () => ({
        groups: [
          {
            groupId: "partial",
            state: "Stable",
            protocol: "range",
            protocolType: "consumer",
            members: [],
          },
        ],
      }),
      fetchTopicOffsets: async () => [{ partition: 0, offset: "5", high: "5", low: "0" }],
    });
    const index = new ConsumerGroupOffsetIndex(readBatch);

    await expect(fetchTopicConsumers(admin, "orders", undefined, index)).resolves.toEqual({
      topic: "orders",
      groups: [{ groupId: "partial", state: "Stable", memberCount: 0, totalLag: "≥3" }],
    });
    expect(index.scanStatus()).toMatchObject({ resolvedGroups: 0, unavailableGroups: 1 });
  });

  it("uses the same batch-backed index regardless of topic-first navigation", async () => {
    const readBatch = vi.fn(async () => ({
      supported: true,
      offsetsByTopic: new Map([
        ["orders", [{ groupId: "shared", committed: [{ partition: 0, offset: "2" }] }]],
        ["payments", [{ groupId: "shared", committed: [{ partition: 0, offset: "3" }] }]],
      ]),
      resolvedGroupIds: new Set(["shared"]),
      unresolvedGroupIds: new Set<string>(),
    }));
    const index = new ConsumerGroupOffsetIndex(readBatch);
    const admin = makeAdmin({
      listGroups: async () => ({ groups: [{ groupId: "shared", protocolType: "consumer" }] }),
      describeGroups: async (groupIds) => ({
        groups: groupIds.map((groupId) => ({
          groupId,
          state: "Stable",
          protocol: "range",
          protocolType: "consumer",
          members: [],
        })),
      }),
      fetchTopicOffsets: async () => [{ partition: 0, offset: "5", high: "5", low: "0" }],
    });

    await fetchTopicConsumers(admin, "orders", undefined, index);
    await expect(index.all(admin)).resolves.toHaveProperty("size", 2);
    expect(readBatch).toHaveBeenCalledTimes(1);
  });

  it("multicasts terminal progress to concurrent topic and health subscribers", async () => {
    let releaseFetch!: () => void;
    let markStarted!: () => void;
    const fetchStarted = new Promise<void>((resolve) => {
      markStarted = resolve;
    });
    const fetchGate = new Promise<void>((resolve) => {
      releaseFetch = resolve;
    });
    const admin = makeAdmin({
      listGroups: async () => ({ groups: [{ groupId: "shared", protocolType: "consumer" }] }),
      fetchOffsets: async () => {
        markStarted();
        await fetchGate;
        return [
          { topic: "orders", partitions: [{ partition: 0, offset: "2", metadata: null }] },
          { topic: "payments", partitions: [{ partition: 0, offset: "3", metadata: null }] },
        ];
      },
    });
    const index = new ConsumerGroupOffsetIndex();
    const topicProgress = vi.fn();
    const healthProgress = vi.fn();

    const topicRequest = index.all(admin, "orders", topicProgress);
    await fetchStarted;
    const healthRequest = index.all(admin, "", healthProgress);
    releaseFetch();
    await Promise.all([topicRequest, healthRequest]);

    expect(topicProgress).toHaveBeenLastCalledWith(
      expect.objectContaining({
        completed: 1,
        total: 1,
        topicConsumerGroup: expect.objectContaining({ groupId: "shared" }),
      }),
    );
    expect(healthProgress).toHaveBeenLastCalledWith({ completed: 1, total: 1 });
  });

  it("replays terminal progress when a completed index is read from cache", async () => {
    const admin = makeAdmin({
      listGroups: async () => ({ groups: [{ groupId: "cached", protocolType: "consumer" }] }),
      fetchOffsets: async () => [],
    });
    const index = new ConsumerGroupOffsetIndex();
    await index.all(admin);
    const progress = vi.fn();

    await index.all(admin, "", progress);

    expect(progress).toHaveBeenCalledOnce();
    expect(progress).toHaveBeenCalledWith({ completed: 1, total: 1 });
  });

  it("rebuilds the all-topic offset index after its TTL expires", async () => {
    let now = 1_000;
    const listGroups = vi.fn(async () => ({ groups: [{ groupId: "cached", protocolType: "consumer" }] }));
    const fetchOffsets = vi.fn(async () => []);
    const admin = makeAdmin({ listGroups, fetchOffsets: fetchOffsets as unknown as Admin["fetchOffsets"] });
    const index = new ConsumerGroupOffsetIndex(undefined, () => now, 100);

    await index.all(admin);
    now += 99;
    await index.all(admin);
    now += 2;
    await index.all(admin);

    expect(listGroups).toHaveBeenCalledTimes(2);
    expect(fetchOffsets).toHaveBeenCalledTimes(2);
  });

  it("clears unavailable group markers when a later TTL generation recovers", async () => {
    let now = 1_000;
    let fail = true;
    const admin = makeAdmin({
      listGroups: async () => ({ groups: [{ groupId: "recovering", protocolType: "consumer" }] }),
      fetchOffsets: async () => {
        if (fail) throw new Error("temporarily unavailable");
        return [{ topic: "orders", partitions: [{ partition: 0, offset: "5", metadata: null }] }];
      },
      describeGroups: async () => ({
        groups: [
          {
            groupId: "recovering",
            state: "Stable",
            protocol: "range",
            protocolType: "consumer",
            members: [],
          },
        ],
      }),
      fetchTopicOffsets: async () => [{ partition: 0, offset: "5", high: "10", low: "0" }],
    });
    const index = new ConsumerGroupOffsetIndex(undefined, () => now, 100);
    await index.all(admin);
    expect(index.isGroupUnavailable("recovering")).toBe(true);

    fail = false;
    now += 101;
    await expect(fetchTopicConsumers(admin, "orders", undefined, index)).resolves.toMatchObject({
      groups: [{ groupId: "recovering", totalLag: "5" }],
    });
    expect(index.isGroupUnavailable("recovering")).toBe(false);
    expect(index.scanStatus().unavailableGroups).toBe(0);
  });

  it("keeps shared work running when only one concurrent subscriber cancels", async () => {
    let releaseFetch!: () => void;
    const fetchGate = new Promise<void>((resolve) => {
      releaseFetch = resolve;
    });
    const admin = makeAdmin({
      listGroups: async () => ({ groups: [{ groupId: "shared", protocolType: "consumer" }] }),
      fetchOffsets: async () => {
        await fetchGate;
        return [{ topic: "orders", partitions: [{ partition: 0, offset: "2", metadata: null }] }];
      },
    });
    const index = new ConsumerGroupOffsetIndex();
    const cancelled = new AbortController();
    const first = index.all(admin, "", undefined, cancelled.signal);
    const second = index.all(admin);

    cancelled.abort();
    await expect(first).rejects.toThrow("cancelled");
    releaseFetch();
    await expect(second).resolves.toEqual(
      new Map([["orders", [{ groupId: "shared", committed: [{ partition: 0, offset: "2" }] }]]]),
    );
  });

  it("aborts work with no subscribers and permits a clean retry", async () => {
    const readBatch = vi.fn(
      (_groupIds: string[], signal?: AbortSignal) =>
        new Promise<never>((_resolve, reject) => {
          signal?.addEventListener("abort", () => reject(new Error("batch cancelled")), { once: true });
        }),
    );
    const admin = makeAdmin({
      listGroups: async () => ({ groups: [{ groupId: "shared", protocolType: "consumer" }] }),
      fetchOffsets: async () => [],
    });
    const index = new ConsumerGroupOffsetIndex(readBatch);
    const cancelled = new AbortController();
    const first = index.all(admin, "", undefined, cancelled.signal);
    await Promise.resolve();
    cancelled.abort();

    await expect(first).rejects.toThrow("cancelled");
    const retryIndex = new ConsumerGroupOffsetIndex(async () => ({
      supported: true,
      offsetsByTopic: new Map(),
      resolvedGroupIds: new Set(["shared"]),
      unresolvedGroupIds: new Set(),
    }));
    await expect(retryIndex.all(admin)).resolves.toEqual(new Map());
  });

  it("starts a replacement build when a new subscriber arrives after owner abort", async () => {
    let firstCall = true;
    let markFirstStarted!: () => void;
    const firstStarted = new Promise<void>((resolve) => {
      markFirstStarted = resolve;
    });
    const readBatch = vi.fn((_groupIds: string[], signal?: AbortSignal) => {
      if (!firstCall) {
        return Promise.resolve({
          supported: true,
          offsetsByTopic: new Map(),
          resolvedGroupIds: new Set(["shared"]),
          unresolvedGroupIds: new Set<string>(),
        });
      }
      firstCall = false;
      markFirstStarted();
      return new Promise<never>((_resolve, reject) => {
        if (signal?.aborted) {
          reject(new Error("first cancelled"));
          return;
        }
        signal?.addEventListener("abort", () => reject(new Error("first cancelled")), { once: true });
      });
    });
    const admin = makeAdmin({
      listGroups: async () => ({ groups: [{ groupId: "shared", protocolType: "consumer" }] }),
      fetchOffsets: async () => [],
    });
    const index = new ConsumerGroupOffsetIndex(readBatch);
    const owner = new AbortController();
    const first = index.all(admin, "", undefined, owner.signal);
    await firstStarted;
    owner.abort();
    const second = index.all(admin);

    await expect(first).rejects.toThrow("cancelled");
    await expect(second).resolves.toEqual(new Map());
    expect(readBatch).toHaveBeenCalledTimes(2);
  });
});
