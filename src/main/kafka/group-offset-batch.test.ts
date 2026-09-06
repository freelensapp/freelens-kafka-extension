import { describe, expect, it, vi } from "vitest";
import { KafkaGroupOffsetBatchReader } from "./group-offset-batch";

import type { KafkaProtocol } from "./group-offset-batch-protocol";

const supportedVersions = { 9: { minVersion: 0, maxVersion: 8 }, 10: { minVersion: 0, maxVersion: 4 } };

function broker(send: (protocol: KafkaProtocol<unknown>) => Promise<unknown>, versions = supportedVersions): object {
  const sendSymbol = Symbol("private:Broker:sendRequest");
  return { versions, [sendSymbol]: vi.fn(send) };
}

function coordinatorResponse(protocol: KafkaProtocol<unknown>, nodeByGroup: Record<string, number>) {
  return {
    throttleTime: 0,
    coordinators: protocol.groupIds.map((key) => ({
      key,
      nodeId: nodeByGroup[key],
      host: `broker-${nodeByGroup[key]}`,
      port: 9092,
      errorCode: nodeByGroup[key] === undefined ? 15 : 0,
      errorMessage: null,
    })),
  };
}

function offsetResponse(protocol: KafkaProtocol<unknown>, errors: Record<string, number> = {}) {
  return {
    throttleTime: 0,
    groups: protocol.groupIds.map((groupId) => ({
      groupId,
      errorCode: errors[groupId] ?? 0,
      topics: [
        {
          topic: "orders",
          partitions: [
            {
              partition: 0,
              committedOffset: String(groupId.length),
              committedLeaderEpoch: 0,
              metadata: null,
              errorCode: 0,
            },
          ],
        },
      ],
    })),
  };
}

describe("KafkaGroupOffsetBatchReader", () => {
  it("does not send custom requests when the broker is unsupported", async () => {
    const send = vi.fn();
    const unsupportedBroker = broker(send, {
      9: { minVersion: 0, maxVersion: 7 },
      10: { minVersion: 0, maxVersion: 4 },
    });
    const cluster = {
      refreshMetadata: vi.fn(async () => undefined),
      getNodeIds: () => ["1"],
      findBroker: vi.fn(async () => unsupportedBroker),
    };
    const reader = new KafkaGroupOffsetBatchReader(cluster);

    await expect(reader.read(["g1", "g2"])).resolves.toMatchObject({
      supported: false,
      resolvedGroupIds: new Set(),
      unresolvedGroupIds: new Set(["g1", "g2"]),
    });
    await reader.read(["g1"]);
    expect(send).not.toHaveBeenCalled();
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1);
  });

  it("groups offset fetches by coordinator and bounded chunk", async () => {
    const seed = broker(async (protocol) => coordinatorResponse(protocol, { g1: 1, g2: 1, g3: 2 }));
    const coordinatorOneSend = vi.fn(async (protocol: KafkaProtocol<unknown>) => offsetResponse(protocol));
    const coordinatorTwoSend = vi.fn(async (protocol: KafkaProtocol<unknown>) => offsetResponse(protocol));
    const brokers = new Map([
      ["0", seed],
      ["1", broker(coordinatorOneSend)],
      ["2", broker(coordinatorTwoSend)],
    ]);
    const cluster = {
      refreshMetadata: async () => undefined,
      getNodeIds: () => ["0", "1", "2"],
      findBroker: async ({ nodeId }: { nodeId: string }) => brokers.get(nodeId)!,
    };
    const reader = new KafkaGroupOffsetBatchReader(cluster, { groupChunkSize: 1 });

    const result = await reader.read(["g1", "g2", "g3"]);

    expect(result.supported).toBe(true);
    expect(result.resolvedGroupIds).toEqual(new Set(["g1", "g2", "g3"]));
    expect(result.unresolvedGroupIds).toEqual(new Set());
    expect(result.offsetsByTopic.get("orders")).toHaveLength(3);
    expect(coordinatorOneSend.mock.calls.map(([protocol]) => protocol.groupIds)).toEqual([["g1"], ["g2"]]);
    expect(coordinatorTwoSend.mock.calls.map(([protocol]) => protocol.groupIds)).toEqual([["g3"]]);
  });

  it("reuses negotiated capability and coordinator mappings within the session", async () => {
    const seedSend = vi.fn(async (protocol: KafkaProtocol<unknown>) => coordinatorResponse(protocol, { g1: 1 }));
    const offsetSend = vi.fn(async (protocol: KafkaProtocol<unknown>) => offsetResponse(protocol));
    const brokers = new Map([
      ["0", broker(seedSend)],
      ["1", broker(offsetSend)],
    ]);
    const refreshMetadata = vi.fn(async () => undefined);
    const reader = new KafkaGroupOffsetBatchReader({
      refreshMetadata,
      getNodeIds: () => ["0", "1"],
      findBroker: async ({ nodeId }) => brokers.get(nodeId)!,
    });

    await reader.read(["g1"]);
    await reader.read(["g1"]);

    expect(refreshMetadata).toHaveBeenCalledTimes(1);
    expect(seedSend).toHaveBeenCalledTimes(1);
    expect(offsetSend).toHaveBeenCalledTimes(2);
  });

  it("preserves successful chunks when another coordinator fails", async () => {
    const seed = broker(async (protocol) => coordinatorResponse(protocol, { good: 1, failed: 2 }));
    const brokers = new Map([
      ["0", seed],
      ["1", broker(async (protocol) => offsetResponse(protocol))],
      ["2", broker(async () => Promise.reject(new Error("coordinator unavailable")))],
    ]);
    const reader = new KafkaGroupOffsetBatchReader({
      refreshMetadata: async () => undefined,
      getNodeIds: () => ["0", "1", "2"],
      findBroker: async ({ nodeId }) => brokers.get(nodeId)!,
    });

    await expect(reader.read(["good", "failed"])).resolves.toMatchObject({
      supported: true,
      resolvedGroupIds: new Set(["good"]),
      unresolvedGroupIds: new Set(["failed"]),
    });
  });

  it("preserves successful partitions from a partial group response", async () => {
    const seed = broker(async (protocol) => coordinatorResponse(protocol, { partial: 1 }));
    const partialBroker = broker(async (protocol: KafkaProtocol<unknown>) => ({
      throttleTime: 0,
      groups: protocol.groupIds.map((groupId) => ({
        groupId,
        errorCode: 0,
        topics: [
          {
            topic: "orders",
            partitions: [
              { partition: 0, committedOffset: "5", committedLeaderEpoch: 0, metadata: null, errorCode: 0 },
              { partition: 1, committedOffset: "7", committedLeaderEpoch: 0, metadata: null, errorCode: 88 },
            ],
          },
        ],
      })),
    }));
    const brokers = new Map([
      ["0", seed],
      ["1", partialBroker],
    ]);
    const reader = new KafkaGroupOffsetBatchReader({
      refreshMetadata: async () => undefined,
      getNodeIds: () => ["0", "1"],
      findBroker: async ({ nodeId }) => brokers.get(nodeId)!,
    });

    await expect(reader.read(["partial"])).resolves.toMatchObject({
      offsetsByTopic: new Map([["orders", [{ groupId: "partial", committed: [{ partition: 0, offset: "5" }] }]]]),
      resolvedGroupIds: new Set(),
      unresolvedGroupIds: new Set(["partial"]),
    });
  });

  it("invalidates a moved coordinator without retrying the custom request", async () => {
    const seedSend = vi.fn(async (protocol: KafkaProtocol<unknown>) => coordinatorResponse(protocol, { moved: 1 }));
    const offsetSend = vi.fn(async (protocol: KafkaProtocol<unknown>) => offsetResponse(protocol, { moved: 16 }));
    const brokers = new Map([
      ["0", broker(seedSend)],
      ["1", broker(offsetSend)],
    ]);
    const reader = new KafkaGroupOffsetBatchReader({
      refreshMetadata: async () => undefined,
      getNodeIds: () => ["0", "1"],
      findBroker: async ({ nodeId }) => brokers.get(nodeId)!,
    });

    await expect(reader.read(["moved"])).resolves.toMatchObject({ unresolvedGroupIds: new Set(["moved"]) });
    await reader.read(["moved"]);

    expect(offsetSend).toHaveBeenCalledTimes(2);
    expect(seedSend).toHaveBeenCalledTimes(2);
  });

  it("falls back during rebalance while retaining the valid coordinator cache", async () => {
    const seedSend = vi.fn(async (protocol: KafkaProtocol<unknown>) =>
      coordinatorResponse(protocol, { rebalancing: 1 }),
    );
    let rebalancing = true;
    const offsetSend = vi.fn(async (protocol: KafkaProtocol<unknown>) =>
      offsetResponse(protocol, rebalancing ? { rebalancing: 27 } : {}),
    );
    const brokers = new Map([
      ["0", broker(seedSend)],
      ["1", broker(offsetSend)],
    ]);
    const reader = new KafkaGroupOffsetBatchReader({
      refreshMetadata: async () => undefined,
      getNodeIds: () => ["0", "1"],
      findBroker: async ({ nodeId }) => brokers.get(nodeId)!,
    });

    await expect(reader.read(["rebalancing"])).resolves.toMatchObject({
      resolvedGroupIds: new Set(),
      unresolvedGroupIds: new Set(["rebalancing"]),
    });
    rebalancing = false;
    await expect(reader.read(["rebalancing"])).resolves.toMatchObject({
      resolvedGroupIds: new Set(["rebalancing"]),
      unresolvedGroupIds: new Set(),
    });

    expect(seedSend).toHaveBeenCalledTimes(1);
    expect(offsetSend).toHaveBeenCalledTimes(2);
  });

  it("chunks coordinator lookup keys independently from offset requests", async () => {
    const nodeByGroup = Object.fromEntries(Array.from({ length: 5 }, (_, index) => [`g${index}`, 1]));
    const seedSend = vi.fn(async (protocol: KafkaProtocol<unknown>) => coordinatorResponse(protocol, nodeByGroup));
    const offsetSend = vi.fn(async (protocol: KafkaProtocol<unknown>) => offsetResponse(protocol));
    const brokers = new Map([
      ["0", broker(seedSend)],
      ["1", broker(offsetSend)],
    ]);
    const reader = new KafkaGroupOffsetBatchReader(
      {
        refreshMetadata: async () => undefined,
        getNodeIds: () => ["0", "1"],
        findBroker: async ({ nodeId }) => brokers.get(nodeId)!,
      },
      { coordinatorChunkSize: 2, groupChunkSize: 3 },
    );

    await reader.read(Object.keys(nodeByGroup));

    expect(seedSend.mock.calls.map(([protocol]) => protocol.groupIds.length)).toEqual([2, 2, 1]);
    expect(offsetSend.mock.calls.map(([protocol]) => protocol.groupIds.length)).toEqual([3, 2]);
  });

  it("bounds concurrent offset requests across chunks", async () => {
    const nodeByGroup = Object.fromEntries(Array.from({ length: 6 }, (_, index) => [`g${index}`, 1]));
    let active = 0;
    let maximumActive = 0;
    const seed = broker(async (protocol) => coordinatorResponse(protocol, nodeByGroup));
    const offsetBroker = broker(async (protocol: KafkaProtocol<unknown>) => {
      active++;
      maximumActive = Math.max(maximumActive, active);
      await new Promise((resolve) => setTimeout(resolve, 1));
      active--;
      return offsetResponse(protocol);
    });
    const brokers = new Map([
      ["0", seed],
      ["1", offsetBroker],
    ]);
    const reader = new KafkaGroupOffsetBatchReader(
      {
        refreshMetadata: async () => undefined,
        getNodeIds: () => ["0", "1"],
        findBroker: async ({ nodeId }) => brokers.get(nodeId)!,
      },
      { concurrency: 2, groupChunkSize: 1 },
    );

    await reader.read(Object.keys(nodeByGroup));

    expect(maximumActive).toBe(2);
  });

  it("stops new custom chunks after an oversized request", async () => {
    const seed = broker(async (protocol) => coordinatorResponse(protocol, { g1: 1, g2: 1, g3: 1 }));
    const oversized = Object.assign(new Error("request is too large"), { type: "MESSAGE_TOO_LARGE" });
    const offsetSend = vi.fn(async () => Promise.reject(oversized));
    const brokers = new Map([
      ["0", seed],
      ["1", broker(offsetSend)],
    ]);
    const cluster = {
      refreshMetadata: vi.fn(async () => undefined),
      getNodeIds: () => ["0", "1"],
      findBroker: async ({ nodeId }: { nodeId: string }) => brokers.get(nodeId)!,
    };
    const reader = new KafkaGroupOffsetBatchReader(cluster, { concurrency: 1, groupChunkSize: 1 });

    await expect(reader.read(["g1", "g2", "g3"])).resolves.toMatchObject({
      supported: false,
      unresolvedGroupIds: new Set(["g1", "g2", "g3"]),
    });
    await reader.read(["g1"]);

    expect(offsetSend).toHaveBeenCalledTimes(1);
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(1);
  });

  it("drains only already in-flight chunks after a concurrent oversized failure", async () => {
    const groupIds = Array.from({ length: 10 }, (_, index) => `g${index}`);
    const nodeByGroup = Object.fromEntries(groupIds.map((groupId) => [groupId, 1]));
    const seed = broker(async (protocol) => coordinatorResponse(protocol, nodeByGroup));
    let started = 0;
    let releaseInitial!: () => void;
    const initialStarted = new Promise<void>((resolve) => {
      releaseInitial = resolve;
    });
    const offsetSend = vi.fn(async (protocol: KafkaProtocol<unknown>) => {
      started++;
      if (started === 3) releaseInitial();
      await initialStarted;
      if (protocol.groupIds[0] === "g0") {
        throw Object.assign(new Error("request is too large"), { type: "MESSAGE_TOO_LARGE" });
      }
      return offsetResponse(protocol);
    });
    const brokers = new Map([
      ["0", seed],
      ["1", broker(offsetSend)],
    ]);
    const reader = new KafkaGroupOffsetBatchReader(
      {
        refreshMetadata: async () => undefined,
        getNodeIds: () => ["0", "1"],
        findBroker: async ({ nodeId }) => brokers.get(nodeId)!,
      },
      { concurrency: 3, groupChunkSize: 1 },
    );

    const result = await reader.read(groupIds);

    expect(offsetSend).toHaveBeenCalledTimes(3);
    expect(offsetSend.mock.calls.map(([protocol]) => protocol.groupIds[0]).sort()).toEqual(["g0", "g1", "g2"]);
    expect(result.supported).toBe(false);
    expect(result.resolvedGroupIds).toEqual(new Set(["g1", "g2"]));
    expect(result.unresolvedGroupIds).toEqual(new Set(groupIds.filter((groupId) => !["g1", "g2"].includes(groupId))));
  });

  it("stops after an unsupported group response without retry storms", async () => {
    const seed = broker(async (protocol) => coordinatorResponse(protocol, { g1: 1, g2: 1 }));
    const offsetSend = vi.fn(async (protocol: KafkaProtocol<unknown>) => offsetResponse(protocol, { g1: 35 }));
    const brokers = new Map([
      ["0", seed],
      ["1", broker(offsetSend)],
    ]);
    const reader = new KafkaGroupOffsetBatchReader(
      {
        refreshMetadata: async () => undefined,
        getNodeIds: () => ["0", "1"],
        findBroker: async ({ nodeId }) => brokers.get(nodeId)!,
      },
      { concurrency: 1, groupChunkSize: 1 },
    );

    await expect(reader.read(["g1", "g2"])).resolves.toMatchObject({
      supported: false,
      unresolvedGroupIds: new Set(["g1", "g2"]),
    });
    expect(offsetSend).toHaveBeenCalledTimes(1);
  });
});
