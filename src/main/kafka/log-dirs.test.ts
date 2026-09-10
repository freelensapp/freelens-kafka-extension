import { describe, expect, it } from "vitest";
import { aggregateTopicSizes, fetchTopicSizes, type LogDirsCluster } from "./log-dirs";

interface KafkaEncoder {
  readonly buffer: Buffer;
  writeInt16(value: number): KafkaEncoder;
  writeInt32(value: number): KafkaEncoder;
  writeInt64(value: number | string): KafkaEncoder;
  writeBoolean(value: boolean): KafkaEncoder;
  writeString(value: string): KafkaEncoder;
  writeArray(value: KafkaEncoder[]): KafkaEncoder;
}

// eslint-disable-next-line @typescript-eslint/no-require-imports
const Encoder = require("kafkajs/src/protocol/encoder") as new () => KafkaEncoder;

function brokerResponse(entries: Array<[topic: string, partition: number, size: number]>): Buffer {
  const byTopic = new Map<string, KafkaEncoder[]>();
  for (const [topic, partition, size] of entries) {
    const list = byTopic.get(topic) ?? [];
    list.push(new Encoder().writeInt32(partition).writeInt64(size).writeInt64(0).writeBoolean(false));
    byTopic.set(topic, list);
  }
  const topics = [...byTopic].map(([topic, partitions]) => new Encoder().writeString(topic).writeArray(partitions));
  const result = new Encoder().writeInt16(0).writeString("/data").writeArray(topics);
  return new Encoder().writeInt32(0).writeArray([result]).buffer;
}

function fakeCluster(
  leaders: Record<string, Array<{ partitionId: number; leader: number | null }>>,
  brokers: Record<string, Buffer | Error>,
): LogDirsCluster {
  return {
    addMultipleTargetTopics: async () => undefined,
    refreshMetadata: async () => undefined,
    getNodeIds: () => Object.keys(brokers),
    findTopicPartitionMetadata: (topic) => leaders[topic] ?? [],
    findBroker: async ({ nodeId }) => ({
      versions: { 35: { minVersion: 0, maxVersion: 4 } },
      [Symbol("private:Broker:sendRequest")]: async (protocol: { response: { decode(buffer: Buffer): unknown } }) => {
        const answer = brokers[nodeId];
        if (answer instanceof Error) throw answer;
        return protocol.response.decode(answer);
      },
    }),
  };
}

describe("aggregateTopicSizes", () => {
  it("sums leader and replica bytes per partition and per topic", () => {
    const leaders = new Map([
      [
        "orders",
        new Map([
          [0, 1],
          [1, 2],
        ]),
      ],
    ]);
    const sizes = aggregateTopicSizes(
      [
        { topic: "orders", partition: 0, nodeId: 1, bytes: 100n, offsetLag: 0n },
        { topic: "orders", partition: 0, nodeId: 2, bytes: 90n, offsetLag: 3n },
        { topic: "orders", partition: 1, nodeId: 2, bytes: 50n, offsetLag: 0n },
        { topic: "orders", partition: 1, nodeId: 1, bytes: 50n, offsetLag: 0n },
      ],
      { leaders },
      [],
    );
    expect(sizes.supported).toBe(true);
    expect(sizes.topics.orders).toEqual({
      leaderBytes: "150",
      replicaBytes: "290",
      exact: true,
      partitions: [
        {
          partition: 0,
          leaderBytes: "100",
          replicaBytes: "190",
          replicas: [
            { nodeId: 1, bytes: "100", offsetLag: "0" },
            { nodeId: 2, bytes: "90", offsetLag: "3" },
          ],
        },
        {
          partition: 1,
          leaderBytes: "50",
          replicaBytes: "100",
          replicas: [
            { nodeId: 1, bytes: "50", offsetLag: "0" },
            { nodeId: 2, bytes: "50", offsetLag: "0" },
          ],
        },
      ],
    });
  });

  it("marks a topic inexact when a partition leader did not report", () => {
    const leaders = new Map([
      [
        "orders",
        new Map([
          [0, 1],
          [1, 3],
        ]),
      ],
    ]);
    const sizes = aggregateTopicSizes(
      [{ topic: "orders", partition: 0, nodeId: 1, bytes: 100n, offsetLag: 0n }],
      { leaders },
      [3],
    );
    expect(sizes.topics.orders.exact).toBe(false);
    expect(sizes.topics.orders.leaderBytes).toBe("100");
    expect(sizes.topics.orders.partitions[1]).toMatchObject({ partition: 1, leaderBytes: "0", replicas: [] });
    expect(sizes.unavailableBrokers).toEqual([3]);
  });
});

describe("fetchTopicSizes", () => {
  it("asks every broker, ignores the failing one and aggregates by leader", async () => {
    const cluster = fakeCluster(
      {
        orders: [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 2 },
        ],
        empty: [],
      },
      {
        "1": brokerResponse([
          ["orders", 0, 1000],
          ["orders", 1, 999],
        ]),
        "2": brokerResponse([
          ["orders", 1, 2000],
          ["orders", 0, 1001],
        ]),
        "3": new Error("connection refused"),
      },
    );
    const sizes = await fetchTopicSizes(cluster, ["orders", "empty", "orders"]);
    expect(sizes.supported).toBe(true);
    expect(sizes.unavailableBrokers).toEqual([3]);
    expect(sizes.topics.orders.leaderBytes).toBe("3000");
    expect(sizes.topics.orders.replicaBytes).toBe("5000");
    expect(sizes.topics.orders.exact).toBe(true);
    expect(sizes.topics.empty).toEqual({ leaderBytes: "0", replicaBytes: "0", exact: true, partitions: [] });
  });

  it("reports an unsupported cluster instead of failing", async () => {
    const cluster: LogDirsCluster = {
      ...fakeCluster({ orders: [{ partitionId: 0, leader: 1 }] }, { "1": brokerResponse([]) }),
      findBroker: async () => ({ versions: { 35: { minVersion: 2, maxVersion: 4 } } }),
    };
    const sizes = await fetchTopicSizes(cluster, ["orders"]);
    expect(sizes.supported).toBe(false);
    expect(sizes.topics.orders.exact).toBe(false);
  });

  it("returns an empty result for no topics without touching the cluster", async () => {
    const cluster = fakeCluster({}, {});
    expect(await fetchTopicSizes(cluster, [])).toEqual({ supported: true, unavailableBrokers: [], topics: {} });
  });
});
