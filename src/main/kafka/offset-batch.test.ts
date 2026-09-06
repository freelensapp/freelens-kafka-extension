import { describe, expect, it } from "vitest";
import { chunkOffsetBatch, fetchHighWatermarks, groupOffsetBatchByBroker, highWatermarkBatch } from "./offset-batch";

describe("offset batch contract", () => {
  it("groups partitions by leader and keeps only high-watermark reads", () => {
    expect(
      groupOffsetBatchByBroker([
        { topic: "orders", partition: 0, leaderId: 1 },
        { topic: "orders", partition: 1, leaderId: 2 },
        { topic: "payments", partition: 0, leaderId: 1 },
      ]),
    ).toEqual([
      {
        leaderId: 1,
        partitions: [
          { topic: "orders", partition: 0 },
          { topic: "payments", partition: 0 },
        ],
      },
      { leaderId: 2, partitions: [{ topic: "orders", partition: 1 }] },
    ]);
    expect(highWatermarkBatch([{ topic: "orders", partition: 0, leaderId: 1 }])).toMatchObject({
      fromBeginning: false,
      lowWatermarkRequests: 0,
    });
  });

  it("chunks batches without dropping or duplicating partitions", () => {
    const partitions = Array.from({ length: 5 }, (_, partition) => ({ topic: "orders", partition, leaderId: 1 }));
    expect(chunkOffsetBatch(partitions, 2).flat()).toEqual(partitions);
  });

  it("fetches all requested high watermarks in grouped batches without low reads", async () => {
    const calls: Array<{ fromBeginning?: boolean; topics: string[] }> = [];
    const cluster = {
      addMultipleTargetTopics: async () => undefined,
      fetchTopicsOffset: async (
        topics: Array<{ topic: string; partitions: Array<{ partition: number }>; fromBeginning?: boolean }>,
      ) => {
        calls.push({ fromBeginning: topics[0]?.fromBeginning, topics: topics.map(({ topic }) => topic) });
        return topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: String(partition + 10) })),
        }));
      },
    };

    await expect(
      fetchHighWatermarks(cluster, [
        { topic: "orders", partition: 0, leaderId: 1 },
        { topic: "orders", partition: 1, leaderId: 1 },
        { topic: "payments", partition: 0, leaderId: 2 },
      ]),
    ).resolves.toEqual(
      new Map([
        [
          "orders",
          new Map([
            [0, "10"],
            [1, "11"],
          ]),
        ],
        ["payments", new Map([[0, "10"]])],
      ]),
    );
    expect(calls).toEqual([
      { fromBeginning: false, topics: ["orders"] },
      { fromBeginning: false, topics: ["payments"] },
    ]);
  });

  it("registers each topic before reading offsets from a cold KafkaJS cluster", async () => {
    const registered = new Set<string>();
    const addMultipleTargetTopicsCalls: string[][] = [];
    const cluster = {
      addMultipleTargetTopics: async (topics: string[]) => {
        addMultipleTargetTopicsCalls.push(topics);
        for (const topic of topics) registered.add(topic);
      },
      fetchTopicsOffset: async (
        topics: Array<{ topic: string; partitions: Array<{ partition: number }>; fromBeginning?: boolean }>,
      ) => {
        expect(topics.every(({ topic }) => registered.has(topic))).toBe(true);
        return topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: "10" })),
        }));
      },
    };

    await fetchHighWatermarks(cluster, [
      { topic: "orders", partition: 0, leaderId: 1 },
      { topic: "orders", partition: 1, leaderId: 2 },
      { topic: "payments", partition: 0, leaderId: 1 },
    ]);

    expect(addMultipleTargetTopicsCalls).toEqual([["orders", "payments"]]);
  });

  it("bounds high-only requests by leader and chunk", async () => {
    const requests: Array<Array<{ partition: number; topic: string }>> = [];
    const cluster = {
      addMultipleTargetTopics: async () => undefined,
      fetchTopicsOffset: async (
        topics: Array<{ topic: string; partitions: Array<{ partition: number }>; fromBeginning?: boolean }>,
      ) => {
        expect(topics.every(({ fromBeginning }) => fromBeginning === false)).toBe(true);
        requests.push(
          topics.flatMap(({ topic, partitions }) => partitions.map(({ partition }) => ({ topic, partition }))),
        );
        return topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: "10" })),
        }));
      },
    };

    await fetchHighWatermarks(
      cluster,
      [
        { topic: "orders", partition: 0, leaderId: 1 },
        { topic: "payments", partition: 0, leaderId: 1 },
        { topic: "inventory", partition: 0, leaderId: 1 },
        { topic: "orders", partition: 1, leaderId: 2 },
        { topic: "payments", partition: 1, leaderId: 2 },
      ],
      2,
    );

    expect(requests).toHaveLength(3);
    expect(requests.flat()).toEqual([
      { topic: "orders", partition: 0 },
      { topic: "payments", partition: 0 },
      { topic: "inventory", partition: 0 },
      { topic: "orders", partition: 1 },
      { topic: "payments", partition: 1 },
    ]);
  });

  it("normalizes results after KafkaJS reroutes stale leader hints", async () => {
    const currentLeaders = new Map([
      ["orders:0", 2],
      ["payments:0", 3],
    ]);
    const brokerRequests = new Map<number, string[]>();
    const cluster = {
      addMultipleTargetTopics: async () => undefined,
      fetchTopicsOffset: async (
        topics: Array<{ topic: string; partitions: Array<{ partition: number }>; fromBeginning?: boolean }>,
      ) => {
        for (const { topic, partitions } of topics) {
          for (const { partition } of partitions) {
            const key = `${topic}:${partition}`;
            const leader = currentLeaders.get(key);
            if (leader === undefined) continue;
            brokerRequests.set(leader, [...(brokerRequests.get(leader) ?? []), key]);
          }
        }
        return topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: "10" })),
        }));
      },
    };

    await expect(
      fetchHighWatermarks(cluster, [
        { topic: "orders", partition: 0, leaderId: 1 },
        { topic: "payments", partition: 0, leaderId: 1 },
      ]),
    ).resolves.toEqual(
      new Map([
        ["orders", new Map([[0, "10"]])],
        ["payments", new Map([[0, "10"]])],
      ]),
    );
    expect(brokerRequests).toEqual(
      new Map([
        [2, ["orders:0"]],
        [3, ["payments:0"]],
      ]),
    );
  });

  it("does not invent offsets for partitions missing from a broker response", async () => {
    const cluster = {
      addMultipleTargetTopics: async () => undefined,
      fetchTopicsOffset: async () => [{ topic: "orders", partitions: [{ partition: 0, offset: "10" }] }],
    };

    await expect(
      fetchHighWatermarks(cluster, [
        { topic: "orders", partition: 0, leaderId: 1 },
        { topic: "orders", partition: 1, leaderId: 1 },
      ]),
    ).resolves.toEqual(new Map([["orders", new Map([[0, "10"]])]]));
  });
});
