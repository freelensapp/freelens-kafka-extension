import { CompressionCodecs, CompressionTypes, Kafka, logLevel } from "kafkajs";
import { describe, expect, it, vi } from "vitest";
import {
  browseMessages,
  browseMessagesWithCluster,
  createKafkaJsReadCluster,
  type KafkaReadCluster,
  loadTopicMetadata,
  MESSAGE_FETCH_MAX_BYTES,
  MESSAGE_PREVIEW_MAX_BYTES,
  MESSAGE_RESPONSE_PREVIEW_MAX_BYTES,
  normalizeMessageBytes,
  normalizeMessageHeaders,
  validateMessageBrowseInput,
} from "./message-fetch";

interface FakeClusterOptions {
  abortedTransactions?: Array<{ firstOffset: string; producerId: string }>;
  earliest?: string;
  latest?: string;
  timestamp?: string;
  messages?: Array<{
    offset: string;
    timestamp: string;
    key: Buffer | null;
    value: Buffer | null;
    headers?: Record<string, Buffer | null | Array<Buffer | null>>;
    isControlRecord?: boolean;
    batchContext?: { producerId: string; inTransaction: boolean };
  }>;
}

function fakeCluster({
  abortedTransactions = [],
  earliest = "2",
  latest = "10",
  timestamp = "5",
  messages = [],
}: FakeClusterOptions = {}) {
  const fetch = vi.fn(async () => ({
    responses: [
      {
        topicName: "orders",
        partitions: [{ partition: 0, highWatermark: latest, abortedTransactions, messages }],
      },
    ],
  }));
  const cluster: KafkaReadCluster = {
    connect: vi.fn(async () => undefined),
    disconnect: vi.fn(async () => undefined),
    refreshMetadata: vi.fn(async () => undefined),
    getNodeIds: vi.fn(() => ["1"]),
    findTopicPartitionMetadata: vi.fn(() => [{ partitionId: 0, leader: 1 }]),
    fetchTopicsOffset: vi.fn(async ([request]) => [
      {
        topic: request.topic,
        partitions: [
          {
            partition: request.partitions[0].partition,
            offset:
              request.fromTimestamp !== undefined ? timestamp : request.fromBeginning === true ? earliest : latest,
          },
        ],
      },
    ]),
    findBroker: vi.fn(async () => ({ fetch })),
  };
  return { cluster, fetch };
}

const record = (offset: string, value = `value-${offset}`) => ({
  offset,
  timestamp: String(1_700_000_000_000 + Number(offset)),
  key: Buffer.from(`key-${offset}`),
  value: Buffer.from(value),
  headers: { source: Buffer.from("fixture") },
});

describe("message Browse validation", () => {
  it("accepts bounded controls and rejects malformed values before a fetch", () => {
    expect(
      validateMessageBrowseInput({ topic: " orders ", partition: 0, startMode: "offset", offset: "12", limit: 50 }),
    ).toMatchObject({ topic: "orders", offset: "12" });

    expect(() =>
      validateMessageBrowseInput({ topic: "orders", partition: -1, startMode: "earliest", limit: 50 }),
    ).toThrow("Partition must be a non-negative integer");
    expect(() =>
      validateMessageBrowseInput({ topic: "orders", partition: 0, startMode: "offset", offset: "1.5", limit: 50 }),
    ).toThrow("Offset must be a non-negative decimal offset");
    expect(() =>
      validateMessageBrowseInput({ topic: "orders", partition: 0, startMode: "timestamp", timestamp: -1, limit: 50 }),
    ).toThrow("Timestamp must be a non-negative epoch millisecond integer");
    expect(() =>
      validateMessageBrowseInput({ topic: "orders", partition: 0, startMode: "earliest", limit: 101 }),
    ).toThrow("Limit must be between 1 and 100");
  });
});

describe("message compression codecs", () => {
  it("registers a working KafkaJS LZ4 codec", async () => {
    const source = Buffer.from('LZ4 record payload {"state":"created"}');
    const codec = CompressionCodecs[CompressionTypes.LZ4]();
    const compressed = await codec.compress({ buffer: source });
    const decompressed = await codec.decompress(compressed);
    expect(Buffer.from(decompressed)).toEqual(source);
  });
});

describe("message byte normalization", () => {
  it("distinguishes null, JSON, UTF-8 and binary without replacement decoding", () => {
    expect(normalizeMessageBytes(null)).toEqual({ format: "null", byteLength: 0, truncated: false });
    expect(normalizeMessageBytes(Buffer.from('{"ok":true}'))).toMatchObject({
      format: "json",
      text: '{"ok":true}',
      truncated: false,
    });
    expect(normalizeMessageBytes(Buffer.from("hello"))).toMatchObject({ format: "text", text: "hello" });
    expect(normalizeMessageBytes(Buffer.from([0xff, 0x00]))).toMatchObject({
      format: "binary",
      base64: "/wA=",
    });
    expect(normalizeMessageBytes(Buffer.from([0xff, 0x00]))).not.toHaveProperty("text");
  });

  it("preserves duplicate and null header values in order", () => {
    const headers = normalizeMessageHeaders({
      trace: [Buffer.from("first"), Buffer.from("second")],
      nullable: null,
    });
    expect(headers.map(({ name, value }) => [name, value.text ?? value.format])).toEqual([
      ["trace", "first"],
      ["trace", "second"],
      ["nullable", "null"],
    ]);
  });

  it("bounds one field preview", () => {
    const normalized = normalizeMessageBytes(Buffer.alloc(MESSAGE_PREVIEW_MAX_BYTES + 10, 0x61));
    expect(normalized).toMatchObject({ format: "text", byteLength: MESSAGE_PREVIEW_MAX_BYTES + 10, truncated: true });
    expect(Buffer.from(normalized.base64 ?? "", "base64")).toHaveLength(MESSAGE_PREVIEW_MAX_BYTES);
  });
});

describe("group-free message Browse", () => {
  it("reads a latest offset window directly from the leader with READ_COMMITTED", async () => {
    const { cluster, fetch } = fakeCluster({ messages: [record("7"), record("8"), record("9")] });

    const result = await browseMessagesWithCluster(cluster, {
      topic: "orders",
      partition: 0,
      startMode: "latest",
      limit: 3,
    });

    expect(result).toMatchObject({
      topic: "orders",
      partition: 0,
      startMode: "latest",
      startOffset: "7",
      nextOffset: "10",
      logStartOffset: "2",
      highWatermark: "10",
      returnedCount: 3,
      hasMore: false,
    });
    expect(result.messages.map(({ offset }) => offset)).toEqual(["7", "8", "9"]);
    expect(cluster.findBroker).toHaveBeenCalledWith({ nodeId: "1" });
    expect(fetch).toHaveBeenCalledWith(
      expect.objectContaining({
        replicaId: -1,
        isolationLevel: 1,
        maxBytes: MESSAGE_FETCH_MAX_BYTES,
        topics: [
          {
            topic: "orders",
            partitions: [{ partition: 0, fetchOffset: "7", maxBytes: MESSAGE_FETCH_MAX_BYTES }],
          },
        ],
      }),
    );
  });

  it("limits records, advances explicitly and preserves an aggregate preview budget", async () => {
    const largeMessages = Array.from({ length: 10 }, (_, index) => record(String(index + 2), "x".repeat(100_000)));
    const { cluster } = fakeCluster({ latest: "20", messages: largeMessages });

    const result = await browseMessagesWithCluster(cluster, {
      topic: "orders",
      partition: 0,
      startMode: "earliest",
      limit: 4,
    });

    expect(result.messages).toHaveLength(4);
    expect(result.nextOffset).toBe("6");
    expect(result.hasMore).toBe(true);
    const previewBytes = result.messages.reduce(
      (total, message) => total + Buffer.from(message.value.base64 ?? "", "base64").length,
      0,
    );
    expect(previewBytes).toBeLessThanOrEqual(MESSAGE_RESPONSE_PREVIEW_MAX_BYTES);
  });

  it("caps aggregate previews after the response budget is exhausted", async () => {
    const largeMessages = Array.from({ length: 10 }, (_, index) => record(String(index + 2), "x".repeat(100_000)));
    const { cluster } = fakeCluster({ latest: "20", messages: largeMessages });
    const result = await browseMessagesWithCluster(cluster, {
      topic: "orders",
      partition: 0,
      startMode: "earliest",
      limit: 10,
    });
    const payloads = result.messages.flatMap((message) => [
      message.key,
      message.value,
      ...message.headers.map(({ value }) => value),
    ]);
    const previewBytes = payloads.reduce(
      (total, payload) => total + Buffer.from(payload.base64 ?? "", "base64").length,
      0,
    );
    expect(previewBytes).toBeLessThanOrEqual(MESSAGE_RESPONSE_PREVIEW_MAX_BYTES);
    expect(result.messages.at(-1)?.value).toMatchObject({ byteLength: 100_000, truncated: true });
  });

  it("filters aborted transactions and control records through KafkaJS Batch", async () => {
    const { cluster } = fakeCluster({
      earliest: "2",
      latest: "5",
      abortedTransactions: [{ firstOffset: "2", producerId: "7" }],
      messages: [
        { ...record("2"), batchContext: { producerId: "7", inTransaction: true } },
        {
          ...record("3"),
          key: Buffer.from([0, 0, 0, 0]),
          isControlRecord: true,
          batchContext: { producerId: "7", inTransaction: true },
        },
        { ...record("4"), batchContext: { producerId: "8", inTransaction: false } },
      ],
    });
    const result = await browseMessagesWithCluster(cluster, {
      topic: "orders",
      partition: 0,
      startMode: "earliest",
      limit: 10,
    });
    expect(result.messages.map(({ offset }) => offset)).toEqual(["4"]);
  });

  it("advances past a compacted offset without repeating the same window", async () => {
    const { cluster } = fakeCluster({ earliest: "0", latest: "10", messages: [record("4")] });
    const result = await browseMessagesWithCluster(cluster, {
      topic: "orders",
      partition: 0,
      startMode: "offset",
      offset: "5",
      limit: 10,
    });
    expect(result).toMatchObject({ startOffset: "5", nextOffset: "6", returnedCount: 0, hasMore: true });
  });

  it("returns an empty timestamp window without issuing Fetch", async () => {
    const { cluster, fetch } = fakeCluster({ timestamp: "-1" });
    const result = await browseMessagesWithCluster(cluster, {
      topic: "orders",
      partition: 0,
      startMode: "timestamp",
      timestamp: 1_700_000_000_000,
      limit: 10,
    });
    expect(result).toMatchObject({ startOffset: "10", nextOffset: "10", returnedCount: 0, hasMore: false });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("rejects an unknown partition before ListOffsets or Fetch", async () => {
    const { cluster, fetch } = fakeCluster();
    vi.mocked(cluster.findTopicPartitionMetadata).mockReturnValue([]);
    await expect(
      browseMessagesWithCluster(cluster, {
        topic: "orders",
        partition: 4,
        startMode: "earliest",
        limit: 10,
      }),
    ).rejects.toThrow("Partition 4 does not exist");
    expect(cluster.fetchTopicsOffset).not.toHaveBeenCalled();
    expect(fetch).not.toHaveBeenCalled();
  });

  it("disconnects exactly once when Browse succeeds or fails", async () => {
    const success = fakeCluster({ latest: "2" }).cluster;
    const successSymbol = Symbol("private:Kafka:createCluster");
    const successKafka = { [successSymbol]: () => success } as unknown as Kafka;
    await browseMessages(successKafka, {
      topic: "orders",
      partition: 0,
      startMode: "earliest",
      limit: 10,
    });
    expect(success.connect).toHaveBeenCalledOnce();
    expect(success.disconnect).toHaveBeenCalledOnce();

    // A cluster that just connected has no metadata: the browser reads it, and here that fails.
    const failed = fakeCluster().cluster;
    vi.mocked(failed.findTopicPartitionMetadata).mockReturnValue([]);
    vi.mocked(failed.refreshMetadata).mockRejectedValue(new Error("metadata failed"));
    const failedSymbol = Symbol("private:Kafka:createCluster");
    const failedKafka = { [failedSymbol]: () => failed } as unknown as Kafka;
    await expect(
      browseMessages(failedKafka, {
        topic: "orders",
        partition: 0,
        startMode: "earliest",
        limit: 10,
      }),
    ).rejects.toThrow("metadata failed");
    expect(failed.disconnect).toHaveBeenCalledOnce();
  });

  it("finds the pinned KafkaJS private cluster factory", () => {
    const cluster = fakeCluster().cluster;
    const symbol = Symbol("private:Kafka:createCluster");
    const kafka = { [symbol]: () => cluster } as unknown as Kafka;
    expect(createKafkaJsReadCluster(kafka)).toBe(cluster);
  });
});

describe("topic metadata of the read cluster", () => {
  it("uses the metadata the cluster has when it knows the topics", async () => {
    const { cluster } = fakeCluster();
    await loadTopicMetadata(cluster, [{ topic: "orders" }, { topic: "payments", partition: 0 }]);
    expect(cluster.refreshMetadata).not.toHaveBeenCalled();
  });

  it("reads the metadata once more when it does not know a topic or a partition yet", async () => {
    const { cluster } = fakeCluster();
    const known = new Map([["orders", 1]]);
    vi.mocked(cluster.findTopicPartitionMetadata).mockImplementation((topic) =>
      Array.from({ length: known.get(topic) ?? 0 }, (_, partitionId) => ({ partitionId, leader: 1 })),
    );
    vi.mocked(cluster.refreshMetadata).mockImplementation(async () => {
      known.set("created-a-moment-ago", 1);
      known.set("orders", 2);
    });

    await loadTopicMetadata(cluster, [{ topic: "orders" }, { topic: "created-a-moment-ago" }]);
    expect(cluster.refreshMetadata).toHaveBeenCalledOnce();
    expect(cluster.findTopicPartitionMetadata("created-a-moment-ago")).toHaveLength(1);

    // A partition added to a known topic, then one that still does not exist: one more read each time.
    known.set("orders", 1);
    await loadTopicMetadata(cluster, [{ topic: "orders", partition: 1 }]);
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
    await loadTopicMetadata(cluster, [{ topic: "orders", partition: 1 }]);
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(2);
    await loadTopicMetadata(cluster, [{ topic: "orders", partition: 9 }]);
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(3);
    // Every wanted partition of a topic counts, not only the last one.
    await loadTopicMetadata(cluster, [
      { topic: "orders", partition: 9 },
      { topic: "orders", partition: 0 },
    ]);
    expect(cluster.refreshMetadata).toHaveBeenCalledTimes(4);
  });

  it("reads the metadata when the cluster has none, and lets any other failure of the lookup through", async () => {
    const { cluster } = fakeCluster();
    let loaded = false;
    // What KafkaJS throws from the lookup while it has no metadata.
    const notLoaded = Object.assign(new Error("Topic metadata not loaded"), { name: "KafkaJSTopicMetadataNotLoaded" });
    vi.mocked(cluster.findTopicPartitionMetadata).mockImplementation(() => {
      if (!loaded) throw notLoaded;
      return [{ partitionId: 0, leader: 1 }];
    });
    vi.mocked(cluster.refreshMetadata).mockImplementation(async () => {
      loaded = true;
    });

    await loadTopicMetadata(cluster, [{ topic: "orders", partition: 0 }]);
    expect(cluster.refreshMetadata).toHaveBeenCalledOnce();

    vi.mocked(cluster.findTopicPartitionMetadata).mockImplementation(() => {
      throw new TypeError("not a metadata problem");
    });
    await expect(loadTopicMetadata(cluster, [{ topic: "orders", partition: 0 }])).rejects.toThrow(
      "not a metadata problem",
    );
    expect(cluster.refreshMetadata).toHaveBeenCalledOnce();
  });

  it("looks a topic up once, however many of its partitions are wanted", async () => {
    const { cluster } = fakeCluster();
    vi.mocked(cluster.findTopicPartitionMetadata).mockImplementation(() =>
      [0, 1, 2].map((partitionId) => ({ partitionId, leader: 1 })),
    );
    await loadTopicMetadata(cluster, [
      { topic: "orders", partition: 0 },
      { topic: "orders", partition: 1 },
      { topic: "orders", partition: 2 },
      { topic: "payments", partition: 0 },
      { topic: "payments", partition: 2 },
    ]);
    expect(vi.mocked(cluster.findTopicPartitionMetadata).mock.calls).toEqual([["orders"], ["payments"]]);
    expect(cluster.refreshMetadata).not.toHaveBeenCalled();
  });

  it("never gives a real KafkaJS cluster a topic name, and reads its metadata only for a missing entry", async () => {
    const cluster = createKafkaJsReadCluster(new Kafka({ brokers: ["127.0.0.1:1"], logLevel: logLevel.NOTHING }));
    // The KafkaJS cluster and broker pool are the real ones. Only the broker that would answer the
    // Metadata request is a stand-in, so the topic list seen here is the one KafkaJS asks for.
    const internals = cluster as unknown as {
      targetTopics: Set<string>;
      brokerPool: {
        metadataExpireAt: number | null;
        createSeedBroker(): Promise<void>;
        findConnectedBroker(): Promise<{ metadata(topics: string[]): Promise<unknown> }>;
      };
    };
    const existing = ["orders"];
    const requested: string[][] = [];
    await internals.brokerPool.createSeedBroker();
    internals.brokerPool.findConnectedBroker = async () => ({
      metadata: async (topics) => {
        requested.push(topics);
        return {
          brokers: [{ nodeId: 1, host: "127.0.0.1", port: 1, rack: null }],
          topicMetadata: existing.map((topic) => ({ topic, partitionMetadata: [{ partitionId: 0, leader: 1 }] })),
        };
      },
    });

    // No metadata yet: the lookup of KafkaJS throws, and the metadata of all topics is read. An
    // empty list is what KafkaJS encodes as "all topics".
    await loadTopicMetadata(cluster, [{ topic: "orders", partition: 0 }]);
    expect(requested).toEqual([[]]);

    // Known entries are not read again, not even when KafkaJS considers the metadata too old: one
    // partition without a leader anywhere would fail that read, and with it a healthy topic.
    expect(internals.brokerPool.metadataExpireAt).toBeGreaterThan(Date.now());
    internals.brokerPool.metadataExpireAt = 0;
    await loadTopicMetadata(cluster, [{ topic: "orders", partition: 0 }, { topic: "orders" }]);
    expect(requested).toHaveLength(1);

    // A topic created since then: all topics are read once more. So does the size reader, every time.
    existing.push("payments");
    await loadTopicMetadata(cluster, [{ topic: "payments" }]);
    await cluster.refreshMetadata();
    expect(requested).toEqual([[], [], []]);
    expect(internals.targetTopics.size).toBe(0);
    expect(cluster.findTopicPartitionMetadata("payments")).toEqual([{ partitionId: 0, leader: 1 }]);
  });

  it("goes on reading a healthy topic while a partition somewhere else has no leader", async () => {
    // The response parser of KafkaJS itself: it is what fails the metadata of all topics on one partition.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { parse } = require("kafkajs/src/protocol/requests/metadata/v6/response") as {
      parse(data: unknown): Promise<unknown>;
    };
    const LEADER_NOT_AVAILABLE = 5;
    const cluster = createKafkaJsReadCluster(
      new Kafka({ brokers: ["127.0.0.1:1"], logLevel: logLevel.NOTHING, retry: { retries: 0 } }),
    );
    const internals = cluster as unknown as {
      brokerPool: {
        metadataExpireAt: number | null;
        createSeedBroker(): Promise<void>;
        findConnectedBroker(): Promise<{ metadata(topics: string[]): Promise<unknown> }>;
      };
    };
    const orders = { partitionErrorCode: 0, partitionId: 0, leader: 1 };
    const topicMetadata = [{ topicErrorCode: 0, topic: "orders", partitionMetadata: [orders] }];
    let requests = 0;
    await internals.brokerPool.createSeedBroker();
    internals.brokerPool.findConnectedBroker = async () => ({
      metadata: async () => {
        requests++;
        return parse({ brokers: [{ nodeId: 1, host: "127.0.0.1", port: 1, rack: null }], topicMetadata });
      },
    });
    await loadTopicMetadata(cluster, [{ topic: "orders", partition: 0 }]);
    expect(requests).toBe(1);

    // A partition of another topic loses its leader, and KafkaJS now considers the metadata too old.
    topicMetadata.push({
      topicErrorCode: 0,
      topic: "degraded",
      partitionMetadata: [{ partitionErrorCode: LEADER_NOT_AVAILABLE, partitionId: 0, leader: -1 }],
    });
    internals.brokerPool.metadataExpireAt = 0;
    await expect(cluster.refreshMetadata()).rejects.toThrow("There is no leader for this topic-partition");
    expect(requests).toBe(2);

    // The size read fails like that, with its reason. A reader of "orders" does not ask at all.
    await loadTopicMetadata(cluster, [{ topic: "orders", partition: 0 }]);
    expect(requests).toBe(2);
    expect(cluster.findTopicPartitionMetadata("orders")).toEqual([orders]);
  });
});
