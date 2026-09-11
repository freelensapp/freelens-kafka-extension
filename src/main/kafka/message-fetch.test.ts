import { CompressionCodecs, CompressionTypes } from "kafkajs";
import { describe, expect, it, vi } from "vitest";
import {
  browseMessages,
  browseMessagesWithCluster,
  createKafkaJsReadCluster,
  type KafkaReadCluster,
  MESSAGE_FETCH_MAX_BYTES,
  MESSAGE_PREVIEW_MAX_BYTES,
  MESSAGE_RESPONSE_PREVIEW_MAX_BYTES,
  normalizeMessageBytes,
  normalizeMessageHeaders,
  validateMessageBrowseInput,
} from "./message-fetch";

import type { Kafka } from "kafkajs";

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
    targetTopics: new Set<string>(),
    connect: vi.fn(async () => undefined),
    disconnect: vi.fn(async () => undefined),
    addTargetTopic: vi.fn(async () => undefined),
    addMultipleTargetTopics: vi.fn(async () => undefined),
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

    const failed = fakeCluster().cluster;
    vi.mocked(failed.addTargetTopic).mockRejectedValue(new Error("metadata failed"));
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
