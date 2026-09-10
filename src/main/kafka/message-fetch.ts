import { CompressionCodecs, CompressionTypes, type Kafka } from "kafkajs";
import { decodeConfluentAvro } from "../schema-registry/avro-decoder";
import { SchemaRegistryClient } from "../schema-registry/client";
import { decodeConfluentProtobuf } from "../schema-registry/protobuf-decoder";

import type {
  KafkaMessageBytesDto,
  KafkaMessageHeaderDto,
  KafkaMessageStartMode,
  KafkaRecordDto,
  MessageBrowseDto,
} from "../../common/ipc";

export const MESSAGE_BROWSE_DEFAULT_LIMIT = 50;
export const MESSAGE_BROWSE_MAX_LIMIT = 100;
export const MESSAGE_FETCH_MAX_BYTES = 1_048_576;
export const MESSAGE_PREVIEW_MAX_BYTES = 65_536;
export const MESSAGE_RESPONSE_PREVIEW_MAX_BYTES = 524_288;

const READ_COMMITTED = 1;
const KAFKA_JS_CREATE_CLUSTER_SYMBOL = "private:Kafka:createCluster";

interface Lz4AsmModule {
  lz4js: {
    compress(source: Uint8Array, options: { frameInfo: { blockMode: number } }): Uint8Array;
    decompress(source: Uint8Array): Uint8Array;
  };
}

type Lz4AsmInitializer = (module: Record<string, unknown>) => Promise<Lz4AsmModule>;

// Use the package's ASM build directly. Its default entry loads an external WASM file through
// fetch(), which is not safe in Node 24/Electron or a self-contained Freelens extension bundle.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const initializeLz4 = require("lz4-asm/dist/lz4asm.js") as Lz4AsmInitializer;
const lz4Ready = initializeLz4({});

const lz4Codec = () => ({
  compress: async ({ buffer }: { buffer: Buffer }) => {
    const { lz4js } = await lz4Ready;
    return Buffer.from(lz4js.compress(buffer, { frameInfo: { blockMode: 1 } }));
  },
  decompress: async (buffer: Buffer) => {
    const { lz4js } = await lz4Ready;
    return Buffer.from(lz4js.decompress(buffer));
  },
});

export function registerKafkaMessageCodecs(): void {
  CompressionCodecs[CompressionTypes.LZ4] = lz4Codec;
}

registerKafkaMessageCodecs();

interface MessageBrowseInput {
  topic: string;
  partition: number;
  startMode: KafkaMessageStartMode;
  offset?: string;
  timestamp?: number;
  limit: number;
  registryUrl?: string;
  registryUsername?: string;
  registryPassword?: string;
}

interface ValidatedMessageBrowseInput extends MessageBrowseInput {
  topic: string;
}

interface RawKafkaMessage {
  offset: string;
  timestamp: string;
  key: Buffer | string | null;
  value: Buffer | string | null;
  headers?: Record<string, Buffer | string | null | Array<Buffer | string | null> | undefined>;
  isControlRecord?: boolean;
  batchContext?: { producerId: string; inTransaction: boolean };
}

interface RawPartitionData {
  partition: number;
  highWatermark: string;
  abortedTransactions?: Array<{ firstOffset: string; producerId: string }>;
  messages: RawKafkaMessage[];
}

interface KafkaJsBatchInstance {
  messages: RawKafkaMessage[];
  lastOffset(): string;
}

interface KafkaReadBroker {
  fetch(request: {
    replicaId: number;
    isolationLevel: number;
    maxWaitTime: number;
    minBytes: number;
    maxBytes: number;
    topics: Array<{
      topic: string;
      partitions: Array<{ partition: number; fetchOffset: string; maxBytes: number }>;
    }>;
  }): Promise<{
    responses: Array<{ topicName: string; partitions: RawPartitionData[] }>;
  }>;
}

export interface KafkaReadCluster {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  addTargetTopic(topic: string): Promise<void>;
  addMultipleTargetTopics(topics: string[]): Promise<void>;
  refreshMetadata(): Promise<void>;
  getNodeIds(): string[];
  findTopicPartitionMetadata(topic: string): Array<{ partitionId: number; leader: number | null }>;
  fetchTopicsOffset(
    topics: Array<{
      topic: string;
      partitions: Array<{ partition: number }>;
      fromBeginning?: boolean;
      fromTimestamp?: number;
    }>,
  ): Promise<Array<{ topic: string; partitions: Array<{ partition: number; offset: string }> }>>;
  findBroker(request: { nodeId: string }): Promise<KafkaReadBroker>;
}

interface KafkaWithPrivateCluster {
  [symbol: symbol]: unknown;
}

type CreateCluster = (options: {
  metadataMaxAge: number;
  allowAutoTopicCreation: boolean;
  maxInFlightRequests: number | null;
  instrumentationEmitter: null;
  isolationLevel: number;
}) => KafkaReadCluster;

type KafkaJsBatchConstructor = new (
  topic: string,
  fetchedOffset: string,
  partitionData: RawPartitionData,
) => KafkaJsBatchInstance;

// KafkaJS has no public group-free record reader. Keep its pinned internal surface behind this file.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const KafkaJsBatch = require("kafkajs/src/consumer/batch") as KafkaJsBatchConstructor;

class PreviewBudget {
  private remaining = MESSAGE_RESPONSE_PREVIEW_MAX_BYTES;

  take(bytes: Buffer): Buffer {
    const length = Math.min(bytes.length, MESSAGE_PREVIEW_MAX_BYTES, this.remaining);
    this.remaining -= length;
    return bytes.subarray(0, length);
  }
}

function decimalOffset(value: string, label: string): bigint {
  if (!/^(0|[1-9]\d*)$/.test(value)) throw new Error(`${label} must be a non-negative decimal offset`);
  return BigInt(value);
}

function offsetValue(
  offsets: Array<{ topic: string; partitions: Array<{ partition: number; offset: string }> }>,
  topic: string,
  partition: number,
  label: string,
): string {
  const value = offsets
    .find((entry) => entry.topic === topic)
    ?.partitions.find((entry) => entry.partition === partition)?.offset;
  if (value === undefined) throw new Error(`Kafka did not return ${label} for partition ${partition}`);
  return value;
}

function validUtf8(bytes: Buffer): string | undefined {
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    return undefined;
  }
}

function validUtf8Preview(bytes: Buffer): { bytes: Buffer; text: string } {
  for (let end = bytes.length; end >= Math.max(0, bytes.length - 3); end -= 1) {
    const candidate = bytes.subarray(0, end);
    const text = validUtf8(candidate);
    if (text !== undefined) return { bytes: candidate, text };
  }
  return { bytes: Buffer.alloc(0), text: "" };
}

function isJson(text: string): boolean {
  if (!text.trim()) return false;
  try {
    JSON.parse(text);
    return true;
  } catch {
    return false;
  }
}

export function normalizeMessageBytes(
  value: Buffer | string | null | undefined,
  budget = new PreviewBudget(),
): KafkaMessageBytesDto {
  if (value === null || value === undefined) {
    return { format: "null", byteLength: 0, truncated: false };
  }

  const bytes = Buffer.isBuffer(value) ? value : Buffer.from(value);
  const fullText = validUtf8(bytes);
  let preview = budget.take(bytes);
  let text: string | undefined;
  if (fullText !== undefined) {
    if (preview.length === bytes.length) {
      text = fullText;
    } else {
      const validPreview = validUtf8Preview(preview);
      preview = validPreview.bytes;
      text = validPreview.text;
    }
  }

  return {
    format: fullText === undefined ? "binary" : isJson(fullText) ? "json" : "text",
    byteLength: bytes.length,
    truncated: preview.length < bytes.length,
    base64: preview.toString("base64"),
    ...(text === undefined ? {} : { text }),
  };
}

export function normalizeMessageHeaders(
  headers: RawKafkaMessage["headers"],
  budget = new PreviewBudget(),
): KafkaMessageHeaderDto[] {
  return Object.entries(headers ?? {}).flatMap(([name, rawValues]) => {
    const values = Array.isArray(rawValues) ? rawValues : [rawValues];
    return values.map((value) => ({ name, value: normalizeMessageBytes(value, budget) }));
  });
}

export function validateMessageBrowseInput(input: MessageBrowseInput): ValidatedMessageBrowseInput {
  const topic = input.topic.trim();
  if (!topic) throw new Error("Topic is required");
  if (!Number.isInteger(input.partition) || input.partition < 0) {
    throw new Error("Partition must be a non-negative integer");
  }
  if (!Number.isInteger(input.limit) || input.limit < 1 || input.limit > MESSAGE_BROWSE_MAX_LIMIT) {
    throw new Error(`Limit must be between 1 and ${MESSAGE_BROWSE_MAX_LIMIT}`);
  }
  if (!(["earliest", "latest", "offset", "timestamp"] as const).includes(input.startMode)) {
    throw new Error("Unsupported message start mode");
  }
  if (input.startMode === "offset") decimalOffset(input.offset ?? "", "Offset");
  if (input.startMode === "timestamp" && (!Number.isSafeInteger(input.timestamp) || (input.timestamp ?? -1) < 0)) {
    throw new Error("Timestamp must be a non-negative epoch millisecond integer");
  }
  return { ...input, topic };
}

export function createKafkaJsReadCluster(kafka: Kafka): KafkaReadCluster {
  const privateKafka = kafka as unknown as KafkaWithPrivateCluster;
  const symbol = Object.getOwnPropertySymbols(kafka).find(
    (candidate) => candidate.description === KAFKA_JS_CREATE_CLUSTER_SYMBOL,
  );
  const createCluster = symbol ? privateKafka[symbol] : undefined;
  if (typeof createCluster !== "function") {
    throw new Error("KafkaJS 2.2.4 group-free Fetch adapter is unavailable");
  }
  return (createCluster as CreateCluster)({
    metadataMaxAge: 300_000,
    allowAutoTopicCreation: false,
    maxInFlightRequests: null,
    instrumentationEmitter: null,
    isolationLevel: READ_COMMITTED,
  });
}

async function topicOffset(
  cluster: KafkaReadCluster,
  topic: string,
  partition: number,
  selector: { fromBeginning: boolean } | { fromTimestamp: number },
  label: string,
): Promise<string> {
  const offsets = await cluster.fetchTopicsOffset([{ topic, partitions: [{ partition }], ...selector }]);
  return offsetValue(offsets, topic, partition, label);
}

function resolveStartOffset(
  input: ValidatedMessageBrowseInput,
  logStartOffset: string,
  highWatermark: string,
  timestampOffset?: string,
): string {
  const logStart = decimalOffset(logStartOffset, "Log start offset");
  const high = decimalOffset(highWatermark, "High watermark");
  if (input.startMode === "earliest") return logStartOffset;
  if (input.startMode === "latest") {
    const candidate = high - BigInt(input.limit);
    return (candidate > logStart ? candidate : logStart).toString();
  }
  if (input.startMode === "timestamp") {
    return timestampOffset === undefined || timestampOffset === "-1" ? highWatermark : timestampOffset;
  }

  const requested = decimalOffset(input.offset ?? "", "Offset");
  if (requested < logStart) throw new Error(`Offset ${requested} is before log start ${logStart}`);
  if (requested > high) throw new Error(`Offset ${requested} is beyond high watermark ${high}`);
  return requested.toString();
}

async function normalizeRecord(
  topic: string,
  partition: number,
  message: RawKafkaMessage,
  budget: PreviewBudget,
  registry?: SchemaRegistryClient,
): Promise<KafkaRecordDto> {
  const record: KafkaRecordDto = {
    topic,
    partition,
    offset: message.offset,
    timestamp: message.timestamp,
    key: normalizeMessageBytes(message.key, budget),
    value: normalizeMessageBytes(message.value, budget),
    headers: normalizeMessageHeaders(message.headers, budget),
  };
  if (registry && Buffer.isBuffer(message.value)) {
    const schemaId = message.value.length >= 5 && message.value[0] === 0 ? message.value.readUInt32BE(1) : undefined;
    if (schemaId !== undefined) {
      try {
        const schema = await registry.getSchemaById(schemaId);
        const decoded =
          schema.schemaType === "PROTOBUF"
            ? await decodeConfluentProtobuf(message.value, { getSchemaById: async () => schema })
            : await decodeConfluentAvro(message.value, registry);
        if (decoded && "decoded" in decoded) record.decodedValue = decoded.decoded;
        else if (decoded) record.decodeWarning = decoded.warning;
      } catch (error) {
        record.decodeWarning = `Schema Registry decode failed for schema ${schemaId}: ${error instanceof Error ? error.message : String(error)}`;
      }
    }
  }
  return record;
}

export async function browseMessagesWithCluster(
  cluster: KafkaReadCluster,
  rawInput: MessageBrowseInput,
): Promise<MessageBrowseDto> {
  const input = validateMessageBrowseInput(rawInput);
  await cluster.addTargetTopic(input.topic);
  const metadata = cluster
    .findTopicPartitionMetadata(input.topic)
    .find((candidate) => candidate.partitionId === input.partition);
  if (!metadata) throw new Error(`Partition ${input.partition} does not exist for topic ${input.topic}`);
  if (metadata.leader === null || metadata.leader < 0) {
    throw new Error(`Partition ${input.partition} has no available leader`);
  }

  const [logStartOffset, highWatermark, timestampOffset] = await Promise.all([
    topicOffset(cluster, input.topic, input.partition, { fromBeginning: true }, "log start offset"),
    topicOffset(cluster, input.topic, input.partition, { fromBeginning: false }, "high watermark"),
    input.startMode === "timestamp"
      ? topicOffset(cluster, input.topic, input.partition, { fromTimestamp: input.timestamp ?? 0 }, "timestamp offset")
      : Promise.resolve(undefined),
  ]);
  const startOffset = resolveStartOffset(input, logStartOffset, highWatermark, timestampOffset);
  const start = decimalOffset(startOffset, "Start offset");
  const high = decimalOffset(highWatermark, "High watermark");

  let messages: KafkaRecordDto[] = [];
  let next = start;
  if (start < high) {
    const broker = await cluster.findBroker({ nodeId: String(metadata.leader) });
    const response = await broker.fetch({
      replicaId: -1,
      isolationLevel: READ_COMMITTED,
      maxWaitTime: 250,
      minBytes: 1,
      maxBytes: MESSAGE_FETCH_MAX_BYTES,
      topics: [
        {
          topic: input.topic,
          partitions: [
            {
              partition: input.partition,
              fetchOffset: startOffset,
              maxBytes: MESSAGE_FETCH_MAX_BYTES,
            },
          ],
        },
      ],
    });
    const partitionData = response.responses
      .find((entry) => entry.topicName === input.topic)
      ?.partitions.find((entry) => entry.partition === input.partition);
    if (!partitionData) throw new Error(`Kafka returned no Fetch response for partition ${input.partition}`);

    const batch = new KafkaJsBatch(input.topic, startOffset, partitionData);
    const selected = batch.messages.slice(0, input.limit);
    const budget = new PreviewBudget();
    const registry = input.registryUrl
      ? new SchemaRegistryClient({
          baseUrl: input.registryUrl,
          username: input.registryUsername,
          password: input.registryPassword,
        })
      : undefined;
    messages = await Promise.all(
      selected.map((message) => normalizeRecord(input.topic, input.partition, message, budget, registry)),
    );
    if (selected.length > 0) {
      next = BigInt(selected[selected.length - 1].offset) + 1n;
    } else {
      next = BigInt(batch.lastOffset()) + 1n;
    }
    if (next > high) next = high;
  }

  return {
    topic: input.topic,
    partition: input.partition,
    startMode: input.startMode,
    ...(input.startMode === "offset" ? { requestedOffset: input.offset } : {}),
    startOffset,
    nextOffset: next.toString(),
    logStartOffset,
    highWatermark,
    returnedCount: messages.length,
    hasMore: next < high,
    messages,
  };
}

export async function browseMessages(kafka: Kafka, input: MessageBrowseInput): Promise<MessageBrowseDto> {
  const validated = validateMessageBrowseInput(input);
  const cluster = createKafkaJsReadCluster(kafka);
  await cluster.connect();
  try {
    return await browseMessagesWithCluster(cluster, validated);
  } finally {
    await cluster.disconnect();
  }
}
