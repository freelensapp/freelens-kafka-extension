const FIND_COORDINATOR_API_KEY = 10;
const OFFSET_FETCH_API_KEY = 9;
const KAFKA_JS_SEND_REQUEST_SYMBOL = "private:Broker:sendRequest";

export const FIND_COORDINATOR_BATCH_VERSION = 4;
export const OFFSET_FETCH_BATCH_VERSION = 8;

interface KafkaEncoder {
  readonly buffer: Buffer;
  writeBoolean(value: boolean): KafkaEncoder;
  writeInt8(value: number): KafkaEncoder;
  writeInt16(value: number): KafkaEncoder;
  writeInt32(value: number): KafkaEncoder;
  writeUVarIntArray(value: KafkaEncoder[] | null): KafkaEncoder;
  writeUVarIntBytes(value?: Buffer): KafkaEncoder;
  writeUVarIntString(value: string | null): KafkaEncoder;
}

interface KafkaDecoder {
  readInt16(): number;
  readInt32(): number;
  readInt64(): bigint;
  readTaggedFields(): unknown;
  readUVarIntArray<T>(reader: (decoder: KafkaDecoder) => T): T[] | null;
  readUVarIntString(): string | null;
}

type KafkaEncoderConstructor = new () => KafkaEncoder;
type KafkaDecoderConstructor = new (buffer: Buffer) => KafkaDecoder;

// eslint-disable-next-line @typescript-eslint/no-require-imports
const Encoder = require("kafkajs/src/protocol/encoder") as KafkaEncoderConstructor;
// eslint-disable-next-line @typescript-eslint/no-require-imports
const Decoder = require("kafkajs/src/protocol/decoder") as KafkaDecoderConstructor;

interface KafkaProtocolRequest {
  apiKey: number;
  apiName: string;
  apiVersion: number;
  encode(): Promise<KafkaEncoder>;
}

interface KafkaProtocolResponse<T> {
  decode(buffer: Buffer): Promise<T & { clientSideThrottleTime: number }>;
  parse(value: T): Promise<T>;
}

export interface KafkaProtocol<T> {
  groupIds: string[];
  request: KafkaProtocolRequest;
  response: KafkaProtocolResponse<T>;
}

export interface KafkaApiVersions {
  [apiKey: number]: { maxVersion: number; minVersion: number } | undefined;
}

export interface KafkaProtocolBroker {
  versions?: KafkaApiVersions;
  [symbol: symbol]: unknown;
}

export interface CoordinatorBatchResult {
  coordinators: Array<{
    errorCode: number;
    errorMessage: string | null;
    host: string;
    key: string;
    nodeId: number;
    port: number;
  }>;
  throttleTime: number;
}

export interface OffsetFetchBatchResult {
  groups: Array<{
    errorCode: number;
    groupId: string;
    topics: Array<{
      partitions: Array<{
        committedLeaderEpoch: number;
        committedOffset: string;
        errorCode: number;
        metadata: string | null;
        partition: number;
      }>;
      topic: string;
    }>;
  }>;
  throttleTime: number;
}

export function supportsGroupOffsetBatch(versions: KafkaApiVersions | undefined): boolean {
  const findCoordinator = versions?.[FIND_COORDINATOR_API_KEY];
  const offsetFetch = versions?.[OFFSET_FETCH_API_KEY];
  return (
    Boolean(
      findCoordinator &&
        findCoordinator.minVersion <= FIND_COORDINATOR_BATCH_VERSION &&
        findCoordinator.maxVersion >= FIND_COORDINATOR_BATCH_VERSION,
    ) &&
    Boolean(
      offsetFetch &&
        offsetFetch.minVersion <= OFFSET_FETCH_BATCH_VERSION &&
        offsetFetch.maxVersion >= OFFSET_FETCH_BATCH_VERSION,
    )
  );
}

function protocolSymbol(value: object, description: string): symbol | undefined {
  let candidate: object | null = value;
  while (candidate) {
    const symbol = Object.getOwnPropertySymbols(candidate).find((item) => item.description === description);
    if (symbol) return symbol;
    candidate = Object.getPrototypeOf(candidate) as object | null;
  }
  return undefined;
}

export async function sendKafkaProtocol<T>(broker: object, protocol: KafkaProtocol<T>): Promise<T> {
  const readOnlyProtocol =
    (protocol.request.apiKey === FIND_COORDINATOR_API_KEY &&
      protocol.request.apiVersion === FIND_COORDINATOR_BATCH_VERSION &&
      protocol.request.apiName === "FindCoordinator") ||
    (protocol.request.apiKey === OFFSET_FETCH_API_KEY &&
      protocol.request.apiVersion === OFFSET_FETCH_BATCH_VERSION &&
      protocol.request.apiName === "OffsetFetch");
  if (!readOnlyProtocol) throw new Error("Refusing non-read-only Kafka batch protocol");
  const protocolBroker = broker as KafkaProtocolBroker;
  if (!supportsGroupOffsetBatch(protocolBroker.versions)) {
    throw new Error("Kafka broker does not support read-only multi-group offset fetch");
  }
  const sendRequestSymbol = protocolSymbol(protocolBroker, KAFKA_JS_SEND_REQUEST_SYMBOL);
  const sendRequest = sendRequestSymbol ? protocolBroker[sendRequestSymbol] : undefined;
  if (typeof sendRequest !== "function") throw new Error("KafkaJS broker request adapter is unavailable");
  return (sendRequest as (value: KafkaProtocol<T>) => Promise<T>).call(protocolBroker, protocol);
}

export function findCoordinatorBatchProtocol(groupIds: string[]): KafkaProtocol<CoordinatorBatchResult> {
  return {
    groupIds: [...groupIds],
    request: {
      apiKey: FIND_COORDINATOR_API_KEY,
      apiVersion: FIND_COORDINATOR_BATCH_VERSION,
      apiName: "FindCoordinator",
      encode: async () =>
        new Encoder()
          .writeUVarIntBytes()
          .writeInt8(0)
          .writeUVarIntArray(groupIds.map((groupId) => new Encoder().writeUVarIntString(groupId)))
          .writeUVarIntBytes(),
    },
    response: {
      decode: async (buffer) => {
        const decoder = new Decoder(buffer);
        decoder.readTaggedFields();
        const throttleTime = decoder.readInt32();
        const coordinators =
          decoder.readUVarIntArray((item) => {
            const coordinator = {
              key: item.readUVarIntString() ?? "",
              nodeId: item.readInt32(),
              host: item.readUVarIntString() ?? "",
              port: item.readInt32(),
              errorCode: item.readInt16(),
              errorMessage: item.readUVarIntString(),
            };
            item.readTaggedFields();
            return coordinator;
          }) ?? [];
        decoder.readTaggedFields();
        return { clientSideThrottleTime: throttleTime, coordinators, throttleTime: 0 };
      },
      parse: async (value) => value,
    },
  };
}

export function offsetFetchBatchProtocol(groupIds: string[]): KafkaProtocol<OffsetFetchBatchResult> {
  return {
    groupIds: [...groupIds],
    request: {
      apiKey: OFFSET_FETCH_API_KEY,
      apiVersion: OFFSET_FETCH_BATCH_VERSION,
      apiName: "OffsetFetch",
      encode: async () =>
        new Encoder()
          .writeUVarIntBytes()
          .writeUVarIntArray(
            groupIds.map((groupId) =>
              new Encoder().writeUVarIntString(groupId).writeUVarIntArray(null).writeUVarIntBytes(),
            ),
          )
          .writeBoolean(false)
          .writeUVarIntBytes(),
    },
    response: {
      decode: async (buffer) => {
        const decoder = new Decoder(buffer);
        decoder.readTaggedFields();
        const throttleTime = decoder.readInt32();
        const groups =
          decoder.readUVarIntArray((groupDecoder) => {
            const groupId = groupDecoder.readUVarIntString() ?? "";
            const topics =
              groupDecoder.readUVarIntArray((topicDecoder) => {
                const topic = topicDecoder.readUVarIntString() ?? "";
                const partitions =
                  topicDecoder.readUVarIntArray((partitionDecoder) => {
                    const partition = {
                      partition: partitionDecoder.readInt32(),
                      committedOffset: partitionDecoder.readInt64().toString(),
                      committedLeaderEpoch: partitionDecoder.readInt32(),
                      metadata: partitionDecoder.readUVarIntString(),
                      errorCode: partitionDecoder.readInt16(),
                    };
                    partitionDecoder.readTaggedFields();
                    return partition;
                  }) ?? [];
                topicDecoder.readTaggedFields();
                return { topic, partitions };
              }) ?? [];
            const errorCode = groupDecoder.readInt16();
            groupDecoder.readTaggedFields();
            return { groupId, topics, errorCode };
          }) ?? [];
        decoder.readTaggedFields();
        return { clientSideThrottleTime: throttleTime, groups, throttleTime: 0 };
      },
      parse: async (value) => value,
    },
  };
}
