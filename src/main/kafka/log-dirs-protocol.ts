import {
  DESCRIBE_LOG_DIRS_API_KEY,
  DESCRIBE_LOG_DIRS_VERSION,
  type KafkaEncoder,
  type KafkaProtocol,
} from "./group-offset-batch-protocol";

interface LogDirsDecoder {
  readInt16(): number;
  readInt32(): number;
  readInt64(): { toString(): string };
  readString(): string;
  readBoolean(): boolean;
  readArray<T>(reader: (decoder: LogDirsDecoder) => T): T[];
}

// eslint-disable-next-line @typescript-eslint/no-require-imports
const Encoder = require("kafkajs/src/protocol/encoder") as new () => KafkaEncoder;
// eslint-disable-next-line @typescript-eslint/no-require-imports
const Decoder = require("kafkajs/src/protocol/decoder") as new (buffer: Buffer) => LogDirsDecoder;

export interface LogDirsRequestTopic {
  topic: string;
  partitions: number[];
}

export interface DescribeLogDirsResult {
  throttleTime: number;
  results: Array<{
    errorCode: number;
    logDir: string;
    topics: Array<{
      topic: string;
      partitions: Array<{ partition: number; size: string; offsetLag: string; isFuture: boolean }>;
    }>;
  }>;
}

/**
 * Read-only DescribeLogDirs (API key 35, version 1, non-flexible encoding): the size on disk
 * of the requested partitions in every log directory of the broker that receives the request.
 * KafkaJS has no Admin call for it, so the request is encoded by hand like the batched group
 * offsets and sent through the private broker transport.
 */
export function describeLogDirsProtocol(topics: LogDirsRequestTopic[]): KafkaProtocol<DescribeLogDirsResult> {
  return {
    groupIds: [],
    request: {
      apiKey: DESCRIBE_LOG_DIRS_API_KEY,
      apiVersion: DESCRIBE_LOG_DIRS_VERSION,
      apiName: "DescribeLogDirs",
      encode: async () => {
        const encoder = new Encoder().writeInt32(topics.length);
        for (const entry of topics) {
          encoder.writeString(entry.topic).writeInt32(entry.partitions.length);
          for (const partition of entry.partitions) encoder.writeInt32(partition);
        }
        return encoder;
      },
    },
    response: {
      decode: async (buffer) => {
        const decoder = new Decoder(buffer);
        const throttleTime = decoder.readInt32();
        const results = decoder.readArray((result) => ({
          errorCode: result.readInt16(),
          logDir: result.readString(),
          topics: result.readArray((topic) => ({
            topic: topic.readString(),
            partitions: topic.readArray((partition) => ({
              partition: partition.readInt32(),
              size: partition.readInt64().toString(),
              offsetLag: partition.readInt64().toString(),
              isFuture: partition.readBoolean(),
            })),
          })),
        }));
        return { clientSideThrottleTime: throttleTime, throttleTime, results };
      },
      parse: async (value) => value,
    },
  };
}
