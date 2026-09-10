import { describe, expect, it } from "vitest";
import { sendKafkaProtocol, supportsDescribeLogDirs } from "./group-offset-batch-protocol";
import { describeLogDirsProtocol } from "./log-dirs-protocol";

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

/** A DescribeLogDirs v1 response with one log dir holding two partitions of one topic. */
function responseBuffer(): Buffer {
  const partition = (index: number, size: string, lag: string, future: boolean) =>
    new Encoder().writeInt32(index).writeInt64(size).writeInt64(lag).writeBoolean(future);
  const topic = new Encoder()
    .writeString("orders")
    .writeArray([partition(0, "1024", "0", false), partition(1, "2048", "5", false), partition(1, "1", "0", true)]);
  const result = new Encoder().writeInt16(0).writeString("/var/lib/kafka/data").writeArray([topic]);
  return new Encoder().writeInt32(7).writeArray([result]).buffer;
}

describe("describeLogDirsProtocol", () => {
  it("encodes the requested topics and partitions with the non-flexible v1 layout", async () => {
    const encoder = await describeLogDirsProtocol([{ topic: "orders", partitions: [0, 1] }]).request.encode();
    const expected = new Encoder().writeInt32(1).writeString("orders").writeInt32(2).writeInt32(0).writeInt32(1);
    expect(encoder.buffer.equals(expected.buffer)).toBe(true);
  });

  it("decodes sizes and offset lags as decimal strings and keeps the future flag", async () => {
    const decoded = await describeLogDirsProtocol([]).response.decode(responseBuffer());
    expect(decoded.throttleTime).toBe(7);
    expect(decoded.results).toEqual([
      {
        errorCode: 0,
        logDir: "/var/lib/kafka/data",
        topics: [
          {
            topic: "orders",
            partitions: [
              { partition: 0, size: "1024", offsetLag: "0", isFuture: false },
              { partition: 1, size: "2048", offsetLag: "5", isFuture: false },
              { partition: 1, size: "1", offsetLag: "0", isFuture: true },
            ],
          },
        ],
      },
    ]);
  });

  it("is accepted by the read-only transport only when the broker offers version 1", async () => {
    expect(supportsDescribeLogDirs({ 35: { minVersion: 0, maxVersion: 4 } })).toBe(true);
    expect(supportsDescribeLogDirs({ 35: { minVersion: 2, maxVersion: 4 } })).toBe(false);
    expect(supportsDescribeLogDirs({})).toBe(false);
    const sent: unknown[] = [];
    const broker = {
      versions: { 35: { minVersion: 0, maxVersion: 4 } },
      [Symbol("private:Broker:sendRequest")]: async (protocol: { response: { decode(buffer: Buffer): unknown } }) => {
        sent.push(protocol);
        return protocol.response.decode(responseBuffer());
      },
    };
    const response = await sendKafkaProtocol(broker, describeLogDirsProtocol([{ topic: "orders", partitions: [0] }]));
    expect(sent).toHaveLength(1);
    expect(response.results[0]?.topics[0]?.partitions).toHaveLength(3);
    await expect(
      sendKafkaProtocol({ versions: { 35: { minVersion: 2, maxVersion: 4 } } }, describeLogDirsProtocol([])),
    ).rejects.toThrow("does not support DescribeLogDirs");
  });
});
