import { describe, expect, it, vi } from "vitest";
import {
  findCoordinatorBatchProtocol,
  offsetFetchBatchProtocol,
  sendKafkaProtocol,
  supportsGroupOffsetBatch,
} from "./group-offset-batch-protocol";

interface KafkaEncoder {
  readonly buffer: Buffer;
  writeInt16(value: number): KafkaEncoder;
  writeInt32(value: number): KafkaEncoder;
  writeInt64(value: number | string): KafkaEncoder;
  writeUVarIntArray(value: KafkaEncoder[]): KafkaEncoder;
  writeUVarIntBytes(value?: Buffer): KafkaEncoder;
  writeUVarIntString(value: string | null): KafkaEncoder;
}

type KafkaEncoderConstructor = new () => KafkaEncoder;

// eslint-disable-next-line @typescript-eslint/no-require-imports
const Encoder = require("kafkajs/src/protocol/encoder") as KafkaEncoderConstructor;

describe("group offset batch protocol", () => {
  it("requires both negotiated batch API versions", () => {
    expect(
      supportsGroupOffsetBatch({ 9: { minVersion: 0, maxVersion: 8 }, 10: { minVersion: 0, maxVersion: 4 } }),
    ).toBe(true);
    expect(
      supportsGroupOffsetBatch({ 9: { minVersion: 0, maxVersion: 7 }, 10: { minVersion: 0, maxVersion: 4 } }),
    ).toBe(false);
    expect(
      supportsGroupOffsetBatch({ 9: { minVersion: 0, maxVersion: 8 }, 10: { minVersion: 0, maxVersion: 3 } }),
    ).toBe(false);
    expect(
      supportsGroupOffsetBatch({ 9: { minVersion: 9, maxVersion: 10 }, 10: { minVersion: 4, maxVersion: 6 } }),
    ).toBe(false);
  });

  it("does not invoke the private transport when capability negotiation fails", async () => {
    const sendSymbol = Symbol("private:Broker:sendRequest");
    const send = vi.fn();
    const broker = {
      versions: { 9: { minVersion: 0, maxVersion: 7 }, 10: { minVersion: 0, maxVersion: 4 } },
      [sendSymbol]: send,
    };

    await expect(sendKafkaProtocol(broker, offsetFetchBatchProtocol(["g1"]))).rejects.toThrow(
      "does not support read-only multi-group offset fetch",
    );
    expect(send).not.toHaveBeenCalled();
  });

  it("rejects protocols outside the two read-only batch APIs", async () => {
    const sendSymbol = Symbol("private:Broker:sendRequest");
    const send = vi.fn();
    const broker = {
      versions: { 9: { minVersion: 0, maxVersion: 8 }, 10: { minVersion: 0, maxVersion: 4 } },
      [sendSymbol]: send,
    };
    const unsafeProtocol = {
      ...offsetFetchBatchProtocol(["g1"]),
      request: { ...offsetFetchBatchProtocol(["g1"]).request, apiKey: 8, apiName: "OffsetCommit" },
    };

    await expect(sendKafkaProtocol(broker, unsafeProtocol)).rejects.toThrow(
      "Refusing non-read-only Kafka batch protocol",
    );
    expect(send).not.toHaveBeenCalled();
  });

  it("sends a negotiated read-only protocol through the KafkaJS broker adapter", async () => {
    const sendSymbol = Symbol("private:Broker:sendRequest");
    const send = vi.fn(async () => ({ coordinators: [], throttleTime: 0 }));
    const broker = {
      versions: { 9: { minVersion: 0, maxVersion: 8 }, 10: { minVersion: 0, maxVersion: 4 } },
      [sendSymbol]: send,
    };
    const protocol = findCoordinatorBatchProtocol(["g1"]);

    await expect(sendKafkaProtocol(broker, protocol)).resolves.toEqual({ coordinators: [], throttleTime: 0 });
    expect(send).toHaveBeenCalledWith(protocol);
  });

  it("finds the KafkaJS transport symbol on the broker prototype", async () => {
    const sendSymbol = Symbol("private:Broker:sendRequest");
    const send = vi.fn(async () => ({ coordinators: [], throttleTime: 0 }));
    const broker = Object.assign(Object.create({ [sendSymbol]: send }) as object, {
      versions: { 9: { minVersion: 0, maxVersion: 8 }, 10: { minVersion: 0, maxVersion: 4 } },
    });

    await expect(sendKafkaProtocol(broker, findCoordinatorBatchProtocol(["g1"]))).resolves.toEqual({
      coordinators: [],
      throttleTime: 0,
    });
    expect(send).toHaveBeenCalledOnce();
  });

  it("encodes flexible FindCoordinator v4 for multiple group keys", async () => {
    const encoded = await findCoordinatorBatchProtocol(["g1", "g2"]).request.encode();

    expect(encoded.buffer.toString("hex")).toBe("00000303673103673200");
  });

  it("decodes a flexible FindCoordinator v4 response", async () => {
    const coordinator = new Encoder()
      .writeUVarIntString("g1")
      .writeInt32(2)
      .writeUVarIntString("broker-2")
      .writeInt32(9092)
      .writeInt16(0)
      .writeUVarIntString(null)
      .writeUVarIntBytes();
    const response = new Encoder()
      .writeUVarIntBytes()
      .writeInt32(4)
      .writeUVarIntArray([coordinator])
      .writeUVarIntBytes();

    await expect(findCoordinatorBatchProtocol(["g1"]).response.decode(response.buffer)).resolves.toMatchObject({
      clientSideThrottleTime: 4,
      coordinators: [{ key: "g1", nodeId: 2, host: "broker-2", port: 9092, errorCode: 0, errorMessage: null }],
    });
  });

  it("encodes flexible OffsetFetch v8 for all offsets of multiple groups", async () => {
    const encoded = await offsetFetchBatchProtocol(["g1", "g2"]).request.encode();

    expect(encoded.buffer.toString("hex")).toBe("0003036731000003673200000000");
  });

  it("decodes and normalizes a flexible OffsetFetch v8 response", async () => {
    const partition = new Encoder()
      .writeInt32(0)
      .writeInt64("42")
      .writeInt32(7)
      .writeUVarIntString(null)
      .writeInt16(0)
      .writeUVarIntBytes();
    const topic = new Encoder().writeUVarIntString("orders").writeUVarIntArray([partition]).writeUVarIntBytes();
    const group = new Encoder().writeUVarIntString("g1").writeUVarIntArray([topic]).writeInt16(0).writeUVarIntBytes();
    const response = new Encoder().writeUVarIntBytes().writeInt32(0).writeUVarIntArray([group]).writeUVarIntBytes();

    await expect(offsetFetchBatchProtocol(["g1"]).response.decode(response.buffer)).resolves.toMatchObject({
      groups: [
        {
          groupId: "g1",
          errorCode: 0,
          topics: [
            {
              topic: "orders",
              partitions: [
                {
                  partition: 0,
                  committedOffset: "42",
                  committedLeaderEpoch: 7,
                  metadata: null,
                  errorCode: 0,
                },
              ],
            },
          ],
        },
      ],
    });
  });
});
