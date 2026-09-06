import protobuf from "protobufjs";
import { describe, expect, it, vi } from "vitest";
import { decodeConfluentProtobuf } from "./protobuf-decoder";

const schema = 'syntax = "proto3"; message Payment { string id = 1; }';

describe("decodeConfluentProtobuf", () => {
  it("decodes a Confluent Protobuf payload", async () => {
    const type = protobuf.parse(schema).root.lookupType("Payment");
    const payload = Buffer.concat([Buffer.from([0, 0, 0, 0, 21]), type.encode({ id: "payment-1" }).finish()]);
    const registry = { getSchemaById: vi.fn().mockResolvedValue({ id: 21, schema, schemaType: "PROTOBUF" }) };

    await expect(decodeConfluentProtobuf(payload, registry)).resolves.toEqual({
      schemaId: 21,
      decoded: { id: "payment-1" },
    });
  });
});
