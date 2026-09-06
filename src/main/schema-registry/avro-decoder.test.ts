import avsc from "avsc";
import { describe, expect, it, vi } from "vitest";
import { decodeConfluentAvro } from "./avro-decoder";

const schema = { type: "record", name: "Order", fields: [{ name: "id", type: "string" }] } as avsc.Schema;

describe("decodeConfluentAvro", () => {
  it("decodes a Confluent wire-format Avro payload", async () => {
    const payload = Buffer.concat([
      Buffer.from([0, 0, 0, 0, 7]),
      avsc.Type.forSchema(schema).toBuffer({ id: "order-1" }),
    ]);
    const registry = { getSchemaById: vi.fn().mockResolvedValue({ id: 7, schema: JSON.stringify(schema) }) };

    await expect(decodeConfluentAvro(payload, registry)).resolves.toEqual({
      schemaId: 7,
      decoded: { id: "order-1" },
    });
  });

  it("returns a non-fatal warning for an unknown schema", async () => {
    const payload = Buffer.from([0, 0, 0, 0, 99, 1, 2]);
    const registry = { getSchemaById: vi.fn().mockRejectedValue(new Error("not found")) };

    await expect(decodeConfluentAvro(payload, registry)).resolves.toEqual({
      warning: "Schema Registry decode failed for schema 99: not found",
    });
  });

  it("does not treat ordinary bytes as Confluent payloads", async () => {
    const registry = { getSchemaById: vi.fn() };
    await expect(decodeConfluentAvro(Buffer.from("plain text"), registry)).resolves.toBeUndefined();
    expect(registry.getSchemaById).not.toHaveBeenCalled();
  });
});
