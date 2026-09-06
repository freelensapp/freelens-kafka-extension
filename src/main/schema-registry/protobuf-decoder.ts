import protobuf from "protobufjs";

import type { SchemaDecodeResult, SchemaLookup } from "./avro-decoder";

export async function decodeConfluentProtobuf(
  value: Buffer,
  registry: SchemaLookup,
): Promise<SchemaDecodeResult | undefined> {
  if (value.length < 5 || value[0] !== 0) return undefined;
  const schemaId = value.readUInt32BE(1);

  try {
    const schema = await registry.getSchemaById(schemaId);
    const parsed = protobuf.parse(schema.schema).root;
    const messageType = parsed.nestedArray.find((entry): entry is protobuf.Type => entry instanceof protobuf.Type);
    if (!messageType) throw new Error("no top-level protobuf message found");
    const decoded = messageType.decode(value.subarray(5));
    return { decoded: messageType.toObject(decoded, { longs: String, enums: String, defaults: true }), schemaId };
  } catch (error) {
    return {
      warning: `Schema Registry decode failed for schema ${schemaId}: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
}
