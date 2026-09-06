import avsc from "avsc";

export interface DecodedSchemaValue {
  decoded: unknown;
  schemaId: number;
}

export interface SchemaDecodeFailure {
  warning: string;
}

export type SchemaDecodeResult = DecodedSchemaValue | SchemaDecodeFailure;

export interface SchemaLookup {
  getSchemaById(id: number): Promise<{ id: number; schema: string }>;
}

export async function decodeConfluentAvro(
  value: Buffer,
  registry: SchemaLookup,
): Promise<SchemaDecodeResult | undefined> {
  if (value.length < 5 || value[0] !== 0) return undefined;
  const schemaId = value.readUInt32BE(1);

  try {
    const schema = await registry.getSchemaById(schemaId);
    const type = avsc.Type.forSchema(JSON.parse(schema.schema));
    return { decoded: type.fromBuffer(value.subarray(5)), schemaId };
  } catch (error) {
    return {
      warning: `Schema Registry decode failed for schema ${schemaId}: ${error instanceof Error ? error.message : String(error)}`,
    };
  }
}
