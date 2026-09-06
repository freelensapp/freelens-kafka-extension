/** Seed deterministic local-only records for protocol and packaged message Browse tests. */

import { CompressionTypes, ConfigResourceTypes } from "kafkajs";
import { connectDirect } from "../../src/main/kafka/kafka-connection";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";
const TOPIC = "freelens-orders";
export const MESSAGE_FIXTURE_TIMESTAMP = 1_700_000_000_000;

async function main(): Promise<void> {
  const connection = await connectDirect({ bootstrap: BROKER });
  try {
    const admin = connection.admin();
    await admin.connect();
    let alreadySeeded = false;
    try {
      await admin.alterConfigs({
        validateOnly: false,
        resources: [
          {
            type: ConfigResourceTypes.TOPIC,
            name: TOPIC,
            configEntries: [{ name: "retention.ms", value: "-1" }],
          },
        ],
      });
      const offsets = await admin.fetchTopicOffsets(TOPIC);
      alreadySeeded = offsets.some(({ high, low }) => BigInt(high) > BigInt(low));
    } finally {
      await admin.disconnect();
    }
    if (alreadySeeded) return;

    const producer = connection.client.producer({ allowAutoTopicCreation: false });
    await producer.connect();
    try {
      await producer.send({
        topic: TOPIC,
        compression: CompressionTypes.LZ4,
        messages: [
          {
            partition: 0,
            timestamp: String(MESSAGE_FIXTURE_TIMESTAMP),
            key: "order-json",
            value: JSON.stringify({ id: 1001, state: "created" }),
            headers: {
              trace: ["first", "second"],
              contentType: "application/json",
            },
          },
          {
            partition: 0,
            timestamp: String(MESSAGE_FIXTURE_TIMESTAMP + 1),
            key: "order-text",
            value: "ready for pickup",
            headers: { contentType: "text/plain" },
          },
          {
            partition: 0,
            timestamp: String(MESSAGE_FIXTURE_TIMESTAMP + 2),
            key: "order-binary",
            value: Buffer.from([0xff, 0x00, 0x7f]),
          },
          {
            partition: 0,
            timestamp: String(MESSAGE_FIXTURE_TIMESTAMP + 3),
            key: "order-null",
            value: null,
          },
        ],
      });
    } finally {
      await producer.disconnect();
    }
  } finally {
    await connection.disconnect();
  }
}

main()
  .then(() => process.exit(0))
  .catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(1);
  });
