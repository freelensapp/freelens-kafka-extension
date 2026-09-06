import avsc from "avsc";
import { connectDirect } from "../../src/main/kafka/kafka-connection";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";
const TOPIC = "freelens-schema-orders";
const SCHEMA_ID = 7;
const schema = { type: "record", name: "Order", fields: [{ name: "id", type: "string" }] } as avsc.Schema;

async function main(): Promise<void> {
  const connection = await connectDirect({ bootstrap: BROKER });
  try {
    const admin = connection.admin();
    await admin.connect();
    try {
      try {
        await admin.deleteTopics({ topics: [TOPIC] });
      } catch {
        // The topic may not exist on the first run.
      }
      try {
        await admin.createTopics({
          topics: [{ topic: TOPIC, numPartitions: 1, replicationFactor: 1 }],
          waitForLeaders: true,
        });
      } catch (error) {
        if (!String(error).toLowerCase().includes("already exists")) throw error;
      }
    } finally {
      await admin.disconnect();
    }

    await connection.disconnect();
    const producerConnection = await connectDirect({ bootstrap: BROKER });
    const producer = producerConnection.client.producer({ allowAutoTopicCreation: false });
    await producer.connect();
    try {
      const payload = Buffer.concat([Buffer.from([0]), Buffer.alloc(4)]);
      payload.writeUInt32BE(SCHEMA_ID, 1);
      await producer.send({
        topic: TOPIC,
        messages: [
          {
            partition: 0,
            key: "schema-order",
            value: Buffer.concat([payload, avsc.Type.forSchema(schema).toBuffer({ id: "decoded-order" })]),
          },
          { partition: 0, key: "unknown-schema", value: Buffer.from([0, 0, 0, 0, 99, 1, 2]) },
        ],
      });
    } finally {
      await producer.disconnect();
      await producerConnection.disconnect();
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
