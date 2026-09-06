import protobuf from "protobufjs";
import { connectDirect } from "../../src/main/kafka/kafka-connection";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";
const TOPIC = "freelens-schema-payments";
const schema = 'syntax = "proto3"; message Payment { string id = 1; }';

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
      await admin.createTopics({
        topics: [{ topic: TOPIC, numPartitions: 1, replicationFactor: 1 }],
        waitForLeaders: true,
      });
    } finally {
      await admin.disconnect();
    }
    const producerConnection = await connectDirect({ bootstrap: BROKER });
    const producer = producerConnection.client.producer({ allowAutoTopicCreation: false });
    await producer.connect();
    try {
      const type = protobuf.parse(schema).root.lookupType("Payment");
      await producer.send({
        topic: TOPIC,
        messages: [
          {
            partition: 0,
            key: "payment",
            value: Buffer.concat([Buffer.from([0, 0, 0, 0, 21]), type.encode({ id: "decoded-payment" }).finish()]),
          },
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
