/** Create, or assert the absence of, a disposable local-only topic for packaged write E2E tests. */

import { connectDirect } from "../../src/main/kafka/kafka-connection";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";
const TOPIC = process.env.KAFKA_TOPIC;
const ACTION = process.env.KAFKA_TOPIC_ACTION ?? "create";

async function main(): Promise<void> {
  if (!TOPIC) throw new Error("KAFKA_TOPIC is required");
  if (BROKER !== "127.0.0.1:19092") {
    throw new Error("Refusing the topic fixture outside the approved loopback fixture");
  }

  const connection = await connectDirect({ bootstrap: BROKER });
  try {
    const admin = connection.admin();
    await admin.connect();
    try {
      if (ACTION === "create") {
        await admin.createTopics({
          topics: [{ topic: TOPIC, numPartitions: 1, replicationFactor: 1 }],
          waitForLeaders: true,
        });
      } else if (ACTION === "assert-absent") {
        // Topic deletion completes asynchronously on the broker: allow a short settle time.
        const deadline = Date.now() + 15_000;
        while ((await admin.listTopics()).includes(TOPIC)) {
          if (Date.now() > deadline) throw new Error(`topic ${TOPIC} still exists on ${BROKER}`);
          await new Promise((resolve) => setTimeout(resolve, 500));
        }
      } else {
        throw new Error(`unknown KAFKA_TOPIC_ACTION ${ACTION}`);
      }
    } finally {
      await admin.disconnect();
    }
  } finally {
    await connection.disconnect();
  }
}

main()
  .then(() => process.exit(0))
  .catch((error: unknown) => {
    console.error(error);
    process.exit(1);
  });
