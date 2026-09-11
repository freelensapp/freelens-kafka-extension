/** Create, or assert the absence of, disposable local-only topics (one name or a comma-separated list) for packaged write E2E tests. */

import { connectDirect } from "../../src/main/kafka/kafka-connection";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";
const TOPICS = (process.env.KAFKA_TOPIC ?? "")
  .split(",")
  .map((topic) => topic.trim())
  .filter(Boolean);
const ACTION = process.env.KAFKA_TOPIC_ACTION ?? "create";

async function main(): Promise<void> {
  if (TOPICS.length === 0) throw new Error("KAFKA_TOPIC is required (one name or a comma-separated list)");
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
          topics: TOPICS.map((topic) => ({ topic, numPartitions: 1, replicationFactor: 1 })),
          waitForLeaders: true,
        });
      } else if (ACTION === "assert-absent") {
        // Topic deletion completes asynchronously on the broker: allow a short settle time.
        const deadline = Date.now() + 15_000;
        for (;;) {
          const existing = await admin.listTopics();
          const remaining = TOPICS.filter((topic) => existing.includes(topic));
          if (remaining.length === 0) break;
          if (Date.now() > deadline) throw new Error(`topics ${remaining.join(", ")} still exist on ${BROKER}`);
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
