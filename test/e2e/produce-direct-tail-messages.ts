/** Produce deterministic local records on partition 0 for tail lifecycle integration tests. */

import { connectDirect } from "../../src/main/kafka/kafka-connection";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";
const TOPIC = process.env.KAFKA_TAIL_TOPIC ?? "freelens-orders";

function readMessages(): string[] {
  const raw = process.env.KAFKA_TAIL_MESSAGES;

  if (!raw) {
    throw new Error("KAFKA_TAIL_MESSAGES is required");
  }

  const parsed = JSON.parse(raw) as unknown;

  if (!Array.isArray(parsed) || parsed.some((item) => typeof item !== "string")) {
    throw new Error("KAFKA_TAIL_MESSAGES must be a JSON string array");
  }

  return parsed;
}

async function main(): Promise<void> {
  const messages = readMessages();

  if (messages.length === 0) {
    return;
  }

  const connection = await connectDirect({ bootstrap: BROKER });

  try {
    const producer = connection.client.producer({ allowAutoTopicCreation: false });

    await producer.connect();

    try {
      const now = Date.now();

      await producer.send({
        topic: TOPIC,
        messages: messages.map((value, index) => ({
          key: `tail-lifecycle-${now}-${index}`,
          partition: 0,
          timestamp: String(now + index),
          value,
        })),
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
