/** Read-only protocol evidence for per-broker configuration against disposable local Kafka. */
import { connectDirect } from "../../src/main/kafka/kafka-connection";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";

const timeout = setTimeout(() => {
  process.stderr.write("broker config integration timed out\n");
  process.exit(2);
}, 60_000);
timeout.unref();

async function main(): Promise<void> {
  const connection = await connectDirect({ bootstrap: BROKER });
  try {
    const overview = await connection.overview();
    const broker = overview.brokers[0];
    if (!broker) throw new Error("local Kafka returned no brokers");

    const config = await connection.brokerConfig(broker.nodeId);
    if (config.brokerId !== broker.nodeId) {
      throw new Error(`expected brokerId=${broker.nodeId}, got ${config.brokerId}`);
    }
    if (config.entries.length === 0) throw new Error("local Kafka returned no broker config entries");
    if (config.entries.some((entry) => entry.sensitive && entry.value !== "****")) {
      throw new Error("sensitive broker config value was not masked");
    }

    process.stdout.write(`BROKER_CONFIG_OK brokerId=${broker.nodeId} entries=${config.entries.length}\n`);
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
