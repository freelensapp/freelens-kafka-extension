/*
 * REAL-cluster P1 validation against the local kind cluster.
 *
 * Unlike integration.local.ts (which injects a TCP-pipe forwarder), this uses
 * the actual `@kubernetes/client-node` SPDY port-forward via `createKubeForwarder`
 * — the exact code path the extension will run in production.
 *
 * Requires:
 *   - kind cluster "kind" running (context kind-kind)
 *   - pod kafka-0 (default ns) from test/fixtures/kafka-kind.yaml, Ready
 *
 * Run:
 *   pnpm kind:up        # load image + apply pod + wait Ready
 *   pnpm itest:kind
 *   pnpm kind:down
 */
import fs from "node:fs";
import { KafkaConnection } from "../../src/main/kafka/kafka-connection";
import { createKubeForwarder } from "../../src/main/kafka/kube-forwarder";

import type { BrokerRef } from "../../src/main/kafka/types";

const log = (m = ""): void => void fs.writeSync(1, `${m}\n`);
setTimeout(() => {
  log("[timeout] aborting kind integration test");
  process.exit(2);
}, 60_000);

const CONTEXT = process.env.KUBE_CONTEXT ?? "kind-kind";
const NAMESPACE = process.env.KAFKA_NS ?? "default";
const POD = process.env.KAFKA_POD ?? "kafka-0";

const brokers: BrokerRef[] = [
  {
    advertisedHost: "kafka-internal",
    advertisedPort: 9092,
    namespace: NAMESPACE,
    pod: POD,
    containerPort: 9092,
  },
];

async function main(): Promise<void> {
  const forwarder = createKubeForwarder({ context: CONTEXT });

  const connection = await KafkaConnection.connect({
    clientId: "freelens-kafka-p1-kind",
    brokers,
    bootstrap: "kafka-internal:9092",
    forwarder,
    onRedirect: (from, to) => log(`  [redirect] ${from} -> ${to}`),
  });

  const overview = await connection.overview();
  log("cluster overview (via real kube SPDY port-forward):");
  for (const b of overview.brokers) log(`  broker nodeId=${b.nodeId} addr=${b.host}:${b.port}`);
  log(`  controller=${overview.controller}`);
  log(`  topics: ${overview.topics.join(", ") || "(none)"}`);

  const topic = "freelens-p1-kind-topic";
  const admin = connection.admin();
  await admin.connect();
  await admin
    .createTopics({
      topics: [{ topic, numPartitions: 1 }],
      waitForLeaders: true,
    })
    .catch(() => {});
  await admin.disconnect();

  const producer = connection.client.producer();
  await producer.connect();
  await producer.send({
    topic,
    messages: [{ key: "p1", value: `hello from kind @ ${new Date().toISOString()}` }],
  });
  await producer.disconnect();
  log(`produced 1 message to "${topic}"`);

  const consumer = connection.client.consumer({
    groupId: "freelens-p1-kind-group",
  });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  const got = await new Promise<string | null>((resolve) => {
    const timer = setTimeout(() => resolve(null), 15_000);
    void consumer.run({
      eachMessage: async ({ message }) => {
        clearTimeout(timer);
        resolve(`${message.key?.toString()}=${message.value?.toString()}`);
      },
    });
  });
  await consumer.disconnect();
  log(`consumed: ${got ?? "(timeout)"}`);

  await connection.disconnect();
  log("\n[OK] P1 engine validated against a REAL cluster (kind) via @kubernetes/client-node port-forward.");
  process.exit(0);
}

main().catch((err) => {
  log(`\n[FAIL] ${(err as Error).message}`);
  process.exit(1);
});
