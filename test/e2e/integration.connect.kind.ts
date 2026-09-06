/*
 * REAL-cluster P2.5 validation on kind: the two-phase `connectDiscovered`.
 *
 * The broker advertises the pod-based DNS `kafka-0.kafka-brokers.default.svc:9092`
 * (unreachable from the laptop). connectDiscovered learns that address from
 * metadata (phase 1), matches it to pod `kafka-0` by host prefix, forwards it
 * (phase 2), and produces/consumes — all via the real client-node port-forward.
 *
 * Run:
 *   pnpm kind:p25:up
 *   pnpm itest:p25:kind
 *   pnpm kind:p25:down
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import { connectDiscovered } from "../../src/main/kafka/connect-discovered";
import { createKubeForwarder } from "../../src/main/kafka/kube-forwarder";

import type { DiscoveredKafka } from "../../src/main/kafka/discovery";

const log = (m = ""): void => void fs.writeSync(1, `${m}\n`);
setTimeout(() => {
  log("[timeout] aborting P2.5 integration test");
  process.exit(2);
}, 60_000);

const CONTEXT = process.env.KUBE_CONTEXT ?? "kind-kind";

// What P2 discovery would have produced for this broker.
const discovered: DiscoveredKafka = {
  source: "strimzi",
  name: "kafka",
  namespace: "default",
  bootstrap: "kafka-0.kafka-brokers.default.svc:9092",
  tls: false,
  port: 9092,
  listeners: [{ name: "plain", port: 9092, tls: false, type: "internal" }],
  brokerPods: [{ brokerId: 0, pod: "kafka-0" }],
};

async function main(): Promise<void> {
  const forwarder = createKubeForwarder({ context: CONTEXT });
  const connection = await connectDiscovered({
    clientId: "freelens-kafka-p25",
    discovered,
    forwarder,
    onRedirect: (from, to) => log(`  [redirect] ${from} -> ${to}`),
    onPhase: (phase, info) => log(`  [phase:${phase}] ${info}`),
  });

  const overview = await connection.overview();
  log(`brokers: ${overview.brokers.map((b) => `${b.nodeId}@${b.host}:${b.port}`).join(", ")}`);
  assert.ok(
    overview.brokers.some((b) => b.host.startsWith("kafka-0.")),
    "advertised host should be the pod-based DNS learned from metadata",
  );

  const topic = "freelens-p25-topic";
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
    messages: [{ key: "p25", value: `hi @ ${new Date().toISOString()}` }],
  });
  await producer.disconnect();
  log(`produced 1 message to "${topic}"`);

  const consumer = connection.client.consumer({
    groupId: "freelens-p25-group",
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
  assert.ok(got, "expected to consume the produced message");

  await connection.disconnect();
  log("\n[OK] P2.5 two-phase connectDiscovered validated on kind (metadata-driven per-broker forwarding).");
  process.exit(0);
}

main().catch((err) => {
  log(`\n[FAIL] ${(err as Error).message}`);
  process.exit(1);
});
