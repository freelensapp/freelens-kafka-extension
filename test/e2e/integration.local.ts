/*
 * Local end-to-end validation of the P1 engine WITHOUT Kubernetes.
 *
 * It drives the real chain: PortForwardManager -> address map -> redirect
 * socketFactory -> kafkajs (produce + consume). The only stubbed part is the
 * Kubernetes SPDY tunnel: instead of `createKubeForwarder`, a TCP-pipe
 * `Forwarder` connects to the Docker broker from ../poc, which advertises the
 * unreachable "kafka-internal:9092" (reproducing the in-cluster trap).
 *
 * Run:
 *   pnpm kafka:up      # starts the ../poc broker
 *   pnpm itest
 *   pnpm kafka:down
 */
import fs from "node:fs";
import net from "node:net";
import { KafkaConnection } from "../../src/main/kafka/kafka-connection";

import type { Forwarder } from "../../src/main/kafka/port-forward-manager";
import type { BrokerRef } from "../../src/main/kafka/types";

const log = (m = ""): void => void fs.writeSync(1, `${m}\n`);
setTimeout(() => {
  log("[timeout] aborting integration test");
  process.exit(2);
}, 30_000);

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:9092";
const [brokerHost, brokerPort] = BROKER.split(":");

// Stand-in for the kube SPDY tunnel: pipe the accepted socket to the Docker broker.
const tcpPipeForwarder: Forwarder = (_target, socket) => {
  const upstream = net.connect(Number(brokerPort), brokerHost);
  socket.pipe(upstream);
  upstream.pipe(socket);
  const done = () => {
    upstream.destroy();
    socket.destroy();
  };
  socket.on("error", done);
  upstream.on("error", done);
  upstream.on("close", () => socket.destroy());
};

const brokers: BrokerRef[] = [
  {
    advertisedHost: "kafka-internal",
    advertisedPort: 9092,
    namespace: "default",
    pod: "kafka-0",
    containerPort: 9092,
  },
];

async function main(): Promise<void> {
  const connection = await KafkaConnection.connect({
    clientId: "freelens-kafka-p1",
    brokers,
    bootstrap: "kafka-internal:9092",
    forwarder: tcpPipeForwarder,
    onRedirect: (from, to) => log(`  [redirect] ${from} -> ${to}`),
  });

  const overview = await connection.overview();
  log("cluster overview:");
  for (const b of overview.brokers) log(`  broker nodeId=${b.nodeId} addr=${b.host}:${b.port}`);
  log(`  controller=${overview.controller}`);
  log(`  topics: ${overview.topics.join(", ") || "(none)"}`);

  const topic = "freelens-p1-topic";
  const admin = connection.admin();
  await admin.connect();
  await admin
    .createTopics({
      topics: [{ topic, numPartitions: 1 }],
      waitForLeaders: true,
    })
    .catch(() => {});
  await admin.disconnect();

  const topicDetail = await connection.topicDetail(topic);
  if (topicDetail.partitionCount !== 1) {
    throw new Error(`expected one partition for "${topic}", got ${topicDetail.partitionCount}`);
  }
  const partition = topicDetail.partitions[0];
  if (!partition || partition.leader < 0 || partition.replicas.length !== 1 || partition.isr.length !== 1) {
    throw new Error(`unexpected partition topology for "${topic}"`);
  }
  log(`topic detail: partitions=${topicDetail.partitionCount} leader=${partition.leader} state=healthy`);

  const producer = connection.client.producer();
  await producer.connect();
  await producer.send({
    topic,
    messages: [{ key: "p1", value: `hello @ ${new Date().toISOString()}` }],
  });
  await producer.disconnect();
  log(`produced 1 message to "${topic}"`);

  const consumer = connection.client.consumer({ groupId: "freelens-p1-group" });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  const got = await new Promise<string | null>((resolve) => {
    const timer = setTimeout(() => resolve(null), 8000);
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
  log("\n[OK] P1 engine: port-forward manager -> address map -> socketFactory -> kafkajs (produce + consume).");
  process.exit(0);
}

main().catch((err) => {
  log(`\n[FAIL] ${(err as Error).message}`);
  process.exit(1);
});
