import { execFileSync } from "node:child_process";
import {
  findCoordinatorBatchProtocol,
  type KafkaApiVersions,
  offsetFetchBatchProtocol,
  sendKafkaProtocol,
  supportsGroupOffsetBatch,
} from "../../src/main/kafka/group-offset-batch-protocol";
import { connectDirect } from "../../src/main/kafka/kafka-connection";
import { createKafkaJsReadCluster } from "../../src/main/kafka/message-fetch";

import type { Admin } from "kafkajs";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";
const TOPIC = "freelens-orders";
const GROUP_IDS = ["freelens-orders-consumer", "freelens-orders-consumer-batch"];
const TEMPORARY_GROUP_ID = GROUP_IDS[1];

interface DockerContainerInspect {
  Config?: {
    Image?: string;
    Labels?: Record<string, string>;
  };
  HostConfig?: {
    PortBindings?: Record<string, Array<{ HostIp?: string; HostPort?: string }> | null>;
  };
  State?: { Running?: boolean };
}

function requireDisposableBroker(broker: string): void {
  if (process.env.ALLOW_LOCAL_MUTATING_KAFKA_FIXTURE !== "1" || broker !== "127.0.0.1:19092") {
    throw new Error("Refusing mutating protocol fixture outside a loopback Kafka broker");
  }
  const inspected = JSON.parse(
    execFileSync("docker", ["inspect", "freelens-kafka-direct"], { encoding: "utf8" }),
  ) as DockerContainerInspect[];
  const container = inspected[0];
  const labels = container?.Config?.Labels ?? {};
  const bindings = container?.HostConfig?.PortBindings?.["19092/tcp"] ?? [];
  const expectedBinding = bindings.some(({ HostIp, HostPort }) => HostIp === "127.0.0.1" && HostPort === "19092");
  if (
    !container?.State?.Running ||
    container.Config?.Image !== "apache/kafka:3.9.0" ||
    labels["com.docker.compose.project"] !== "freelens-kafka-e2e-direct" ||
    labels["com.docker.compose.service"] !== "kafka-direct" ||
    !expectedBinding
  ) {
    throw new Error("Disposable Kafka fixture identity is unavailable");
  }
}

const timeout = setTimeout(() => {
  process.stderr.write("multi-group offset protocol integration timed out\n");
  process.exit(2);
}, 60_000);
timeout.unref();

function normalizedOffsets(
  topics: Array<{ topic: string; partitions: Array<{ partition: number; offset: string }> }>,
): Array<{ topic: string; partitions: Array<{ partition: number; offset: string }> }> {
  return topics
    .map(({ topic, partitions }) => ({
      topic,
      partitions: partitions
        .filter(({ offset }) => offset !== "-1")
        .map(({ partition, offset }) => ({ partition, offset }))
        .sort((left, right) => left.partition - right.partition),
    }))
    .filter(({ partitions }) => partitions.length > 0)
    .sort((left, right) => left.topic.localeCompare(right.topic));
}

async function publicOffsets(admin: Admin) {
  await admin.connect();
  try {
    const results = await Promise.all(
      GROUP_IDS.map(async (groupId) => ({
        groupId,
        topics: normalizedOffsets(await admin.fetchOffsets({ groupId, resolveOffsets: false })),
      })),
    );
    return results.sort((left, right) => left.groupId.localeCompare(right.groupId));
  } finally {
    await admin.disconnect();
  }
}

async function main(): Promise<void> {
  requireDisposableBroker(BROKER);
  const connection = await connectDirect({ bootstrap: BROKER });
  const cluster = createKafkaJsReadCluster(connection.client);
  let temporaryGroupCreated = false;
  try {
    const setupAdmin = connection.admin();
    await setupAdmin.connect();
    try {
      await setupAdmin.setOffsets({
        groupId: TEMPORARY_GROUP_ID,
        topic: TOPIC,
        partitions: [{ partition: 0, offset: "1" }],
      });
      temporaryGroupCreated = true;
    } finally {
      await setupAdmin.disconnect();
    }
    await cluster.connect();
    await cluster.refreshMetadata();
    const bootstrapNodeId = cluster.getNodeIds()[0];
    if (!bootstrapNodeId) throw new Error("Kafka metadata returned no broker");
    const bootstrapBroker = await cluster.findBroker({ nodeId: bootstrapNodeId });
    const versions = (bootstrapBroker as unknown as { versions?: KafkaApiVersions }).versions;
    if (!supportsGroupOffsetBatch(versions)) throw new Error("Kafka fixture does not support multi-group APIs");

    const coordinatorResponse = await sendKafkaProtocol(bootstrapBroker, findCoordinatorBatchProtocol(GROUP_IDS));
    const coordinators = GROUP_IDS.map((groupId) =>
      coordinatorResponse.coordinators.find(({ key }) => key === groupId),
    );
    if (coordinators.some((coordinator) => !coordinator || coordinator.errorCode !== 0)) {
      throw new Error("FindCoordinator v4 did not resolve every test group");
    }
    const coordinator = coordinators[0]!;
    if (coordinators.some((candidate) => candidate!.nodeId !== coordinator.nodeId)) {
      throw new Error("single-broker fixture assigned different group coordinators");
    }

    const coordinatorBroker = await cluster.findBroker({ nodeId: String(coordinator.nodeId) });
    const batchResponse = await sendKafkaProtocol(coordinatorBroker, offsetFetchBatchProtocol(GROUP_IDS));
    const batchGroups = GROUP_IDS.map((groupId) =>
      batchResponse.groups.find((candidate) => candidate.groupId === groupId),
    );
    if (batchGroups.some((group) => !group || group.errorCode !== 0)) {
      throw new Error("OffsetFetch v8 did not return every test group");
    }
    if (
      batchGroups.some((group) =>
        group!.topics.some(({ partitions }) => partitions.some(({ errorCode }) => errorCode !== 0)),
      )
    ) {
      throw new Error("OffsetFetch v8 returned a partition error");
    }

    const batched = batchGroups
      .map((group) => ({
        groupId: group!.groupId,
        topics: normalizedOffsets(
          group!.topics.map(({ topic, partitions }) => ({
            topic,
            partitions: partitions.map(({ partition, committedOffset }) => ({ partition, offset: committedOffset })),
          })),
        ),
      }))
      .sort((left, right) => left.groupId.localeCompare(right.groupId));
    const fallback = await publicOffsets(connection.admin());
    if (JSON.stringify(batched) !== JSON.stringify(fallback)) {
      throw new Error("OffsetFetch v8 result differs from the public KafkaJS fallback");
    }

    process.stdout.write(
      `GROUP_OFFSET_BATCH_OK groups=${GROUP_IDS.length} coordinators=1 topics=${batched.reduce((sum, group) => sum + group.topics.length, 0)} fallbackEqual=true\n`,
    );
  } finally {
    await cluster.disconnect().catch(() => undefined);
    if (temporaryGroupCreated) {
      const cleanupAdmin = connection.admin();
      await cleanupAdmin.connect();
      try {
        await cleanupAdmin.deleteGroups([TEMPORARY_GROUP_ID]);
      } finally {
        await cleanupAdmin.disconnect();
      }
    }
    await connection.disconnect();
  }
}

main()
  .then(() => process.exit(0))
  .catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(1);
  });
