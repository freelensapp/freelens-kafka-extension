import { execFileSync } from "node:child_process";
import { fetchClusterHealth } from "../../src/main/kafka/cluster-health";
import { ConsumerGroupOffsetIndex } from "../../src/main/kafka/group-fetch";
import { connectDirect } from "../../src/main/kafka/kafka-connection";

import type { Admin, Kafka } from "kafkajs";

import type { ClusterOverviewHealthDto } from "../../src/common/ipc";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";
const FIXED_NOW = 1_000;
const WRITE_METHODS = new Set([
  "alterConfigs",
  "createAcls",
  "createPartitions",
  "createTopics",
  "deleteAcls",
  "deleteGroups",
  "deleteTopicRecords",
  "deleteTopics",
  "resetOffsets",
  "setOffsets",
]);

interface DockerContainerInspect {
  Config?: { Image?: string; Labels?: Record<string, string> };
  HostConfig?: { PortBindings?: Record<string, Array<{ HostIp?: string; HostPort?: string }> | null> };
  State?: { Running?: boolean };
}

interface RequestCounts {
  describeCluster: number;
  fetchOffsets: number;
  fetchTopicMetadata: number;
  fetchTopicOffsets: number;
  forcedWatermarkFallbacks: number;
  listGroups: number;
  writeAttempts: number;
}

function requireDisposableBroker(): void {
  if (BROKER !== "127.0.0.1:19092") throw new Error("Refusing fallback integration outside loopback Kafka");
  const inspected = JSON.parse(
    execFileSync("docker", ["inspect", "freelens-kafka-direct"], { encoding: "utf8" }),
  ) as DockerContainerInspect[];
  const container = inspected[0];
  const labels = container?.Config?.Labels ?? {};
  const bindings = container?.HostConfig?.PortBindings?.["19092/tcp"] ?? [];
  const loopback = bindings.some(({ HostIp, HostPort }) => HostIp === "127.0.0.1" && HostPort === "19092");
  if (
    !container?.State?.Running ||
    container.Config?.Image !== "apache/kafka:3.9.0" ||
    labels["com.docker.compose.project"] !== "freelens-kafka-e2e-direct" ||
    labels["com.docker.compose.service"] !== "kafka-direct" ||
    !loopback
  ) {
    throw new Error("Disposable Kafka fallback fixture identity is unavailable");
  }
}

function guardedAdmin(admin: Admin, counts: RequestCounts): Admin {
  return new Proxy(admin, {
    get(target, property, receiver) {
      const value = Reflect.get(target, property, receiver);
      if (typeof property !== "string" || typeof value !== "function") return value;
      return (...args: unknown[]) => {
        if (WRITE_METHODS.has(property)) {
          counts.writeAttempts++;
          throw new Error(`Health attempted forbidden Admin write method ${property}`);
        }
        if (property in counts) counts[property as keyof RequestCounts]++;
        return value.apply(target, args);
      };
    },
  }) as Admin;
}

function blockWriteCapableClients(kafka: Kafka, counts: RequestCounts): () => void {
  const originalProducer = kafka.producer.bind(kafka);
  const originalConsumer = kafka.consumer.bind(kafka);
  kafka.producer = () => {
    counts.writeAttempts++;
    throw new Error("Health attempted to create a Kafka producer");
  };
  kafka.consumer = () => {
    counts.writeAttempts++;
    throw new Error("Health attempted to create a Kafka consumer group member");
  };
  return () => {
    kafka.producer = originalProducer;
    kafka.consumer = originalConsumer;
  };
}

function normalizeHealth(health: ClusterOverviewHealthDto): ClusterOverviewHealthDto {
  return {
    ...health,
    topologyMeasuredAt: FIXED_NOW,
    ...(health.consumerGroupLagCoverage
      ? {
          consumerGroupLagCoverage: {
            ...health.consumerGroupLagCoverage,
            startedAt: FIXED_NOW,
            completedAt: FIXED_NOW,
          },
        }
      : {}),
  };
}

async function runHealth(mode: "supported" | "fallback"): Promise<{
  counts: RequestCounts;
  health: ClusterOverviewHealthDto;
}> {
  const counts: RequestCounts = {
    describeCluster: 0,
    fetchOffsets: 0,
    fetchTopicMetadata: 0,
    fetchTopicOffsets: 0,
    forcedWatermarkFallbacks: 0,
    listGroups: 0,
    writeAttempts: 0,
  };
  const connection = await connectDirect({ bootstrap: BROKER });
  const kafka = connection.client;
  const originalAdmin = kafka.admin.bind(kafka);
  const restoreClients = blockWriteCapableClients(kafka, counts);
  kafka.admin = () => guardedAdmin(originalAdmin(), counts);
  try {
    if (mode === "supported") return { counts, health: normalizeHealth(await connection.health()) };

    const admin = kafka.admin();
    await admin.connect();
    try {
      const forcedUnsupported = new ConsumerGroupOffsetIndex(
        async (groupIds) => ({
          offsetsByTopic: new Map(),
          resolvedGroupIds: new Set(),
          supported: false,
          unresolvedGroupIds: new Set(groupIds),
        }),
        () => FIXED_NOW,
      );
      const health = await fetchClusterHealth(
        admin,
        forcedUnsupported,
        undefined,
        async () => {
          counts.forcedWatermarkFallbacks++;
          throw new Error("Forced compatibility watermark fallback");
        },
        undefined,
        () => FIXED_NOW,
      );
      return { counts, health: normalizeHealth(health) };
    } finally {
      await admin.disconnect();
    }
  } finally {
    kafka.admin = originalAdmin;
    restoreClients();
    await connection.disconnect();
  }
}

async function main(): Promise<void> {
  requireDisposableBroker();
  const supported = await runHealth("supported");
  const fallback = await runHealth("fallback");
  if (JSON.stringify(supported.health) !== JSON.stringify(fallback.health)) {
    throw new Error("Forced compatibility fallback changed the complete aggregate Health DTO");
  }
  const coverage = fallback.health.consumerGroupLagCoverage;
  if (!coverage?.complete || coverage.totalGroups < 1 || coverage.unavailableGroups !== 0) {
    throw new Error("Forced compatibility fallback did not preserve exact group coverage");
  }
  if (supported.counts.fetchOffsets !== 0 || supported.counts.fetchTopicOffsets !== 0) {
    throw new Error("Supported Health unexpectedly used a public KafkaJS fallback");
  }
  if (
    fallback.counts.describeCluster !== 1 ||
    fallback.counts.fetchTopicMetadata !== 1 ||
    fallback.counts.listGroups !== 1 ||
    fallback.counts.fetchOffsets < 1 ||
    fallback.counts.fetchOffsets !== coverage.totalGroups ||
    fallback.counts.fetchTopicOffsets < 1 ||
    fallback.counts.fetchTopicOffsets > coverage.totalGroups ||
    fallback.counts.forcedWatermarkFallbacks !== 1
  ) {
    throw new Error("Forced compatibility fallback exceeded its bounded single-pass request budget");
  }
  if (supported.counts.writeAttempts !== 0 || fallback.counts.writeAttempts !== 0) {
    throw new Error("Aggregate Health attempted a forbidden Kafka write");
  }
  process.stdout.write(
    `HEALTH_FALLBACK_OK exact=true equal=true groups=${coverage.totalGroups} groupFallbacks=${fallback.counts.fetchOffsets} topicFallbacks=${fallback.counts.fetchTopicOffsets} writes=0\n`,
  );
}

const timeout = setTimeout(() => {
  process.stderr.write("aggregate Health fallback integration timed out\n");
  process.exit(2);
}, 120_000);
timeout.unref();

main()
  .then(() => process.exit(0))
  .catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(1);
  });
