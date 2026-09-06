/**
 * Opt-in READ-ONLY timing probe for one explicitly authorized Kafka target.
 * It reads Kubernetes workload configuration and Kafka Admin metadata only.
 * No endpoint, topic, group, workload, credential or Secret value is printed.
 *
 * Usage:
 *   ALLOW_REAL_READ_ONLY=1 \
 *   KUBE_CONTEXT=my-context \
 *   AUTHORIZED_KUBE_CONTEXT=my-context \
 *   AUTHORIZED_KUBE_CONTEXT_SHA256=<sha256-of-context> \
 *   KAFKA_TARGET_HOST=broker.example.test \
 *   AUTHORIZED_KAFKA_TARGET_HOST=broker.example.test \
 *   AUTHORIZED_KAFKA_TARGET_HOST_SHA256=<sha256-of-normalized-host> \
 *   pnpm tsx test/e2e/performance-timing.real.ts
 */

import { randomUUID } from "node:crypto";
import { assertSanitizedPerformanceEvidence } from "../../src/common/performance-evidence";
import { firstBrokerAddress, splitBootstrap } from "../../src/common/reachability";
import { authorizeReadOnlyTarget, selectSingleAuthorizedTarget } from "../../src/common/read-only-target-authorization";
import { probeAclSupport } from "../../src/main/kafka/acl";
import { aggregateHealthWorkerKey, KafkaAggregateHealthManager } from "../../src/main/kafka/aggregate-health-manager";
import { type AnyDiscoveredKafka, discoverAllKafkas } from "../../src/main/kafka/discovery";
import { resolveExternalCredentials } from "../../src/main/kafka/external-credentials";
import { fetchConsumerGroupDetail, fetchTopicConsumers } from "../../src/main/kafka/group-fetch";
import { type KafkaApiVersions, supportsGroupOffsetBatch } from "../../src/main/kafka/group-offset-batch-protocol";
import { connectDirect, type KafkaConnection } from "../../src/main/kafka/kafka-connection";
import { createKubeReader, type KubeReader } from "../../src/main/kafka/kube-reader";
import { probeBootstrapReachable } from "../../src/main/kafka/reachability";

import type { Admin } from "kafkajs";

const STAGE_TIMEOUT_MS = 180_000;
const KAFKA_JS_CREATE_CLUSTER_SYMBOL = "private:Kafka:createCluster";

class StageTimeoutError extends Error {}

function withTimeout<T>(promise: Promise<T>, label: string): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  const bounded = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => reject(new StageTimeoutError(`${label} timed out`)), STAGE_TIMEOUT_MS);
    timeout.unref();
  });
  return Promise.race([promise, bounded]).finally(() => {
    if (timeout) clearTimeout(timeout);
  });
}

async function timed<T>(action: () => Promise<T>): Promise<{ durationMs: number; value: T }> {
  const startedAt = performance.now();
  const value = await action();
  return { durationMs: Math.round(performance.now() - startedAt), value };
}

function countingReader(reader: KubeReader): {
  reader: KubeReader;
  calls: Record<"customResources" | "pods" | "services" | "workloads" | "configMaps" | "secrets", number>;
} {
  const calls = { customResources: 0, pods: 0, services: 0, workloads: 0, configMaps: 0, secrets: 0 };
  return {
    calls,
    reader: {
      listCustomResources: (...args) => {
        calls.customResources++;
        return reader.listCustomResources(...args);
      },
      listPods: (...args) => {
        calls.pods++;
        return reader.listPods(...args);
      },
      listServices: (...args) => {
        calls.services++;
        return reader.listServices(...args);
      },
      listWorkloads: (...args) => {
        calls.workloads++;
        return reader.listWorkloads(...args);
      },
      getConfigMap: (...args) => {
        calls.configMaps++;
        return reader.getConfigMap(...args);
      },
      getSecret: (...args) => {
        calls.secrets++;
        return reader.getSecret(...args);
      },
    },
  };
}

function matchesHost(candidate: AnyDiscoveredKafka, targetHost: string): boolean {
  return splitBootstrap(candidate.bootstrap).some(
    (entry) => firstBrokerAddress(entry).host.toLowerCase() === targetHost,
  );
}

interface FreshAdminTiming<T> {
  totalMs: number;
  clientCreateMs: number;
  adminConnectMs: number;
  queryMs: number;
  adminDisconnectMs: number;
  protocolRequests: {
    findCoordinator: number;
    offsetFetch: number;
    listOffsets: number;
  };
  value: T;
}

interface ListOffsetsRequest {
  topics: Array<{ topic: string; partitions: Array<{ timestamp: number }> }>;
}

interface ListOffsetsBroker {
  listOffsets(request: ListOffsetsRequest): Promise<unknown>;
  versions?: KafkaApiVersions;
  [symbol: symbol]: unknown;
}

interface InstrumentedCluster {
  findBroker(request: { nodeId: string }): Promise<ListOffsetsBroker>;
}

interface AggregateHealthTiming {
  totalMs: number;
  phasesMs: {
    topology: number;
    groups: number;
    watermarks: number;
  };
  protocolRequests: {
    fetchTopicOffsetsFallback: number;
    fetchOffsetsFallback: number;
    findCoordinator: number;
    listOffsets: number;
    lowOffsets: number;
    offsetFetch: number;
    offsetFetchGroups: number;
  };
  batch: {
    brokers: number;
    partitions: number;
    topics: number;
  };
  groupBatch: {
    findCoordinatorRequests: number;
    offsetFetchGroups: number;
    offsetFetchRequests: number;
    publicFallbacks: number;
  };
  capabilities: {
    batchSupported: boolean;
    findCoordinator: { maxVersion: number; minVersion: number } | null;
    offsetFetch: { maxVersion: number; minVersion: number } | null;
  };
  coverage: {
    exact: boolean;
    unavailableGroups: number;
    unavailableTopics: number;
  };
  warmCacheMs: number;
  warmLoaderCalls: number;
  warmRequestCount: number;
}

function privateSymbol(value: object, description: string): symbol | undefined {
  let candidate: object | null = value;
  while (candidate) {
    const symbol = Object.getOwnPropertySymbols(candidate).find((item) => item.description === description);
    if (symbol) return symbol;
    candidate = Object.getPrototypeOf(candidate) as object | null;
  }
  return undefined;
}

async function measureAggregateHealth(connection: KafkaConnection): Promise<AggregateHealthTiming> {
  const kafka = connection.client;
  const originalAdmin = kafka.admin.bind(kafka);
  const privateKafka = kafka as unknown as Record<symbol, unknown>;
  const createClusterSymbol = Object.getOwnPropertySymbols(kafka).find(
    (symbol) => symbol.description === KAFKA_JS_CREATE_CLUSTER_SYMBOL,
  );
  if (!createClusterSymbol) throw new Error("KafkaJS read-only cluster adapter is unavailable");
  const originalCreateCluster = privateKafka[createClusterSymbol] as (options: unknown) => InstrumentedCluster;
  const brokerIds = new Set<string>();
  const topics = new Set<string>();
  let fetchOffsetsCalls = 0;
  let fetchTopicOffsetsFallback = 0;
  let findCoordinatorBatchCalls = 0;
  let listOffsetsCalls = 0;
  let lowOffsets = 0;
  let offsetFetchBatchCalls = 0;
  let offsetFetchBatchGroups = 0;
  let partitions = 0;
  let negotiatedVersions: KafkaApiVersions | undefined;
  let loaderCalls = 0;
  const manager = new KafkaAggregateHealthManager();
  const managerKey = aggregateHealthWorkerKey("authorized-read-only", "authorized-target", "resolved-security");

  privateKafka[createClusterSymbol] = (options: unknown) => {
    const cluster = originalCreateCluster(options);
    const originalFindBroker = cluster.findBroker.bind(cluster);
    const instrumentedBrokers = new WeakSet<object>();
    cluster.findBroker = async (request) => {
      const broker = await originalFindBroker(request);
      negotiatedVersions ??= broker.versions;
      if (!instrumentedBrokers.has(broker)) {
        instrumentedBrokers.add(broker);
        const sendRequestSymbol = privateSymbol(broker, "private:Broker:sendRequest");
        if (!sendRequestSymbol) throw new Error("KafkaJS broker request adapter is unavailable");
        const originalSendRequest = broker[sendRequestSymbol] as (protocol: {
          groupIds?: string[];
          request?: { apiName?: string };
        }) => Promise<unknown>;
        broker[sendRequestSymbol] = async (protocol) => {
          if (protocol.request?.apiName === "FindCoordinator") findCoordinatorBatchCalls++;
          if (protocol.request?.apiName === "OffsetFetch") {
            offsetFetchBatchCalls++;
            offsetFetchBatchGroups += protocol.groupIds?.length ?? 0;
          }
          return originalSendRequest.call(broker, protocol);
        };
        const originalListOffsets = broker.listOffsets.bind(broker);
        broker.listOffsets = async (listOffsetsRequest) => {
          brokerIds.add(request.nodeId);
          listOffsetsCalls++;
          for (const topic of listOffsetsRequest.topics) {
            topics.add(topic.topic);
            partitions += topic.partitions.length;
            lowOffsets += topic.partitions.filter(({ timestamp }) => timestamp !== -1).length;
          }
          return originalListOffsets(listOffsetsRequest);
        };
      }
      return broker;
    };
    return cluster;
  };

  kafka.admin = () => {
    const admin = originalAdmin();
    return new Proxy(admin, {
      get(target, property, receiver) {
        const value = Reflect.get(target, property, receiver);
        if (typeof value !== "function") return value;
        return (...args: unknown[]) => {
          if (property === "fetchOffsets") fetchOffsetsCalls++;
          if (property === "fetchTopicOffsets") fetchTopicOffsetsFallback++;
          return value.apply(target, args);
        };
      },
    }) as Admin;
  };

  const startedAt = performance.now();
  let topologyCompletedAt: number | undefined;
  let groupsCompletedAt: number | undefined;
  let watermarksCompletedAt: number | undefined;
  try {
    const loader = async (
      publish: Parameters<Parameters<KafkaAggregateHealthManager["load"]>[1]>[0],
      signal: AbortSignal,
    ) => {
      loaderCalls++;
      return connection.health((progress) => {
        publish(progress);
        const elapsed = performance.now() - startedAt;
        if (progress.phase === "topology") topologyCompletedAt = elapsed;
        if (progress.phase === "groups" && progress.total !== undefined && progress.completed === progress.total) {
          groupsCompletedAt = elapsed;
        }
        if (progress.phase === "watermarks" && progress.total !== undefined && progress.completed === progress.total) {
          watermarksCompletedAt = elapsed;
        }
      }, signal);
    };
    const health = await withTimeout(manager.load(managerKey, loader), "Aggregate health");
    const totalMs = Math.round(performance.now() - startedAt);
    if (topologyCompletedAt === undefined || groupsCompletedAt === undefined || watermarksCompletedAt === undefined) {
      throw new Error("Aggregate health did not report every terminal phase");
    }
    const requestCount = () =>
      fetchOffsetsCalls +
      fetchTopicOffsetsFallback +
      findCoordinatorBatchCalls +
      listOffsetsCalls +
      offsetFetchBatchCalls;
    const loaderCallsBeforeWarm = loaderCalls;
    const requestsBeforeWarm = requestCount();
    const warm = await timed(() => manager.load(managerKey, loader));
    const warmLoaderCalls = loaderCalls - loaderCallsBeforeWarm;
    const warmRequestCount = requestCount() - requestsBeforeWarm;
    if (warmLoaderCalls !== 0 || warmRequestCount !== 0 || JSON.stringify(warm.value) !== JSON.stringify(health)) {
      throw new Error("Aggregate health warm cache invoked read work or changed the measured result");
    }
    return {
      totalMs,
      phasesMs: {
        topology: Math.round(topologyCompletedAt),
        groups: Math.round(groupsCompletedAt - topologyCompletedAt),
        watermarks: Math.round(watermarksCompletedAt - groupsCompletedAt),
      },
      protocolRequests: {
        fetchTopicOffsetsFallback,
        fetchOffsetsFallback: fetchOffsetsCalls,
        findCoordinator: findCoordinatorBatchCalls + fetchOffsetsCalls,
        listOffsets: listOffsetsCalls,
        lowOffsets,
        offsetFetch: offsetFetchBatchCalls + fetchOffsetsCalls,
        offsetFetchGroups: offsetFetchBatchGroups,
      },
      batch: {
        brokers: brokerIds.size,
        partitions,
        topics: topics.size,
      },
      groupBatch: {
        findCoordinatorRequests: findCoordinatorBatchCalls,
        offsetFetchGroups: offsetFetchBatchGroups,
        offsetFetchRequests: offsetFetchBatchCalls,
        publicFallbacks: fetchOffsetsCalls,
      },
      capabilities: {
        batchSupported: supportsGroupOffsetBatch(negotiatedVersions),
        findCoordinator: negotiatedVersions?.[10] ?? null,
        offsetFetch: negotiatedVersions?.[9] ?? null,
      },
      coverage: {
        exact: health.consumerGroupLagCoverage?.complete ?? false,
        unavailableGroups: health.consumerGroupLagUnavailableGroups ?? 0,
        unavailableTopics: health.consumerGroupLagUnavailableTopics ?? 0,
      },
      warmCacheMs: warm.durationMs,
      warmLoaderCalls,
      warmRequestCount,
    };
  } finally {
    manager.clear();
    kafka.admin = originalAdmin;
    privateKafka[createClusterSymbol] = originalCreateCluster;
  }
}

function instrumentAdmin(admin: Admin): {
  admin: Admin;
  protocolRequests: FreshAdminTiming<unknown>["protocolRequests"];
} {
  const protocolRequests = { findCoordinator: 0, offsetFetch: 0, listOffsets: 0 };
  const methodProtocolCounts: Record<string, Partial<typeof protocolRequests>> = {
    describeGroups: { findCoordinator: 1 },
    fetchOffsets: { findCoordinator: 1, offsetFetch: 1 },
    fetchTopicOffsets: { listOffsets: 2 },
  };
  const instrumented = new Proxy(admin, {
    get(target, property, receiver) {
      const value = Reflect.get(target, property, receiver);
      if (typeof property !== "string" || typeof value !== "function") return value;
      return (...args: unknown[]) => {
        const counts = methodProtocolCounts[property];
        if (counts) {
          const multiplier = property === "describeGroups" ? Number((args[0] as { length?: number })?.length ?? 0) : 1;
          for (const key of Object.keys(counts) as Array<keyof typeof protocolRequests>) {
            protocolRequests[key] += (counts[key] ?? 0) * multiplier;
          }
        }
        return value.apply(target, args);
      };
    },
  });
  return { admin: instrumented, protocolRequests };
}

async function withFreshAdmin<T>(
  target: AnyDiscoveredKafka,
  security: Awaited<ReturnType<typeof resolveExternalCredentials>>,
  query: (admin: Admin) => Promise<T>,
): Promise<FreshAdminTiming<T>> {
  const totalStartedAt = performance.now();
  const created = await timed(() =>
    connectDirect({
      bootstrap: target.bootstrap,
      ssl: security.ssl ?? (target.tls ? true : undefined),
      sasl: security.sasl,
    }),
  );
  const instrumented = instrumentAdmin(created.value.admin());
  const admin = instrumented.admin;
  let adminConnectMs = 0;
  let adminDisconnectMs = 0;
  let queryMs = 0;
  let value: T;
  try {
    adminConnectMs = (await timed(() => withTimeout(admin.connect(), "Kafka Admin connect"))).durationMs;
    const result = await timed(() => withTimeout(query(admin), "Kafka Admin query"));
    queryMs = result.durationMs;
    value = result.value;
  } finally {
    adminDisconnectMs = (await timed(() => admin.disconnect().catch(() => undefined))).durationMs;
    await created.value.disconnect();
  }
  return {
    totalMs: Math.round(performance.now() - totalStartedAt),
    clientCreateMs: created.durationMs,
    adminConnectMs,
    queryMs,
    adminDisconnectMs,
    protocolRequests: instrumented.protocolRequests,
    value,
  };
}

async function main(): Promise<void> {
  const { context, targetHost } = authorizeReadOnlyTarget(process.env);
  const workloadReader = countingReader(createKubeReader({ context }));
  const discoveryReader = countingReader(createKubeReader({ context }));

  const workloads = await timed(() => withTimeout(workloadReader.reader.listWorkloads(), "Kubernetes workload list"));
  const discovery = await timed(() => withTimeout(discoverAllKafkas(discoveryReader.reader), "Kafka discovery"));
  const target = selectSingleAuthorizedTarget(discovery.value, targetHost, matchesHost);

  const reachability = await timed(() => withTimeout(probeBootstrapReachable(target.bootstrap, 5_000), "TCP probe"));
  if (!reachability.value) throw new Error("Authorized Kafka target is not reachable from this machine");

  const credentialReader = countingReader(createKubeReader({ context }));
  const credentials = await timed(() =>
    target.source === "workload"
      ? withTimeout(
          resolveExternalCredentials(credentialReader.reader, { bootstrap: target.bootstrap }),
          "Credential resolution",
        )
      : Promise.resolve({ detected: false, hint: { tls: target.tls, auth: "none" as const } }),
  );

  const overview = await withFreshAdmin(target, credentials.value, async (admin) => {
    const cluster = await timed(() => admin.describeCluster());
    const topics = await timed(() => admin.listTopics());
    const acls = await timed(() => probeAclSupport(admin));
    return {
      brokerCount: cluster.value.brokers.length,
      topics: topics.value,
      phasesMs: {
        describeCluster: cluster.durationMs,
        listTopics: topics.durationMs,
        aclProbe: acls.durationMs,
      },
    };
  });

  const groups = await withFreshAdmin(target, credentials.value, async (admin) => {
    const listed = await timed(() => admin.listGroups());
    const groupIds = listed.value.groups.map((group) => group.groupId);
    const described = await timed(() =>
      groupIds.length === 0 ? Promise.resolve({ groups: [] }) : admin.describeGroups(groupIds),
    );
    return {
      groups: described.value.groups.map((group) => ({
        groupId: group.groupId,
        state: group.state,
        memberCount: group.members.length,
      })),
      phasesMs: { listGroups: listed.durationMs, describeGroups: described.durationMs },
    };
  });
  const representativeGroup =
    groups.value.groups.find((group) => group.memberCount > 0) ??
    groups.value.groups.find((group) => group.state !== "Empty" && group.state !== "Dead") ??
    groups.value.groups[0];
  const groupDetail = representativeGroup
    ? await withFreshAdmin(target, credentials.value, (admin) =>
        fetchConsumerGroupDetail(admin, representativeGroup.groupId),
      )
    : undefined;
  const representativeTopic = groupDetail?.value.topicOffsets[0]?.topic ?? overview.value.topics[0];
  const topicMetadata = representativeTopic
    ? await withFreshAdmin(target, credentials.value, (admin) =>
        admin.fetchTopicMetadata({ topics: [representativeTopic] }),
      )
    : undefined;
  const topicConsumers = representativeTopic
    ? await withFreshAdmin(target, credentials.value, (admin) => fetchTopicConsumers(admin, representativeTopic))
    : undefined;
  const aggregateHealthConnection = await connectDirect({
    bootstrap: target.bootstrap,
    ssl: credentials.value.ssl ?? (target.tls ? true : undefined),
    sasl: credentials.value.sasl,
  });
  let aggregateHealth: AggregateHealthTiming;
  try {
    aggregateHealth = await measureAggregateHealth(aggregateHealthConnection);
  } finally {
    await aggregateHealthConnection.disconnect();
  }

  const operationTiming = <T>(timing: FreshAdminTiming<T> | undefined) =>
    timing
      ? {
          total: timing.totalMs,
          clientCreate: timing.clientCreateMs,
          adminConnect: timing.adminConnectMs,
          query: timing.queryMs,
          adminDisconnect: timing.adminDisconnectMs,
          protocolRequests: timing.protocolRequests,
        }
      : undefined;

  const evidence = {
    captureId: randomUUID(),
    capturedAt: new Date().toISOString(),
    mode: "read-only",
    counts: {
      workloads: workloads.value.length,
      discoveredTargets: discovery.value.length,
      brokers: overview.value.brokerCount,
      topics: overview.value.topics.length,
      groups: groups.value.groups.length,
      groupDetailTopics: groupDetail?.value.topicOffsets.length ?? 0,
      topicConsumerGroups: topicConsumers?.value.groups.length ?? 0,
    },
    durationsMs: {
      workloadList: workloads.durationMs,
      discovery: discovery.durationMs,
      reachability: reachability.durationMs,
      credentialResolution: credentials.durationMs,
      overview: operationTiming(overview),
      overviewQueries: overview.value.phasesMs,
      groups: operationTiming(groups),
      groupQueries: groups.value.phasesMs,
      topicMetadata: operationTiming(topicMetadata),
      groupDetail: operationTiming(groupDetail),
      topicConsumers: operationTiming(topicConsumers),
      aggregateHealth,
    },
  };
  assertSanitizedPerformanceEvidence(evidence);
  process.stdout.write(`${JSON.stringify(evidence)}\n`);
}

main().catch((error: unknown) => {
  const category = error instanceof StageTimeoutError ? "timeout" : "failed";
  process.stderr.write(`Read-only performance probe ${category}\n`);
  process.exit(1);
});
