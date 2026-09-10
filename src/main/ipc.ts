import { Main } from "@freelensapp/extensions";
import {
  aggregateHealthSnapshotKey,
  decodeAggregateHealthSnapshot,
  encodeAggregateHealthSnapshot,
} from "../common/aggregate-health-snapshot";
import {
  KAFKA_ADMIN_TIMEOUT_MS,
  KAFKA_CLUSTER_HEALTH_TIMEOUT_MS,
  KAFKA_TOPIC_CONSUMERS_TIMEOUT_MS,
} from "../common/constants";
import {
  type AclsRequest,
  type AclWriteRequest,
  type AggregateHealthInvalidateRequest,
  type BrokerConfigRequest,
  type ClusterHealthRequest,
  type DeleteTopicRequest,
  type DiscoveredKafkaInfo,
  type DiscoverRequest,
  type GroupDetailRequest,
  type GroupsRequest,
  KAFKA_IPC,
  type KafkaConnectCreateRequest,
  type KafkaConnectDetailRequest,
  type KafkaConnectRequest,
  type MessageBrowseRequest,
  type OverviewRequest,
  type ProduceRequest,
  type ReachabilityRequest,
  type ResetOffsetsRequest,
  type SchemaDeleteSubjectRequest,
  type SchemaRegisterRequest,
  type SchemaSubjectDetailRequest,
  type SchemaSubjectNamesRequest,
  type SchemaSubjectsRequest,
  type TopicConfigRequest,
  type TopicConsumersRequest,
  type TopicRequest,
  type TopicSizesRequest,
  type WriteModeRequest,
} from "../common/ipc";
import { kafkaPersistentStateStore } from "../common/kafka-persistent-state-store";
import { createKafkaTargetId } from "../common/kafka-target";
import { createCatalogKubeReader } from "./catalog-kube-reader";
import { resolveForwarderOptions } from "./forwarder-options";
import { createAcl, deleteAcl } from "./kafka/acl";
import { aggregateHealthWorkerKey, KafkaAggregateHealthManager } from "./kafka/aggregate-health-manager";
import { connectDiscovered } from "./kafka/connect-discovered";
import { resolveStrimziCredentials } from "./kafka/credentials";
import { type AnyDiscoveredKafka, discoverAllKafkas, discoverStrimziKafkas } from "./kafka/discovery";
import {
  applySecurityOverride,
  credentialProfileKey,
  KafkaCredentialProfileCache,
  resolveExternalCredentials,
} from "./kafka/external-credentials";
import { connectDirect, type KafkaConnection } from "./kafka/kafka-connection";
import { createKubeForwarder } from "./kafka/kube-forwarder";
import { createKubeReader, type KubeReader } from "./kafka/kube-reader";
import { createProgressReporter, type KafkaProgressReporter } from "./kafka/progress";
import { probeBootstrapReachable } from "./kafka/reachability";
import { KafkaSessionManager } from "./kafka/session-manager";
import { KafkaTargetSessionRegistry } from "./kafka/target-session-registry";
import { KafkaConnectClient } from "./kafka-connect/client";
import { withFinalizer, withTimeout } from "./operation-timeout";
import { SchemaRegistryClient } from "./schema-registry/client";
import { WriteModeRegistry } from "./write-mode";

import type { KafkaSecurityHint, KafkaSecuritySummary } from "../common/ipc";
import type { ForwarderRequest } from "./forwarder-options";
import type { KubeForwarderOptions } from "./kafka/kube-forwarder";

/**
 * The kubeconfig file and context for the request's cluster, used by the real port-forward:
 * the file the cluster was added to Freelens from (`kubeConfigPath`), which may live outside
 * the default `~/.kube/config` / `$KUBECONFIG` resolution.
 */
function clusterForwarderOptions(request: ForwarderRequest): KubeForwarderOptions {
  return resolveForwarderOptions(Main.Catalog.getAllClusters(), request);
}

/** The id of the cluster the user is currently viewing (falls back to the first). */
function activeClusterId(): string {
  const clusters = Main.Catalog.getAllClusters();
  const active = clusters.find((c) => c.isActive) ?? clusters[0];
  if (!active) throw new Error("no Kubernetes cluster is available");
  return active.id;
}

function aggregateHealthIdentity(request: OverviewRequest): { contextId: string; targetId: string } {
  return {
    contextId: request.clusterId ?? request.context ?? "active",
    targetId:
      request.targetId ??
      createKafkaTargetId(request.bootstrap ?? `${request.namespace}/${request.clusterName}/${request.user ?? ""}`),
  };
}

/**
 * Read Kubernetes objects through Freelens's connected cluster (`Main.K8s`) by default, so
 * no kubeconfig has to be loaded or authenticated again for reads.
 * An explicit kubeconfig in the request (e.g. tests) still uses direct client-node access.
 */
function createReader(request: { clusterId?: string; kubeConfigPath?: string; context?: string }): KubeReader {
  if (request.kubeConfigPath || request.context) {
    return createKubeReader({ kubeConfigPath: request.kubeConfigPath, context: request.context });
  }
  return createCatalogKubeReader(request.clusterId ?? activeClusterId());
}

/** Map an engine discovery result to the serializable DTO returned to the renderer. */
function toDiscoveredKafkaInfo(discovered: AnyDiscoveredKafka): DiscoveredKafkaInfo {
  const base = {
    targetId: createKafkaTargetId(discovered.bootstrap),
    name: discovered.name,
    namespace: discovered.namespace,
    bootstrap: discovered.bootstrap,
    tls: discovered.tls,
    port: discovered.port,
  };
  if (discovered.source === "strimzi") {
    return {
      ...base,
      source: "strimzi",
      listeners: discovered.listeners,
      brokerPods: discovered.brokerPods,
      provider: "Strimzi",
      external: false,
      securityHint: { tls: discovered.tls, auth: "none" },
    };
  }
  if (discovered.source === "service") {
    return {
      ...base,
      source: "service",
      listeners: [],
      brokerPods: [],
      provider: "In-cluster",
      external: false,
      securityHint: { tls: discovered.tls, auth: "none" },
    };
  }
  return {
    ...base,
    source: "workload",
    listeners: [],
    brokerPods: [],
    provider: discovered.provider,
    external: discovered.external,
    referencedBy: discovered.referencedBy,
    sourceLocator: discovered.sourceLocator,
    securityHint: discovered.securityHint ?? { tls: discovered.tls, auth: "none" },
  };
}

/**
 * Build a live {@link KafkaConnection} for the requested cluster, choosing the strategy by source:
 * Strimzi → per-broker port-forward; any other (external/managed, or in-cluster Service) → Direct.
 */
async function connectForRequest(
  request: OverviewRequest,
  reader: KubeReader,
  report: KafkaProgressReporter,
  credentialProfiles: KafkaCredentialProfileCache,
  sessions: KafkaSessionManager,
  onSessionKey: (sessionKey: string) => void,
): Promise<{ connection: KafkaConnection; security: KafkaSecuritySummary; sessionKey: string }> {
  report({
    value: 4,
    phase: "strategy",
    label: "Selecting connection strategy",
    detail: request.source === "strimzi" ? "Preparing Kubernetes port-forwarding." : "Preparing a direct connection.",
  });
  if (request.source && request.source !== "strimzi") {
    if (!request.bootstrap) {
      throw new Error(`no bootstrap address provided for "${request.clusterName}"`);
    }
    const bootstrap = request.bootstrap;
    const sessionKey = credentialProfileKey({
      bootstrap,
      contextId: request.clusterId ?? request.context ?? "active",
      namespace: request.namespace,
      sourceLocator: request.sourceLocator,
      security: request.security,
      targetId: request.targetId,
      user: request.user,
    });
    onSessionKey(sessionKey);
    const automatic =
      request.source === "workload"
        ? await credentialProfiles.getOrResolve(sessionKey, () =>
            resolveExternalCredentials(
              reader,
              { bootstrap, namespace: request.namespace, sourceLocator: request.sourceLocator },
              (progress) => {
                const value = progress.total === 0 ? 40 : 10 + Math.round((progress.completed / progress.total) * 30);
                report({
                  value,
                  phase: "security",
                  label: "Resolving workload security",
                  detail: "Matching the bootstrap and reading only its relevant ConfigMap/Secret references.",
                  completed: progress.completed,
                  total: progress.total,
                });
              },
            ),
          )
        : { detected: false as const, hint: { tls: false, auth: "none" as const } };
    const applied = applySecurityOverride({
      automatic,
      automaticHint: automatic.detected ? automatic.hint : { tls: Boolean(request.tls), auth: "none" },
      fallbackTls: Boolean(request.tls),
      override: request.security,
      source: automatic.detected ? "workload" : "inferred",
    });
    report({
      value: 46,
      phase: "security",
      label: "Security profile ready",
      detail: `${applied.summary.auth === "none" ? "No authentication" : applied.summary.auth}; ${
        applied.summary.tls ? "TLS enabled" : "TLS disabled"
      }.`,
    });
    report({
      value: 54,
      phase: "connection",
      label: "Preparing direct connection",
      detail: "Configuring Kafka brokers and socket lifecycle.",
    });
    onSessionKey(sessionKey);
    return {
      connection: (
        await sessions.acquire(sessionKey, () => connectDirect({ bootstrap, ssl: applied.ssl, sasl: applied.sasl }))
      ).connection,
      security: applied.summary,
      sessionKey,
    };
  }
  report({ value: 10, phase: "security", label: "Locating Strimzi cluster", detail: request.clusterName });
  const kafkas = await discoverStrimziKafkas(reader, request.namespace);
  const discovered = kafkas.find((k) => k.name === request.clusterName);
  if (!discovered) {
    throw new Error(`Kafka "${request.clusterName}" not found in namespace "${request.namespace}"`);
  }
  const sessionKey = credentialProfileKey({
    bootstrap: discovered.bootstrap,
    contextId: request.clusterId ?? request.context ?? "active",
    namespace: request.namespace,
    security: request.security,
    targetId: request.targetId,
    user: request.user,
  });
  onSessionKey(sessionKey);
  const credentials = await resolveStrimziCredentials(reader, {
    namespace: request.namespace,
    clusterName: request.clusterName,
    tls: discovered.tls,
    user: request.user,
  });
  report({
    value: 24,
    phase: "security",
    label: "Strimzi security resolved",
    detail: "Read the selected listener and optional KafkaUser Secret.",
  });
  const sslObject = typeof credentials.ssl === "object" ? credentials.ssl : undefined;
  const automaticHint: KafkaSecurityHint = {
    tls: Boolean(credentials.ssl) || discovered.tls,
    auth:
      sslObject && "cert" in sslObject && "key" in sslObject
        ? "mtls"
        : credentials.sasl
          ? (credentials.sasl.mechanism as KafkaSecurityHint["auth"])
          : "none",
  };
  const applied = applySecurityOverride({
    automatic: credentials,
    automaticHint,
    fallbackTls: discovered.tls,
    override: request.security,
    source: "strimzi",
  });
  const forwarder = createKubeForwarder(clusterForwarderOptions(request));
  report({
    value: 30,
    phase: "connection",
    label: "Opening bootstrap port-forward",
    detail: "Creating a temporary tunnel to one broker to read authoritative metadata.",
  });
  onSessionKey(sessionKey);
  return {
    connection: (
      await sessions.acquire(sessionKey, () =>
        connectDiscovered({
          discovered,
          forwarder,
          sasl: applied.sasl,
          ssl: applied.ssl,
          onPhase: (phase, info) => {
            report(
              phase === "metadata"
                ? { value: 48, phase: "connection", label: "Bootstrap metadata received", detail: info }
                : { value: 56, phase: "connection", label: "Opening broker port-forwards", detail: info },
            );
          },
        }),
      )
    ).connection,
    security: applied.summary,
    sessionKey,
  };
}

/**
 * Main-process IPC surface for the Kafka extension. The renderer invokes these
 * over the built-in per-extension `Ipc` channel (see ARCHITECTURE.md §6.1).
 */
export class KafkaIpcMain extends Main.Ipc {
  private readonly aggregateHealth = new KafkaAggregateHealthManager();
  private closed = false;
  private readonly credentialProfiles = new KafkaCredentialProfileCache();
  private readonly healthWorkersByTarget = new Map<string, Map<string, string>>();
  private readonly pendingConnectionsByTarget = new Map<string, Set<Promise<unknown>>>();
  private readonly sessions = new KafkaSessionManager();
  private readonly targetSessions = new KafkaTargetSessionRegistry();
  private readonly writeMode = new WriteModeRegistry();

  constructor(extension: Main.LensExtension) {
    super(extension);

    this.handle(KAFKA_IPC.discover, async (_event, request: DiscoverRequest) => {
      const reader = createReader(request);
      const report = createProgressReporter({
        operationId: request.operationId,
        operation: "discovery",
        emit: (progress) => this.broadcast(KAFKA_IPC.progress, progress),
      });
      const discovered = await discoverAllKafkas(reader, request.namespace, report);
      return discovered.map(toDiscoveredKafkaInfo);
    });

    this.handle(KAFKA_IPC.reachability, async (_event, request: ReachabilityRequest) => {
      const entries = await Promise.all(
        request.bootstraps.map(async (bootstrap) => [bootstrap, await probeBootstrapReachable(bootstrap)] as const),
      );
      return Object.fromEntries(entries) as Record<string, boolean>;
    });

    this.handle(KAFKA_IPC.healthInvalidate, async (_event, request: AggregateHealthInvalidateRequest) => {
      const targetKey = aggregateHealthSnapshotKey(request.clusterId ?? request.context ?? "active", request.targetId);
      await this.invalidateHealthTarget(targetKey, request.closeSessions === true);
      if (request.removePersisted) kafkaPersistentStateStore().removeItem(targetKey);
    });

    this.handle(KAFKA_IPC.overview, async (_event, request: OverviewRequest) => {
      const reader = createReader(request);
      const report = createProgressReporter({
        operationId: request.operationId,
        operation: "overview",
        emit: (progress) => this.broadcast(KAFKA_IPC.progress, progress),
      });
      const { connection, security } = await this.resolveConnection(request, reader, report);
      try {
        const overview = await connection.overview(report);
        report({
          value: 100,
          phase: "complete",
          label: "Cluster metadata ready",
          detail: `${overview.brokers.length} broker(s), ${overview.topics.length} topic(s).`,
        });
        return { ...overview, security };
      } finally {
        await this.sessions.release(connection);
      }
    });

    this.handle(KAFKA_IPC.health, async (_event, request: ClusterHealthRequest) => {
      const reader = createReader(request);
      const report = createProgressReporter({
        operationId: request.operationId,
        operation: "health",
        emit: (progress) => this.broadcast(KAFKA_IPC.progress, progress),
      });
      const identity = aggregateHealthIdentity(request);
      const persistentKey = aggregateHealthSnapshotKey(identity.contextId, identity.targetId);
      const persistentStore = kafkaPersistentStateStore();
      const restored = decodeAggregateHealthSnapshot(persistentStore.getItem(persistentKey));
      if (restored) {
        report({
          value: 0,
          phase: "cache",
          label: "Previous cluster health",
          detail: "Showing the previous non-secret aggregate while reconnecting.",
          healthSnapshot: restored.data,
          healthSource: "persisted",
          healthUpdatedAt: restored.updatedAt,
        });
      }
      if (request.refresh) await this.invalidateHealthTarget(persistentKey, false);
      const { connection, sessionKey } = await this.resolveConnection(request, reader, report);
      try {
        if (request.refresh) connection.invalidateHealth();
        const workerKey = aggregateHealthWorkerKey(identity.contextId, identity.targetId, sessionKey);
        const targetWorkers = this.healthWorkersByTarget.get(persistentKey) ?? new Map<string, string>();
        targetWorkers.set(workerKey, sessionKey);
        this.healthWorkersByTarget.set(persistentKey, targetWorkers);
        if (restored) this.aggregateHealth.restore(workerKey, restored);
        const servedFromCache = !request.refresh && this.aggregateHealth.read(workerKey).fresh;
        let restoredProgressSeen = false;
        const health = await this.aggregateHealth.load(
          workerKey,
          async (publish, signal) => {
            const operationAbort = new AbortController();
            const abortFromWorker = () => operationAbort.abort();
            signal.addEventListener("abort", abortFromWorker, { once: true });
            const operation = connection.health(publish, operationAbort.signal);
            try {
              return await withTimeout(operation, KAFKA_CLUSTER_HEALTH_TIMEOUT_MS, "cluster health", async () => {
                operationAbort.abort();
                await operation.catch(() => undefined);
              });
            } finally {
              signal.removeEventListener("abort", abortFromWorker);
            }
          },
          (progress) => {
            if (restored && progress.phase === "cache" && !restoredProgressSeen) {
              restoredProgressSeen = true;
              return;
            }
            report(progress);
          },
          request.refresh,
        );
        const snapshot = this.aggregateHealth.read(workerKey);
        if (snapshot.data && snapshot.updatedAt !== undefined) {
          persistentStore.setItem(
            persistentKey,
            encodeAggregateHealthSnapshot({
              data: snapshot.data,
              lastComplete: snapshot.lastComplete,
              updatedAt: snapshot.updatedAt,
            }),
          );
        }
        report({
          value: 100,
          phase: "complete",
          label: "Cluster health ready",
          detail:
            health.consumerGroupLagUnavailableTopics || health.consumerGroupLagUnavailableGroups
              ? `Broker and partition health is ready; consumer lag is a minimum because ${health.consumerGroupLagUnavailableGroups ?? 0} group(s) and ${health.consumerGroupLagUnavailableTopics ?? 0} topic(s) were unavailable.`
              : "The asynchronous cluster health snapshot is complete.",
          healthSnapshot: health,
          healthSource: servedFromCache ? "cache" : "network",
          healthUpdatedAt: this.aggregateHealth.read(workerKey).updatedAt,
        });
        return health;
      } finally {
        await this.sessions.release(connection);
      }
    });

    this.handle(KAFKA_IPC.topic, async (_event, request: TopicRequest) => {
      const reader = createReader(request);
      const report = createProgressReporter({
        operationId: request.operationId,
        operation: "topic",
        emit: (progress) => this.broadcast(KAFKA_IPC.progress, progress),
      });
      const { connection } = await this.resolveConnection(request, reader, report);
      try {
        const detail = await connection.topicDetail(request.topic, report);
        report({
          value: 100,
          phase: "complete",
          label: "Topic metadata ready",
          detail: `${detail.partitionCount} partition(s), ${detail.underReplicatedPartitions} under-replicated.`,
        });
        return detail;
      } finally {
        await this.sessions.release(connection);
      }
    });

    this.handle(KAFKA_IPC.topicSizes, async (_event, request: TopicSizesRequest) => {
      const reader = createReader(request);
      const connectionPromise = this.resolveConnection(request, reader, () => undefined);
      const operation = connectionPromise.then(async ({ connection }) => {
        return connection.topicSizes(Array.isArray(request.topics) ? request.topics.map(String) : []);
      });
      const finalized = withFinalizer(operation, async () => {
        const resolved = await connectionPromise.catch(() => undefined);
        if (resolved) await this.sessions.release(resolved.connection);
      });
      return withTimeout(finalized, KAFKA_ADMIN_TIMEOUT_MS, "Topic sizes");
    });

    this.handle(KAFKA_IPC.topicConfig, async (_event, request: TopicConfigRequest) => {
      const reader = createReader(request);
      const report = createProgressReporter({
        operationId: request.operationId,
        operation: "topicConfig",
        emit: (progress) => this.broadcast(KAFKA_IPC.progress, progress),
      });
      const connectionPromise = this.resolveConnection(request, reader, report);
      const operation = connectionPromise.then(async ({ connection }) => {
        report({
          value: 62,
          phase: "config",
          label: "Reading topic configuration",
          detail: `Reading config entries for \"${request.topic}\".`,
        });
        const detail = await connection.topicConfig(request.topic, report);
        report({
          value: 100,
          phase: "complete",
          label: "Topic configuration ready",
          detail: `${detail.entries.length} config entr${detail.entries.length === 1 ? "y" : "ies"}.`,
        });
        return detail;
      });
      const finalized = withFinalizer(operation, async () => {
        const resolved = await connectionPromise.catch(() => undefined);
        if (resolved) await this.sessions.release(resolved.connection);
      });
      return withTimeout(finalized, KAFKA_ADMIN_TIMEOUT_MS, "Topic configuration");
    });

    this.handle(KAFKA_IPC.topicConsumers, async (_event, request: TopicConsumersRequest) => {
      const reader = createReader(request);
      let cancelled = false;
      let connection: KafkaConnection | undefined;
      const abortController = new AbortController();
      const report = createProgressReporter({
        operationId: request.operationId,
        operation: "topicConsumers",
        emit: (progress) => {
          if (!cancelled) this.broadcast(KAFKA_IPC.progress, progress);
        },
      });
      const connectionPromise = this.resolveConnection(request, reader, report);
      const operation = connectionPromise.then(async (resolved) => {
        connection = resolved.connection;
        if (cancelled) throw new Error("Topic Consumers was cancelled");
        report({
          value: 62,
          phase: "groups",
          label: "Reading topic consumers",
          detail: `Finding consumer groups with committed offsets on "${request.topic}".`,
        });
        const detail = await connection.topicConsumers(request.topic, report, abortController.signal);
        if (cancelled) throw new Error("Topic Consumers was cancelled");
        report({
          value: 100,
          phase: "complete",
          label: "Topic consumers ready",
          detail: `${detail.groups.length} consumer group(s).`,
        });
        return detail;
      });
      try {
        return await withTimeout(operation, KAFKA_TOPIC_CONSUMERS_TIMEOUT_MS, "Topic Consumers", async () => {
          cancelled = true;
          abortController.abort();
          if (connection) await operation.catch(() => undefined);
        });
      } catch (error) {
        cancelled = true;
        abortController.abort();
        throw error;
      } finally {
        if (connection) {
          await this.sessions.release(connection);
        } else {
          void connectionPromise
            .then(async (resolved) => this.sessions.release(resolved.connection))
            .catch(() => undefined);
        }
      }
    });

    this.handle(KAFKA_IPC.brokerConfig, async (_event, request: BrokerConfigRequest) => {
      const reader = createReader(request);
      let cancelled = false;
      let connection: KafkaConnection | undefined;
      const report = createProgressReporter({
        operationId: request.operationId,
        operation: "brokerConfig",
        emit: (progress) => {
          if (!cancelled) this.broadcast(KAFKA_IPC.progress, progress);
        },
      });
      const connectionPromise = this.resolveConnection(request, reader, report);
      const operation = connectionPromise.then(async (resolved) => {
        connection = resolved.connection;
        if (cancelled) throw new Error("Broker configuration was cancelled");
        report({
          value: 62,
          phase: "config",
          label: "Reading broker configuration",
          detail: `Reading config entries for broker ${request.brokerId}.`,
        });
        const detail = await connection.brokerConfig(request.brokerId);
        if (cancelled) throw new Error("Broker configuration was cancelled");
        report({
          value: 100,
          phase: "complete",
          label: "Broker configuration ready",
          detail: `${detail.entries.length} config entr${detail.entries.length === 1 ? "y" : "ies"}.`,
        });
        return detail;
      });
      const finalized = withFinalizer(operation, async () => {
        const resolved = await connectionPromise.catch(() => undefined);
        if (resolved) await this.sessions.release(resolved.connection);
      });
      try {
        return await withTimeout(finalized, KAFKA_ADMIN_TIMEOUT_MS, "Broker configuration");
      } catch (error) {
        cancelled = true;
        throw error;
      }
    });

    this.handle(KAFKA_IPC.messagesBrowse, async (_event, request: MessageBrowseRequest) => {
      const reader = createReader(request);
      let cancelled = false;
      let connection: KafkaConnection | undefined;
      const report = createProgressReporter({
        operationId: request.operationId,
        operation: "messagesBrowse",
        emit: (progress) => {
          if (!cancelled) this.broadcast(KAFKA_IPC.progress, progress);
        },
      });
      const connectionPromise = this.resolveConnection(request, reader, report);
      const operation = connectionPromise.then(async (resolved) => {
        connection = resolved.connection;
        if (cancelled) throw new Error("Message Browse was cancelled");
        report({
          value: 62,
          phase: "offsets",
          label: "Resolving message window",
          detail: `Reading partition ${request.partition} offsets without a consumer group.`,
        });
        const result = await connection.browseMessages(request);
        if (cancelled) throw new Error("Message Browse was cancelled");
        report({
          value: 100,
          phase: "complete",
          label: "Message window ready",
          detail: `${result.returnedCount} committed record(s) returned.`,
        });
        return result;
      });
      const finalized = withFinalizer(operation, async () => {
        const resolved = await connectionPromise.catch(() => undefined);
        if (resolved) await this.sessions.release(resolved.connection);
      });
      try {
        return await withTimeout(finalized, KAFKA_ADMIN_TIMEOUT_MS, "Message Browse");
      } catch (error) {
        cancelled = true;
        throw error;
      }
    });

    this.handle(KAFKA_IPC.groups, async (_event, request: GroupsRequest) => {
      const reader = createReader(request);
      const report = createProgressReporter({
        operationId: request.operationId,
        operation: "groups",
        emit: (progress) => this.broadcast(KAFKA_IPC.progress, progress),
      });
      const connectionPromise = this.resolveConnection(request, reader, report);
      const operation = connectionPromise.then(async ({ connection }) => {
        report({
          value: 62,
          phase: "listing",
          label: "Listing consumer groups",
          detail: "Reading group state and member counts.",
        });
        const groups = await connection.listGroups();
        report({
          value: 100,
          phase: "complete",
          label: "Consumer groups ready",
          detail: `${groups.length} group(s) found.`,
        });
        return { groups };
      });
      const finalized = withFinalizer(operation, async () => {
        const resolved = await connectionPromise.catch(() => undefined);
        if (resolved) await this.sessions.release(resolved.connection);
      });
      return withTimeout(finalized, KAFKA_ADMIN_TIMEOUT_MS, "Consumer Groups list");
    });

    this.handle(KAFKA_IPC.groupDetail, async (_event, request: GroupDetailRequest) => {
      const reader = createReader(request);
      const report = createProgressReporter({
        operationId: request.operationId,
        operation: "groupDetail",
        emit: (progress) => this.broadcast(KAFKA_IPC.progress, progress),
      });
      const connectionPromise = this.resolveConnection(request, reader, report);
      const operation = connectionPromise.then(async ({ connection }) => {
        report({
          value: 62,
          phase: "describing",
          label: "Describing consumer group",
          detail: `Reading offsets and lag for "${request.groupId}".`,
        });
        const detail = await connection.groupDetail(request.groupId);
        report({
          value: 100,
          phase: "complete",
          label: "Group detail ready",
          detail: `${detail.topicOffsets.length} subscribed topic(s).`,
        });
        return detail;
      });
      const finalized = withFinalizer(operation, async () => {
        const resolved = await connectionPromise.catch(() => undefined);
        if (resolved) await this.sessions.release(resolved.connection);
      });
      return withTimeout(finalized, KAFKA_ADMIN_TIMEOUT_MS, "Consumer Group detail");
    });

    this.handle(KAFKA_IPC.writeMode, async (_event, request: WriteModeRequest) => {
      this.writeMode.set(String(request.targetId ?? ""), Boolean(request.enabled));
    });

    this.handle(KAFKA_IPC.produce, async (_event, request: ProduceRequest) => {
      this.writeMode.assertEnabled(request.targetId, "Produce message");
      const reader = createReader(request);
      const { connection } = await this.resolveConnection(request, reader, () => undefined);
      try {
        return await connection.produce(request);
      } finally {
        await this.sessions.release(connection);
      }
    });

    this.handle(KAFKA_IPC.deleteTopic, async (_event, request: DeleteTopicRequest) => {
      this.writeMode.assertEnabled(request.targetId, "Delete topic");
      const reader = createReader(request);
      const { connection } = await this.resolveConnection(request, reader, () => undefined);
      try {
        return await connection.deleteTopic(request.topic);
      } finally {
        await this.sessions.release(connection);
      }
    });

    this.handle(KAFKA_IPC.resetOffsets, async (_event, request: ResetOffsetsRequest) => {
      this.writeMode.assertEnabled(request.targetId, "Reset offsets");
      const reader = createReader(request);
      const { connection } = await this.resolveConnection(request, reader, () => undefined);
      try {
        return await connection.resetOffsets(request);
      } finally {
        await this.sessions.release(connection);
      }
    });

    this.handle(KAFKA_IPC.schemaSubjects, async (_event, request: SchemaSubjectsRequest) => {
      const client = new SchemaRegistryClient({
        baseUrl: request.registryUrl,
        username: request.registryUsername,
        password: request.registryPassword,
      });
      const subjects = await client.listSubjects();
      return Promise.all(subjects.map((subject) => client.getSubjectSummary(subject)));
    });

    this.handle(KAFKA_IPC.schemaSubjectNames, async (_event, request: SchemaSubjectNamesRequest) => {
      return new SchemaRegistryClient({
        baseUrl: request.registryUrl,
        username: request.registryUsername,
        password: request.registryPassword,
      }).listSubjects();
    });

    this.handle(KAFKA_IPC.schemaSubjectDetail, async (_event, request: SchemaSubjectDetailRequest) => {
      const client = new SchemaRegistryClient({
        baseUrl: request.registryUrl,
        username: request.registryUsername,
        password: request.registryPassword,
      });
      return client.getSubjectDetail(request.subject);
    });

    this.handle(KAFKA_IPC.schemaRegister, async (_event, request: SchemaRegisterRequest) => {
      this.writeMode.assertEnabled(request.targetId, "Register schema");
      const client = new SchemaRegistryClient({
        baseUrl: request.registryUrl,
        username: request.registryUsername,
        password: request.registryPassword,
      });
      return client.registerSchema(request.subject, request.schema, request.schemaType);
    });

    this.handle(KAFKA_IPC.schemaDeleteSubject, async (_event, request: SchemaDeleteSubjectRequest) => {
      this.writeMode.assertEnabled(request.targetId, "Delete subject");
      const client = new SchemaRegistryClient({
        baseUrl: request.registryUrl,
        username: request.registryUsername,
        password: request.registryPassword,
      });
      return client.deleteSubject(request.subject);
    });

    this.handle(KAFKA_IPC.connectList, async (_event, request: KafkaConnectRequest) => {
      return new KafkaConnectClient({
        baseUrl: request.connectUrl,
        username: request.connectUsername,
        password: request.connectPassword,
      }).listConnectors();
    });

    this.handle(KAFKA_IPC.connectNames, async (_event, request: KafkaConnectRequest) => {
      return new KafkaConnectClient({
        baseUrl: request.connectUrl,
        username: request.connectUsername,
        password: request.connectPassword,
      }).listConnectorNames();
    });

    this.handle(KAFKA_IPC.connectDetail, async (_event, request: KafkaConnectDetailRequest) => {
      return new KafkaConnectClient({
        baseUrl: request.connectUrl,
        username: request.connectUsername,
        password: request.connectPassword,
      }).getConnector(request.connector);
    });

    this.handle(KAFKA_IPC.acls, async (_event, request: AclsRequest) => {
      const reader = createReader(request);
      const connectionPromise = this.resolveConnection(request, reader, () => undefined);
      const operation = connectionPromise.then(async ({ connection }) => {
        return connection.acls();
      });
      const finalized = withFinalizer(operation, async () => {
        const resolved = await connectionPromise.catch(() => undefined);
        if (resolved) await this.sessions.release(resolved.connection);
      });
      return withTimeout(finalized, KAFKA_ADMIN_TIMEOUT_MS, "ACL inspection");
    });

    this.handle(KAFKA_IPC.aclCreate, async (_event, request: AclWriteRequest) => {
      this.writeMode.assertEnabled(request.targetId, "Create ACL");
      const reader = createReader(request);
      const { connection } = await this.resolveConnection(request, reader, () => undefined);
      try {
        return await withTimeout(createAcl(connection.admin(), request.acl), KAFKA_ADMIN_TIMEOUT_MS, "ACL creation");
      } finally {
        await this.sessions.release(connection);
      }
    });
    this.handle(KAFKA_IPC.aclDelete, async (_event, request: AclWriteRequest) => {
      this.writeMode.assertEnabled(request.targetId, "Delete ACL");
      const reader = createReader(request);
      const { connection } = await this.resolveConnection(request, reader, () => undefined);
      try {
        return await withTimeout(deleteAcl(connection.admin(), request.acl), KAFKA_ADMIN_TIMEOUT_MS, "ACL deletion");
      } finally {
        await this.sessions.release(connection);
      }
    });

    this.handle(KAFKA_IPC.connectPause, async (_event, request: KafkaConnectDetailRequest) => {
      this.writeMode.assertEnabled(request.targetId, "Pause connector");
      return new KafkaConnectClient({
        baseUrl: request.connectUrl,
        username: request.connectUsername,
        password: request.connectPassword,
      }).pauseConnector(request.connector);
    });
    this.handle(KAFKA_IPC.connectResume, async (_event, request: KafkaConnectDetailRequest) => {
      this.writeMode.assertEnabled(request.targetId, "Resume connector");
      return new KafkaConnectClient({
        baseUrl: request.connectUrl,
        username: request.connectUsername,
        password: request.connectPassword,
      }).resumeConnector(request.connector);
    });
    this.handle(KAFKA_IPC.connectDelete, async (_event, request: KafkaConnectDetailRequest) => {
      this.writeMode.assertEnabled(request.targetId, "Delete connector");
      return new KafkaConnectClient({
        baseUrl: request.connectUrl,
        username: request.connectUsername,
        password: request.connectPassword,
      }).deleteConnector(request.connector);
    });
    this.handle(KAFKA_IPC.connectRestart, async (_event, request: KafkaConnectDetailRequest) => {
      this.writeMode.assertEnabled(request.targetId, "Restart connector");
      return new KafkaConnectClient({
        baseUrl: request.connectUrl,
        username: request.connectUsername,
        password: request.connectPassword,
      }).restartConnector(request.connector);
    });
    this.handle(KAFKA_IPC.connectUpdate, async (_event, request: KafkaConnectCreateRequest) => {
      this.writeMode.assertEnabled(request.targetId, "Update connector");
      return new KafkaConnectClient({
        baseUrl: request.connectUrl,
        username: request.connectUsername,
        password: request.connectPassword,
      }).updateConnector(request.config.name, request.config);
    });
    this.handle(KAFKA_IPC.connectCreate, async (_event, request: KafkaConnectCreateRequest) => {
      this.writeMode.assertEnabled(request.targetId, "Create connector");
      return new KafkaConnectClient({
        baseUrl: request.connectUrl,
        username: request.connectUsername,
        password: request.connectPassword,
      }).createConnector(request.config);
    });
  }

  private resolveConnection(
    request: OverviewRequest,
    reader: KubeReader,
    report: KafkaProgressReporter,
  ): Promise<{ connection: KafkaConnection; security: KafkaSecuritySummary; sessionKey: string }> {
    if (this.closed) return Promise.reject(new Error("Kafka extension is shutting down"));
    const identity = aggregateHealthIdentity(request);
    const targetKey = aggregateHealthSnapshotKey(identity.contextId, identity.targetId);
    let operation!: Promise<{ connection: KafkaConnection; security: KafkaSecuritySummary; sessionKey: string }>;
    operation = this.resolveConnectionOwned(request, reader, report).finally(() => {
      const pending = this.pendingConnectionsByTarget.get(targetKey);
      pending?.delete(operation);
      if (pending?.size === 0) this.pendingConnectionsByTarget.delete(targetKey);
    });
    const pending = this.pendingConnectionsByTarget.get(targetKey) ?? new Set<Promise<unknown>>();
    pending.add(operation);
    this.pendingConnectionsByTarget.set(targetKey, pending);
    return operation;
  }

  private async resolveConnectionOwned(
    request: OverviewRequest,
    reader: KubeReader,
    report: KafkaProgressReporter,
  ): Promise<{ connection: KafkaConnection; security: KafkaSecuritySummary; sessionKey: string }> {
    if (this.closed) throw new Error("Kafka extension is shutting down");
    const identity = aggregateHealthIdentity(request);
    const targetKey = aggregateHealthSnapshotKey(identity.contextId, identity.targetId);
    const generation = this.targetSessions.begin(targetKey);
    let sessionKey: string | undefined;
    try {
      const resolved = await connectForRequest(
        request,
        reader,
        report,
        this.credentialProfiles,
        this.sessions,
        (candidate) => {
          sessionKey = candidate;
          if (this.closed || !this.targetSessions.register(targetKey, generation, candidate)) {
            throw new Error("Kafka target was invalidated while connecting");
          }
        },
      );
      if (this.closed || !this.targetSessions.isCurrent(targetKey, generation)) {
        this.credentialProfiles.delete(resolved.sessionKey);
        await this.sessions.clear(resolved.sessionKey);
        throw new Error("Kafka target was invalidated while connecting");
      }
      return resolved;
    } catch (error) {
      if (sessionKey && (this.closed || !this.targetSessions.isCurrent(targetKey, generation))) {
        this.credentialProfiles.delete(sessionKey);
        await this.sessions.clear(sessionKey);
      }
      throw error;
    }
  }

  async shutdown(): Promise<void> {
    this.closed = true;
    this.aggregateHealth.clear();
    this.healthWorkersByTarget.clear();
    this.targetSessions.clear();
    this.credentialProfiles.clear();
    const pending = [...this.pendingConnectionsByTarget.values()].flatMap((operations) => [...operations]);
    const sessionCleanup = this.sessions.clear();
    await Promise.allSettled([...pending, sessionCleanup]);
    this.pendingConnectionsByTarget.clear();
    await this.sessions.clear();
  }

  private async invalidateHealthTarget(targetKey: string, closeSessions: boolean): Promise<void> {
    const workers = this.healthWorkersByTarget.get(targetKey);
    this.healthWorkersByTarget.delete(targetKey);
    for (const workerKey of workers?.keys() ?? []) {
      if (closeSessions) this.aggregateHealth.remove(workerKey);
      else this.aggregateHealth.invalidate(workerKey);
    }
    if (closeSessions) {
      const sessionKeys = new Set([...(workers?.values() ?? []), ...this.targetSessions.invalidate(targetKey)]);
      await Promise.all(
        [...sessionKeys].map(async (sessionKey) => {
          this.credentialProfiles.delete(sessionKey);
          await this.sessions.clear(sessionKey);
        }),
      );
      const pending = [...(this.pendingConnectionsByTarget.get(targetKey) ?? [])];
      await Promise.allSettled(pending);
    }
  }
}
