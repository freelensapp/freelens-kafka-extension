import { type Admin, Kafka, logLevel, type SASLOptions } from "kafkajs";
import { splitBootstrap } from "../../common/reachability";
import { fetchAcls, probeAclSupport } from "./acl";
import { fetchBrokerConfig } from "./broker-config";
import { fetchClusterHealth } from "./cluster-health";
import {
  ConsumerGroupOffsetIndex,
  fetchConsumerGroupDetail,
  fetchTopicConsumers,
  listConsumerGroups,
} from "./group-fetch";
import { KafkaGroupOffsetBatchReader } from "./group-offset-batch";
import { browseMessagesWithCluster, createKafkaJsReadCluster, type KafkaReadCluster } from "./message-fetch";
import { fetchHighWatermarks, type OffsetBatchPartition } from "./offset-batch";
import { PortForwardManager } from "./port-forward-manager";
import { createDirectSocketFactory, createRedirectSocketFactory } from "./redirect";
import { fetchTopicConfig } from "./topic-config";
import { toTopicDetail } from "./topic-metadata";
import type { ConnectionOptions as TlsOptions } from "node:tls";

import type {
  BrokerConfigDto,
  ClusterOverviewHealthDto,
  ConsumerGroupDetailDto,
  ConsumerGroupSummaryDto,
  DeleteTopicResultDto,
  MessageBrowseDto,
  MessageBrowseRequest,
  ProduceRequest,
  ProduceResultDto,
  ResetOffsetsRequest,
  ResetOffsetsResultDto,
  TopicConfigDto,
  TopicConsumersDto,
  TopicDetailDto,
} from "../../common/ipc";
import type { Forwarder } from "./port-forward-manager";
import type { KafkaProgressReporter } from "./progress";
import type { BrokerRef } from "./types";

export interface ConnectOptions {
  clientId?: string;
  /** Brokers to reach, each mapped to its backing pod (port-forward strategy). Omit for a direct connection. */
  brokers?: BrokerRef[];
  /** Advertised bootstrap `"host:port"` — or a comma-separated list for a direct connection. */
  bootstrap: string;
  /** How local sockets reach broker pods (port-forward strategy). Omit for a direct connection. */
  forwarder?: Forwarder;
  sasl?: SASLOptions;
  ssl?: TlsOptions | boolean;
  onRedirect?: (from: string, to: string) => void;
}

export interface ClusterOverview {
  brokers: { nodeId: number; host: string; port: number }[];
  controller: number | null;
  topics: string[];
  aclsAvailable: boolean;
}

/**
 * Owns the full Main-side connection lifecycle: opens per-broker port-forwards,
 * builds the advertised->local map, wires the kafkajs client through the
 * redirect socketFactory, and tears everything down on disconnect.
 */
export class KafkaConnection {
  private readonly portForwards?: PortForwardManager;
  private kafka!: Kafka;
  private directClose?: () => void;
  private disconnected = false;
  private messageCluster?: KafkaReadCluster;
  private messageClusterConnect?: Promise<KafkaReadCluster>;
  private groupOffsetBatchReader?: KafkaGroupOffsetBatchReader;
  private readonly consumerGroupOffsets: ConsumerGroupOffsetIndex;

  private constructor(private readonly options: ConnectOptions) {
    this.portForwards = options.forwarder ? new PortForwardManager(options.forwarder) : undefined;
    this.consumerGroupOffsets = new ConsumerGroupOffsetIndex((groupIds, signal) =>
      this.fetchGroupOffsets(groupIds, signal),
    );
  }

  static async connect(options: ConnectOptions): Promise<KafkaConnection> {
    const connection = new KafkaConnection(options);
    await connection.init();
    return connection;
  }

  private async init(): Promise<void> {
    // Port-forward strategy: open per-broker forwards and redirect advertised -> local.
    // Direct strategy (no forwarder/brokers): kafkajs connects straight to the bootstrap.
    let socketFactory: ReturnType<typeof createRedirectSocketFactory> | undefined;
    if (this.portForwards && this.options.brokers && this.options.brokers.length > 0) {
      const map = await this.portForwards.open(this.options.brokers);
      socketFactory = createRedirectSocketFactory(map, this.options.onRedirect);
    } else {
      const direct = createDirectSocketFactory();
      this.directClose = direct.closeAll;
      socketFactory = direct.socketFactory;
    }
    this.kafka = new Kafka({
      clientId: this.options.clientId ?? "freelens-kafka",
      brokers: splitBootstrap(this.options.bootstrap),
      ssl: this.options.ssl,
      sasl: this.options.sasl,
      logLevel: logLevel.NOTHING,
      socketFactory,
      connectionTimeout: 10_000,
      retry: { retries: 5 },
    });
  }

  /** The underlying kafkajs client (for producer/consumer). */
  get client(): Kafka {
    return this.kafka;
  }

  admin(): Admin {
    return this.kafka.admin();
  }

  /** A minimal cluster snapshot: brokers, controller and topics. */
  async overview(report?: KafkaProgressReporter): Promise<ClusterOverview> {
    const admin = this.kafka.admin();
    report?.({
      value: 62,
      phase: "connect",
      label: "Connecting to Kafka",
      detail: "Opening a read-only Kafka admin connection.",
    });
    await admin.connect();
    try {
      report?.({
        value: 72,
        phase: "brokers",
        label: "Reading broker metadata",
        detail: "Requesting broker and controller information from Kafka.",
      });
      const cluster = await admin.describeCluster();
      report?.({
        value: 84,
        phase: "brokers",
        label: "Broker metadata received",
        detail: `${cluster.brokers.length} broker(s); controller ${cluster.controller}.`,
      });
      report?.({
        value: 88,
        phase: "topics",
        label: "Reading topic metadata",
        detail: "Listing topic names to calculate the current topic count.",
      });
      const topics = await admin.listTopics();
      report?.({
        value: 98,
        phase: "topics",
        label: "Topic metadata received",
        detail: `${topics.length} topic(s) found.`,
      });
      const aclsAvailable = await probeAclSupport(admin);
      return {
        brokers: cluster.brokers.map((b) => ({
          nodeId: b.nodeId,
          host: b.host,
          port: b.port,
        })),
        controller: cluster.controller,
        topics,
        aclsAvailable,
      };
    } finally {
      await admin.disconnect();
    }
  }

  async health(report?: KafkaProgressReporter, signal?: AbortSignal): Promise<ClusterOverviewHealthDto> {
    const admin = this.kafka.admin();
    if (signal?.aborted) throw new Error("Cluster health was cancelled");
    const cancel = () => void admin.disconnect().catch(() => undefined);
    signal?.addEventListener("abort", cancel, { once: true });
    report?.({
      value: 62,
      phase: "connect",
      label: "Connecting to Kafka",
      detail: "Opening a read-only Kafka admin connection for cluster health.",
    });
    await admin.connect();
    try {
      if (signal?.aborted) throw new Error("Cluster health was cancelled");
      report?.({
        value: 72,
        phase: "health",
        label: "Reading cluster topology",
        detail: "Measuring brokers and partition replication before consumer lag.",
      });
      let lastProgressPhase = "";
      let lastProgressValue = -1;
      const health = await fetchClusterHealth(
        admin,
        this.consumerGroupOffsets,
        (progress) => {
          const ratio = progress.total ? (progress.completed ?? 0) / progress.total : 0;
          const value =
            progress.phase === "topology"
              ? 74
              : progress.phase === "groups"
                ? 74 + Math.round(ratio * 16)
                : progress.phase === "watermarks"
                  ? 90 + Math.round(ratio * 8)
                  : 98;
          const label =
            progress.phase === "topology"
              ? "Broker and partition health ready"
              : progress.phase === "groups"
                ? "Reading consumer group offsets"
                : progress.phase === "watermarks"
                  ? "Calculating consumer lag"
                  : "Cluster health measured";
          const detail =
            progress.detail ??
            (progress.phase === "topology"
              ? "Broker and partition values are available while consumer lag continues."
              : progress.total === undefined
                ? undefined
                : `${progress.completed ?? 0}/${progress.total} ${progress.phase === "groups" ? "consumer group(s)" : "topic watermark(s)"} checked.`);
          const terminalCount = progress.total !== undefined && progress.completed === progress.total;
          if (progress.phase === lastProgressPhase && value === lastProgressValue && !terminalCount) return;
          lastProgressPhase = progress.phase;
          lastProgressValue = value;
          report?.({
            value,
            phase: progress.phase,
            label,
            detail,
            completed: progress.completed,
            total: progress.total,
            healthSnapshot: progress.health,
          });
        },
        (partitions, readSignal) => this.fetchHighWatermarks(partitions, readSignal),
        signal,
      );
      return health;
    } finally {
      signal?.removeEventListener("abort", cancel);
      await admin.disconnect();
    }
  }

  invalidateHealth(): void {
    this.consumerGroupOffsets.clear();
  }

  async topicDetail(topic: string, report?: KafkaProgressReporter): Promise<TopicDetailDto> {
    const admin = this.kafka.admin();
    report?.({
      value: 62,
      phase: "connect",
      label: "Connecting to Kafka",
      detail: "Opening a read-only Kafka admin connection.",
    });
    await admin.connect();
    try {
      report?.({
        value: 78,
        phase: "metadata",
        label: "Reading topic metadata",
        detail: `Requesting partition topology for ${topic}.`,
      });
      const response = await admin.fetchTopicMetadata({ topics: [topic] });
      const metadata = response.topics.find((candidate) => candidate.name === topic);
      if (!metadata) throw new Error(`Topic "${topic}" was not returned by Kafka`);
      const detail = toTopicDetail(metadata);
      report?.({
        value: 98,
        phase: "metadata",
        label: "Topic metadata received",
        detail: `${detail.partitionCount} partition(s) found.`,
      });
      return detail;
    } finally {
      await admin.disconnect();
    }
  }

  /** Read topic-level config entries through the Admin API (read-only). */
  async topicConfig(topic: string, report?: KafkaProgressReporter): Promise<TopicConfigDto> {
    const admin = this.kafka.admin();
    report?.({
      value: 62,
      phase: "connect",
      label: "Connecting to Kafka",
      detail: "Opening a read-only Kafka admin connection.",
    });
    await admin.connect();
    try {
      report?.({
        value: 78,
        phase: "config",
        label: "Reading topic configuration",
        detail: `Requesting config entries for ${topic}.`,
      });
      const config = await fetchTopicConfig(admin, topic);
      report?.({
        value: 98,
        phase: "config",
        label: "Topic configuration received",
        detail: `${config.entries.length} config entr${config.entries.length === 1 ? "y" : "ies"}.`,
      });
      return config;
    } finally {
      await admin.disconnect();
    }
  }

  /** List consumer groups with committed offsets on one topic and aggregate their lag. */
  async topicConsumers(
    topic: string,
    report?: KafkaProgressReporter,
    signal?: AbortSignal,
  ): Promise<TopicConsumersDto> {
    const admin = this.kafka.admin();
    if (signal?.aborted) throw new Error("Topic Consumers was cancelled");
    const cancel = () => void admin.disconnect().catch(() => undefined);
    signal?.addEventListener("abort", cancel, { once: true });
    await admin.connect();
    try {
      return await fetchTopicConsumers(
        admin,
        topic,
        ({ completed, total, topicConsumerGroup }) => {
          report?.({
            value: total === 0 ? 92 : 62 + Math.round((completed / total) * 30),
            phase: "groups",
            label: "Scanning consumer group offsets",
            detail: `${completed}/${total} consumer group(s) checked.`,
            completed,
            total,
            topicConsumerGroup,
          });
        },
        this.consumerGroupOffsets,
        signal,
      );
    } finally {
      signal?.removeEventListener("abort", cancel);
      await admin.disconnect();
    }
  }

  /** Read configuration entries for one broker through the Admin API. */
  async brokerConfig(brokerId: number): Promise<BrokerConfigDto> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      return await fetchBrokerConfig(admin, brokerId);
    } finally {
      await admin.disconnect();
    }
  }

  async acls(): Promise<import("../../common/ipc").KafkaAclResultDto> {
    const admin = this.kafka.admin();
    const result = await fetchAcls(admin);
    return result;
  }

  /** Read one bounded partition window directly through Kafka Fetch, without a consumer group. */
  browseMessages(
    request: Pick<MessageBrowseRequest, "topic" | "partition" | "startMode" | "offset" | "timestamp" | "limit">,
  ): Promise<MessageBrowseDto> {
    return this.getMessageCluster().then((cluster) => browseMessagesWithCluster(cluster, request));
  }

  /** List all consumer groups with state and member count (read-only, no join or commit). */
  async listGroups(): Promise<ConsumerGroupSummaryDto[]> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      return await listConsumerGroups(admin, this.consumerGroupOffsets);
    } finally {
      await admin.disconnect();
    }
  }

  /** Describe one consumer group with per-partition committed offsets, high-watermarks and lag. */
  async groupDetail(groupId: string): Promise<ConsumerGroupDetailDto> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      return await fetchConsumerGroupDetail(admin, groupId, this.consumerGroupOffsets);
    } finally {
      await admin.disconnect();
    }
  }

  async produce(
    request: Pick<ProduceRequest, "topic" | "key" | "value" | "headers" | "partition">,
  ): Promise<ProduceResultDto> {
    const producer = this.kafka.producer({ idempotent: true });
    await producer.connect();
    try {
      const [result] = await producer.send({
        topic: request.topic,
        messages: [{ key: request.key, value: request.value, headers: request.headers, partition: request.partition }],
      });
      if (result.baseOffset === undefined) throw new Error("Kafka did not return the produced record offset");
      return { topic: request.topic, partition: result.partition, offset: result.baseOffset };
    } finally {
      await producer.disconnect();
    }
  }

  /** Delete one topic through the Admin API: the brokers discard its partitions and records. */
  async deleteTopic(topic: string): Promise<DeleteTopicResultDto> {
    if (!topic) throw new Error("a topic name is required");
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      await admin.deleteTopics({ topics: [topic] });
      return { topic };
    } finally {
      await admin.disconnect();
    }
  }

  async resetOffsets(
    request: Pick<ResetOffsetsRequest, "groupId" | "topic" | "partition" | "mode" | "offset" | "timestamp">,
  ): Promise<ResetOffsetsResultDto> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      let offset = request.offset;
      if (request.mode === "earliest" || request.mode === "latest") {
        const offsets = await admin.fetchTopicOffsets(request.topic);
        const selected = offsets.find((entry) => entry.partition === request.partition);
        offset = request.mode === "earliest" ? selected?.low : selected?.offset;
      } else if (request.mode === "timestamp") {
        if (request.timestamp === undefined) throw new Error("timestamp is required for timestamp reset");
        const offsets = await admin.fetchTopicOffsetsByTimestamp(request.topic, request.timestamp);
        offset = offsets.find((entry) => entry.partition === request.partition)?.offset;
      }
      if (!offset || !/^\d+$/.test(offset)) throw new Error("a valid reset offset could not be resolved");
      await admin.setOffsets({
        groupId: request.groupId,
        topic: request.topic,
        partitions: [{ partition: request.partition, offset }],
      });
      return { groupId: request.groupId, topic: request.topic, partition: request.partition, offset };
    } finally {
      await admin.disconnect();
    }
  }

  /** Tear down the connection: close the port-forwards (or the tracked direct sockets). */
  async disconnect(): Promise<void> {
    this.disconnected = true;
    this.consumerGroupOffsets.clear();
    this.groupOffsetBatchReader = undefined;
    const pendingCluster = this.messageClusterConnect;
    const cluster = this.messageCluster ?? (await pendingCluster?.catch(() => undefined));
    this.messageCluster = undefined;
    this.messageClusterConnect = undefined;
    await cluster?.disconnect().catch(() => undefined);
    this.portForwards?.closeAll();
    this.directClose?.();
  }

  private async getMessageCluster(): Promise<KafkaReadCluster> {
    if (this.disconnected) throw new Error("Kafka connection is disconnected");
    if (this.messageCluster) return this.messageCluster;
    if (!this.messageClusterConnect) {
      const cluster = createKafkaJsReadCluster(this.kafka);
      this.messageClusterConnect = cluster
        .connect()
        .then(() => {
          if (this.disconnected) {
            return cluster.disconnect().then(() => {
              throw new Error("Kafka connection was disconnected while opening the read cluster");
            });
          }
          this.messageCluster = cluster;
          return cluster;
        })
        .catch((error: unknown) => {
          this.messageClusterConnect = undefined;
          throw error;
        });
    }
    return this.messageClusterConnect;
  }

  private async fetchHighWatermarks(
    partitions: OffsetBatchPartition[],
    signal?: AbortSignal,
  ): Promise<Map<string, Map<number, string>>> {
    return fetchHighWatermarks(await this.getMessageCluster(), partitions, 500, signal);
  }

  private async fetchGroupOffsets(groupIds: string[], signal?: AbortSignal) {
    if (!this.groupOffsetBatchReader) {
      this.groupOffsetBatchReader = new KafkaGroupOffsetBatchReader(await this.getMessageCluster());
    }
    return this.groupOffsetBatchReader.read(groupIds, signal);
  }
}

/**
 * Direct connection strategy: kafkajs straight to the bootstrap (no port-forward) — for a Kafka
 * reachable from this machine (public / VPN, e.g. MSK).
 */
export function connectDirect(options: {
  clientId?: string;
  bootstrap: string;
  ssl?: TlsOptions | boolean;
  sasl?: SASLOptions;
}): Promise<KafkaConnection> {
  return KafkaConnection.connect({
    clientId: options.clientId ?? "freelens-kafka-direct",
    bootstrap: options.bootstrap,
    ssl: options.ssl,
    sasl: options.sasl,
  });
}
