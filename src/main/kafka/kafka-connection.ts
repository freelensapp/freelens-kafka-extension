import { mkdir, rename, writeFile } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";
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
import { answerWithin, describeError, fetchTopicSizes, TopicSizesStepError } from "./log-dirs";
import { browseMessagesWithCluster, createKafkaJsReadCluster, type KafkaReadCluster } from "./message-fetch";
import { fetchHighWatermarks, type OffsetBatchPartition } from "./offset-batch";
import { PortForwardManager } from "./port-forward-manager";
import { createDirectSocketFactory, createRedirectSocketFactory } from "./redirect";
import { describeConnections, type ProbeCluster, probeTopicSizes, ReaderActivity } from "./size-probe";
import { fetchTopicConfig } from "./topic-config";
import { toTopicDetail } from "./topic-metadata";
import type { ConnectionOptions as TlsOptions } from "node:tls";

import type {
  BrokerConfigDto,
  ClusterOverviewHealthDto,
  ConsumerGroupDetailDto,
  ConsumerGroupSummaryDto,
  DeleteTopicResultDto,
  DeleteTopicsResultDto,
  MessageBrowseDto,
  MessageBrowseRequest,
  ProduceRequest,
  ProduceResultDto,
  ResetOffsetsRequest,
  ResetOffsetsResultDto,
  TopicConfigDto,
  TopicConsumersDto,
  TopicDetailDto,
  TopicSizesDto,
} from "../../common/ipc";
import type { Forwarder } from "./port-forward-manager";
import type { KafkaProgressReporter } from "./progress";
import type { BrokerRef } from "./types";

/** Issue #82 diagnostic build: at most one size probe every ten minutes per connection. */
const SIZE_PROBE_EVERY_MS = 10 * 60_000;

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
  /** Issue #82 diagnostic build: the sockets of the connection, for the probe client. */
  private socketFactory?: ReturnType<typeof createRedirectSocketFactory>;
  private disconnected = false;
  private messageCluster?: KafkaReadCluster;
  private messageClusterConnect?: Promise<KafkaReadCluster>;
  private groupOffsetBatchReader?: KafkaGroupOffsetBatchReader;
  private readonly consumerGroupOffsets: ConsumerGroupOffsetIndex;
  /** Issue #82 diagnostic build: what the readers of the shared read cluster do, and the last probe. */
  private readonly activity = new ReaderActivity();
  private sizeProbe?: { shown: string; startedAt: number; running: boolean; summary?: string };

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
    this.socketFactory = socketFactory;
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

  /**
   * Issue #82 diagnostic build: a client for the size probe, like the one of the connection (same
   * credentials, TLS and sockets, so a disconnect closes its sockets too) but with the KafkaJS
   * retries off, so that a connection the broker closes is reported instead of retried, and on the
   * given bootstrap addresses only when the probe asks one broker at a time.
   */
  private probeClient(bootstrap?: readonly string[]): Kafka {
    return new Kafka({
      clientId: this.options.clientId ?? "freelens-kafka",
      brokers: bootstrap ? [...bootstrap] : splitBootstrap(this.options.bootstrap),
      ssl: this.options.ssl,
      sasl: this.options.sasl,
      logLevel: logLevel.NOTHING,
      socketFactory: this.socketFactory,
      connectionTimeout: 10_000,
      retry: { retries: 0 },
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
    return this.activity.track(
      "messages (Metadata if needed, ListOffsets, Fetch)",
      () => this.getMessageCluster().then((cluster) => browseMessagesWithCluster(cluster, request)),
      (page) => `${page.messages.length} record(s)`,
    );
  }

  /**
   * Sizes on disk per topic and partition through DescribeLogDirs on every broker (read-only). When
   * the metadata read fails because the peer dropped the connection, the sizes are read once more:
   * KafkaJS opens a new connection by itself. A failure reaches the page with the step, the broker,
   * what is known of the connection and how long the attempt took; what a successful read had to
   * work around (a second read, a broker left out) travels in `notes`. With a `deadline`
   * (`Date.now()` milliseconds) both reads end by then, each step failing with its name: the
   * caller sets it before its own timeout, which can only say that the sizes timed out.
   */
  async topicSizes(
    topics: readonly string[],
    signal?: AbortSignal,
    deadline = Number.POSITIVE_INFINITY,
  ): Promise<TopicSizesDto> {
    const notes: string[] = [];
    const log = (line: string) => {
      notes.push(line);
      console.warn(`[kafka] topic sizes: ${line}`);
    };
    const attempt = async (): Promise<{ sizes: TopicSizesDto } | { error: unknown; detail: string }> => {
      const startedAt = Date.now();
      try {
        const sizes = await this.activity.track("topic sizes (Metadata, DescribeLogDirs v1)", async () => {
          const cluster = await answerWithin(deadline - Date.now(), () => this.getMessageCluster()).catch(
            (error: unknown) => {
              throw new TopicSizesStepError("opening the read cluster", error);
            },
          );
          return fetchTopicSizes(cluster, topics, { signal, log, deadline });
        });
        return { sizes };
      } catch (error) {
        const reason = error instanceof TopicSizesStepError ? error.message : describeError(error);
        return { error, detail: `${reason} (after ${Date.now() - startedAt} ms)` };
      }
    };
    const done = (sizes: TopicSizesDto): TopicSizesDto => (notes.length > 0 ? { ...sizes, notes } : sizes);
    const failed = (message: string): Error => Object.assign(new Error(message), { name: "TopicSizesError" });

    // The caller spent all the time before the read could start: no step ran, so none is blamed.
    if (!(deadline - Date.now() > 0)) throw failed("not read: no time was left when the read could start");
    const first = await attempt();
    if ("sizes" in first) return done(first.sizes);
    if (!(first.error instanceof TopicSizesStepError) || signal?.aborted || this.disconnected) throw first.error;
    if (!first.error.dropped) throw failed(`${first.detail}${this.sizeProbeNote(topics, first.detail)}`);
    log(`${first.detail}; reading once more`);
    const second = await attempt();
    if ("sizes" in second) {
      log("the second read succeeded");
      // Issue #82 diagnostic build: a connection the broker closed is what the probe is for, even when the
      // second read worked; the hover text of the sizes then says where its report goes.
      const note = this.sizeProbeNote(topics, `first attempt: ${first.detail}; the second read succeeded`);
      if (note) log(note.replace(/^\. /, ""));
      return done(second.sizes);
    }
    const reason = `${second.detail}; first attempt: ${first.detail}`;
    throw failed(`${reason}${this.sizeProbeNote(topics, reason)}`);
  }

  /**
   * Issue #82 diagnostic build: after a failed size read, start the probe of `size-probe.ts` in the
   * background (at most one every ten minutes) and say in the hover text where its report goes; a
   * later failure says it again with the one-line summary of the finished probe.
   */
  private sizeProbeNote(topics: readonly string[], failure: string): string {
    // The unit tests of this class turn it off: the probe would run against their fake clusters.
    if (process.env.FREELENS_KAFKA_SIZE_PROBE === "off") return "";
    // Whatever goes wrong here must not replace the error of the size read.
    try {
      return this.startSizeProbe(topics, failure);
    } catch {
      return "";
    }
  }

  private startSizeProbe(topics: readonly string[], failure: string): string {
    const current = this.sizeProbe;
    if (current?.running) return `. Diagnostic probe running, report: ${current.shown}`;
    if (current && Date.now() - current.startedAt < SIZE_PROBE_EVERY_MS) {
      return `. Diagnostic report: ${current.shown}${current.summary ? ` (probe: ${current.summary})` : ""}`;
    }
    const stamp = new Date()
      .toISOString()
      .replace(/[-:]/g, "")
      .replace(/\.\d+Z$/, "Z");
    const override = process.env.FREELENS_KAFKA_DIAGNOSTICS_DIR;
    let directory = override || join(homedir(), "freelens-kafka-diagnostics");
    let file = join(directory, `topic-sizes-${stamp}.txt`);
    const probe = {
      shown: override ? file : `~/freelens-kafka-diagnostics/topic-sizes-${stamp}.txt`,
      startedAt: Date.now(),
      running: true,
      summary: undefined as string | undefined,
    };
    this.sizeProbe = probe;
    // The whole report each time, through a file of its own and a rename: a quit in the middle leaves the last one.
    const writeAtomically = async (text: string) => {
      await mkdir(directory, { recursive: true });
      await writeFile(`${file}.partial`, text, "utf8");
      await rename(`${file}.partial`, file);
    };
    const write = async (text: string) => {
      try {
        await writeAtomically(text);
      } catch {
        // A sandbox may forbid the home folder: fall back to the temporary folder once.
        if (directory.startsWith(tmpdir())) throw new Error("the report could not be written");
        directory = join(tmpdir(), "freelens-kafka-diagnostics");
        file = join(directory, `topic-sizes-${stamp}.txt`);
        probe.shown = file;
        await writeAtomically(text);
      }
    };
    void probeTopicSizes(
      {
        failure,
        topics,
        bootstrap: splitBootstrap(this.options.bootstrap),
        settings: this.describeSettings(),
        activity: this.activity.describe(),
        sharedConnections: describeConnections(this.messageCluster as ProbeCluster | undefined),
        names: this.consumerGroupOffsets.knownGroupIds(),
      },
      {
        createCluster: (bootstrap) => createKafkaJsReadCluster(this.probeClient(bootstrap)) as ProbeCluster,
        createAdmin: () => this.probeClient().admin(),
        write,
        stopped: () => this.disconnected,
        home: homedir(),
      },
    )
      .then(
        (summary) => {
          probe.summary = summary;
        },
        (error: unknown) => {
          probe.summary = `the probe failed (${error instanceof Error ? error.name : "unknown error"})`;
        },
      )
      .finally(() => {
        probe.running = false;
      });
    return `. Diagnostic probe started, report in about two minutes: ${probe.shown}`;
  }

  /** SASL mechanism, TLS and strategy, without addresses or secrets. */
  private describeSettings(): string {
    const sasl = this.options.sasl ? `SASL ${this.options.sasl.mechanism}` : "no SASL";
    const tls = this.options.ssl ? "TLS" : "no TLS";
    const strategy = this.options.forwarder ? "port-forward" : "direct";
    return `${sasl}, ${tls}, ${strategy} connection`;
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

  /**
   * Delete several topics through one Admin session, one request per topic: a batch DeleteTopics
   * call surfaces only the first broker error, so the per-topic outcome would be lost (SPEC-009 REQ-205).
   */
  async deleteTopics(topics: string[]): Promise<DeleteTopicsResultDto> {
    const names = [...new Set(topics.filter((topic) => Boolean(topic)))];
    if (names.length === 0) throw new Error("at least one topic name is required");
    const admin = this.kafka.admin();
    await admin.connect();
    const result: DeleteTopicsResultDto = { deleted: [], failed: [] };
    try {
      for (const topic of names) {
        try {
          await admin.deleteTopics({ topics: [topic] });
          result.deleted.push(topic);
        } catch (error) {
          result.failed.push({ topic, error: error instanceof Error ? error.message : String(error) });
        }
      }
      return result;
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
    return this.activity.track(
      `watermarks of ${partitions.length} partition(s) (Metadata if needed, ListOffsets)`,
      async () => fetchHighWatermarks(await this.getMessageCluster(), partitions, 500, signal),
    );
  }

  private async fetchGroupOffsets(groupIds: string[], signal?: AbortSignal) {
    const reader = "lag of consumer groups (Metadata once, FindCoordinator v4, OffsetFetch v8)";
    return this.activity.track(
      reader,
      async () => {
        if (!this.groupOffsetBatchReader) {
          this.groupOffsetBatchReader = new KafkaGroupOffsetBatchReader(await this.getMessageCluster(), {
            onRequestError: (request, error) => {
              try {
                this.activity.note(reader, `${request} failed: ${describeError(error)}`);
              } catch {
                // A diagnostic note must never break the lag reader.
              }
            },
          });
        }
        return this.groupOffsetBatchReader.read(groupIds, signal);
      },
      (result) =>
        `${result.resolvedGroupIds.size} of ${groupIds.length} group(s) resolved${result.supported ? "" : ", batch not supported"}`,
    );
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
