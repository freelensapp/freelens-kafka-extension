/** Read-only consumer group operations via KafkaJS Admin — no join, no commit, no mutation. */

import { KAFKA_CLUSTER_HEALTH_CACHE_TTL_MS } from "../../common/constants";

import type { Admin } from "kafkajs";

import type {
  ConsumerGroupDetailDto,
  ConsumerGroupSummaryDto,
  GroupTopicOffsetDto,
  TopicConsumerGroupDto,
  TopicConsumersDto,
} from "../../common/ipc";
import type { GroupOffsetBatchResult } from "./group-offset-batch";

const TOPIC_CONSUMER_CONCURRENCY = 16;

export interface TopicConsumerScanProgress {
  completed: number;
  total: number;
  topicConsumerGroup?: TopicConsumerGroupDto;
}

export interface IndexedTopicConsumer {
  groupId: string;
  committed: Array<{ partition: number; offset: string }>;
}

export type GroupOffsetBatchReader = (groupIds: string[], signal?: AbortSignal) => Promise<GroupOffsetBatchResult>;

export interface ConsumerGroupOffsetScanStatus {
  completedAt?: number;
  resolvedGroups: number;
  startedAt?: number;
  totalGroups: number;
  unavailableGroups: number;
}

interface ScanProgressListener {
  report: (progress: TopicConsumerScanProgress) => void;
  requestedTopic: string;
}

interface ListedGroup {
  groupId: string;
  protocolType: string;
}

export class ConsumerGroupOffsetIndex {
  private buildAbort?: AbortController;
  private data?: Map<string, IndexedTopicConsumer[]>;
  private inFlight?: Promise<Map<string, IndexedTopicConsumer[]>>;
  private generation = 0;
  private listedGroups?: ListedGroup[];
  private listedGroupsAt?: number;
  private listedGroupsInFlight?: Promise<ListedGroup[]>;
  private readonly progressListeners = new Map<symbol, ScanProgressListener>();
  private readonly subscribers = new Set<symbol>();
  private scan: ConsumerGroupOffsetScanStatus = {
    resolvedGroups: 0,
    totalGroups: 0,
    unavailableGroups: 0,
  };
  private unavailableGroupIds = new Set<string>();

  constructor(
    private readonly readBatch?: GroupOffsetBatchReader,
    private readonly now: () => number = Date.now,
    private readonly ttlMs = KAFKA_CLUSTER_HEALTH_CACHE_TTL_MS,
  ) {}

  async entriesForTopic(
    admin: Admin,
    topic: string,
    onProgress?: (progress: TopicConsumerScanProgress) => void,
    signal?: AbortSignal,
  ): Promise<IndexedTopicConsumer[]> {
    return (await this.all(admin, topic, onProgress, signal)).get(topic) ?? [];
  }

  async all(
    admin: Admin,
    requestedTopic = "",
    onProgress?: (progress: TopicConsumerScanProgress) => void,
    signal?: AbortSignal,
  ): Promise<Map<string, IndexedTopicConsumer[]>> {
    this.throwIfAborted(signal);
    if (this.data && (this.scan.completedAt ?? 0) + this.ttlMs > this.now()) {
      onProgress?.({ completed: this.scan.totalGroups, total: this.scan.totalGroups });
      return this.data;
    }
    if (this.data) this.data = undefined;
    if (this.inFlight && this.buildAbort?.signal.aborted) {
      const cancelled = this.inFlight;
      await cancelled.catch(() => undefined);
      if (this.inFlight === cancelled) {
        this.inFlight = undefined;
        this.buildAbort = undefined;
      }
    }
    const listenerId = Symbol("consumer-group-offset-progress");
    const subscriberId = Symbol("consumer-group-offset-subscriber");
    this.subscribers.add(subscriberId);
    let rejectAbort: ((error: Error) => void) | undefined;
    const aborted = signal
      ? new Promise<never>((_resolve, reject) => {
          rejectAbort = reject;
        })
      : undefined;
    const onAbort = () => {
      rejectAbort?.(new Error("Consumer group offset scan was cancelled"));
      this.removeSubscriber(subscriberId);
    };
    signal?.addEventListener("abort", onAbort, { once: true });
    if (onProgress) {
      this.progressListeners.set(listenerId, { report: onProgress, requestedTopic });
      if (this.scan.startedAt !== undefined) {
        onProgress({
          completed: this.scan.resolvedGroups + this.scan.unavailableGroups,
          total: this.scan.totalGroups,
        });
      }
    }
    if (!this.inFlight) {
      const generation = this.generation;
      const buildAbort = new AbortController();
      this.buildAbort = buildAbort;
      const scan: ConsumerGroupOffsetScanStatus = {
        resolvedGroups: 0,
        startedAt: this.now(),
        totalGroups: 0,
        unavailableGroups: 0,
      };
      this.scan = scan;
      let pending!: Promise<Map<string, IndexedTopicConsumer[]>>;
      pending = this.build(admin, scan, buildAbort.signal)
        .then((data) => {
          if (generation === this.generation) this.data = data;
          return data;
        })
        .catch((error: unknown) => {
          throw error;
        })
        .finally(() => {
          if (generation === this.generation && this.inFlight === pending) this.inFlight = undefined;
          if (generation === this.generation && this.buildAbort === buildAbort) this.buildAbort = undefined;
        });
      this.inFlight = pending;
    }
    try {
      return await (aborted ? Promise.race([this.inFlight, aborted]) : this.inFlight);
    } finally {
      signal?.removeEventListener("abort", onAbort);
      this.progressListeners.delete(listenerId);
      this.removeSubscriber(subscriberId);
    }
  }

  clear(): void {
    this.generation++;
    this.buildAbort?.abort();
    this.buildAbort = undefined;
    this.data = undefined;
    this.inFlight = undefined;
    this.listedGroups = undefined;
    this.listedGroupsAt = undefined;
    this.listedGroupsInFlight = undefined;
    this.progressListeners.clear();
    this.subscribers.clear();
    this.unavailableGroupIds.clear();
    this.scan = { resolvedGroups: 0, totalGroups: 0, unavailableGroups: 0 };
  }

  scanStatus(): ConsumerGroupOffsetScanStatus {
    return { ...this.scan };
  }

  isGroupUnavailable(groupId: string): boolean {
    return this.unavailableGroupIds.has(groupId);
  }

  async groups(admin: Admin, signal?: AbortSignal): Promise<ListedGroup[]> {
    this.throwIfAborted(signal);
    if (this.listedGroups && (this.listedGroupsAt ?? 0) + this.ttlMs > this.now()) return this.listedGroups;
    if (!this.listedGroupsInFlight) {
      let pending!: Promise<ListedGroup[]>;
      pending = admin
        .listGroups()
        .then(({ groups }) => {
          this.throwIfAborted(signal);
          this.listedGroups = groups.map(({ groupId, protocolType }) => ({ groupId, protocolType }));
          this.listedGroupsAt = this.now();
          return this.listedGroups;
        })
        .finally(() => {
          if (this.listedGroupsInFlight === pending) this.listedGroupsInFlight = undefined;
        });
      this.listedGroupsInFlight = pending;
    }
    return this.listedGroupsInFlight;
  }

  groupsIfAvailable(): ListedGroup[] | Promise<ListedGroup[]> | undefined {
    if (this.listedGroups && (this.listedGroupsAt ?? 0) + this.ttlMs > this.now()) return this.listedGroups;
    return this.listedGroupsInFlight;
  }

  entriesForGroupIfAvailable(
    groupId: string,
  ): Promise<Array<{ topic: string; committed: IndexedTopicConsumer["committed"] }> | undefined> | undefined {
    const source =
      this.data && (this.scan.completedAt ?? 0) + this.ttlMs > this.now() ? Promise.resolve(this.data) : this.inFlight;
    return source?.then((byTopic) => {
      if (this.isGroupUnavailable(groupId)) return undefined;
      return [...byTopic].flatMap(([topic, consumers]) => {
        const consumer = consumers.find((candidate) => candidate.groupId === groupId);
        return consumer ? [{ topic, committed: consumer.committed }] : [];
      });
    });
  }

  private removeGroup(byTopic: Map<string, IndexedTopicConsumer[]>, groupId: string): void {
    for (const [topic, consumers] of byTopic) {
      const remaining = consumers.filter((consumer) => consumer.groupId !== groupId);
      if (remaining.length === 0) byTopic.delete(topic);
      else byTopic.set(topic, remaining);
    }
  }

  private emitProgress(
    scan: ConsumerGroupOffsetScanStatus,
    byTopic?: Map<string, IndexedTopicConsumer[]>,
    groupId?: string,
  ): void {
    if (scan !== this.scan) return;
    for (const { report, requestedTopic } of this.progressListeners.values()) {
      const requestedMatch = groupId
        ? byTopic?.get(requestedTopic)?.find((consumer) => consumer.groupId === groupId)
        : undefined;
      try {
        report({
          completed: scan.resolvedGroups + scan.unavailableGroups,
          total: scan.totalGroups,
          ...(requestedMatch
            ? {
                topicConsumerGroup: {
                  groupId: requestedMatch.groupId,
                  state: "Loading",
                  memberCount: 0,
                  totalLag: "—",
                },
              }
            : {}),
        });
      } catch {
        // A progress observer must not abort the read-only scan.
      }
    }
  }

  private async build(
    admin: Admin,
    scan: ConsumerGroupOffsetScanStatus,
    signal: AbortSignal,
  ): Promise<Map<string, IndexedTopicConsumer[]>> {
    const unavailableGroupIds = new Set<string>();
    this.throwIfAborted(signal);
    const listedGroups = await this.groups(admin, signal);
    this.throwIfAborted(signal);
    const groupIds = listedGroups.filter((group) => group.protocolType === "consumer").map((group) => group.groupId);
    scan.totalGroups = groupIds.length;
    const byTopic = new Map<string, IndexedTopicConsumer[]>();
    this.emitProgress(scan);
    let fallbackGroupIds = groupIds;
    if (this.readBatch) {
      try {
        const batch = await this.readBatch(groupIds, signal);
        this.throwIfAborted(signal);
        const resolvedGroupIds = new Set([...batch.resolvedGroupIds].filter((groupId) => groupIds.includes(groupId)));
        for (const [topic, consumers] of batch.offsetsByTopic) {
          const requestedConsumers = consumers
            .filter(({ groupId }) => groupIds.includes(groupId))
            .map(({ groupId, committed }) => ({ groupId, committed }));
          if (requestedConsumers.length > 0) byTopic.set(topic, requestedConsumers);
        }
        for (const groupId of groupIds) {
          if (!resolvedGroupIds.has(groupId)) continue;
          scan.resolvedGroups++;
          this.emitProgress(scan, byTopic, groupId);
        }
        fallbackGroupIds = groupIds.filter((groupId) => !resolvedGroupIds.has(groupId));
      } catch {
        this.throwIfAborted(signal);
        fallbackGroupIds = groupIds;
      }
    }
    await mapWithConcurrency(fallbackGroupIds, TOPIC_CONSUMER_CONCURRENCY, async (groupId) => {
      this.throwIfAborted(signal);
      try {
        const offsets = await admin.fetchOffsets({ groupId, resolveOffsets: false });
        this.throwIfAborted(signal);
        this.removeGroup(byTopic, groupId);
        for (const topicOffsets of offsets) {
          const committed = topicOffsets.partitions
            .filter((partition) => partition.offset !== "-1")
            .map((partition) => ({ partition: partition.partition, offset: partition.offset }));
          if (committed.length === 0) continue;
          const entries = byTopic.get(topicOffsets.topic) ?? [];
          entries.push({ groupId, committed });
          byTopic.set(topicOffsets.topic, entries);
        }
        scan.resolvedGroups++;
      } catch {
        this.throwIfAborted(signal);
        scan.unavailableGroups++;
        unavailableGroupIds.add(groupId);
      } finally {
        this.emitProgress(scan, byTopic, groupId);
      }
    });
    this.throwIfAborted(signal);
    this.unavailableGroupIds = unavailableGroupIds;
    scan.completedAt = this.now();
    return byTopic;
  }

  private removeSubscriber(subscriberId: symbol): void {
    this.subscribers.delete(subscriberId);
    if (this.subscribers.size === 0 && this.inFlight) this.buildAbort?.abort();
  }

  private throwIfAborted(signal?: AbortSignal): void {
    if (signal?.aborted) throw new Error("Consumer group offset scan was cancelled");
  }
}

function partitionLag(committed: string, highWatermark?: string): string {
  if (committed === "-1" || highWatermark === undefined) return "—";
  const difference = BigInt(highWatermark) - BigInt(committed);
  return (difference < 0n ? 0n : difference).toString();
}

function totalLag(lags: string[]): string {
  const known = lags.filter((lag) => lag !== "—");
  return known.length === 0 ? "—" : known.reduce((sum, lag) => (BigInt(sum) + BigInt(lag)).toString(), "0");
}

export async function mapWithConcurrency<T, R>(
  items: T[],
  limit: number,
  mapper: (item: T) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let nextIndex = 0;

  const worker = async (): Promise<void> => {
    while (nextIndex < items.length) {
      const index = nextIndex++;
      results[index] = await mapper(items[index]);
    }
  };

  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return results;
}

export async function listConsumerGroups(
  admin: Admin,
  index = new ConsumerGroupOffsetIndex(),
): Promise<ConsumerGroupSummaryDto[]> {
  const shared = index.groupsIfAvailable();
  const overview = shared
    ? await Promise.resolve(shared).catch(async () => (await admin.listGroups()).groups)
    : (await admin.listGroups()).groups;
  if (overview.length === 0) return [];

  const { groups: descriptions } = await admin.describeGroups(overview.map((g) => g.groupId));

  return descriptions
    .map((desc) => ({
      groupId: desc.groupId,
      state: desc.state,
      protocolType: desc.protocolType,
      memberCount: desc.members.length,
    }))
    .sort((a, b) => a.groupId.localeCompare(b.groupId));
}

export async function fetchConsumerGroupDetail(
  admin: Admin,
  groupId: string,
  index?: ConsumerGroupOffsetIndex,
): Promise<ConsumerGroupDetailDto> {
  const { groups } = await admin.describeGroups([groupId]);
  const desc = groups[0];
  if (!desc) throw new Error(`Consumer group "${groupId}" not found`);

  const indexed = index?.entriesForGroupIfAvailable(groupId);
  const indexedOffsets = indexed ? await indexed.catch(() => undefined) : undefined;
  const committedRaw = indexedOffsets
    ? indexedOffsets.map(({ topic, committed }) => ({
        topic,
        partitions: committed.map(({ partition, offset }) => ({ partition, offset, metadata: null })),
      }))
    : await admin.fetchOffsets({ groupId });

  const uniqueTopics = [...new Set(committedRaw.map((t) => t.topic))];

  const hwmMap = new Map<string, Map<number, string>>();
  const topicWatermarks = await mapWithConcurrency(uniqueTopics, TOPIC_CONSUMER_CONCURRENCY, async (topic) => {
    const topicOffsets = await admin.fetchTopicOffsets(topic);
    return [topic, new Map(topicOffsets.map((p) => [p.partition, p.high]))] as const;
  });
  for (const [topic, watermarks] of topicWatermarks) hwmMap.set(topic, watermarks);

  const topicOffsets: GroupTopicOffsetDto[] = committedRaw.map((topicData) => {
    const topicHwm = hwmMap.get(topicData.topic);
    const partitions = topicData.partitions
      .map((p) => {
        const hwm = topicHwm?.get(p.partition) ?? null;
        const committed = p.offset;
        const lag = partitionLag(committed, hwm ?? undefined);
        return { partition: p.partition, committedOffset: committed, highWatermark: hwm ?? "—", lag };
      })
      .sort((a, b) => a.partition - b.partition);

    return { topic: topicData.topic, partitions, totalLag: totalLag(partitions.map((partition) => partition.lag)) };
  });

  return {
    groupId: desc.groupId,
    state: desc.state,
    protocol: desc.protocol,
    protocolType: desc.protocolType,
    members: desc.members.map((m) => ({
      memberId: m.memberId,
      clientId: m.clientId,
      clientHost: m.clientHost,
    })),
    topicOffsets,
  };
}

export async function fetchTopicConsumers(
  admin: Admin,
  topic: string,
  onProgress?: (progress: TopicConsumerScanProgress) => void,
  index = new ConsumerGroupOffsetIndex(),
  signal?: AbortSignal,
): Promise<TopicConsumersDto> {
  const subscribed = await index.entriesForTopic(admin, topic, onProgress, signal);
  if (signal?.aborted) throw new Error("Topic Consumers was cancelled");
  if (subscribed.length === 0) return { topic, groups: [] };

  const [{ groups: descriptions }, topicOffsets] = await Promise.all([
    admin.describeGroups(subscribed.map((candidate) => candidate.groupId)),
    admin.fetchTopicOffsets(topic),
  ]);
  const descriptionsByGroupId = new Map(descriptions.map((description) => [description.groupId, description]));
  const highWatermarks = new Map(topicOffsets.map((partition) => [partition.partition, partition.high]));

  const groups: TopicConsumerGroupDto[] = subscribed
    .map(({ groupId, committed }) => {
      const description = descriptionsByGroupId.get(groupId);
      return {
        groupId,
        state: description?.state ?? "Unknown",
        memberCount: description?.members.length ?? 0,
        totalLag: (() => {
          const measured = totalLag(
            committed.map((partition) => partitionLag(partition.offset, highWatermarks.get(partition.partition))),
          );
          return index.isGroupUnavailable(groupId) && measured !== "—" ? `≥${measured}` : measured;
        })(),
      };
    })
    .sort((left, right) => left.groupId.localeCompare(right.groupId));

  return { topic, groups };
}
