import { ConsumerGroupOffsetIndex, type IndexedTopicConsumer, mapWithConcurrency } from "./group-fetch";
import { toTopicDetail } from "./topic-metadata";

import type { Admin } from "kafkajs";

import type { ClusterOverviewHealthDto } from "../../common/ipc";
import type { OffsetBatchPartition } from "./offset-batch";

const HEALTH_TOPIC_CONCURRENCY = 16;

export type HighWatermarkReader = (
  partitions: OffsetBatchPartition[],
  signal?: AbortSignal,
) => Promise<Map<string, Map<number, string>>>;

export interface ClusterHealthProgress {
  completed?: number;
  detail?: string;
  health: Partial<ClusterOverviewHealthDto>;
  phase: "topology" | "groups" | "watermarks" | "complete";
  total?: number;
}

interface TopicLagMeasurement {
  complete: boolean;
  knownLag: bigint;
}

export async function fetchClusterHealth(
  admin: Admin,
  consumerGroupOffsets: ConsumerGroupOffsetIndex,
  onProgress?: (progress: ClusterHealthProgress) => void,
  readHighWatermarks?: HighWatermarkReader,
  signal?: AbortSignal,
  now: () => number = Date.now,
): Promise<ClusterOverviewHealthDto> {
  if (signal?.aborted) throw new Error("Cluster health was cancelled");
  const [cluster, metadata] = await Promise.all([admin.describeCluster(), admin.fetchTopicMetadata()]);
  if (signal?.aborted) throw new Error("Cluster health was cancelled");
  const topicHealth = metadata.topics.map(toTopicDetail);
  const topologyMeasuredAt = now();
  const topologyHealth: ClusterOverviewHealthDto = {
    onlineBrokers: cluster.brokers.length,
    unavailablePartitions: topicHealth.reduce((sum, topic) => sum + topic.unavailablePartitions, 0),
    underReplicatedPartitions: topicHealth.reduce((sum, topic) => sum + topic.underReplicatedPartitions, 0),
    topologyMeasuredAt,
    consumerGroupLag: "—",
  };
  const topologySnapshot: Partial<ClusterOverviewHealthDto> = {
    onlineBrokers: topologyHealth.onlineBrokers,
    unavailablePartitions: topologyHealth.unavailablePartitions,
    underReplicatedPartitions: topologyHealth.underReplicatedPartitions,
    topologyMeasuredAt,
  };
  onProgress?.({
    phase: "topology",
    completed: metadata.topics.length,
    total: metadata.topics.length,
    health: topologySnapshot,
  });

  let offsetsByTopic: Map<string, IndexedTopicConsumer[]>;
  try {
    offsetsByTopic = await consumerGroupOffsets.all(
      admin,
      "",
      ({ completed, total }) => {
        onProgress?.({ phase: "groups", completed, total, health: topologySnapshot });
      },
      signal,
    );
  } catch (error) {
    if (signal?.aborted) throw error;
    const health = { ...topologyHealth, consumerGroupLag: "Unavailable" };
    onProgress?.({
      phase: "complete",
      detail: "Consumer lag is unavailable; broker and partition health is ready.",
      health,
    });
    return health;
  }

  const topicEntries = [...offsetsByTopic.entries()];
  const leaderByTopicPartition = new Map(
    metadata.topics.flatMap((topic) =>
      (topic.partitions ?? []).map(
        (partition) => [`${topic.name}:${partition.partitionId}`, partition.leader] as const,
      ),
    ),
  );
  const batchPartitions: OffsetBatchPartition[] = topicEntries.flatMap(([topic, groups]) =>
    [...new Set(groups.flatMap((group) => group.committed.map((partition) => partition.partition)))].flatMap(
      (partition) => {
        const leaderId = leaderByTopicPartition.get(`${topic}:${partition}`);
        return leaderId === undefined || leaderId === null ? [] : [{ topic, partition, leaderId }];
      },
    ),
  );
  let completedTopics = 0;
  const calculateTopicLags = async (watermarksByTopic?: Map<string, Map<number, string>>, reportProgress = true) =>
    mapWithConcurrency(topicEntries, HEALTH_TOPIC_CONCURRENCY, async ([topic, groups]) => {
      try {
        const watermarks =
          watermarksByTopic && watermarksByTopic.has(topic)
            ? (watermarksByTopic.get(topic) ?? new Map<number, string>())
            : new Map((await admin.fetchTopicOffsets(topic)).map((partition) => [partition.partition, partition.high]));
        let complete = true;
        let knownLag = 0n;
        for (const group of groups) {
          for (const partition of group.committed) {
            const highWatermark = watermarks.get(partition.partition);
            if (highWatermark === undefined) {
              complete = false;
              continue;
            }
            const difference = BigInt(highWatermark) - BigInt(partition.offset);
            knownLag += difference < 0n ? 0n : difference;
          }
        }
        return { complete, knownLag };
      } catch {
        return { complete: false, knownLag: 0n };
      } finally {
        completedTopics++;
        if (reportProgress) {
          onProgress?.({
            phase: "watermarks",
            completed: completedTopics,
            total: topicEntries.length,
            health: topologySnapshot,
          });
        }
      }
    });
  let topicLags: TopicLagMeasurement[];
  if (readHighWatermarks) {
    try {
      const watermarks = await readHighWatermarks(batchPartitions, signal);
      completedTopics = 0;
      topicLags = await calculateTopicLags(watermarks);
    } catch (error) {
      if (signal?.aborted) throw error;
      completedTopics = 0;
      topicLags = await calculateTopicLags();
    }
  } else {
    topicLags = await calculateTopicLags();
  }

  const unavailableTopics = topicLags.filter(({ complete }) => !complete).length;
  const knownLag = topicLags.reduce<bigint>((sum, lag) => sum + lag.knownLag, 0n);
  const groupScan = consumerGroupOffsets.scanStatus();
  const incomplete = unavailableTopics > 0 || groupScan.unavailableGroups > 0;
  const health = {
    ...topologyHealth,
    consumerGroupLag: incomplete ? `≥${knownLag}` : knownLag.toString(),
    consumerGroupLagUnavailableTopics: unavailableTopics || undefined,
    consumerGroupLagUnavailableGroups: groupScan.unavailableGroups || undefined,
    consumerGroupLagCoverage: {
      complete: !incomplete,
      completedAt: groupScan.completedAt,
      resolvedGroups: groupScan.resolvedGroups,
      startedAt: groupScan.startedAt,
      totalGroups: groupScan.totalGroups,
      unavailableGroups: groupScan.unavailableGroups,
    },
  };
  onProgress?.({
    phase: "complete",
    detail: incomplete
      ? `Consumer lag is a measured minimum; ${groupScan.unavailableGroups} group(s) and ${unavailableTopics} topic(s) were unavailable.`
      : undefined,
    health,
  });
  return health;
}
