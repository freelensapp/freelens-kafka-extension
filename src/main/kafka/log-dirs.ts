import { type KafkaApiVersions, sendKafkaProtocol, supportsDescribeLogDirs } from "./group-offset-batch-protocol";
import { describeLogDirsProtocol } from "./log-dirs-protocol";

import type { PartitionSizeDto, TopicSizeDto, TopicSizesDto } from "../../common/ipc";

const DEFAULT_CONCURRENCY = 8;

/** The part of the KafkaJS cluster the size reader needs (see `KafkaReadCluster`). */
export interface LogDirsCluster {
  /** The topics the KafkaJS cluster refreshes metadata for; it never forgets a name on its own. */
  targetTopics: Set<string>;
  addMultipleTargetTopics(topics: string[]): Promise<void>;
  refreshMetadata(): Promise<void>;
  getNodeIds(): string[];
  findTopicPartitionMetadata(topic: string): Array<{ partitionId: number; leader: number | null }>;
  findBroker(request: { nodeId: string }): Promise<object>;
}

/** One replica of one partition as reported by the broker that hosts it. */
export interface ReplicaSizeEntry {
  topic: string;
  partition: number;
  nodeId: number;
  bytes: bigint;
  offsetLag: bigint;
}

export interface TopicLeaders {
  /** topic -> partition -> leader node id (null when the partition has no leader). */
  leaders: Map<string, Map<number, number | null>>;
}

/**
 * Sum the replica sizes per partition and per topic. `leaderBytes` follows the leader replica,
 * which is what most consoles show as the topic size; `replicaBytes` sums every replica, which
 * is the disk footprint across the cluster. A topic is `exact` only when every partition has a
 * leader entry: otherwise its leader size is a lower bound (a broker was unavailable or refused).
 */
export function aggregateTopicSizes(
  entries: readonly ReplicaSizeEntry[],
  { leaders }: TopicLeaders,
  unavailableBrokers: readonly number[],
  supported = true,
): TopicSizesDto {
  const byTopic = new Map<string, Map<number, PartitionSizeDto & { leaderSeen: boolean }>>();
  const partitionsOf = (topic: string) => {
    let partitions = byTopic.get(topic);
    if (!partitions) {
      partitions = new Map();
      byTopic.set(topic, partitions);
    }
    return partitions;
  };
  for (const [topic, topicLeaders] of leaders) {
    const partitions = partitionsOf(topic);
    for (const partition of topicLeaders.keys()) {
      partitions.set(partition, {
        partition,
        leaderBytes: "0",
        replicaBytes: "0",
        replicas: [],
        leaderSeen: false,
      });
    }
  }
  const replicaTotals = new Map<string, Map<number, bigint>>();
  const leaderTotals = new Map<string, Map<number, bigint>>();
  for (const entry of entries) {
    const partitions = partitionsOf(entry.topic);
    const record = partitions.get(entry.partition) ?? {
      partition: entry.partition,
      leaderBytes: "0",
      replicaBytes: "0",
      replicas: [],
      leaderSeen: false,
    };
    partitions.set(entry.partition, record);
    record.replicas.push({
      nodeId: entry.nodeId,
      bytes: entry.bytes.toString(),
      offsetLag: entry.offsetLag.toString(),
    });
    const totals = replicaTotals.get(entry.topic) ?? new Map<number, bigint>();
    replicaTotals.set(entry.topic, totals);
    totals.set(entry.partition, (totals.get(entry.partition) ?? 0n) + entry.bytes);
    const leader = leaders.get(entry.topic)?.get(entry.partition);
    if (leader !== undefined && leader !== null && leader === entry.nodeId) {
      record.leaderSeen = true;
      const leaderMap = leaderTotals.get(entry.topic) ?? new Map<number, bigint>();
      leaderTotals.set(entry.topic, leaderMap);
      leaderMap.set(entry.partition, entry.bytes);
    }
  }
  const topics: Record<string, TopicSizeDto> = {};
  for (const [topic, partitions] of byTopic) {
    let leaderBytes = 0n;
    let replicaBytes = 0n;
    let exact = true;
    const list: PartitionSizeDto[] = [];
    for (const record of [...partitions.values()].sort((a, b) => a.partition - b.partition)) {
      const leaderSize = leaderTotals.get(topic)?.get(record.partition) ?? 0n;
      const replicaSize = replicaTotals.get(topic)?.get(record.partition) ?? 0n;
      if (!record.leaderSeen) exact = false;
      leaderBytes += leaderSize;
      replicaBytes += replicaSize;
      record.replicas.sort((a, b) => a.nodeId - b.nodeId);
      list.push({
        partition: record.partition,
        leaderBytes: leaderSize.toString(),
        replicaBytes: replicaSize.toString(),
        replicas: record.replicas,
      });
    }
    topics[topic] = {
      leaderBytes: leaderBytes.toString(),
      replicaBytes: replicaBytes.toString(),
      exact,
      partitions: list,
    };
  }
  return { supported, unavailableBrokers: [...unavailableBrokers].sort((a, b) => a - b), topics };
}

function toBigInt(value: string): bigint {
  try {
    const parsed = BigInt(value);
    return parsed < 0n ? 0n : parsed;
  } catch {
    return 0n;
  }
}

/**
 * Ask every broker for the log directory sizes of the given topics (one DescribeLogDirs per
 * broker, bounded concurrency) and aggregate them per topic and partition. Brokers that fail
 * are listed in `unavailableBrokers`; a cluster that does not offer the API returns
 * `supported: false` instead of failing the page.
 */
export async function fetchTopicSizes(
  cluster: LogDirsCluster,
  topics: readonly string[],
  options: { concurrency?: number; signal?: AbortSignal } = {},
): Promise<TopicSizesDto> {
  const names = [...new Set(topics)].filter(Boolean);
  const leaders = new Map<string, Map<number, number | null>>();
  if (names.length === 0) return aggregateTopicSizes([], { leaders }, []);
  await cluster.addMultipleTargetTopics(names);
  try {
    await cluster.refreshMetadata();
  } catch (error) {
    // The shared read cluster still targets the topics deleted since its last call, and KafkaJS
    // fails the whole metadata refresh on an unknown one: drop the names outside this request
    // (built from a fresh topic list) and retry once.
    if ((error as { type?: string }).type !== "UNKNOWN_TOPIC_OR_PARTITION") throw error;
    const wanted = new Set(names);
    for (const topic of [...cluster.targetTopics]) if (!wanted.has(topic)) cluster.targetTopics.delete(topic);
    await cluster.refreshMetadata();
  }
  const request = names.map((topic) => {
    const partitions = cluster.findTopicPartitionMetadata(topic);
    leaders.set(topic, new Map(partitions.map((partition) => [partition.partitionId, partition.leader])));
    return { topic, partitions: partitions.map((partition) => partition.partitionId) };
  });
  const nodeIds = cluster.getNodeIds();
  if (nodeIds.length === 0) return aggregateTopicSizes([], { leaders }, [], false);
  const first = (await cluster.findBroker({ nodeId: nodeIds[0] })) as { versions?: KafkaApiVersions };
  if (!supportsDescribeLogDirs(first.versions)) return aggregateTopicSizes([], { leaders }, [], false);

  const entries: ReplicaSizeEntry[] = [];
  const unavailable: number[] = [];
  const queue = [...nodeIds];
  const worker = async () => {
    for (let nodeId = queue.shift(); nodeId !== undefined; nodeId = queue.shift()) {
      if (options.signal?.aborted) throw new Error("Topic size fetch aborted");
      const numericNodeId = Number(nodeId);
      try {
        const broker = await cluster.findBroker({ nodeId });
        const response = await sendKafkaProtocol(broker, describeLogDirsProtocol(request));
        for (const result of response.results) {
          if (result.errorCode !== 0) continue;
          for (const topic of result.topics) {
            for (const partition of topic.partitions) {
              if (partition.isFuture) continue;
              entries.push({
                topic: topic.topic,
                partition: partition.partition,
                nodeId: numericNodeId,
                bytes: toBigInt(partition.size),
                offsetLag: toBigInt(partition.offsetLag),
              });
            }
          }
        }
      } catch {
        unavailable.push(numericNodeId);
      }
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(options.concurrency ?? DEFAULT_CONCURRENCY, nodeIds.length) }, worker),
  );
  return aggregateTopicSizes(entries, { leaders }, unavailable);
}
