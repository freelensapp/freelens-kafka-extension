import type { KafkaReadCluster } from "./message-fetch";

export interface OffsetBatchPartition {
  leaderId: number;
  partition: number;
  topic: string;
}

export interface GroupedOffsetBatch {
  leaderId: number;
  partitions: Array<Omit<OffsetBatchPartition, "leaderId">>;
}

export interface HighWatermarkBatch {
  fromBeginning: false;
  lowWatermarkRequests: 0;
  partitions: OffsetBatchPartition[];
}

export function groupOffsetBatchByBroker(partitions: OffsetBatchPartition[]): GroupedOffsetBatch[] {
  const groups = new Map<number, GroupedOffsetBatch>();
  for (const { leaderId, topic, partition } of partitions) {
    const group = groups.get(leaderId) ?? { leaderId, partitions: [] };
    group.partitions.push({ topic, partition });
    groups.set(leaderId, group);
  }
  return [...groups.values()];
}

export function chunkOffsetBatch<T>(items: T[], chunkSize: number): T[][] {
  const normalizedSize = Math.max(1, Math.floor(chunkSize));
  const chunks: T[][] = [];
  for (let index = 0; index < items.length; index += normalizedSize) {
    chunks.push(items.slice(index, index + normalizedSize));
  }
  return chunks;
}

export function highWatermarkBatch(partitions: OffsetBatchPartition[]): HighWatermarkBatch {
  return {
    fromBeginning: false,
    lowWatermarkRequests: 0,
    partitions: [...partitions],
  };
}

export async function fetchHighWatermarks(
  cluster: Pick<KafkaReadCluster, "addMultipleTargetTopics" | "fetchTopicsOffset">,
  partitions: OffsetBatchPartition[],
  chunkSize = 500,
  signal?: AbortSignal,
): Promise<Map<string, Map<number, string>>> {
  const throwIfAborted = () => {
    if (signal?.aborted) throw new Error("Cluster health was cancelled");
  };
  throwIfAborted();
  const result = new Map<string, Map<number, string>>();
  const topics = [...new Set(partitions.map(({ topic }) => topic))];
  if (topics.length === 0) return result;
  await cluster.addMultipleTargetTopics(topics);
  throwIfAborted();

  for (const group of groupOffsetBatchByBroker(partitions)) {
    for (const chunk of chunkOffsetBatch(group.partitions, chunkSize)) {
      throwIfAborted();
      const topics = new Map<string, number[]>();
      for (const { topic, partition } of chunk) {
        const topicPartitions = topics.get(topic) ?? [];
        topicPartitions.push(partition);
        topics.set(topic, topicPartitions);
      }
      const response = await cluster.fetchTopicsOffset(
        [...topics].map(([topic, partitions]) => ({
          topic,
          partitions: partitions.map((partition) => ({ partition })),
          fromBeginning: false,
        })),
      );
      throwIfAborted();
      for (const topicResult of response) {
        const offsets = result.get(topicResult.topic) ?? new Map<number, string>();
        for (const partitionResult of topicResult.partitions) {
          offsets.set(partitionResult.partition, partitionResult.offset);
        }
        result.set(topicResult.topic, offsets);
      }
    }
  }
  return result;
}
