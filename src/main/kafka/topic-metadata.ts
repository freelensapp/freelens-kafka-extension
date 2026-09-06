import type { ITopicMetadata } from "kafkajs";

import type { TopicDetailDto } from "../../common/ipc";

export function isInternalTopic(name: string): boolean {
  return name.startsWith("__");
}

export function toTopicDetail(metadata: ITopicMetadata): TopicDetailDto {
  const partitions = metadata.partitions
    .map((partition) => ({
      partitionId: partition.partitionId,
      leader: partition.leader,
      replicas: partition.replicas,
      isr: partition.isr,
      offlineReplicas: partition.offlineReplicas ?? [],
      errorCode: partition.partitionErrorCode,
      underReplicated: partition.isr.length < partition.replicas.length,
      unavailable: partition.leader < 0 || partition.partitionErrorCode !== 0,
    }))
    .sort((left, right) => left.partitionId - right.partitionId);

  return {
    name: metadata.name,
    internal: isInternalTopic(metadata.name),
    partitions,
    partitionCount: partitions.length,
    replicationFactor: partitions[0]?.replicas.length ?? 0,
    underReplicatedPartitions: partitions.filter((partition) => partition.underReplicated).length,
    unavailablePartitions: partitions.filter((partition) => partition.unavailable).length,
  };
}
