import {
  findCoordinatorBatchProtocol,
  type KafkaApiVersions,
  offsetFetchBatchProtocol,
  sendKafkaProtocol,
  supportsGroupOffsetBatch,
} from "./group-offset-batch-protocol";

const DEFAULT_COORDINATOR_CHUNK_SIZE = 500;
const DEFAULT_GROUP_CHUNK_SIZE = 100;
const DEFAULT_CONCURRENCY = 8;
const BATCH_DISABLING_ERROR_CODES = new Set([10, 35, 42]);
const COORDINATOR_ERROR_CODES = new Set([14, 15, 16]);

interface GroupOffsetBatchCluster {
  findBroker(request: { nodeId: string }): Promise<object>;
  getNodeIds(): string[];
  refreshMetadata(): Promise<void>;
}

export interface BatchedTopicConsumer {
  committed: Array<{ offset: string; partition: number }>;
  groupId: string;
}

export interface GroupOffsetBatchResult {
  offsetsByTopic: Map<string, BatchedTopicConsumer[]>;
  resolvedGroupIds: Set<string>;
  supported: boolean;
  unresolvedGroupIds: Set<string>;
}

interface GroupOffsetBatchOptions {
  concurrency?: number;
  coordinatorChunkSize?: number;
  groupChunkSize?: number;
}

interface VersionedBroker {
  versions?: KafkaApiVersions;
}

function chunks<T>(items: T[], chunkSize: number): T[][] {
  const size = Math.max(1, Math.floor(chunkSize));
  const result: T[][] = [];
  for (let index = 0; index < items.length; index += size) result.push(items.slice(index, index + size));
  return result;
}

async function mapBounded<T>(
  items: T[],
  concurrency: number,
  operation: (item: T) => Promise<void>,
  shouldContinue: () => boolean,
): Promise<void> {
  let nextIndex = 0;
  const worker = async () => {
    while (nextIndex < items.length && shouldContinue()) await operation(items[nextIndex++]);
  };
  await Promise.all(Array.from({ length: Math.min(Math.max(1, concurrency), items.length) }, worker));
}

function batchDisablingError(error: unknown): boolean {
  const type = typeof error === "object" && error !== null && "type" in error ? error.type : undefined;
  const code = typeof error === "object" && error !== null && "code" in error ? error.code : undefined;
  return (
    type === "UNSUPPORTED_VERSION" ||
    type === "MESSAGE_TOO_LARGE" ||
    type === "INVALID_REQUEST" ||
    code === 35 ||
    (error instanceof Error &&
      (error.message.includes("does not support read-only multi-group offset fetch") ||
        error.message.includes("KafkaJS broker request adapter is unavailable")))
  );
}

export class KafkaGroupOffsetBatchReader {
  private capability: "supported" | "unknown" | "unsupported" = "unknown";
  private readonly coordinatorByGroup = new Map<string, string>();
  private metadataReady = false;

  constructor(
    private readonly cluster: GroupOffsetBatchCluster,
    private readonly options: GroupOffsetBatchOptions = {},
  ) {}

  async read(groupIds: string[], signal?: AbortSignal): Promise<GroupOffsetBatchResult> {
    const offsetsByTopic = new Map<string, BatchedTopicConsumer[]>();
    const resolvedGroupIds = new Set<string>();
    const unresolvedGroupIds = new Set(groupIds);
    this.throwIfAborted(signal);
    if (groupIds.length === 0) return { offsetsByTopic, resolvedGroupIds, supported: true, unresolvedGroupIds };
    if (this.capability === "unsupported") {
      return { offsetsByTopic, resolvedGroupIds, supported: false, unresolvedGroupIds };
    }

    if (!this.metadataReady) {
      await this.cluster.refreshMetadata();
      this.throwIfAborted(signal);
      this.metadataReady = true;
    }
    const bootstrapNodeId = this.cluster.getNodeIds()[0];
    if (!bootstrapNodeId) return { offsetsByTopic, resolvedGroupIds, supported: false, unresolvedGroupIds };
    const bootstrapBroker = await this.cluster.findBroker({ nodeId: bootstrapNodeId });
    if (this.capability === "unknown") {
      if (!supportsGroupOffsetBatch((bootstrapBroker as VersionedBroker).versions)) {
        this.capability = "unsupported";
        return { offsetsByTopic, resolvedGroupIds, supported: false, unresolvedGroupIds };
      }
      this.capability = "supported";
    }

    const unknownGroups = groupIds.filter((groupId) => !this.coordinatorByGroup.has(groupId));
    let stopCustomRequests = false;
    for (const groupChunk of chunks(
      unknownGroups,
      this.options.coordinatorChunkSize ?? DEFAULT_COORDINATOR_CHUNK_SIZE,
    )) {
      if (stopCustomRequests) break;
      this.throwIfAborted(signal);
      try {
        const response = await sendKafkaProtocol(bootstrapBroker, findCoordinatorBatchProtocol(groupChunk));
        for (const coordinator of response.coordinators) {
          if (coordinator.errorCode === 0 && groupChunk.includes(coordinator.key)) {
            this.coordinatorByGroup.set(coordinator.key, String(coordinator.nodeId));
          }
          if (BATCH_DISABLING_ERROR_CODES.has(coordinator.errorCode)) {
            this.capability = "unsupported";
            stopCustomRequests = true;
          }
        }
      } catch (error) {
        this.throwIfAborted(signal);
        if (batchDisablingError(error)) {
          this.capability = "unsupported";
          stopCustomRequests = true;
        }
      }
    }

    if (stopCustomRequests) {
      return { offsetsByTopic, resolvedGroupIds, supported: false, unresolvedGroupIds };
    }

    const groupsByCoordinator = new Map<string, string[]>();
    for (const groupId of groupIds) {
      const nodeId = this.coordinatorByGroup.get(groupId);
      if (!nodeId) continue;
      groupsByCoordinator.set(nodeId, [...(groupsByCoordinator.get(nodeId) ?? []), groupId]);
    }
    const requests = [...groupsByCoordinator].flatMap(([nodeId, coordinatorGroups]) =>
      chunks(coordinatorGroups, this.options.groupChunkSize ?? DEFAULT_GROUP_CHUNK_SIZE).map((chunk) => ({
        groupIds: chunk,
        nodeId,
      })),
    );

    await mapBounded(
      requests,
      this.options.concurrency ?? DEFAULT_CONCURRENCY,
      async ({ groupIds: chunk, nodeId }) => {
        if (stopCustomRequests || signal?.aborted) return;
        try {
          const broker = await this.cluster.findBroker({ nodeId });
          if (stopCustomRequests || signal?.aborted) return;
          const response = await sendKafkaProtocol(broker, offsetFetchBatchProtocol(chunk));
          for (const groupId of chunk) {
            const group = response.groups.find((candidate) => candidate.groupId === groupId);
            if (!group || group.errorCode !== 0) {
              if (group && COORDINATOR_ERROR_CODES.has(group.errorCode)) {
                this.coordinatorByGroup.delete(groupId);
                this.metadataReady = false;
              }
              if (group && BATCH_DISABLING_ERROR_CODES.has(group.errorCode)) {
                this.capability = "unsupported";
                stopCustomRequests = true;
              }
              continue;
            }
            let complete = true;
            for (const topic of group.topics) {
              const committed = topic.partitions
                .filter(({ committedOffset, errorCode }) => {
                  if (errorCode !== 0) {
                    complete = false;
                    return false;
                  }
                  return committedOffset !== "-1";
                })
                .map(({ committedOffset, partition }) => ({ offset: committedOffset, partition }));
              if (committed.length === 0) continue;
              offsetsByTopic.set(topic.topic, [...(offsetsByTopic.get(topic.topic) ?? []), { groupId, committed }]);
            }
            if (!complete) continue;
            resolvedGroupIds.add(groupId);
            unresolvedGroupIds.delete(groupId);
          }
        } catch (error) {
          this.throwIfAborted(signal);
          for (const groupId of chunk) this.coordinatorByGroup.delete(groupId);
          this.metadataReady = false;
          if (batchDisablingError(error)) {
            this.capability = "unsupported";
            stopCustomRequests = true;
          }
        }
      },
      () => !stopCustomRequests && !signal?.aborted,
    );
    this.throwIfAborted(signal);

    return {
      offsetsByTopic,
      resolvedGroupIds,
      supported: this.capability === "supported",
      unresolvedGroupIds,
    };
  }

  private throwIfAborted(signal?: AbortSignal): void {
    if (signal?.aborted) throw new Error("Consumer group offset scan was cancelled");
  }
}
