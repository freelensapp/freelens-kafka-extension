import { Renderer } from "@freelensapp/extensions";
import {
  type AclsRequest,
  type AclWriteRequest,
  type AggregateHealthInvalidateRequest,
  type BrokerConfigDto,
  type BrokerConfigRequest,
  type ClusterHealthRequest,
  type ClusterOverviewDto,
  type ClusterOverviewHealthDto,
  type ConnectorDetailDto,
  type ConnectorSummaryDto,
  type ConsumerGroupDetailDto,
  type ConsumerGroupsDto,
  type DeleteTopicRequest,
  type DeleteTopicResultDto,
  type DeleteTopicsRequest,
  type DeleteTopicsResultDto,
  type DiscoveredKafkaInfo,
  type DiscoverRequest,
  type ExtensionVersionDto,
  type GroupDetailRequest,
  type GroupsRequest,
  KAFKA_IPC,
  type KafkaAclResultDto,
  type KafkaConnectCreateRequest,
  type KafkaConnectDetailRequest,
  type KafkaConnectRequest,
  type KafkaProgressEvent,
  type MessageBrowseDto,
  type MessageBrowseRequest,
  type OverviewRequest,
  type ProduceRequest,
  type ProduceResultDto,
  type ResetOffsetsRequest,
  type ResetOffsetsResultDto,
  type SchemaDeleteSubjectRequest,
  type SchemaRegisterRequest,
  type SchemaSubjectDetail,
  type SchemaSubjectDetailRequest,
  type SchemaSubjectNamesRequest,
  type SchemaSubjectSummary,
  type SchemaSubjectsRequest,
  type TopicConfigDto,
  type TopicConfigRequest,
  type TopicConsumersDto,
  type TopicConsumersRequest,
  type TopicDetailDto,
  type TopicRequest,
  type TopicSizesDto,
  type TopicSizesRequest,
} from "../common/ipc";
import { createIpcRequestDeduper, type IpcRequest } from "./kafka-ipc-deduper";
import { KafkaProgressPull } from "./kafka-progress-pull";

import type { MainVersionProbe } from "./kafka-version-skew";

/** Renderer-side client for the Kafka extension's Main IPC handlers. */
export class KafkaIpcRenderer extends Renderer.Ipc {
  private readonly dedupe = createIpcRequestDeduper();
  private readonly progress = new KafkaProgressPull({
    listen: (listener) => this.listen(KAFKA_IPC.progress, (_event, signal: unknown) => listener(signal)),
    pull: (operationId) => this.invoke(KAFKA_IPC.progressEvent, { operationId }) as Promise<KafkaProgressEvent | null>,
  });

  // The operation id of a request this frame sends is this frame's while it runs: its progress is fetched.
  private read<T>(channel: string, request: IpcRequest): Promise<T> {
    return this.dedupe(channel, request, async () => {
      const release = this.progress.own((request as { operationId?: unknown } | undefined)?.operationId);

      try {
        return (await this.invoke(channel, request)) as T;
      } finally {
        release();
      }
    });
  }

  /** Version of the main bundle Freelens is running, or the fact that it predates the probe (SPEC-016). */
  async mainVersion(): Promise<MainVersionProbe> {
    try {
      const answer = (await this.invoke(KAFKA_IPC.version)) as ExtensionVersionDto;

      return { version: answer.version };
    } catch (error) {
      if (isMissingIpcHandler(error, KAFKA_IPC.version)) return { missing: true };
      throw error;
    }
  }

  discover(request: DiscoverRequest = {}): Promise<DiscoveredKafkaInfo[]> {
    return this.read(KAFKA_IPC.discover, request);
  }

  overview(request: OverviewRequest): Promise<ClusterOverviewDto> {
    return this.read(KAFKA_IPC.overview, request);
  }

  health(request: ClusterHealthRequest): Promise<ClusterOverviewHealthDto> {
    return this.read(KAFKA_IPC.health, request);
  }

  invalidateHealth(request: AggregateHealthInvalidateRequest): Promise<void> {
    return this.invoke(KAFKA_IPC.healthInvalidate, request) as Promise<void>;
  }

  topic(request: TopicRequest): Promise<TopicDetailDto> {
    return this.read(KAFKA_IPC.topic, request);
  }

  topicConfig(request: TopicConfigRequest): Promise<TopicConfigDto> {
    return this.read(KAFKA_IPC.topicConfig, request);
  }

  topicConsumers(request: TopicConsumersRequest): Promise<TopicConsumersDto> {
    return this.read(KAFKA_IPC.topicConsumers, request);
  }

  brokerConfig(request: BrokerConfigRequest): Promise<BrokerConfigDto> {
    return this.read(KAFKA_IPC.brokerConfig, request);
  }

  messagesBrowse(request: MessageBrowseRequest): Promise<MessageBrowseDto> {
    return this.read(KAFKA_IPC.messagesBrowse, request);
  }

  groups(request: GroupsRequest): Promise<ConsumerGroupsDto> {
    return this.read(KAFKA_IPC.groups, request);
  }

  groupDetail(request: GroupDetailRequest): Promise<ConsumerGroupDetailDto> {
    return this.read(KAFKA_IPC.groupDetail, request);
  }

  topicSizes(request: TopicSizesRequest): Promise<TopicSizesDto> {
    return this.read(KAFKA_IPC.topicSizes, request);
  }

  produce(request: ProduceRequest): Promise<ProduceResultDto> {
    return this.invoke(KAFKA_IPC.produce, request) as Promise<ProduceResultDto>;
  }

  deleteTopic(request: DeleteTopicRequest): Promise<DeleteTopicResultDto> {
    return this.invoke(KAFKA_IPC.deleteTopic, request) as Promise<DeleteTopicResultDto>;
  }

  async deleteTopics(request: DeleteTopicsRequest): Promise<DeleteTopicsResultDto> {
    try {
      return (await this.invoke(KAFKA_IPC.deleteTopics, request)) as DeleteTopicsResultDto;
    } catch (error) {
      if (!isMissingIpcHandler(error, KAFKA_IPC.deleteTopics)) throw error;

      return this.deleteTopicsIndividually(request);
    }
  }

  resetOffsets(request: ResetOffsetsRequest): Promise<ResetOffsetsResultDto> {
    return this.invoke(KAFKA_IPC.resetOffsets, request) as Promise<ResetOffsetsResultDto>;
  }

  schemaSubjects(request: SchemaSubjectsRequest): Promise<SchemaSubjectSummary[]> {
    return this.read(KAFKA_IPC.schemaSubjects, request);
  }

  schemaSubjectNames(request: SchemaSubjectNamesRequest): Promise<string[]> {
    return this.read(KAFKA_IPC.schemaSubjectNames, request);
  }

  schemaSubjectDetail(request: SchemaSubjectDetailRequest): Promise<SchemaSubjectDetail> {
    return this.read(KAFKA_IPC.schemaSubjectDetail, request);
  }

  schemaRegister(request: SchemaRegisterRequest): Promise<{ id: number }> {
    return this.invoke(KAFKA_IPC.schemaRegister, request) as Promise<{ id: number }>;
  }

  schemaDeleteSubject(request: SchemaDeleteSubjectRequest): Promise<number[]> {
    return this.invoke(KAFKA_IPC.schemaDeleteSubject, request) as Promise<number[]>;
  }

  connectList(request: KafkaConnectRequest): Promise<ConnectorSummaryDto[]> {
    return this.read(KAFKA_IPC.connectList, request);
  }

  connectNames(request: KafkaConnectRequest): Promise<string[]> {
    return this.read(KAFKA_IPC.connectNames, request);
  }

  connectDetail(request: KafkaConnectDetailRequest): Promise<ConnectorDetailDto> {
    return this.read(KAFKA_IPC.connectDetail, request);
  }

  acls(request: AclsRequest): Promise<KafkaAclResultDto> {
    return this.read(KAFKA_IPC.acls, request);
  }

  aclCreate(request: AclWriteRequest): Promise<void> {
    return this.invoke(KAFKA_IPC.aclCreate, request) as Promise<void>;
  }

  aclDelete(request: AclWriteRequest): Promise<void> {
    return this.invoke(KAFKA_IPC.aclDelete, request) as Promise<void>;
  }

  connectPause(request: KafkaConnectDetailRequest): Promise<void> {
    return this.invoke(KAFKA_IPC.connectPause, request) as Promise<void>;
  }
  connectResume(request: KafkaConnectDetailRequest): Promise<void> {
    return this.invoke(KAFKA_IPC.connectResume, request) as Promise<void>;
  }
  connectDelete(request: KafkaConnectDetailRequest): Promise<void> {
    return this.invoke(KAFKA_IPC.connectDelete, request) as Promise<void>;
  }
  connectRestart(request: KafkaConnectDetailRequest): Promise<void> {
    return this.invoke(KAFKA_IPC.connectRestart, request) as Promise<void>;
  }
  connectUpdate(request: KafkaConnectCreateRequest): Promise<void> {
    return this.invoke(KAFKA_IPC.connectUpdate, request) as Promise<void>;
  }
  connectCreate(request: KafkaConnectCreateRequest): Promise<{ name: string }> {
    return this.invoke(KAFKA_IPC.connectCreate, request) as Promise<{ name: string }>;
  }

  reachability(bootstraps: string[]): Promise<Record<string, boolean>> {
    return this.read(KAFKA_IPC.reachability, { bootstraps });
  }

  /** The progress events of the operations this frame started, and of no other (SPEC-019, REQ-227). */
  onProgress(listener: (progress: KafkaProgressEvent) => void): () => void {
    return this.progress.subscribe(listener);
  }

  /** Stops listening and fetching: the extension is deactivated. */
  dispose(): void {
    this.progress.dispose();
  }

  private async deleteTopicsIndividually(request: DeleteTopicsRequest): Promise<DeleteTopicsResultDto> {
    const topics = [...new Set(request.topics.filter(Boolean))];
    if (topics.length === 0) throw new Error("at least one topic name is required");

    const result: DeleteTopicsResultDto = { deleted: [], failed: [] };
    for (const topic of topics) {
      try {
        await this.deleteTopic({ ...request, topic });
        result.deleted.push(topic);
      } catch (error) {
        result.failed.push({ topic, error: error instanceof Error ? error.message : String(error) });
      }
    }
    return result;
  }
}

/** Electron's answer when the running main process never registered `channel` (an older main). */
function isMissingIpcHandler(error: unknown, channel: string): boolean {
  if (!(error instanceof Error)) return false;
  const escaped = channel.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

  return new RegExp(`No handler registered for ['"].*:${escaped}['"]`).test(error.message);
}
