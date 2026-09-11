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
  type WriteModeRequest,
} from "../common/ipc";
import { createIpcRequestDeduper, type IpcRequest } from "./kafka-ipc-deduper";

/** Renderer-side client for the Kafka extension's Main IPC handlers. */
export class KafkaIpcRenderer extends Renderer.Ipc {
  private readonly dedupe = createIpcRequestDeduper();

  private read<T>(channel: string, request: IpcRequest): Promise<T> {
    return this.dedupe(channel, request, () => this.invoke(channel, request) as Promise<T>);
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

  deleteTopics(request: DeleteTopicsRequest): Promise<DeleteTopicsResultDto> {
    return this.invoke(KAFKA_IPC.deleteTopics, request) as Promise<DeleteTopicsResultDto>;
  }

  resetOffsets(request: ResetOffsetsRequest): Promise<ResetOffsetsResultDto> {
    return this.invoke(KAFKA_IPC.resetOffsets, request) as Promise<ResetOffsetsResultDto>;
  }

  writeMode(request: WriteModeRequest): Promise<void> {
    return this.invoke(KAFKA_IPC.writeMode, request) as Promise<void>;
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

  onProgress(listener: (progress: KafkaProgressEvent) => void): () => void {
    return this.listen(KAFKA_IPC.progress, (_event, progress: KafkaProgressEvent) => listener(progress));
  }
}
