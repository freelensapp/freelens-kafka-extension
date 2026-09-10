/** IPC contract shared between the Main (engine) and Renderer (UI) processes. */
import type { KafkaEtaStatus } from "./kafka-phase-progress";

export const KAFKA_IPC = {
  discover: "kafka:discover",
  overview: "kafka:overview",
  health: "kafka:health",
  healthInvalidate: "kafka:health:invalidate",
  topic: "kafka:topic",
  topicConfig: "kafka:topic:config",
  topicConsumers: "kafka:topic:consumers",
  topicSizes: "kafka:topics:sizes",
  brokerConfig: "kafka:broker:config",
  messagesBrowse: "kafka:messages:browse",
  groups: "kafka:groups",
  groupDetail: "kafka:group:detail",
  produce: "kafka:produce",
  deleteTopic: "kafka:topic:delete",
  resetOffsets: "kafka:group:reset-offsets",
  schemaSubjects: "kafka:schema-registry:subjects",
  schemaSubjectNames: "kafka:schema-registry:subject-names",
  schemaSubjectDetail: "kafka:schema-registry:subject-detail",
  schemaRegister: "kafka:schema-registry:register",
  schemaDeleteSubject: "kafka:schema-registry:delete-subject",
  connectList: "kafka:connect:list",
  connectNames: "kafka:connect:names",
  connectDetail: "kafka:connect:detail",
  connectPause: "kafka:connect:pause",
  connectResume: "kafka:connect:resume",
  connectDelete: "kafka:connect:delete",
  connectCreate: "kafka:connect:create",
  connectUpdate: "kafka:connect:update",
  connectRestart: "kafka:connect:restart",
  acls: "kafka:acls",
  aclCreate: "kafka:acl:create",
  aclDelete: "kafka:acl:delete",
  reachability: "kafka:reachability",
  progress: "kafka:progress",
} as const;

export interface DiscoverRequest {
  operationId?: string;
  clusterId?: string;
  kubeConfigPath?: string;
  context?: string;
  namespace?: string;
}

export interface OverviewRequest {
  operationId?: string;
  /** Stable non-secret renderer identity used to correlate shared metadata cache entries. */
  targetId?: string;
  clusterId?: string;
  kubeConfigPath?: string;
  context?: string;
  namespace: string;
  clusterName: string;
  user?: string;
  /** Discovery source of the target; when not `"strimzi"` the connection uses the Direct strategy. */
  source?: string;
  /** Bootstrap `host:port` (comma list) for the Direct strategy (non-Strimzi). */
  bootstrap?: string;
  /** Whether the target listener uses TLS (Direct strategy). */
  tls?: boolean;
  /** Optional per-connection override. Password is sent only over internal IPC and is never persisted. */
  security?: KafkaSecurityOverride;
  /** Non-secret source coordinates for targeted workload security resolution. */
  sourceLocator?: KafkaWorkloadSourceLocator;
}

export interface TopicRequest extends OverviewRequest {
  topic: string;
}

export interface ClusterHealthRequest extends OverviewRequest {
  /** Explicitly bypass Main and renderer health TTLs for this target. */
  refresh?: boolean;
}

export interface AggregateHealthInvalidateRequest {
  clusterId?: string;
  context?: string;
  targetId: string;
  closeSessions?: boolean;
  removePersisted?: boolean;
}

export interface TopicConfigRequest extends TopicRequest {}

/** Sizes on disk of the given topics, read through DescribeLogDirs on every broker. */
export interface TopicSizesRequest extends OverviewRequest {
  topics: string[];
}

export interface PartitionSizeDto {
  partition: number;
  /** Bytes of the leader replica (int64 as a decimal string). */
  leaderBytes: string;
  /** Bytes of every replica summed. */
  replicaBytes: string;
  replicas: Array<{ nodeId: number; bytes: string; offsetLag: string }>;
}

export interface TopicSizeDto {
  leaderBytes: string;
  replicaBytes: string;
  /** False when a partition leader was not reported (its size is then a lower bound). */
  exact: boolean;
  partitions: PartitionSizeDto[];
}

export interface TopicSizesDto {
  /** False when the brokers do not offer DescribeLogDirs. */
  supported: boolean;
  unavailableBrokers: number[];
  topics: Record<string, TopicSizeDto>;
}

export interface TopicConsumersRequest extends TopicRequest {}

export interface BrokerConfigRequest extends OverviewRequest {
  brokerId: number;
}

export type KafkaMessageStartMode = "earliest" | "latest" | "offset" | "timestamp";

export interface MessageBrowseRequest extends TopicRequest {
  partition: number;
  startMode: KafkaMessageStartMode;
  offset?: string;
  timestamp?: number;
  limit: number;
  registryUrl?: string;
  registryUsername?: string;
}

export type KafkaProgressOperation =
  | "discovery"
  | "overview"
  | "health"
  | "topic"
  | "topicConfig"
  | "topicConsumers"
  | "brokerConfig"
  | "messagesBrowse"
  | "groups"
  | "groupDetail";

export interface KafkaProgressEvent {
  operationId: string;
  operation: KafkaProgressOperation;
  /** Milliseconds since the operation progress reporter was created. */
  elapsedMs?: number;
  /** Milliseconds since the preceding progress update. */
  stageDurationMs?: number;
  /** Monotonic weighted phase completion in the inclusive range 0–100. */
  value: number;
  phase: string;
  /** Completion of the current phase only; never a weighted global percentage. */
  phasePercent?: number;
  /** Elapsed milliseconds since the current phase began. */
  phaseElapsedMs?: number;
  /** Smoothed remaining time, emitted only after confidence thresholds are met. */
  etaMs?: number;
  etaStatus?: KafkaEtaStatus;
  label: string;
  detail?: string;
  completed?: number;
  total?: number;
  /** Incremental non-secret health values measured before the full health operation completes. */
  healthSnapshot?: Partial<ClusterOverviewHealthDto>;
  /** Capture time of a restored or completed aggregate-health snapshot. */
  healthUpdatedAt?: number;
  /** Explicit origin of an aggregate-health snapshot. */
  healthSource?: "cache" | "network" | "persisted";
  /** Incremental read-only match emitted while building the Topic -> Consumer Groups index. */
  topicConsumerGroup?: TopicConsumerGroupDto;
}

export type KafkaSaslMechanism = "plain" | "scram-sha-256" | "scram-sha-512";
export type KafkaTlsMode = "auto" | "enabled" | "disabled";
export type KafkaAuthMode = "auto" | "none" | KafkaSaslMechanism;

export interface KafkaSecurityOverride {
  tlsMode: KafkaTlsMode;
  authMode: KafkaAuthMode;
  username?: string;
  password?: string;
}

export interface KafkaSecurityHint {
  tls: boolean;
  auth: "none" | "mtls" | KafkaSaslMechanism;
}

export interface KafkaSecuritySummary extends KafkaSecurityHint {
  source: "inferred" | "workload" | "strimzi" | "override";
}

/** Non-secret workload coordinates used for targeted security refreshes. */
export interface KafkaWorkloadSourceLocator {
  namespace: string;
  kind: string;
  name: string;
  container?: string;
}

/** Bootstraps to TCP-probe for reachability from the machine running Freelens. */
export interface ReachabilityRequest {
  bootstraps: string[];
}

export interface KafkaListenerInfo {
  name: string;
  port: number;
  tls: boolean;
  type?: string;
}

/** Serializable Kafka cluster info returned to the renderer by `discover`. */
export interface DiscoveredKafkaInfo {
  /** Stable, non-secret connection identity suitable for route parameters and renderer state. */
  targetId: string;
  source: string;
  name: string;
  namespace: string;
  bootstrap: string;
  tls: boolean;
  port: number;
  listeners: KafkaListenerInfo[];
  brokerPods: { brokerId: number; pod: string }[];
  /** Human-readable provider: `"Strimzi"` | `"In-cluster"` | `"MSK"` | `"Confluent"` | `"Aiven"` | … */
  provider?: string;
  /** Whether the bootstrap endpoint is external to the cluster (a managed/remote Kafka). */
  external?: boolean;
  /** For workload-sourced entries: the workloads that reference it (`ns/kind/name`). */
  referencedBy?: string[];
  /** Non-secret coordinates of one workload/container that references this bootstrap. */
  sourceLocator?: KafkaWorkloadSourceLocator;
  /** Non-secret security mode inferred from the workload configuration. */
  securityHint?: KafkaSecurityHint;
}

export interface BrokerInfo {
  nodeId: number;
  host: string;
  port: number;
}

export interface TopicPartitionDto {
  partitionId: number;
  leader: number;
  replicas: number[];
  isr: number[];
  offlineReplicas: number[];
  errorCode: number;
  underReplicated: boolean;
  unavailable: boolean;
}

export interface TopicDetailDto {
  name: string;
  internal: boolean;
  partitions: TopicPartitionDto[];
  partitionCount: number;
  replicationFactor: number;
  underReplicatedPartitions: number;
  unavailablePartitions: number;
}

export interface KafkaConfigEntryDto {
  name: string;
  value: string;
  source: string;
  readOnly: boolean;
  sensitive: boolean;
}

export type TopicConfigEntryDto = KafkaConfigEntryDto;

export interface TopicConfigDto {
  topic: string;
  entries: TopicConfigEntryDto[];
}

export interface BrokerConfigDto {
  brokerId: number;
  entries: KafkaConfigEntryDto[];
}

export interface TopicConsumerGroupDto {
  groupId: string;
  state: string;
  memberCount: number;
  totalLag: string;
}

export interface TopicConsumersDto {
  topic: string;
  groups: TopicConsumerGroupDto[];
}

export type KafkaMessageBytesFormat = "null" | "json" | "text" | "binary";

export interface KafkaMessageBytesDto {
  format: KafkaMessageBytesFormat;
  byteLength: number;
  truncated: boolean;
  /** Exact bounded preview bytes. Omitted only for Kafka null. */
  base64?: string;
  /** Present only when the full source bytes are valid UTF-8. */
  text?: string;
}

export interface KafkaMessageHeaderDto {
  name: string;
  value: KafkaMessageBytesDto;
}

export interface KafkaRecordDto {
  topic: string;
  partition: number;
  offset: string;
  timestamp: string;
  key: KafkaMessageBytesDto;
  value: KafkaMessageBytesDto;
  headers: KafkaMessageHeaderDto[];
  decodedValue?: unknown;
  decodeWarning?: string;
}

export interface MessageBrowseDto {
  topic: string;
  partition: number;
  startMode: KafkaMessageStartMode;
  requestedOffset?: string;
  startOffset: string;
  nextOffset: string;
  logStartOffset: string;
  highWatermark: string;
  returnedCount: number;
  hasMore: boolean;
  messages: KafkaRecordDto[];
}

/** Serializable cluster snapshot returned to the renderer by `overview`. */
export interface ClusterOverviewHealthDto {
  unavailablePartitions: number;
  underReplicatedPartitions: number;
  onlineBrokers: number;
  topologyMeasuredAt?: number;
  consumerGroupLag: string;
  consumerGroupLagUnavailableTopics?: number;
  consumerGroupLagUnavailableGroups?: number;
  consumerGroupLagCoverage?: {
    complete: boolean;
    completedAt?: number;
    resolvedGroups: number;
    startedAt?: number;
    totalGroups: number;
    unavailableGroups: number;
  };
}

export interface ClusterOverviewDto {
  brokers: BrokerInfo[];
  controller: number | null;
  topics: string[];
  /** Whether this broker answered a DescribeAcls probe, gating the ACL page. */
  aclsAvailable?: boolean;
  /** Short cluster health summary for the Overview dashboard. */
  health?: ClusterOverviewHealthDto;
  /** Effective (non-secret) security profile used for this metadata read. */
  security?: KafkaSecuritySummary;
}

// ─── Consumer Groups ──────────────────────────────────────────────────────────

export interface GroupsRequest extends OverviewRequest {}

export interface GroupDetailRequest extends OverviewRequest {
  groupId: string;
}

export interface ProduceRequest extends OverviewRequest {
  topic: string;
  key?: string;
  value: string;
  headers?: Record<string, string>;
  partition?: number;
}

export interface ProduceResultDto {
  topic: string;
  partition: number;
  offset: string;
}

/** Destructive: deletes one topic with all its partitions and records (SPEC-009 REQ-194). */
export interface DeleteTopicRequest extends OverviewRequest {
  topic: string;
}

export interface DeleteTopicResultDto {
  topic: string;
}

export type OffsetResetMode = "earliest" | "latest" | "offset" | "timestamp";

export interface ResetOffsetsRequest extends OverviewRequest {
  groupId: string;
  topic: string;
  partition: number;
  mode: OffsetResetMode;
  offset?: string;
  timestamp?: number;
}

export interface ResetOffsetsResultDto {
  groupId: string;
  topic: string;
  partition: number;
  offset: string;
}

export interface SchemaRegistryRequest {
  registryUrl: string;
  registryUsername?: string;
}

export interface SchemaSubjectsRequest extends SchemaRegistryRequest {}
export interface SchemaSubjectNamesRequest extends SchemaRegistryRequest {}

export interface SchemaSubjectDetailRequest extends SchemaRegistryRequest {
  subject: string;
}

export interface SchemaRegisterRequest extends SchemaRegistryRequest {
  subject: string;
  schema: string;
  schemaType?: SchemaType;
}

export interface SchemaDeleteSubjectRequest extends SchemaRegistryRequest {
  subject: string;
}

export interface KafkaConnectRequest {
  connectUrl: string;
  connectUsername?: string;
}

export interface KafkaConnectDetailRequest extends KafkaConnectRequest {
  connector: string;
}

export interface KafkaConnectCreateRequest extends KafkaConnectRequest {
  config: Record<string, string>;
}

export interface AclsRequest extends OverviewRequest {}

export interface KafkaAclDto {
  resourceType: string;
  resourceName: string;
  patternType: string;
  principal: string;
  host: string;
  operation: string;
  permissionType: string;
}

export interface KafkaAclResultDto {
  available: boolean;
  acls: KafkaAclDto[];
  message?: string;
}

export interface AclWriteRequest extends AclsRequest {
  acl: KafkaAclDto;
}

export interface ConnectorSummaryDto {
  name: string;
  type?: string;
  status: string;
  taskCount: number;
}

export interface ConnectorTaskDto {
  id: number;
  state: string;
  workerId?: string;
  trace?: string;
}

export interface ConnectorDetailDto {
  name: string;
  config: Record<string, string>;
  connector: { state: string; workerId?: string; version?: string };
  tasks: ConnectorTaskDto[];
}

export type SchemaType = "AVRO" | "PROTOBUF" | "JSON" | string;

export interface SchemaSubjectSummary {
  subject: string;
  latestVersion: number;
  schemaType: SchemaType;
  compatibility?: string;
}

export interface SchemaVersion {
  id: number;
  version: number;
  subject: string;
  schema: string;
  schemaType: SchemaType;
  references?: Array<{ name: string; subject: string; version: number }>;
}

export interface SchemaSubjectDetail {
  subject: string;
  compatibility?: string;
  versions: SchemaVersion[];
}

export interface ConsumerGroupSummaryDto {
  groupId: string;
  state: string;
  protocolType: string;
  memberCount: number;
}

export interface ConsumerGroupsDto {
  groups: ConsumerGroupSummaryDto[];
}

export interface GroupMemberDto {
  memberId: string;
  clientId: string;
  clientHost: string;
}

export interface GroupPartitionOffsetDto {
  partition: number;
  /** Decimal string; "-1" means the group has never committed an offset for this partition. */
  committedOffset: string;
  highWatermark: string;
  /** Decimal string lag, or "—" when committedOffset is -1. */
  lag: string;
}

export interface GroupTopicOffsetDto {
  topic: string;
  partitions: GroupPartitionOffsetDto[];
  /** Sum of non-unknown partition lags, or "—" when all partitions are uncommitted. */
  totalLag: string;
}

export interface ConsumerGroupDetailDto {
  groupId: string;
  state: string;
  protocol: string;
  protocolType: string;
  members: GroupMemberDto[];
  topicOffsets: GroupTopicOffsetDto[];
}
