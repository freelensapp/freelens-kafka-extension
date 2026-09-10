import { Renderer } from "@freelensapp/extensions";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { kafkaPersistentStateStore } from "../common/kafka-persistent-state-store";
import { formatBytes, sumBytes } from "./format-bytes";
import { kafkaListWindow } from "./kafka-list-window";
import { KafkaMessagesBrowser } from "./kafka-messages";
import { implementedKafkaTopicView, rememberKafkaClusterSelection, saveKafkaReloadRoute } from "./kafka-navigation";
import { OperationProgress, TopicMetadata, type TopicState } from "./kafka-overview";
import { KafkaMetricStrip, KafkaPageShell } from "./kafka-page-shell";
import {
  KafkaResourceActions,
  type KafkaResourcePageDependencies,
  KafkaResourceState,
  useKafkaPageParam,
  useKafkaResourcePage,
} from "./kafka-resource-pages";
import { createOperationId, filterTopicNames, kafkaDecimalSortKey } from "./kafka-view-model";
import { canSubmitWriteAction, getWriteConfirmationLabel } from "./kafka-write-policy";
import { useKafkaWriteMode } from "./kafka-write-settings";

import type { CSSProperties, HTMLAttributes } from "react";

import type {
  DeleteTopicRequest,
  DeleteTopicResultDto,
  KafkaProgressEvent,
  MessageBrowseDto,
  MessageBrowseRequest,
  ProduceRequest,
  ProduceResultDto,
  TopicConfigDto,
  TopicConfigEntryDto,
  TopicConfigRequest,
  TopicConsumerGroupDto,
  TopicConsumersDto,
  TopicConsumersRequest,
  TopicDetailDto,
  TopicRequest,
  TopicSizesDto,
  TopicSizesRequest,
} from "../common/ipc";
import type { KafkaEndpointSecretsStore } from "./kafka-endpoint-secrets";
import type { KafkaSchemaRegistrySettingsStore } from "./kafka-schema-registry-settings";

interface KafkaTopicsPageParams {
  target: string;
  query: string;
  topic: string;
  view: string;
  keyFilter: string;
  valueFilter: string;
  headerKey: string;
  headerValue: string;
  timestamp: string;
}

const TOPIC_NAME_COLUMN_STYLE: CSSProperties = { flex: "1 1 0", minWidth: 0, width: 0 };
const TOPIC_TYPE_COLUMN_STYLE: CSSProperties = { flex: "0 0 112px", minWidth: 112, width: 112 };
const TOPIC_SIZE_COLUMN_STYLE: CSSProperties = { flex: "0 0 120px", minWidth: 120, width: 120 };
const TOPIC_ACTION_COLUMN_STYLE: CSSProperties = { flex: "0 0 44px", minWidth: 44, width: 44 };
const TOPIC_CONFIG_NAME_COLUMN_STYLE: CSSProperties = { flex: "0 0 220px", minWidth: 220, width: 220 };
const TOPIC_CONFIG_VALUE_COLUMN_STYLE: CSSProperties = { flex: "1 1 0", minWidth: 0, width: 0 };
const TOPIC_CONFIG_SOURCE_COLUMN_STYLE: CSSProperties = { flex: "0 0 230px", minWidth: 230, width: 230 };
const TOPIC_CONFIG_FLAGS_COLUMN_STYLE: CSSProperties = { flex: "0 0 140px", minWidth: 140, width: 140 };
const TOPIC_CONSUMER_NAME_COLUMN_STYLE: CSSProperties = { flex: "1 1 0", minWidth: 0, width: 0 };
const TOPIC_CONSUMER_STATE_COLUMN_STYLE: CSSProperties = { flex: "0 0 136px", minWidth: 136, width: 136 };
const TOPIC_CONSUMER_MEMBERS_COLUMN_STYLE: CSSProperties = { flex: "0 0 90px", minWidth: 90, width: 90 };
const TOPIC_CONSUMER_LAG_COLUMN_STYLE: CSSProperties = { flex: "0 0 120px", minWidth: 120, width: 120 };
const TOPIC_CONSUMER_ACTION_COLUMN_STYLE: CSSProperties = { flex: "0 0 44px", minWidth: 44, width: 44 };

interface TopicConfigState {
  topic?: string;
  loading: boolean;
  data?: TopicConfigDto;
  error?: string;
  progress?: KafkaProgressEvent;
}

interface TopicSizesState {
  loading: boolean;
  targetId?: string;
  data?: TopicSizesDto;
  error?: string;
}

interface TopicConsumersState {
  topic?: string;
  loading: boolean;
  data?: TopicConsumersDto;
  error?: string;
  progress?: KafkaProgressEvent;
}

function TopicConsumerStateBadge({ state }: { state: string }) {
  const tone =
    state === "Stable"
      ? "KafkaHealthyBadge"
      : state === "Dead" || state === "Unknown"
        ? "KafkaErrorBadge"
        : "KafkaWarningBadge";
  return <Renderer.Component.Badge small className={tone} label={state || "Unknown"} />;
}

function TopicConsumerLag({ lag }: { lag: string }) {
  if (lag === "—") return <span className="KafkaLagUnknown">—</span>;
  const value = BigInt(lag);
  const tone = value === 0n ? "KafkaLagZero" : value < 1000n ? "KafkaLagLow" : "KafkaLagHigh";
  return <strong className={tone}>{lag}</strong>;
}

function TopicConsumers({
  state,
  onRetry,
  onOpenGroup,
}: {
  state: TopicConsumersState;
  onRetry: () => void;
  onOpenGroup: (groupId: string) => void;
}) {
  if (state.loading && state.progress && !state.data) {
    return <OperationProgress progress={state.progress} />;
  }
  if (state.loading && !state.data) {
    return <Renderer.Component.Spinner center />;
  }

  if (state.error) {
    return (
      <div className="KafkaTopicInlineError" role="alert">
        <span>{state.error}</span>
        <Renderer.Component.Button outlined onClick={onRetry}>
          <Renderer.Component.Icon material="refresh" />
          Retry
        </Renderer.Component.Button>
      </div>
    );
  }

  if (!state.data || state.data.groups.length === 0) {
    return (
      <div className="KafkaPageState empty">
        <Renderer.Component.Icon material="group_off" />
        <div>
          <strong>No consumer groups</strong>
          <span>No consumer group has committed offsets on this topic.</span>
        </div>
      </div>
    );
  }

  return (
    <Renderer.Component.Table<TopicConsumerGroupDto>
      className="KafkaTopicConsumersTable"
      tableId="kafka-topic-consumers"
      autoSize={false}
      scrollable
      sortSyncWithUrl={false}
      sortByDefault={{ sortBy: "group", orderBy: "asc" }}
      sortable={{
        group: (group) => group.groupId.toLowerCase(),
        lag: (group) => kafkaDecimalSortKey(group.totalLag),
      }}
    >
      <Renderer.Component.TableHead sticky={false} nowrap>
        <Renderer.Component.TableCell
          className="consumerGroupCell"
          sortBy="group"
          style={TOPIC_CONSUMER_NAME_COLUMN_STYLE}
        >
          Consumer group
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="consumerStateCell" style={TOPIC_CONSUMER_STATE_COLUMN_STYLE}>
          State
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="consumerMembersCell" style={TOPIC_CONSUMER_MEMBERS_COLUMN_STYLE}>
          Members
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="consumerLagCell" sortBy="lag" style={TOPIC_CONSUMER_LAG_COLUMN_STYLE}>
          Total lag
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="consumerActionCell" style={TOPIC_CONSUMER_ACTION_COLUMN_STYLE} />
      </Renderer.Component.TableHead>
      {state.data.groups.map((group) => {
        const open = (): void => onOpenGroup(group.groupId);
        const interaction: HTMLAttributes<HTMLDivElement> = {
          role: "button",
          tabIndex: 0,
          "aria-label": `Open consumer group ${group.groupId}`,
          onClick: open,
          onKeyDown: (event) => {
            if (event.key === "Enter" || event.key === " ") {
              event.preventDefault();
              open();
            }
          },
        };
        return (
          <Renderer.Component.TableRow
            {...interaction}
            className="KafkaInteractiveRow"
            key={group.groupId}
            sortItem={group}
            data-group={group.groupId}
            nowrap
          >
            <Renderer.Component.TableCell
              className="consumerGroupCell"
              title={group.groupId}
              style={TOPIC_CONSUMER_NAME_COLUMN_STYLE}
            >
              <Renderer.Component.Icon material="group" />
              <span className="KafkaEllipsis">{group.groupId}</span>
            </Renderer.Component.TableCell>
            <Renderer.Component.TableCell className="consumerStateCell" style={TOPIC_CONSUMER_STATE_COLUMN_STYLE}>
              <TopicConsumerStateBadge state={group.state} />
            </Renderer.Component.TableCell>
            <Renderer.Component.TableCell className="consumerMembersCell" style={TOPIC_CONSUMER_MEMBERS_COLUMN_STYLE}>
              {group.memberCount}
            </Renderer.Component.TableCell>
            <Renderer.Component.TableCell className="consumerLagCell" style={TOPIC_CONSUMER_LAG_COLUMN_STYLE}>
              <TopicConsumerLag lag={group.totalLag} />
            </Renderer.Component.TableCell>
            <Renderer.Component.TableCell className="consumerActionCell" style={TOPIC_CONSUMER_ACTION_COLUMN_STYLE}>
              <Renderer.Component.Icon material="chevron_right" />
            </Renderer.Component.TableCell>
          </Renderer.Component.TableRow>
        );
      })}
    </Renderer.Component.Table>
  );
}

function TopicConfiguration({
  topic,
  state,
  onRetry,
}: {
  topic: string;
  state: TopicConfigState;
  onRetry: () => void;
}) {
  const hasEntries = (state.data?.entries.length ?? 0) > 0;

  if (state.loading && !state.data) {
    return <Renderer.Component.Spinner center />;
  }

  if (state.error) {
    return (
      <div className="KafkaTopicInlineError" role="alert">
        <span>{state.error}</span>
        <Renderer.Component.Button outlined onClick={onRetry}>
          <Renderer.Component.Icon material="refresh" />
          Retry
        </Renderer.Component.Button>
      </div>
    );
  }

  if (!hasEntries) {
    return (
      <div className="KafkaTopicPrompt">
        <Renderer.Component.Icon material="tune" />
        <span>No configuration entries returned for {topic}.</span>
      </div>
    );
  }

  return (
    <Renderer.Component.Table<TopicConfigEntryDto>
      className="KafkaTopicConfigTable"
      tableId="kafka-topic-configuration"
      autoSize={false}
      scrollable
      sortSyncWithUrl={false}
      sortByDefault={{ sortBy: "name", orderBy: "asc" }}
      sortable={{ name: (entry) => entry.name.toLowerCase() }}
    >
      <Renderer.Component.TableHead sticky={false} nowrap>
        <Renderer.Component.TableCell className="configNameCell" sortBy="name" style={TOPIC_CONFIG_NAME_COLUMN_STYLE}>
          Name
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="configValueCell" style={TOPIC_CONFIG_VALUE_COLUMN_STYLE}>
          Value
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="configSourceCell" style={TOPIC_CONFIG_SOURCE_COLUMN_STYLE}>
          Source
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="configFlagsCell" style={TOPIC_CONFIG_FLAGS_COLUMN_STYLE}>
          Flags
        </Renderer.Component.TableCell>
      </Renderer.Component.TableHead>
      {state.data?.entries.map((entry) => (
        <Renderer.Component.TableRow key={entry.name} sortItem={entry} nowrap>
          <Renderer.Component.TableCell
            className="configNameCell"
            title={entry.name}
            style={TOPIC_CONFIG_NAME_COLUMN_STYLE}
          >
            <span className="KafkaEllipsis">{entry.name}</span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell
            className="configValueCell"
            title={entry.value}
            style={TOPIC_CONFIG_VALUE_COLUMN_STYLE}
          >
            <span className="KafkaEllipsis KafkaMono">{entry.value || "—"}</span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell className="configSourceCell" style={TOPIC_CONFIG_SOURCE_COLUMN_STYLE}>
            <span className="KafkaConfigSource">{entry.source}</span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell className="configFlagsCell" style={TOPIC_CONFIG_FLAGS_COLUMN_STYLE}>
            <div className="KafkaConfigFlags">
              {entry.readOnly ? <Renderer.Component.Badge small label="Read-only" /> : null}
              {entry.sensitive ? <Renderer.Component.Badge small label="Sensitive" /> : null}
              {!entry.readOnly && !entry.sensitive ? <span>—</span> : null}
            </div>
          </Renderer.Component.TableCell>
        </Renderer.Component.TableRow>
      ))}
    </Renderer.Component.Table>
  );
}

export interface KafkaTopicsPageProps extends KafkaResourcePageDependencies {
  params?: {
    target: Renderer.Navigation.PageParam<KafkaTopicsPageParams["target"]>;
    query: Renderer.Navigation.PageParam<KafkaTopicsPageParams["query"]>;
    topic: Renderer.Navigation.PageParam<KafkaTopicsPageParams["topic"]>;
    view: Renderer.Navigation.PageParam<KafkaTopicsPageParams["view"]>;
    keyFilter: Renderer.Navigation.PageParam<KafkaTopicsPageParams["keyFilter"]>;
    valueFilter: Renderer.Navigation.PageParam<KafkaTopicsPageParams["valueFilter"]>;
    headerKey: Renderer.Navigation.PageParam<KafkaTopicsPageParams["headerKey"]>;
    headerValue: Renderer.Navigation.PageParam<KafkaTopicsPageParams["headerValue"]>;
    timestamp: Renderer.Navigation.PageParam<KafkaTopicsPageParams["timestamp"]>;
  };
  topic: (request: TopicRequest) => Promise<TopicDetailDto>;
  topicConfig: (request: TopicConfigRequest) => Promise<TopicConfigDto>;
  topicConsumers: (request: TopicConsumersRequest) => Promise<TopicConsumersDto>;
  onOpenGroup: (targetId: string, groupId: string) => void;
  messagesBrowse: (request: MessageBrowseRequest) => Promise<MessageBrowseDto>;
  produce: (request: ProduceRequest) => Promise<ProduceResultDto>;
  deleteTopic: (request: DeleteTopicRequest) => Promise<DeleteTopicResultDto>;
  topicSizes: (request: TopicSizesRequest) => Promise<TopicSizesDto>;
  schemaRegistrySettings: KafkaSchemaRegistrySettingsStore;
  endpointSecrets: KafkaEndpointSecretsStore;
}

export function KafkaTopicsPage({
  params,
  topic,
  topicConfig,
  topicConsumers,
  onOpenGroup,
  messagesBrowse,
  produce,
  deleteTopic,
  topicSizes,
  schemaRegistrySettings,
  endpointSecrets,
  ...dependencies
}: KafkaTopicsPageProps) {
  const state = useKafkaResourcePage({ ...dependencies, params });
  const canWrite = useKafkaWriteMode(dependencies.writeSettings, state.selectedCluster?.targetId);
  const [query, setQuery] = useKafkaPageParam(params?.query);
  const [topicName, setTopicName] = useKafkaPageParam(params?.topic);
  const [rawView, setRawView] = useKafkaPageParam(params?.view);
  const [keyFilter, setKeyFilter] = useKafkaPageParam(params?.keyFilter);
  const [valueFilter, setValueFilter] = useKafkaPageParam(params?.valueFilter);
  const [headerKey, setHeaderKey] = useKafkaPageParam(params?.headerKey);
  const [headerValue, setHeaderValue] = useKafkaPageParam(params?.headerValue);
  const [timestamp, setTimestamp] = useKafkaPageParam(params?.timestamp);
  // "messages"/"partitions" are only valid inside a topic workspace; fall back to overview otherwise.
  const view = topicName ? implementedKafkaTopicView(rawView) : "overview";
  const [topicState, setTopicState] = useState<TopicState>({ loading: false });
  const [topicConfigState, setTopicConfigState] = useState<TopicConfigState>({ loading: false });
  const [topicConsumersState, setTopicConsumersState] = useState<TopicConsumersState>({ loading: false });
  const [sizesState, setSizesState] = useState<TopicSizesState>({ loading: false });
  const [produceOpen, setProduceOpen] = useState(false);
  const [produceConfirmed, setProduceConfirmed] = useState(false);
  const [produceKey, setProduceKey] = useState("");
  const [produceValue, setProduceValue] = useState("");
  const [produceHeaders, setProduceHeaders] = useState("");
  const [producePartition, setProducePartition] = useState("");
  const [produceResult, setProduceResult] = useState<ProduceResultDto>();
  const [produceError, setProduceError] = useState<string>();
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deleteConfirmed, setDeleteConfirmed] = useState(false);
  const [deleteText, setDeleteText] = useState("");
  const [deleteBusy, setDeleteBusy] = useState(false);
  const [deleteError, setDeleteError] = useState<string>();
  const [writeStatus, setWriteStatus] = useState<string>();
  // The cluster and topic locked when the delete confirmation opened (SPEC-009 REQ-106).
  const deleteTarget = useRef<{ targetId: string; topic: string }>();
  const topicOperationId = useRef<string>();
  const topicConfigOperationId = useRef<string>();
  const topicConsumersOperationId = useRef<string>();
  const workspaceTargetId = useRef<string>();
  const [topicPage, setTopicPage] = useState(0);

  useEffect(() => setTopicPage(0), [query]);

  const clearPageParam = useCallback((param?: Renderer.Navigation.PageParam<string>) => {
    const candidate = param as Renderer.Navigation.PageParam<string> & { clear?: () => void };
    if (typeof candidate?.clear === "function") candidate.clear();
    else param?.set("", { replaceHistory: true });
  }, []);

  useEffect(() => {
    if (rawView !== view) setRawView(view, true);
  }, [rawView, setRawView, view]);

  useEffect(() => {
    if (!topicName || !state.selectedCluster) return;
    const saveRoute = () => {
      saveKafkaReloadRoute({
        pageId: "kafka-topics",
        params: {
          target: state.selectedCluster?.targetId ?? "",
          topic: topicName,
          view,
          query,
          keyFilter,
          valueFilter,
          headerKey,
          headerValue,
          timestamp,
        },
      });
    };
    saveRoute();
    window.addEventListener("beforeunload", saveRoute);
    window.addEventListener("pagehide", saveRoute);
    return () => {
      window.removeEventListener("beforeunload", saveRoute);
      window.removeEventListener("pagehide", saveRoute);
    };
  }, [headerKey, headerValue, keyFilter, query, state.selectedCluster, timestamp, topicName, valueFilter, view]);

  // Switching Kafka cluster from Topic Workspace must clear topic-specific URL state.
  useEffect(() => {
    const selectedTargetId = state.selectedCluster?.targetId;

    if (!selectedTargetId) {
      // Keep the previous target during transient reload gaps so cluster-switch reset
      // still triggers when the new target becomes available.
      return;
    }

    if (!topicName) {
      workspaceTargetId.current = selectedTargetId;
      return;
    }

    const previousTargetId = workspaceTargetId.current;
    workspaceTargetId.current = selectedTargetId;

    if (previousTargetId && previousTargetId !== selectedTargetId) {
      clearPageParam(params?.topic);
      clearPageParam(params?.query);
    }
  }, [clearPageParam, params?.query, params?.topic, state.selectedCluster?.targetId, topicName]);

  useEffect(
    () =>
      dependencies.subscribeProgress((progress) => {
        if (progress.operation === "topic" && progress.operationId === topicOperationId.current) {
          setTopicState((current) => ({ ...current, progress }));
        }
        if (progress.operation === "topicConfig" && progress.operationId === topicConfigOperationId.current) {
          setTopicConfigState((current) => ({ ...current, progress }));
        }
        if (progress.operation === "topicConsumers" && progress.operationId === topicConsumersOperationId.current) {
          setTopicConsumersState((current) => {
            const partial = progress.topicConsumerGroup;
            if (!partial) return { ...current, progress };
            const groups = current.data?.groups ?? [];
            return {
              ...current,
              data: {
                topic: current.topic ?? topicName,
                groups: [...groups.filter((group) => group.groupId !== partial.groupId), partial].sort((left, right) =>
                  left.groupId.localeCompare(right.groupId),
                ),
              },
              progress,
            };
          });
        }
      }),
    [dependencies.subscribeProgress, topicName],
  );

  // Topic sizes (SPEC-015): one DescribeLogDirs round per broker, after the metadata snapshot.
  const sizeCluster = state.selectedCluster;
  const sizeRequestBase = useMemo(
    () =>
      sizeCluster
        ? {
            targetId: sizeCluster.targetId,
            source: sizeCluster.source,
            bootstrap: sizeCluster.bootstrap,
            tls: sizeCluster.tls,
            namespace: sizeCluster.namespace,
            clusterName: sizeCluster.name,
          }
        : undefined,
    [
      sizeCluster?.targetId,
      sizeCluster?.source,
      sizeCluster?.bootstrap,
      sizeCluster?.tls,
      sizeCluster?.namespace,
      sizeCluster?.name,
    ],
  );
  const metadataTopics = state.metadataState.data?.topics;
  useEffect(() => {
    if (!sizeRequestBase || !metadataTopics || metadataTopics.length === 0) return;
    let cancelled = false;
    const targetId = sizeRequestBase.targetId;
    setSizesState((previous) => ({
      loading: true,
      targetId,
      data: previous.targetId === targetId ? previous.data : undefined,
    }));
    topicSizes({ ...sizeRequestBase, topics: [...metadataTopics] })
      .then((data) => {
        if (!cancelled) setSizesState({ loading: false, targetId, data });
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          setSizesState({ loading: false, targetId, error: error instanceof Error ? error.message : String(error) });
        }
      });
    return () => {
      cancelled = true;
    };
  }, [metadataTopics, sizeRequestBase, topicSizes]);
  const currentSizes = sizesState.targetId === sizeCluster?.targetId ? sizesState.data : undefined;
  const renderTopicSize = (name: string) => {
    const data = currentSizes;
    if (data) {
      if (!data.supported) {
        return (
          <span className="KafkaTopicSize muted" title="The brokers do not offer DescribeLogDirs">
            n/a
          </span>
        );
      }
      const size = data.topics[name];
      if (!size) return <span className="KafkaTopicSize muted">—</span>;
      const title = `Leader replicas ${formatBytes(size.leaderBytes)}, all replicas ${formatBytes(size.replicaBytes)}${
        size.exact ? "" : " (lower bound: a broker did not report)"
      }`;
      return (
        <span className="KafkaTopicSize" title={title}>
          {size.exact ? "" : "≥ "}
          {formatBytes(size.leaderBytes)}
        </span>
      );
    }
    if (sizesState.error && sizesState.targetId === sizeCluster?.targetId) {
      return (
        <span className="KafkaTopicSize muted" title={sizesState.error}>
          n/a
        </span>
      );
    }
    return <span className="KafkaTopicSize muted">…</span>;
  };

  const loadTopic = useCallback(() => {
    const cluster = state.selectedCluster;
    const topics = state.metadataState.data?.topics;
    if (!topicName || !cluster || !topics) {
      topicOperationId.current = undefined;
      setTopicState({ loading: false });
      return;
    }
    if (!topics.includes(topicName)) {
      topicOperationId.current = undefined;
      setTopicState({
        name: topicName,
        loading: false,
        error: `Topic ${topicName} was not found in this Kafka cluster.`,
      });
      return;
    }

    const operationId = createOperationId("topic");
    topicOperationId.current = operationId;
    const progress: KafkaProgressEvent = {
      operationId,
      operation: "topic",
      value: 1,
      phase: "strategy",
      label: "Preparing topic details",
      detail: "Selecting the existing read-only connection path.",
    };
    const cacheKey = dependencies.kubernetesClusterId ?? "active";
    const cached = dependencies.resourceCache.readTopic(cacheKey, cluster.targetId, topicName);
    setTopicState({ name: topicName, loading: true, data: cached.data, progress });

    dependencies.resourceCache
      .loadTopic(cacheKey, cluster.targetId, topicName, () =>
        topic({
          operationId,
          targetId: cluster.targetId,
          topic: topicName,
          namespace: cluster.namespace,
          clusterName: cluster.name,
          source: cluster.source,
          bootstrap: cluster.bootstrap,
          tls: cluster.tls,
          sourceLocator: cluster.sourceLocator,
          security: dependencies.connectionSettings.get(dependencies.kubernetesClusterId ?? "active", cluster.targetId),
        }),
      )
      .then((data) => {
        if (topicOperationId.current === operationId) {
          setTopicState({ name: topicName, loading: false, data });
        }
      })
      .catch((error: unknown) => {
        if (topicOperationId.current !== operationId) return;
        setTopicState((current) => ({
          name: topicName,
          loading: false,
          data: current.data,
          error: error instanceof Error ? error.message : String(error),
          progress,
        }));
      });
  }, [
    dependencies.connectionSettings,
    dependencies.kubernetesClusterId,
    dependencies.resourceCache,
    state.metadataState.data?.topics,
    state.selectedCluster,
    topic,
    topicName,
  ]);

  useEffect(() => {
    loadTopic();
    return () => {
      topicOperationId.current = undefined;
    };
  }, [loadTopic]);

  const loadTopicConfig = useCallback(() => {
    const cluster = state.selectedCluster;
    const topics = state.metadataState.data?.topics;
    if (!topicName || !cluster || !topics) {
      topicConfigOperationId.current = undefined;
      setTopicConfigState({ loading: false });
      return;
    }
    if (!topics.includes(topicName)) {
      topicConfigOperationId.current = undefined;
      setTopicConfigState({
        topic: topicName,
        loading: false,
        error: `Topic ${topicName} was not found in this Kafka cluster.`,
      });
      return;
    }

    const operationId = createOperationId("topicConfig");
    topicConfigOperationId.current = operationId;
    const progress: KafkaProgressEvent = {
      operationId,
      operation: "topicConfig",
      value: 1,
      phase: "strategy",
      label: "Preparing topic configuration",
      detail: "Selecting the existing read-only connection path.",
    };

    setTopicConfigState({ topic: topicName, loading: true, progress });

    topicConfig({
      operationId,
      targetId: cluster.targetId,
      topic: topicName,
      namespace: cluster.namespace,
      clusterName: cluster.name,
      source: cluster.source,
      bootstrap: cluster.bootstrap,
      tls: cluster.tls,
      sourceLocator: cluster.sourceLocator,
      security: dependencies.connectionSettings.get(dependencies.kubernetesClusterId ?? "active", cluster.targetId),
    })
      .then((data) => {
        if (topicConfigOperationId.current === operationId) {
          setTopicConfigState({ topic: topicName, loading: false, data });
        }
      })
      .catch((error: unknown) => {
        if (topicConfigOperationId.current !== operationId) return;
        setTopicConfigState({
          topic: topicName,
          loading: false,
          error: error instanceof Error ? error.message : String(error),
          progress,
        });
      });
  }, [
    dependencies.connectionSettings,
    dependencies.kubernetesClusterId,
    state.metadataState.data?.topics,
    state.selectedCluster,
    topicConfig,
    topicName,
  ]);

  useEffect(() => {
    if (view !== "configuration") return;
    if (topicConfigState.topic === topicName) return;
    loadTopicConfig();
  }, [loadTopicConfig, topicConfigState.topic, topicName, view]);

  const loadTopicConsumers = useCallback(() => {
    const cluster = state.selectedCluster;
    const topics = state.metadataState.data?.topics;
    if (!topicName || !cluster || !topics) {
      topicConsumersOperationId.current = undefined;
      setTopicConsumersState({ loading: false });
      return;
    }
    if (!topics.includes(topicName)) {
      topicConsumersOperationId.current = undefined;
      setTopicConsumersState({
        topic: topicName,
        loading: false,
        error: `Topic ${topicName} was not found in this Kafka cluster.`,
      });
      return;
    }

    const operationId = createOperationId("topicConsumers");
    topicConsumersOperationId.current = operationId;
    const progress: KafkaProgressEvent = {
      operationId,
      operation: "topicConsumers",
      value: 1,
      phase: "strategy",
      label: "Preparing topic consumers",
      detail: "Selecting the existing read-only connection path.",
    };
    setTopicConsumersState({ topic: topicName, loading: true, progress });

    topicConsumers({
      operationId,
      targetId: cluster.targetId,
      topic: topicName,
      namespace: cluster.namespace,
      clusterName: cluster.name,
      source: cluster.source,
      bootstrap: cluster.bootstrap,
      tls: cluster.tls,
      sourceLocator: cluster.sourceLocator,
      security: dependencies.connectionSettings.get(dependencies.kubernetesClusterId ?? "active", cluster.targetId),
    })
      .then((data) => {
        if (topicConsumersOperationId.current === operationId) {
          setTopicConsumersState({ topic: topicName, loading: false, data });
        }
      })
      .catch((error: unknown) => {
        if (topicConsumersOperationId.current !== operationId) return;
        setTopicConsumersState({
          topic: topicName,
          loading: false,
          error: error instanceof Error ? error.message : String(error),
          progress,
        });
      });
  }, [
    dependencies.connectionSettings,
    dependencies.kubernetesClusterId,
    state.metadataState.data?.topics,
    state.selectedCluster,
    topicConsumers,
    topicName,
  ]);

  useEffect(() => {
    if (view !== "consumers") return;
    if (topicConsumersState.topic === topicName) return;
    loadTopicConsumers();
  }, [loadTopicConsumers, topicConsumersState.topic, topicName, view]);

  useEffect(
    () => () => {
      topicConfigOperationId.current = undefined;
      topicConsumersOperationId.current = undefined;
    },
    [],
  );

  const selectWorkspaceCluster = (targetId: string): void => {
    const cluster = state.clusters.find((candidate) => candidate.targetId === targetId);
    if (!cluster) return;
    if (dependencies.kubernetesClusterId) {
      rememberKafkaClusterSelection(kafkaPersistentStateStore(), dependencies.kubernetesClusterId, cluster);
    }
    clearPageParam(params?.topic);
    clearPageParam(params?.query);
    state.selectCluster(targetId);
  };

  const resetProduceDraft = useCallback(() => {
    setProduceConfirmed(false);
    setProduceKey("");
    setProduceValue("");
    setProduceHeaders("");
    setProducePartition("");
    setProduceResult(undefined);
    setProduceError(undefined);
  }, []);

  const openDeleteTopic = useCallback(() => {
    if (!state.selectedCluster || !topicName) return;
    deleteTarget.current = { targetId: state.selectedCluster.targetId, topic: topicName };
    setDeleteConfirmed(false);
    setDeleteText("");
    setDeleteError(undefined);
    setWriteStatus(undefined);
    setDeleteOpen(true);
  }, [state.selectedCluster, topicName]);

  const selectedTargetId = state.selectedCluster?.targetId;
  useEffect(() => {
    // Locked context (REQ-106): a cluster or topic change cancels the pending deletion.
    const locked = deleteTarget.current;
    if (!deleteOpen || !locked) return;
    if (locked.targetId !== selectedTargetId || locked.topic !== topicName) {
      setDeleteOpen(false);
      setWriteStatus("Topic deletion cancelled: the cluster or topic changed before confirmation.");
    }
  }, [deleteOpen, selectedTargetId, topicName]);

  const canSubmitDelete =
    !deleteBusy &&
    canSubmitWriteAction({
      confirmationAccepted: deleteConfirmed,
      requiredResourceName: deleteTarget.current?.topic ?? topicName,
      enteredResourceName: deleteText,
    });

  const submitDeleteTopic = () => {
    const cluster = state.selectedCluster;
    const locked = deleteTarget.current;
    if (!cluster || !locked || locked.targetId !== cluster.targetId || locked.topic !== topicName) return;
    if (!canSubmitDelete) return;
    setDeleteBusy(true);
    setDeleteError(undefined);
    void deleteTopic({
      targetId: cluster.targetId,
      source: cluster.source,
      bootstrap: cluster.bootstrap,
      tls: cluster.tls,
      namespace: cluster.namespace,
      clusterName: cluster.name,
      topic: locked.topic,
    })
      .then((result) => {
        setDeleteOpen(false);
        setWriteStatus(`Deleted topic ${result.topic}`);
        state.refresh();
        setRawView("overview", true);
        setTopicName("");
      })
      .catch((error: unknown) => setDeleteError(error instanceof Error ? error.message : String(error)))
      .finally(() => setDeleteBusy(false));
  };

  if (topicName) {
    return (
      <KafkaPageShell
        title={topicName}
        subtitle={state.selectedCluster ? `Topics / ${state.selectedCluster.name}` : "Topic Workspace"}
        actions={
          <>
            <Renderer.Component.Button
              outlined
              onClick={() => {
                setRawView("overview", true);
                setTopicName("");
              }}
            >
              <Renderer.Component.Icon material="arrow_back" />
              Topics
            </Renderer.Component.Button>
            {canWrite && (
              <Renderer.Component.Button
                outlined
                data-testid="kafka-produce-message-button"
                onClick={() => {
                  resetProduceDraft();
                  setProduceOpen(true);
                }}
              >
                <Renderer.Component.Icon material="edit" />
                Produce message
              </Renderer.Component.Button>
            )}
            {canWrite && (
              <Renderer.Component.Button
                outlined
                className="KafkaDangerButton"
                data-testid="kafka-delete-topic-button"
                onClick={openDeleteTopic}
              >
                <Renderer.Component.Icon material="delete" />
                Delete topic
              </Renderer.Component.Button>
            )}
            <KafkaResourceActions state={state} onSelectCluster={selectWorkspaceCluster} />
          </>
        }
      >
        <KafkaResourceState state={state} />
        {writeStatus && (
          <div className="KafkaWriteStatus" role="status" data-testid="kafka-topic-write-status">
            <Renderer.Component.Icon material="check_circle" />
            <span>{writeStatus}</span>
          </div>
        )}
        {state.selectedCluster && state.metadataState.data && (
          <main className="KafkaResourcePage KafkaTopicWorkspace" data-testid="kafka-topic-workspace">
            {produceOpen && (
              <section
                className="KafkaWriteDrawer"
                data-testid="kafka-produce-message-drawer"
                aria-label="Produce message confirmation"
              >
                <div className="KafkaPageState warning">
                  <Renderer.Component.Icon material="edit" />
                  <div>
                    <strong>Produce message</strong>
                    <span>Write operations must be confirmed before any Kafka call is made.</span>
                  </div>
                  {produceResult && (
                    <div role="status">
                      Sent to partition {produceResult.partition} at offset {produceResult.offset}
                    </div>
                  )}
                  {produceError && <div role="alert">{produceError}</div>}
                </div>
                <div className="KafkaWriteForm" style={{ display: "grid", gap: 12 }}>
                  <Renderer.Component.Input
                    value={produceKey}
                    onChange={setProduceKey}
                    placeholder="Key (optional)"
                    aria-label="Message key"
                  />
                  <Renderer.Component.Input
                    value={produceValue}
                    onChange={setProduceValue}
                    placeholder="Value"
                    aria-label="Message value"
                  />
                  <Renderer.Component.Input
                    value={produceHeaders}
                    onChange={setProduceHeaders}
                    placeholder="Headers (key=value, one per line)"
                    aria-label="Message headers"
                  />
                  <Renderer.Component.Input
                    value={producePartition}
                    onChange={setProducePartition}
                    placeholder="Partition (optional)"
                    aria-label="Message partition"
                  />
                  <div className="KafkaWriteSummary">
                    <strong>Target</strong>
                    <span>{state.selectedCluster.name}</span>
                    <strong>Topic</strong>
                    <span>{topicName}</span>
                  </div>
                  <label style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                    <span>{getWriteConfirmationLabel({ destructive: false, resourceName: topicName })}</span>
                    <Renderer.Component.Switch
                      aria-label="Confirm produce message"
                      data-testid="kafka-produce-confirmation-switch"
                      checked={produceConfirmed}
                      onChange={setProduceConfirmed}
                    />
                  </label>
                  <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
                    <Renderer.Component.Button outlined onClick={() => setProduceOpen(false)}>
                      Cancel
                    </Renderer.Component.Button>
                    <Renderer.Component.Button
                      primary
                      disabled={
                        !canSubmitWriteAction({
                          confirmationAccepted: produceConfirmed,
                          requiredResourceName: undefined,
                        })
                      }
                      onClick={() => {
                        if (
                          !canSubmitWriteAction({
                            confirmationAccepted: produceConfirmed,
                            requiredResourceName: undefined,
                          })
                        ) {
                          return;
                        }
                        if (!state.selectedCluster || !topicName) return;
                        setProduceError(undefined);
                        void produce({
                          targetId: state.selectedCluster.targetId,
                          source: state.selectedCluster.source,
                          bootstrap: state.selectedCluster.bootstrap,
                          tls: state.selectedCluster.tls,
                          namespace: state.selectedCluster.namespace,
                          clusterName: state.selectedCluster.name,
                          topic: topicName,
                          key: produceKey || undefined,
                          value: produceValue,
                          headers: Object.fromEntries(
                            produceHeaders
                              .split("\n")
                              .map((line) => line.split("=", 2))
                              .filter(([key, value]) => key && value),
                          ),
                          partition: producePartition ? Number(producePartition) : undefined,
                        })
                          .then((result) => setProduceResult(result))
                          .catch((error: unknown) =>
                            setProduceError(error instanceof Error ? error.message : String(error)),
                          );
                      }}
                    >
                      Send message
                    </Renderer.Component.Button>
                  </div>
                </div>
              </section>
            )}
            {deleteOpen && (
              <section
                className="KafkaWriteDrawer"
                data-testid="kafka-delete-topic-drawer"
                aria-label="Delete topic confirmation"
              >
                <div className="KafkaPageState warning">
                  <Renderer.Component.Icon material="delete_forever" />
                  <div>
                    <strong>Delete topic</strong>
                    <span>
                      Removes the topic with all its partitions and records from the brokers. This cannot be undone.
                    </span>
                  </div>
                  {deleteError && <div role="alert">{deleteError}</div>}
                </div>
                <div className="KafkaWriteForm" style={{ display: "grid", gap: 12 }}>
                  <div className="KafkaWriteSummary">
                    <strong>Target</strong>
                    <span>{state.selectedCluster.name}</span>
                    <strong>Topic</strong>
                    <span>{topicName}</span>
                    <strong>Partitions</strong>
                    <span>{topicState.data?.partitions.length ?? "unknown"}</span>
                  </div>
                  <label style={{ display: "grid", gap: 6 }}>
                    <span>{getWriteConfirmationLabel({ destructive: true, resourceName: topicName })}</span>
                    <Renderer.Component.Input
                      value={deleteText}
                      onChange={setDeleteText}
                      placeholder={topicName}
                      aria-label="Type topic to confirm deletion"
                    />
                  </label>
                  <label style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                    <span>I understand that every record of this topic is lost</span>
                    <Renderer.Component.Switch
                      aria-label="Confirm topic deletion"
                      data-testid="kafka-delete-topic-confirmation-switch"
                      checked={deleteConfirmed}
                      onChange={setDeleteConfirmed}
                    />
                  </label>
                  <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
                    <Renderer.Component.Button outlined onClick={() => setDeleteOpen(false)}>
                      Cancel
                    </Renderer.Component.Button>
                    <Renderer.Component.Button
                      primary
                      className="KafkaDangerButton"
                      data-testid="kafka-delete-topic-submit"
                      disabled={!canSubmitDelete}
                      onClick={submitDeleteTopic}
                    >
                      Delete topic
                    </Renderer.Component.Button>
                  </div>
                </div>
              </section>
            )}
            <nav className="KafkaEntityTabs" role="tablist" aria-label="Topic sections">
              {(["overview", "messages", "partitions", "consumers", "configuration"] as const).map((tab) => (
                <Renderer.Component.Button
                  key={tab}
                  plain
                  role="tab"
                  active={view === tab}
                  aria-selected={view === tab}
                  onClick={() => setRawView(tab)}
                >
                  {tab === "overview"
                    ? "Overview"
                    : tab === "messages"
                      ? "Messages"
                      : tab === "partitions"
                        ? "Partitions"
                        : tab === "consumers"
                          ? "Consumers"
                          : "Configuration"}
                </Renderer.Component.Button>
              ))}
            </nav>
            {view === "messages" ? (
              <KafkaMessagesBrowser
                cluster={state.selectedCluster}
                topic={topicName}
                partitions={topicState.data?.partitions ?? []}
                metadataLoading={topicState.loading}
                metadataError={topicState.error}
                metadataProgress={topicState.progress}
                security={dependencies.connectionSettings.get(
                  dependencies.kubernetesClusterId ?? "active",
                  state.selectedCluster.targetId,
                )}
                schemaRegistry={
                  state.selectedCluster
                    ? (() => {
                        const configured = schemaRegistrySettings.get(state.selectedCluster.targetId);
                        return configured
                          ? {
                              registryUrl: configured.registryUrl,
                              username: configured.username,
                              password: endpointSecrets.get(state.selectedCluster.targetId)?.registryPassword,
                            }
                          : undefined;
                      })()
                    : undefined
                }
                browse={messagesBrowse}
                subscribeProgress={dependencies.subscribeProgress}
                onRetryMetadata={loadTopic}
                filters={{ key: keyFilter, value: valueFilter, headerKey, headerValue }}
                timestamp={timestamp}
                onTimestampChange={(value) => setTimestamp(value, true)}
                onFilterChange={(name, value) => {
                  const setters = {
                    key: setKeyFilter,
                    value: setValueFilter,
                    headerKey: setHeaderKey,
                    headerValue: setHeaderValue,
                  };
                  setters[name](value, true);
                }}
              />
            ) : view === "configuration" ? (
              <TopicConfiguration topic={topicName} state={topicConfigState} onRetry={loadTopicConfig} />
            ) : view === "consumers" ? (
              <TopicConsumers
                state={topicConsumersState}
                onRetry={loadTopicConsumers}
                onOpenGroup={(groupId) => onOpenGroup(state.selectedCluster?.targetId ?? "", groupId)}
              />
            ) : (
              <TopicMetadata
                state={topicState}
                onRetry={loadTopic}
                view={view === "partitions" ? "partitions" : "overview"}
                showHeader={false}
                sizes={topicName ? currentSizes?.topics[topicName] : undefined}
                sizesSupported={currentSizes?.supported}
              />
            )}
          </main>
        )}
      </KafkaPageShell>
    );
  }

  const allTopics = state.metadataState.data?.topics ?? [];
  const listSizes = currentSizes;
  const sizeSummary = listSizes
    ? listSizes.supported
      ? formatBytes(sumBytes(allTopics.map((name) => listSizes.topics[name]?.leaderBytes)))
      : "n/a"
    : sizesState.error
      ? "n/a"
      : "…";
  const filteredTopics = filterTopicNames(allTopics, query);
  const topicWindow = kafkaListWindow(filteredTopics, topicPage);
  const internalCount = allTopics.filter((name) => name.startsWith("__")).length;

  return (
    <KafkaPageShell
      title="Topics"
      subtitle={state.selectedCluster ? state.selectedCluster.name : "Browse topics in a Kafka cluster"}
      actions={<KafkaResourceActions state={state} />}
    >
      <KafkaResourceState state={state} />
      {writeStatus && (
        <div className="KafkaWriteStatus" role="status" data-testid="kafka-topic-write-status">
          <Renderer.Component.Icon material="check_circle" />
          <span>{writeStatus}</span>
        </div>
      )}
      {state.selectedCluster && state.metadataState.data && (
        <main className="KafkaResourcePage KafkaTopicsPage" data-testid="kafka-topics-page">
          <KafkaMetricStrip
            ariaLabel="Kafka topic summary"
            className="KafkaResourceMetricStrip"
            metrics={[
              { label: "Topics", value: allTopics.length },
              { label: "Application", value: allTopics.length - internalCount },
              { label: "Internal", value: internalCount },
              { label: "Size", value: sizeSummary },
            ]}
          />
          {allTopics.length === 0 ? (
            <div className="KafkaPageState empty">
              <Renderer.Component.Icon material="topic" />
              <div>
                <strong>No topics returned</strong>
                <span>Kafka returned an empty topic list.</span>
              </div>
            </div>
          ) : (
            <>
              <Renderer.Component.Input
                className="KafkaTopicSearch"
                value={query}
                onChange={(nextQuery) => setQuery(nextQuery, true)}
                iconLeft="search"
                placeholder="Filter topics"
                aria-label="Filter Kafka topics"
              />
              <Renderer.Component.Table<string>
                className="KafkaTopicTable KafkaTopicPageTable"
                tableId="kafka-topics-page"
                autoSize={false}
                scrollable
                sortSyncWithUrl={false}
                sortByDefault={{ sortBy: "name", orderBy: "asc" }}
                sortable={{
                  name: (name) => name.toLowerCase(),
                  size: (name) => Number(currentSizes?.topics[name]?.leaderBytes ?? -1),
                }}
                noItems={
                  <div className="KafkaTopicPrompt">
                    <Renderer.Component.Icon material="filter_alt_off" />
                    <span>No topics match the current filter.</span>
                  </div>
                }
              >
                <Renderer.Component.TableHead sticky={false} nowrap>
                  <Renderer.Component.TableCell className="topicNameCell" sortBy="name" style={TOPIC_NAME_COLUMN_STYLE}>
                    Topic
                  </Renderer.Component.TableCell>
                  <Renderer.Component.TableCell className="topicTypeCell" style={TOPIC_TYPE_COLUMN_STYLE}>
                    Type
                  </Renderer.Component.TableCell>
                  <Renderer.Component.TableCell className="topicSizeCell" sortBy="size" style={TOPIC_SIZE_COLUMN_STYLE}>
                    Size
                  </Renderer.Component.TableCell>
                  <Renderer.Component.TableCell className="topicActionCell" style={TOPIC_ACTION_COLUMN_STYLE} />
                </Renderer.Component.TableHead>
                {topicWindow.items.map((name) => {
                  const open = (event: { stopPropagation: () => void }): void => {
                    event.stopPropagation();
                    setRawView("overview", true);
                    setTopicName(name);
                  };
                  const interaction: HTMLAttributes<HTMLDivElement> = {
                    role: "button",
                    tabIndex: 0,
                    "aria-label": `Open topic ${name}`,
                    onClick: open,
                    onKeyDown: (event) => {
                      if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault();
                        open(event);
                      }
                    },
                  };
                  return (
                    <Renderer.Component.TableRow
                      {...interaction}
                      className="KafkaInteractiveRow"
                      key={name}
                      sortItem={name}
                      data-topic={name}
                      nowrap
                    >
                      <Renderer.Component.TableCell
                        className="topicNameCell"
                        title={name}
                        style={TOPIC_NAME_COLUMN_STYLE}
                      >
                        <Renderer.Component.Icon material="topic" />
                        <span className="KafkaEllipsis">{name}</span>
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="topicTypeCell" style={TOPIC_TYPE_COLUMN_STYLE}>
                        {name.startsWith("__") ? (
                          <Renderer.Component.Badge small className="KafkaInternalTopicBadge" label="Internal" />
                        ) : (
                          <span className="KafkaTopicType">Application</span>
                        )}
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="topicSizeCell" style={TOPIC_SIZE_COLUMN_STYLE}>
                        {renderTopicSize(name)}
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="topicActionCell" style={TOPIC_ACTION_COLUMN_STYLE}>
                        <Renderer.Component.Icon material="chevron_right" />
                      </Renderer.Component.TableCell>
                    </Renderer.Component.TableRow>
                  );
                })}
              </Renderer.Component.Table>
              {topicWindow.pageCount > 1 && (
                <div className="KafkaListPagination" aria-label="Topic pages">
                  <Renderer.Component.Button
                    outlined
                    disabled={topicWindow.page === 0}
                    onClick={() => setTopicPage((page) => Math.max(0, page - 1))}
                    aria-label="Previous topic page"
                  >
                    Previous
                  </Renderer.Component.Button>
                  <span>
                    Page {topicWindow.page + 1} of {topicWindow.pageCount} ({topicWindow.total} topics)
                  </span>
                  <Renderer.Component.Button
                    outlined
                    disabled={topicWindow.page >= topicWindow.pageCount - 1}
                    onClick={() => setTopicPage((page) => Math.min(topicWindow.pageCount - 1, page + 1))}
                    aria-label="Next topic page"
                  >
                    Next
                  </Renderer.Component.Button>
                </div>
              )}
            </>
          )}
        </main>
      )}
    </KafkaPageShell>
  );
}
