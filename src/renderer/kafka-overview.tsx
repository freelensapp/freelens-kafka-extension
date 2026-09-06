import { Renderer } from "@freelensapp/extensions";
import { useCallback, useEffect, useRef, useState } from "react";
import { aggregateHealthSnapshotKey } from "../common/aggregate-health-snapshot";
import { kafkaPersistentStateStore } from "../common/kafka-persistent-state-store";
import { formatKafkaEta } from "../common/kafka-phase-progress";
import { createKafkaTargetId } from "../common/kafka-target";
import { chooseStrategy, firstBrokerAddress } from "../common/reachability";
import { KafkaClusterCatalogStore } from "./kafka-cluster-catalog";
import { loadManualKafkaEndpoints, mergeKafkaClusters, saveManualKafkaEndpoints } from "./kafka-manual-endpoints";
import { forgetKafkaClusterSelection, rememberKafkaClusterSelection } from "./kafka-navigation";
import { KafkaMetricStrip, KafkaPageShell } from "./kafka-page-shell";
import {
  createOperationId,
  kafkaHealthProgressIsCompact,
  kafkaProgressCount,
  kafkaProgressIsDeterminate,
  kafkaProgressPhasePercent,
  kafkaUsageContext,
  kafkaUsageSummary,
  topicPartitionHealth,
} from "./kafka-view-model";

import type { CSSProperties, HTMLAttributes } from "react";

import type {
  ClusterOverviewDto,
  DiscoveredKafkaInfo,
  DiscoverRequest,
  KafkaAuthMode,
  KafkaProgressEvent,
  KafkaProgressOperation,
  KafkaSecurityHint,
  KafkaSecurityOverride,
  KafkaTlsMode,
  OverviewRequest,
  TopicDetailDto,
  TopicPartitionDto,
} from "../common/ipc";
import type { KafkaConnectSettingsStore } from "./kafka-connect-settings";
import type { KafkaConnectionSettingsStore } from "./kafka-connection-settings";
import type { KafkaResourceCache } from "./kafka-resource-cache";
import type { KafkaSchemaRegistrySettingsStore } from "./kafka-schema-registry-settings";
import type { KafkaWriteSettingsStore } from "./kafka-write-settings";

export interface KafkaOverviewPageProps {
  connectionSettings: KafkaConnectionSettingsStore;
  writeSettings: KafkaWriteSettingsStore;
  schemaRegistrySettings: KafkaSchemaRegistrySettingsStore;
  connectSettings: KafkaConnectSettingsStore;
  kubernetesClusterId?: string;
  resourceCache: KafkaResourceCache;
  query?: string;
  onQueryChange?: (query: string) => void;
  onOpenCluster: (cluster: DiscoveredKafkaInfo) => void;
  discover: (request?: DiscoverRequest) => Promise<DiscoveredKafkaInfo[]>;
  overview: (request: OverviewRequest) => Promise<ClusterOverviewDto>;
  invalidateHealth: (
    targetId: string,
    options: { closeSessions?: boolean; removePersisted?: boolean },
  ) => Promise<void>;
  reachability: (bootstraps: string[]) => Promise<Record<string, boolean>>;
  subscribeProgress: (listener: (progress: KafkaProgressEvent) => void) => () => void;
}

interface DiscoveryState {
  loading: boolean;
  error?: string;
  kafkas: DiscoveredKafkaInfo[];
}

interface DetailState {
  loading: boolean;
  error?: string;
  data?: ClusterOverviewDto;
  progress?: KafkaProgressEvent;
}

export interface TopicState {
  name?: string;
  loading: boolean;
  error?: string;
  data?: TopicDetailDto;
  progress?: KafkaProgressEvent;
}

type ReachabilityFilter = "all" | "pc" | "pods";

const TLS_MODE_OPTIONS: Renderer.Component.SelectOption<KafkaTlsMode>[] = [
  { value: "auto", label: "Automatic" },
  { value: "enabled", label: "TLS enabled" },
  { value: "disabled", label: "TLS disabled" },
];

const AUTH_MODE_OPTIONS: Renderer.Component.SelectOption<KafkaAuthMode>[] = [
  { value: "auto", label: "Automatic" },
  { value: "none", label: "No authentication" },
  { value: "plain", label: "SASL/PLAIN" },
  { value: "scram-sha-256", label: "SCRAM-SHA-256" },
  { value: "scram-sha-512", label: "SCRAM-SHA-512" },
];

const sourceLabel = (kafka: DiscoveredKafkaInfo): string => kafka.provider ?? kafka.source;

export function ReachabilityBadge({ value, label }: { value?: boolean; label?: boolean }) {
  if (value === undefined) {
    return (
      <Renderer.Component.Badge
        className="KafkaStatusBadge checking"
        small
        label={
          <>
            <Renderer.Component.Icon material="schedule" />
            <span>Checking</span>
          </>
        }
      />
    );
  }

  const text = label ? (value ? "Reachable" : "Unavailable") : value ? "Yes" : "No";
  return (
    <Renderer.Component.Badge
      className={`KafkaStatusBadge ${value ? "reachable" : "unreachable"}`}
      small
      label={
        <>
          <Renderer.Component.Icon material={value ? "check_circle" : "cancel"} />
          <span>{text}</span>
        </>
      }
    />
  );
}

function PodReachabilityBadge({ kafka, label }: { kafka: DiscoveredKafkaInfo; label?: boolean }) {
  if (kafka.source === "manual") {
    return (
      <Renderer.Component.Badge
        className="KafkaStatusBadge unknown"
        small
        label={
          <>
            <Renderer.Component.Icon material="help_outline" />
            <span>Unknown</span>
          </>
        }
      />
    );
  }
  return <ReachabilityBadge value={true} label={label} />;
}

export function SourceBadge({ kafka }: { kafka: DiscoveredKafkaInfo }) {
  const icon =
    kafka.source === "manual" ? "add_link" : kafka.external ? "cloud" : kafka.source === "strimzi" ? "hub" : "dns";
  return (
    <Renderer.Component.Badge
      className={`KafkaSourceBadge source-${kafka.source}`}
      small
      label={
        <>
          <Renderer.Component.Icon material={icon} />
          <span>{sourceLabel(kafka)}</span>
        </>
      }
      title={kafka.external ? "External Kafka" : "Kubernetes cluster Kafka"}
    />
  );
}

function securityLabel(security: KafkaSecurityHint): string {
  if (security.auth === "mtls") return "mTLS";
  if (security.auth === "none") return security.tls ? "TLS" : "PLAINTEXT";
  const auth = security.auth === "plain" ? "PLAIN" : security.auth.toUpperCase();
  return security.tls ? `${auth} · TLS` : auth;
}

export function SecurityBadge({ security }: { security: KafkaSecurityHint }) {
  const icon = security.auth === "mtls" ? "verified_user" : security.tls ? "lock" : "lock_open";
  return (
    <Renderer.Component.Badge
      className={`KafkaTlsBadge ${security.tls ? "enabled" : "disabled"}`}
      small
      label={
        <>
          <Renderer.Component.Icon material={icon} />
          <span>{securityLabel(security)}</span>
        </>
      }
    />
  );
}

const PROGRESS_STEPS = {
  discovery: ["Cluster", "Resources", "Workloads", "Merge"],
  overview: ["Security", "Connection", "Brokers", "Topics"],
  health: ["Connection", "Topology", "Groups", "Lag"],
  topic: ["Security", "Connection", "Metadata", "Ready"],
  topicConfig: ["Security", "Connection", "Configuration", "Ready"],
  topicConsumers: ["Security", "Connection", "Groups", "Ready"],
  brokerConfig: ["Security", "Connection", "Configuration", "Ready"],
  messagesBrowse: ["Security", "Connection", "Offsets", "Records"],
  groups: ["Security", "Connection", "Groups", "Ready"],
  groupDetail: ["Security", "Connection", "Offsets", "Ready"],
} as const satisfies Record<KafkaProgressOperation, readonly string[]>;

function progressStep(progress: KafkaProgressEvent): number {
  if (progress.operation === "discovery") {
    if (progress.value >= 97) return 3;
    if (progress.value >= 23) return 2;
    if (progress.value >= 13) return 1;
    return 0;
  }
  if (progress.operation === "topic") {
    if (progress.value >= 98) return 3;
    if (progress.value >= 78) return 2;
    if (progress.value >= 46) return 1;
    return 0;
  }
  if (progress.operation === "messagesBrowse") {
    if (progress.value >= 100) return 3;
    if (progress.value >= 62) return 2;
    if (progress.value >= 46) return 1;
    return 0;
  }
  if (progress.operation === "health") {
    if (progress.phase === "complete" || progress.phase === "watermarks") return 3;
    if (progress.phase === "groups") return 2;
    if (progress.phase === "topology" || progress.phase === "health") return 1;
    return 0;
  }
  if (
    progress.operation === "topicConfig" ||
    progress.operation === "topicConsumers" ||
    progress.operation === "brokerConfig" ||
    progress.operation === "groups" ||
    progress.operation === "groupDetail"
  ) {
    if (progress.value >= 100) return 3;
    if (progress.value >= 62) return 2;
    if (progress.value >= 46) return 1;
    return 0;
  }
  if (progress.value >= 88) return 3;
  if (progress.value >= 72) return 2;
  if (progress.value >= 46) return 1;
  return 0;
}

export function OperationProgress({ progress, error }: { progress: KafkaProgressEvent; error?: string }) {
  const activeStep = progressStep(progress);
  const phasePercent = kafkaProgressPhasePercent(progress);
  const determinate = kafkaProgressIsDeterminate(progress);
  const steps = PROGRESS_STEPS[progress.operation];
  const progressUnit =
    progress.operation === "discovery" || progress.phase === "security"
      ? "workloads"
      : progress.operation === "health" && (progress.phase === "topology" || progress.phase === "watermarks")
        ? "topics"
        : "groups";
  return (
    <div
      className={`KafkaOperationProgress ${error ? "error" : ""}`}
      role="status"
      aria-live="polite"
      data-operation={progress.operation}
      data-phase={progress.phase}
      data-operation-state={progress.label}
      data-progress={phasePercent}
      data-progress-mode={determinate ? "determinate" : "indeterminate"}
      data-eta-ms={progress.etaMs}
      data-eta-state={progress.etaStatus ?? "unavailable"}
      data-elapsed-ms={progress.elapsedMs}
      data-phase-elapsed-ms={progress.phaseElapsedMs}
      data-stage-duration-ms={progress.stageDurationMs}
      data-completed={progress.completed}
      data-total={progress.total}
    >
      <div className="KafkaProgressHeader">
        <div>
          <strong>{progress.label}</strong>
          {progress.detail && <span>{progress.detail}</span>}
        </div>
        <div className="KafkaProgressMeasure">
          <strong>{determinate ? `${phasePercent}% of phase` : "Updating"}</strong>
          {progress.etaStatus === "ready" && progress.etaMs !== undefined && (
            <span data-testid="kafka-progress-eta">{formatKafkaEta(progress.etaMs)}</span>
          )}
        </div>
      </div>
      <Renderer.Component.LineProgress
        className={`KafkaProgressLine ${determinate ? "" : "indeterminate"}`.trim()}
        min={0}
        max={100}
        value={determinate ? (phasePercent ?? 100) : 100}
        aria-label={`${progress.label} phase progress`}
        aria-valuetext={determinate ? `${phasePercent}% of phase` : "Updating"}
      />
      {progress.total !== undefined && (
        <div className="KafkaProgressCount">
          {progress.completed ?? 0} / {progress.total} {progressUnit}
        </div>
      )}
      <ol className="KafkaProgressSteps">
        {steps.map((step, index) => (
          <li key={step} className={index < activeStep ? "complete" : index === activeStep ? "current" : "pending"}>
            <Renderer.Component.Icon
              material={
                index < activeStep
                  ? "check_circle"
                  : index === activeStep
                    ? "radio_button_checked"
                    : "radio_button_unchecked"
              }
            />
            <span>{step}</span>
          </li>
        ))}
      </ol>
      {error && (
        <div className="KafkaProgressError" role="alert">
          <Renderer.Component.Icon material="error_outline" />
          <span>{error}</span>
        </div>
      )}
    </div>
  );
}

export function HealthProgress({
  progress,
  error,
  hasSnapshot = false,
}: {
  progress: KafkaProgressEvent;
  error?: string;
  hasSnapshot?: boolean;
}) {
  const compact = kafkaHealthProgressIsCompact(progress, error, hasSnapshot);
  if (!compact) {
    return (
      <div className="KafkaHealthProgress" data-testid="kafka-health-progress" data-compact="false">
        <OperationProgress progress={progress} error={error} />
      </div>
    );
  }

  const phasePercent = kafkaProgressPhasePercent(progress);
  const count = kafkaProgressCount(progress);
  const percent = phasePercent === undefined ? "Updating" : `${phasePercent}%`;
  const eta =
    progress.etaStatus === "ready" && progress.etaMs !== undefined ? formatKafkaEta(progress.etaMs) : undefined;
  const liveStatus = [progress.label, count, percent, eta]
    .filter((value): value is string => Boolean(value))
    .join(", ");
  return (
    <details
      className="KafkaHealthProgress KafkaHealthProgressCompact"
      data-testid="kafka-health-progress"
      data-compact="true"
    >
      <summary>
        <Renderer.Component.Icon material="sync" />
        <span className="KafkaHealthProgressLabel">{progress.label}</span>
        {count && <span aria-hidden="true">{count}</span>}
        <strong aria-hidden="true">{percent}</strong>
        {eta && <span aria-hidden="true">{eta}</span>}
        <Renderer.Component.Icon material="expand_more" />
        <span className="KafkaProgressLiveStatus" role="status" aria-live="polite">
          {liveStatus}
        </span>
      </summary>
      <OperationProgress progress={progress} />
    </details>
  );
}

function UsageCell({ kafka }: { kafka: DiscoveredKafkaInfo }) {
  const usage = kafkaUsageSummary(kafka);
  return (
    <div className="KafkaUsageCell" title={usage.title}>
      <span>{usage.primary}</span>
      <small>{usage.secondary}</small>
    </div>
  );
}

function PartitionHealthBadge({ partition }: { partition: TopicPartitionDto }) {
  const health = topicPartitionHealth(partition);
  const className = health.toLowerCase().replace("-", "");
  const icon = health === "Healthy" ? "check_circle" : health === "Under-replicated" ? "warning_amber" : "error";
  return (
    <div className="KafkaPartitionHealth">
      <Renderer.Component.Badge
        className={`KafkaPartitionBadge ${className}`}
        small
        label={
          <>
            <Renderer.Component.Icon material={icon} />
            <span>{health}</span>
          </>
        }
      />
      {partition.offlineReplicas.length > 0 && <small>Offline: {partition.offlineReplicas.join(", ")}</small>}
      {partition.errorCode !== 0 && <small>Error code: {partition.errorCode}</small>}
    </div>
  );
}

function TopicMetricStrip({ data }: { data: TopicDetailDto }) {
  return (
    <KafkaMetricStrip
      ariaLabel="Kafka topic metadata summary"
      className="KafkaTopicMetricStrip"
      metrics={[
        { label: "Partitions", value: data.partitionCount },
        { label: "Replication", value: data.replicationFactor },
        {
          label: "Under-replicated",
          value: data.underReplicatedPartitions,
          tone: data.underReplicatedPartitions > 0 ? "warning" : undefined,
        },
        {
          label: "Unavailable",
          value: data.unavailablePartitions,
          tone: data.unavailablePartitions > 0 ? "error" : undefined,
        },
      ]}
    />
  );
}

const PARTITION_ID_COLUMN_STYLE: CSSProperties = { flex: "0 0 112px", minWidth: 112, width: 112 };
const PARTITION_LEADER_COLUMN_STYLE: CSSProperties = { flex: "0 0 84px", minWidth: 84, width: 84 };
const PARTITION_REPLICAS_COLUMN_STYLE: CSSProperties = { flex: "1 1 160px", minWidth: 120, width: 0 };
const PARTITION_ISR_COLUMN_STYLE: CSSProperties = { flex: "1 1 160px", minWidth: 120, width: 0 };
const PARTITION_STATE_COLUMN_STYLE: CSSProperties = { flex: "0 0 156px", minWidth: 156, width: 156 };

function TopicPartitions({ data }: { data: TopicDetailDto }) {
  if (data.partitions.length === 0) {
    return <div className="KafkaTopicPrompt">Kafka returned no partitions for this topic.</div>;
  }

  return (
    <Renderer.Component.Table<TopicPartitionDto>
      className="KafkaPartitionTable"
      tableId="kafka-topic-partitions"
      autoSize={false}
      scrollable
      sortSyncWithUrl={false}
      sortByDefault={{ sortBy: "partition", orderBy: "asc" }}
      sortable={{ partition: (partition) => partition.partitionId }}
    >
      <Renderer.Component.TableHead sticky={false} nowrap>
        <Renderer.Component.TableCell className="partitionIdCell" sortBy="partition" style={PARTITION_ID_COLUMN_STYLE}>
          Partition
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="partitionLeaderCell" style={PARTITION_LEADER_COLUMN_STYLE}>
          Leader
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="partitionReplicasCell" style={PARTITION_REPLICAS_COLUMN_STYLE}>
          Replicas
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="partitionIsrCell" style={PARTITION_ISR_COLUMN_STYLE}>
          ISR
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="partitionHealthCell" style={PARTITION_STATE_COLUMN_STYLE}>
          State
        </Renderer.Component.TableCell>
      </Renderer.Component.TableHead>
      {data.partitions.map((partition) => (
        <Renderer.Component.TableRow key={partition.partitionId} sortItem={partition} nowrap>
          <Renderer.Component.TableCell className="partitionIdCell" style={PARTITION_ID_COLUMN_STYLE}>
            {partition.partitionId}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell className="partitionLeaderCell" style={PARTITION_LEADER_COLUMN_STYLE}>
            {partition.leader < 0 ? "—" : partition.leader}
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell className="partitionReplicasCell" style={PARTITION_REPLICAS_COLUMN_STYLE}>
            <span className="KafkaReplicaSet">{partition.replicas.join(", ") || "—"}</span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell className="partitionIsrCell" style={PARTITION_ISR_COLUMN_STYLE}>
            <span className="KafkaReplicaSet">{partition.isr.join(", ") || "—"}</span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell className="partitionHealthCell" style={PARTITION_STATE_COLUMN_STYLE}>
            <PartitionHealthBadge partition={partition} />
          </Renderer.Component.TableCell>
        </Renderer.Component.TableRow>
      ))}
    </Renderer.Component.Table>
  );
}

export function TopicMetadata({
  state,
  onRetry,
  view = "all",
  showHeader = true,
}: {
  state: TopicState;
  onRetry: () => void;
  view?: "all" | "overview" | "partitions";
  showHeader?: boolean;
}) {
  if (!state.name) {
    return (
      <div className="KafkaTopicPrompt">
        <Renderer.Component.Icon material="topic" />
        <span>Select a topic to inspect partition topology.</span>
      </div>
    );
  }

  return (
    <section className="KafkaTopicMetadata" aria-label={`Topic details for ${state.name}`}>
      {showHeader && (
        <div className="KafkaTopicMetadataHeader">
          <div>
            <Renderer.Component.DrawerTitle size="sub-title">{state.name}</Renderer.Component.DrawerTitle>
            {state.data?.internal && (
              <Renderer.Component.Badge small className="KafkaInternalTopicBadge" label="Internal" />
            )}
          </div>
          {state.error && (
            <Renderer.Component.Button outlined onClick={onRetry}>
              <Renderer.Component.Icon material="refresh" />
              Retry
            </Renderer.Component.Button>
          )}
        </div>
      )}

      {!showHeader && state.error && (
        <div className="KafkaTopicInlineError" role="alert">
          <span>{state.error}</span>
          <Renderer.Component.Button outlined onClick={onRetry}>
            <Renderer.Component.Icon material="refresh" />
            Retry
          </Renderer.Component.Button>
        </div>
      )}

      {(state.loading || state.error) && state.progress && (
        <OperationProgress progress={state.progress} error={state.error} />
      )}

      {state.data && view !== "partitions" && <TopicMetricStrip data={state.data} />}
      {state.data && view !== "overview" && <TopicPartitions data={state.data} />}
    </section>
  );
}

function ConnectionSettingsDrawer({
  kafka,
  pcReachable,
  detail,
  securityOverride,
  writeSettings,
  schemaRegistrySettings,
  connectSettings,
  onApplySecurity,
  onClose,
  onRemove,
}: {
  kafka: DiscoveredKafkaInfo;
  pcReachable?: boolean;
  detail: DetailState;
  securityOverride?: KafkaSecurityOverride;
  writeSettings: KafkaWriteSettingsStore;
  schemaRegistrySettings: KafkaSchemaRegistrySettingsStore;
  connectSettings: KafkaConnectSettingsStore;
  onApplySecurity: (security?: KafkaSecurityOverride) => void;
  onClose: () => void;
  onRemove?: () => void;
}) {
  const [tlsMode, setTlsMode] = useState<KafkaTlsMode>(securityOverride?.tlsMode ?? "auto");
  const [authMode, setAuthMode] = useState<KafkaAuthMode>(securityOverride?.authMode ?? "auto");
  const [username, setUsername] = useState(securityOverride?.username ?? "");
  const [password, setPassword] = useState(securityOverride?.password ?? "");
  const [settingsError, setSettingsError] = useState<string>();
  const [writeModeEnabled, setWriteModeEnabled] = useState<boolean>(writeSettings.get(kafka.targetId));
  const [registryUrl, setRegistryUrl] = useState(() => schemaRegistrySettings.get(kafka.targetId)?.registryUrl ?? "");
  const [registryTls, setRegistryTls] = useState(() => schemaRegistrySettings.get(kafka.targetId)?.tls ?? false);
  const [registryUsername, setRegistryUsername] = useState(
    () => schemaRegistrySettings.get(kafka.targetId)?.username ?? "",
  );
  const [connectUrl, setConnectUrl] = useState(() => connectSettings.get(kafka.targetId)?.connectUrl ?? "");
  const [connectUsername, setConnectUsername] = useState(() => connectSettings.get(kafka.targetId)?.username ?? "");

  useEffect(() => {
    setTlsMode(securityOverride?.tlsMode ?? "auto");
    setAuthMode(securityOverride?.authMode ?? "auto");
    setUsername(securityOverride?.username ?? "");
    setPassword(securityOverride?.password ?? "");
    setWriteModeEnabled(writeSettings.get(kafka.targetId));
    const registry = schemaRegistrySettings.get(kafka.targetId);
    setRegistryUrl(registry?.registryUrl ?? "");
    setRegistryTls(registry?.tls ?? false);
    setRegistryUsername(registry?.username ?? "");
    const connect = connectSettings.get(kafka.targetId);
    setConnectUrl(connect?.connectUrl ?? "");
    setConnectUsername(connect?.username ?? "");
    setSettingsError(undefined);
  }, [connectSettings, kafka, schemaRegistrySettings, securityOverride, writeSettings]);

  const strategy = pcReachable === undefined ? undefined : chooseStrategy(kafka.source, pcReachable);
  const strategyLabel =
    strategy === "direct" ? "Direct" : strategy === "portForward" ? "Port-forward" : "Relay required";
  const security = detail.data?.security ?? kafka.securityHint ?? { tls: kafka.tls, auth: "none" };
  const explicitAuth = authMode !== "auto" && authMode !== "none";

  const applySettings = (): void => {
    if (explicitAuth && (!username.trim() || !password)) {
      setSettingsError("Username and password are required for explicit SASL authentication.");
      return;
    }
    setSettingsError(undefined);
    if (tlsMode === "auto" && authMode === "auto") {
      onApplySecurity(undefined);
      return;
    }
    onApplySecurity({
      tlsMode,
      authMode,
      ...(explicitAuth ? { username: username.trim(), password } : {}),
    });
  };

  return (
    <Renderer.Component.Drawer
      className="KafkaConnectionSettingsDrawer"
      contentClass="KafkaConnectionSettingsBody"
      open
      usePortal
      size="min(520px, calc(100vw - 24px))"
      title={`Connection settings: ${kafka.name}`}
      onClose={onClose}
      data-testid="kafka-connection-settings"
    >
      <div className="KafkaDetailContext">{kafkaUsageContext(kafka)}</div>
      <div className="KafkaDetailBadges">
        <SourceBadge kafka={kafka} />
        <SecurityBadge security={security} />
        {writeSettings.get(kafka.targetId) && <Renderer.Component.Badge small label="Write mode" />}
      </div>

      <Renderer.Component.DrawerItem name="Bootstrap" title={kafka.bootstrap}>
        <code className="KafkaBootstrapDetail">{kafka.bootstrap}</code>
      </Renderer.Component.DrawerItem>
      <Renderer.Component.DrawerItem name="From PC">
        <ReachabilityBadge value={pcReachable} label />
      </Renderer.Component.DrawerItem>
      <Renderer.Component.DrawerItem name="From pods">
        <PodReachabilityBadge kafka={kafka} label />
      </Renderer.Component.DrawerItem>
      <Renderer.Component.DrawerItem name="Connection">
        <Renderer.Component.Badge
          className={`KafkaStrategyBadge strategy-${strategy ?? "checking"}`}
          small
          label={strategyLabel}
        />
      </Renderer.Component.DrawerItem>

      <section className="KafkaConnectionSettings" aria-label="Kafka connection settings">
        <h2>Write mode</h2>
        <div className="KafkaWriteModeRow">
          <div>
            <strong>Enable write actions for this Kafka target</strong>
            <span>Write actions stay disabled unless this cluster is explicitly enabled.</span>
          </div>
          <Renderer.Component.Switch
            aria-label="Enable write mode for this Kafka target"
            data-testid="kafka-write-mode-toggle"
            checked={writeModeEnabled}
            onChange={(value) => {
              writeSettings.set(kafka.targetId, value);
              setWriteModeEnabled(value);
            }}
          />
        </div>
      </section>

      <section className="KafkaConnectionSettings" aria-label="Schema Registry settings">
        <h2>Schema Registry</h2>
        <Renderer.Component.Input
          value={registryUrl}
          onChange={setRegistryUrl}
          placeholder="http://127.0.0.1:18081"
          aria-label="Schema Registry URL"
        />
        <Renderer.Component.Input
          value={registryUsername}
          onChange={setRegistryUsername}
          placeholder="Username (optional)"
          aria-label="Schema Registry username"
        />
        <Renderer.Component.Switch
          checked={registryTls}
          onChange={setRegistryTls}
          aria-label="Enable Schema Registry TLS"
        />
        <Renderer.Component.Button
          outlined
          onClick={() =>
            schemaRegistrySettings.set(kafka.targetId, {
              registryUrl,
              tls: registryTls,
              ...(registryUsername.trim() ? { username: registryUsername.trim() } : {}),
            })
          }
        >
          Save Schema Registry settings
        </Renderer.Component.Button>
      </section>

      <section className="KafkaConnectionSettings" aria-label="Kafka Connect settings">
        <h2>Kafka Connect</h2>
        <Renderer.Component.Input
          value={connectUrl}
          onChange={setConnectUrl}
          placeholder="http://127.0.0.1:18083"
          aria-label="Kafka Connect URL"
        />
        <Renderer.Component.Input
          value={connectUsername}
          onChange={setConnectUsername}
          placeholder="Username (optional)"
          aria-label="Kafka Connect username"
        />
        <Renderer.Component.Button
          outlined
          onClick={() =>
            connectSettings.set(kafka.targetId, {
              connectUrl,
              tls: false,
              ...(connectUsername.trim() ? { username: connectUsername.trim() } : {}),
            })
          }
        >
          Save Kafka Connect settings
        </Renderer.Component.Button>
      </section>

      <section className="KafkaConnectionSettings" aria-label="Kafka connection settings">
        <h2>Security override</h2>
        <form
          className="KafkaSecurityForm"
          onSubmit={(event) => {
            event.preventDefault();
            applySettings();
          }}
        >
          <label>
            <span>TLS</span>
            <Renderer.Component.Select
              options={TLS_MODE_OPTIONS}
              value={tlsMode}
              onChange={(option: Renderer.Component.SelectOption<KafkaTlsMode> | null) => {
                setTlsMode(option?.value ?? "auto");
                setSettingsError(undefined);
              }}
              isDisabled={detail.loading}
              themeName="lens"
              menuPosition="fixed"
            />
          </label>
          <label>
            <span>Authentication</span>
            <Renderer.Component.Select
              options={AUTH_MODE_OPTIONS}
              value={authMode}
              onChange={(option: Renderer.Component.SelectOption<KafkaAuthMode> | null) => {
                setAuthMode(option?.value ?? "auto");
                setSettingsError(undefined);
              }}
              isDisabled={detail.loading}
              themeName="lens"
              menuPosition="fixed"
            />
          </label>
          {explicitAuth && (
            <>
              <label>
                <span>Username</span>
                <Renderer.Component.Input
                  value={username}
                  onChange={(value) => {
                    setUsername(value);
                    setSettingsError(undefined);
                  }}
                  disabled={detail.loading}
                  autoComplete="username"
                  aria-label="Kafka SASL username"
                  aria-required="true"
                />
              </label>
              <label>
                <span>Password</span>
                <Renderer.Component.Input
                  type="password"
                  value={password}
                  onChange={(value) => {
                    setPassword(value);
                    setSettingsError(undefined);
                  }}
                  disabled={detail.loading}
                  autoComplete="current-password"
                  aria-label="Kafka SASL password"
                  aria-required="true"
                />
              </label>
            </>
          )}
          <p>
            Automatic mode reads matching workload env, ConfigMaps and Secrets. Password overrides stay in memory only.
          </p>
          {settingsError && (
            <div className="KafkaSecurityError" role="alert">
              {settingsError}
            </div>
          )}
          <Renderer.Component.Button primary type="submit" waiting={detail.loading}>
            <Renderer.Component.Icon material="sync" />
            Apply and reconnect
          </Renderer.Component.Button>
        </form>
      </section>

      {onRemove && (
        <Renderer.Component.Button outlined className="KafkaRemoveEndpoint" onClick={onRemove}>
          <Renderer.Component.Icon material="delete_outline" />
          Remove endpoint
        </Renderer.Component.Button>
      )}

      {kafka.referencedBy?.length ? (
        <details className="KafkaReferences">
          <summary>
            Used by {kafka.referencedBy.length} workload{kafka.referencedBy.length === 1 ? "" : "s"}
          </summary>
          <ul>
            {kafka.referencedBy.map((reference) => (
              <li key={reference}>{reference}</li>
            ))}
          </ul>
        </details>
      ) : null}

      {(detail.loading || detail.error) && detail.progress && (
        <OperationProgress progress={detail.progress} error={detail.error} />
      )}
      {detail.data && !detail.loading && !detail.error && (
        <div className="KafkaConnectionVerified" role="status">
          <Renderer.Component.Icon material="check_circle" />
          <div>
            <strong>Connection verified</strong>
            <span>The effective settings are active for this session.</span>
          </div>
        </div>
      )}
    </Renderer.Component.Drawer>
  );
}

export function KafkaOverviewPage({
  connectionSettings,
  writeSettings,
  schemaRegistrySettings,
  connectSettings,
  kubernetesClusterId,
  resourceCache,
  invalidateHealth,
  query: controlledQuery,
  onQueryChange,
  onOpenCluster,
  discover,
  overview,
  reachability,
  subscribeProgress,
}: KafkaOverviewPageProps) {
  const catalogContext = kubernetesClusterId ?? "active";
  const [persistentState] = useState(() => kafkaPersistentStateStore());
  const [catalogStore] = useState(() => new KafkaClusterCatalogStore(persistentState));
  const savedCatalog = catalogStore.get(catalogContext);
  const [state, setState] = useState<DiscoveryState>(() => ({
    loading: false,
    kafkas: catalogStore.targets(catalogContext),
  }));
  const [autoScanEnabled, setAutoScanEnabled] = useState(savedCatalog.autoScan);
  const [manualKafkas, setManualKafkas] = useState<DiscoveredKafkaInfo[]>(() =>
    loadManualKafkaEndpoints(persistentState),
  );
  const [settingsTarget, setSettingsTarget] = useState<DiscoveredKafkaInfo | null>(null);
  const [detail, setDetail] = useState<DetailState>({ loading: false });
  const [reach, setReach] = useState<Record<string, boolean>>({});
  const [discoveryProgress, setDiscoveryProgress] = useState<KafkaProgressEvent>();
  const [detailProgress, setDetailProgress] = useState<KafkaProgressEvent>();
  const discoveryOperationId = useRef<string>();
  const detailOperationId = useRef<string>();
  const [localQuery, setLocalQuery] = useState("");
  const [reachabilityFilter, setReachabilityFilter] = useState<ReachabilityFilter>("all");
  const [manualOpen, setManualOpen] = useState(false);
  const [manualBootstrap, setManualBootstrap] = useState("");
  const [manualTls, setManualTls] = useState(false);
  const [manualError, setManualError] = useState<string>();
  const [reconnectRequest, setReconnectRequest] = useState<{ revision: number; targetId: string }>();
  const query = controlledQuery ?? localQuery;
  const setQuery = onQueryChange ?? setLocalQuery;

  const selectKafkaCluster = useCallback(
    (kafka: DiscoveredKafkaInfo) => {
      if (kubernetesClusterId) rememberKafkaClusterSelection(persistentState, kubernetesClusterId, kafka);
      onOpenCluster(kafka);
    },
    [kubernetesClusterId, onOpenCluster],
  );

  const openConnectionSettings = useCallback((kafka: DiscoveredKafkaInfo) => {
    setReconnectRequest(undefined);
    setDetail({ loading: false });
    setDetailProgress(undefined);
    setSettingsTarget(kafka);
  }, []);

  useEffect(
    () =>
      subscribeProgress((progress) => {
        if (progress.operation === "discovery" && progress.operationId === discoveryOperationId.current) {
          setDiscoveryProgress(progress);
        } else if (progress.operation === "overview" && progress.operationId === detailOperationId.current) {
          setDetailProgress(progress);
        }
      }),
    [subscribeProgress],
  );

  const loadDiscovery = useCallback(
    (force = false) => {
      if (force) resourceCache.invalidateDiscovery(kubernetesClusterId ?? "active");
      let cancelled = false;
      const operationId = createOperationId("discovery");
      discoveryOperationId.current = operationId;
      setDiscoveryProgress({
        operationId,
        operation: "discovery",
        value: 1,
        phase: "starting",
        label: "Starting Kafka discovery",
        detail: "Preparing the active Kubernetes cluster connection.",
      });
      setState((current) => ({ loading: true, kafkas: current.kafkas }));
      discover({ operationId })
        .then((kafkas) => {
          if (cancelled) return;
          catalogStore.setDiscovered(catalogContext, kafkas);
          setState({ loading: false, kafkas: catalogStore.targets(catalogContext) });
        })
        .catch((error: unknown) => {
          if (!cancelled) {
            const message = error instanceof Error ? error.message : String(error);
            setState((current) => ({ loading: false, kafkas: current.kafkas, error: message }));
            setDiscoveryProgress((current) =>
              current ? { ...current, label: "Discovery stopped", detail: current.label } : current,
            );
          }
        });
      return () => {
        cancelled = true;
      };
    },
    [catalogContext, catalogStore, discover, kubernetesClusterId, reachability, resourceCache],
  );

  useEffect(() => {
    const catalog = catalogStore.get(catalogContext);
    setState({ loading: false, kafkas: catalogStore.targets(catalogContext) });
    setAutoScanEnabled(catalog.autoScan);
    setReach({});
  }, [catalogContext, catalogStore]);

  useEffect(() => {
    if (!catalogStore.get(catalogContext).autoScan) return;
    return loadDiscovery();
  }, [catalogContext, catalogStore, loadDiscovery]);

  useEffect(() => {
    saveManualKafkaEndpoints(persistentState, manualKafkas);

    const bootstraps = manualKafkas.map((kafka) => kafka.bootstrap);
    if (bootstraps.length === 0) return;
    let cancelled = false;
    reachability(bootstraps)
      .then((result) => {
        if (!cancelled) setReach((current) => ({ ...current, ...result }));
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [manualKafkas, persistentState, reachability]);

  useEffect(() => {
    const bootstraps = [...new Set(state.kafkas.map((kafka) => kafka.bootstrap))];
    if (bootstraps.length === 0) return;
    let cancelled = false;
    reachability(bootstraps)
      .then((result) => {
        if (!cancelled) setReach((current) => ({ ...current, ...result }));
      })
      .catch(() => undefined);
    return () => {
      cancelled = true;
    };
  }, [reachability, state.kafkas]);

  useEffect(() => {
    if (!settingsTarget || reconnectRequest?.targetId !== settingsTarget.targetId) {
      setDetail({ loading: false });
      setDetailProgress(undefined);
      return;
    }
    let cancelled = false;
    const operationId = createOperationId("overview");
    detailOperationId.current = operationId;
    const initialProgress: KafkaProgressEvent = {
      operationId,
      operation: "overview",
      value: 1,
      phase: "strategy",
      label: "Preparing cluster details",
      detail: "Selecting a safe read-only connection path.",
    };
    setDetailProgress(initialProgress);
    setDetail({ loading: true, progress: initialProgress });
    overview({
      operationId,
      targetId: settingsTarget.targetId,
      namespace: settingsTarget.namespace,
      clusterName: settingsTarget.name,
      source: settingsTarget.source,
      bootstrap: settingsTarget.bootstrap,
      tls: settingsTarget.tls,
      sourceLocator: settingsTarget.sourceLocator,
      security: connectionSettings.get(kubernetesClusterId ?? "active", settingsTarget.targetId),
    })
      .then((data) => {
        if (!cancelled) setDetail({ loading: false, data });
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          const message = error instanceof Error ? error.message : String(error);
          setDetail((current) => ({ loading: false, error: message, progress: current.progress }));
        }
      });
    return () => {
      cancelled = true;
    };
  }, [connectionSettings, kubernetesClusterId, overview, reconnectRequest, settingsTarget]);

  const allKafkas = mergeKafkaClusters(state.kafkas, manualKafkas);

  const addManualEndpoint = (): void => {
    const bootstrap = manualBootstrap.trim();
    const { host, port } = firstBrokerAddress(bootstrap);
    if (!host) {
      setManualError("Enter a Kafka bootstrap host or host:port.");
      return;
    }
    const targetId = createKafkaTargetId(bootstrap);
    if (allKafkas.some((kafka) => kafka.targetId === targetId)) {
      setManualError("This endpoint is already listed.");
      return;
    }

    const kafka: DiscoveredKafkaInfo = {
      targetId,
      source: "manual",
      name: host,
      namespace: "",
      bootstrap,
      tls: manualTls,
      port,
      listeners: [],
      brokerPods: [],
      provider: "Manual",
      external: true,
      securityHint: { tls: manualTls, auth: "none" },
    };
    setManualKafkas((current) => [...current, kafka]);
    resourceCache.invalidateDiscovery(kubernetesClusterId ?? "active");
    setManualBootstrap("");
    setManualTls(false);
    setManualError(undefined);
    setManualOpen(false);
  };

  const removeManualEndpoint = (kafka: DiscoveredKafkaInfo): void => {
    const contextId = kubernetesClusterId ?? "active";
    resourceCache.invalidateDiscovery(contextId);
    resourceCache.invalidateTarget(contextId, kafka.targetId);
    persistentState.removeItem(aggregateHealthSnapshotKey(contextId, kafka.targetId));
    void invalidateHealth(kafka.targetId, { closeSessions: true, removePersisted: true });
    setManualKafkas((current) => current.filter((entry) => entry.targetId !== kafka.targetId));
    connectionSettings.removeTarget(contextId, kafka.targetId);
    if (kubernetesClusterId) {
      forgetKafkaClusterSelection(persistentState, kubernetesClusterId, kafka.targetId);
    }
    setSettingsTarget(null);
  };

  const applySecurity = (kafka: DiscoveredKafkaInfo, security?: KafkaSecurityOverride): void => {
    const key = kafka.targetId;
    resourceCache.invalidateTarget(kubernetesClusterId ?? "active", key);
    void invalidateHealth(key, { closeSessions: true });
    connectionSettings.set(kubernetesClusterId ?? "active", key, security);
    setReconnectRequest((current) => ({ revision: (current?.revision ?? 0) + 1, targetId: key }));
  };

  const normalizedQuery = query.trim().toLowerCase();
  const manualTargetIds = new Set(manualKafkas.map((kafka) => kafka.targetId));
  const missingTargetIds = new Set(
    savedCatalog.missing.filter((kafka) => !manualTargetIds.has(kafka.targetId)).map((kafka) => kafka.targetId),
  );
  const catalogStatus = savedCatalog.lastScanAt
    ? `Last scan ${new Date(savedCatalog.lastScanAt).toLocaleString()}`
    : "Not scanned yet";
  const filteredKafkas = allKafkas.filter((kafka) => {
    const pc = reach[kafka.bootstrap];
    if (reachabilityFilter === "pc" && pc !== true) return false;
    if (reachabilityFilter === "pods" && pc !== false) return false;
    if (!normalizedQuery) return true;
    return [kafka.name, kafka.namespace, kafka.bootstrap, sourceLabel(kafka), ...(kafka.referencedBy ?? [])]
      .join(" ")
      .toLowerCase()
      .includes(normalizedQuery);
  });

  const reachableCount = allKafkas.filter((kafka) => reach[kafka.bootstrap] === true).length;
  const podOnlyCount = allKafkas.filter(
    (kafka) => kafka.source !== "manual" && reach[kafka.bootstrap] === false,
  ).length;

  return (
    <KafkaPageShell
      title="Kafka clusters"
      subtitle={
        state.loading
          ? "Discovering clusters"
          : `${allKafkas.length} cluster${allKafkas.length === 1 ? "" : "s"} available`
      }
      actions={
        <>
          <Renderer.Component.Button outlined onClick={() => setManualOpen((open) => !open)}>
            <Renderer.Component.Icon material="add_link" />
            Add endpoint
          </Renderer.Component.Button>
          <Renderer.Component.Button
            outlined
            className="KafkaRefreshButton"
            waiting={state.loading}
            onClick={() => loadDiscovery(true)}
            title="Scan Kubernetes for Kafka clusters"
          >
            <Renderer.Component.Icon material="search" />
            Scan Kubernetes
          </Renderer.Component.Button>
          <div className="KafkaAutoScanControl">
            <span>Auto-scan</span>
            <Renderer.Component.Switch
              checked={autoScanEnabled}
              onChange={(enabled) => {
                setAutoScanEnabled(enabled);
                catalogStore.setAutoScan(catalogContext, enabled);
              }}
              aria-label="Scan automatically when opening Kafka clusters"
            />
          </div>
          <span className="KafkaCacheStatus" data-testid="kafka-catalog-status" title={catalogStatus}>
            {state.loading ? "Scanning" : catalogStatus}
          </span>
        </>
      }
    >
      {manualOpen && (
        <form
          className="KafkaManualEndpoint"
          onSubmit={(event) => {
            event.preventDefault();
            addManualEndpoint();
          }}
        >
          <Renderer.Component.Input
            className="KafkaManualBootstrap"
            value={manualBootstrap}
            onChange={(value) => {
              setManualBootstrap(value);
              setManualError(undefined);
            }}
            iconLeft="link"
            placeholder="broker-1.example.com:9092,broker-2.example.com:9092"
            aria-label="Kafka bootstrap servers"
            autoFocus
          />
          <label className="KafkaTlsSwitch">
            <span>TLS</span>
            <Renderer.Component.Switch checked={manualTls} onChange={setManualTls} />
          </label>
          <Renderer.Component.Button primary type="submit" disabled={!manualBootstrap.trim()}>
            Add
          </Renderer.Component.Button>
          <Renderer.Component.Button plain onClick={() => setManualOpen(false)}>
            Cancel
          </Renderer.Component.Button>
          {manualError && <span className="KafkaManualError">{manualError}</span>}
        </form>
      )}

      {state.loading && discoveryProgress && <OperationProgress progress={discoveryProgress} />}
      {state.error && discoveryProgress && (
        <div className="KafkaDiscoveryError">
          <OperationProgress progress={discoveryProgress} error={state.error} />
          <Renderer.Component.Button outlined onClick={() => loadDiscovery(true)}>
            Retry
          </Renderer.Component.Button>
        </div>
      )}
      {!state.loading && !state.error && allKafkas.length === 0 && (
        <div className="KafkaPageState empty" data-testid="kafka-clusters-onboarding">
          <Renderer.Component.Icon material="hub" />
          <div>
            <strong>Choose how to add Kafka clusters</strong>
            <span>Scan Kubernetes for discovered endpoints or add a Kafka bootstrap manually.</span>
          </div>
          <div className="KafkaOnboardingActions">
            <Renderer.Component.Button primary onClick={() => loadDiscovery(true)}>
              <Renderer.Component.Icon material="search" />
              Scan Kubernetes
            </Renderer.Component.Button>
            <Renderer.Component.Button outlined onClick={() => setManualOpen(true)}>
              <Renderer.Component.Icon material="add_link" />
              Add manually
            </Renderer.Component.Button>
          </div>
        </div>
      )}

      {!state.loading && allKafkas.length > 0 && (
        <>
          <section className="KafkaToolbar" aria-label="Kafka cluster filters">
            <Renderer.Component.Input
              className="KafkaSearchInput"
              value={query}
              onChange={setQuery}
              iconLeft="search"
              placeholder="Filter by name, namespace, bootstrap or workload"
              aria-label="Filter Kafka clusters"
            />
            <div className="KafkaFilterGroup" role="group" aria-label="Reachability filter">
              {(
                [
                  ["all", `All ${allKafkas.length}`],
                  ["pc", `From PC ${reachableCount}`],
                  ["pods", `Pod only ${podOnlyCount}`],
                ] as const
              ).map(([value, label]) => (
                <Renderer.Component.Button
                  key={value}
                  plain
                  active={reachabilityFilter === value}
                  onClick={() => setReachabilityFilter(value)}
                >
                  {label}
                </Renderer.Component.Button>
              ))}
            </div>
          </section>

          <main className="KafkaWorkspace">
            <section className="KafkaListPane" aria-label="Kafka clusters">
              <Renderer.Component.Table<DiscoveredKafkaInfo>
                className="KafkaClusterTable"
                tableId="kafka-clusters"
                autoSize={false}
                selectable
                scrollable
                sortSyncWithUrl={false}
                sortByDefault={{ sortBy: "name", orderBy: "asc" }}
                sortable={{
                  name: (kafka) => kafka.name.toLowerCase(),
                  namespace: (kafka) => kafka.namespace.toLowerCase(),
                  usage: (kafka) => kafkaUsageSummary(kafka).sortValue,
                  source: (kafka) => sourceLabel(kafka).toLowerCase(),
                  bootstrap: (kafka) => kafka.bootstrap.toLowerCase(),
                  security: (kafka) =>
                    securityLabel(kafka.securityHint ?? { tls: kafka.tls, auth: "none" }).toLowerCase(),
                  pc: (kafka) => Number(reach[kafka.bootstrap] === true),
                }}
                noItems={
                  <div className="KafkaNoMatches">
                    <Renderer.Component.Icon material="filter_alt_off" />
                    No clusters match the current filters.
                  </div>
                }
              >
                <Renderer.Component.TableHead nowrap>
                  <Renderer.Component.TableCell className="nameCell" sortBy="name">
                    Cluster
                  </Renderer.Component.TableCell>
                  <Renderer.Component.TableCell className="usageCell" sortBy="usage">
                    Kubernetes usage
                  </Renderer.Component.TableCell>
                  <Renderer.Component.TableCell className="sourceCell" sortBy="source">
                    Source
                  </Renderer.Component.TableCell>
                  <Renderer.Component.TableCell className="bootstrapCell" sortBy="bootstrap">
                    Bootstrap
                  </Renderer.Component.TableCell>
                  <Renderer.Component.TableCell className="securityCell" sortBy="security">
                    Security
                  </Renderer.Component.TableCell>
                  <Renderer.Component.TableCell className="reachCell" sortBy="pc">
                    From PC
                  </Renderer.Component.TableCell>
                  <Renderer.Component.TableCell className="podsCell">From pods</Renderer.Component.TableCell>
                  <Renderer.Component.TableCell className="actionCell" />
                </Renderer.Component.TableHead>

                {filteredKafkas.map((kafka) => {
                  const settingsOpen = settingsTarget?.targetId === kafka.targetId;
                  const pc = reach[kafka.bootstrap];
                  const strategy = pc === undefined ? undefined : chooseStrategy(kafka.source, pc);
                  const inspectable =
                    kafka.source === "strimzi" ||
                    strategy === "direct" ||
                    (pc === undefined && (kafka.source === "manual" || kafka.external === true));
                  const inspectTitle =
                    kafka.source === "strimzi"
                      ? "Connect via port-forward to the broker pods"
                      : pc === undefined
                        ? "Checking reachability…"
                        : strategy === "direct"
                          ? "Connect directly (reachable from this machine)"
                          : "Reachable only from the cluster's pods — relay connect coming soon";
                  const rowInteraction: HTMLAttributes<HTMLDivElement> = inspectable
                    ? {
                        role: "button",
                        tabIndex: 0,
                        "aria-label": `Open Overview for ${kafka.name}`,
                        onClick: (event) => {
                          event.stopPropagation();
                          selectKafkaCluster(kafka);
                        },
                        onKeyDown: (event) => {
                          if (event.key === "Enter" || event.key === " ") {
                            event.preventDefault();
                            selectKafkaCluster(kafka);
                          }
                        },
                      }
                    : { "aria-label": inspectTitle, title: inspectTitle };
                  return (
                    <Renderer.Component.TableRow
                      {...rowInteraction}
                      key={kafka.targetId}
                      sortItem={kafka}
                      selected={settingsOpen}
                      nowrap
                      data-bootstrap={kafka.bootstrap}
                    >
                      <Renderer.Component.TableCell className="nameCell" title={kafka.name}>
                        <div className="KafkaClusterName">
                          <Renderer.Component.Icon material="hub" />
                          <span>{kafka.name}</span>
                          {missingTargetIds.has(kafka.targetId) && (
                            <Renderer.Component.Badge small label="Not found in latest scan" />
                          )}
                        </div>
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="usageCell">
                        <UsageCell kafka={kafka} />
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="sourceCell">
                        <SourceBadge kafka={kafka} />
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="bootstrapCell" title={kafka.bootstrap}>
                        <code className="KafkaEllipsis">{kafka.bootstrap}</code>
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="securityCell">
                        <SecurityBadge security={kafka.securityHint ?? { tls: kafka.tls, auth: "none" }} />
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="reachCell">
                        <ReachabilityBadge value={pc} />
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="podsCell">
                        <PodReachabilityBadge kafka={kafka} />
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="actionCell">
                        <Renderer.Component.Button
                          plain
                          round
                          className="KafkaIconButton"
                          title={`Connection settings for ${kafka.name}`}
                          aria-label={`Connection settings for ${kafka.name}`}
                          onClick={(event) => {
                            event.stopPropagation();
                            openConnectionSettings(kafka);
                          }}
                        >
                          <Renderer.Component.Icon material="tune" />
                        </Renderer.Component.Button>
                      </Renderer.Component.TableCell>
                    </Renderer.Component.TableRow>
                  );
                })}
              </Renderer.Component.Table>
            </section>

            {settingsTarget && (
              <ConnectionSettingsDrawer
                key={settingsTarget.targetId}
                kafka={settingsTarget}
                pcReachable={reach[settingsTarget.bootstrap]}
                detail={{ ...detail, progress: detailProgress }}
                securityOverride={connectionSettings.get(kubernetesClusterId ?? "active", settingsTarget.targetId)}
                writeSettings={writeSettings}
                schemaRegistrySettings={schemaRegistrySettings}
                connectSettings={connectSettings}
                onApplySecurity={(security) => applySecurity(settingsTarget, security)}
                onClose={() => setSettingsTarget(null)}
                onRemove={settingsTarget.source === "manual" ? () => removeManualEndpoint(settingsTarget) : undefined}
              />
            )}
          </main>
        </>
      )}
    </KafkaPageShell>
  );
}
