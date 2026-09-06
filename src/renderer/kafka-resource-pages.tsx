import { Renderer } from "@freelensapp/extensions";
import { useCallback, useEffect, useRef, useState } from "react";
import { aggregateHealthSnapshotKey, decodeAggregateHealthSnapshot } from "../common/aggregate-health-snapshot";
import { DEFAULT_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS, KAFKA_OVERVIEW_AUTO_REFRESH_OPTIONS } from "../common/constants";
import { kafkaPersistentStateStore } from "../common/kafka-persistent-state-store";
import { chooseStrategy } from "../common/reachability";
import { KafkaClusterCatalogStore } from "./kafka-cluster-catalog";
import { loadManualKafkaEndpoints, mergeKafkaClusters } from "./kafka-manual-endpoints";
import {
  rememberedKafkaTargetId,
  rememberKafkaClusterSelection,
  resolveKafkaClusterSelection,
} from "./kafka-navigation";
import { HealthProgress, OperationProgress, ReachabilityBadge, SecurityBadge, SourceBadge } from "./kafka-overview";
import { KafkaClusterSelector, KafkaMetricStrip, KafkaPageShell } from "./kafka-page-shell";
import { formatKafkaCacheAge, type KafkaResourceCache } from "./kafka-resource-cache";
import {
  clusterHealthMetricValue,
  createOperationId,
  kafkaHealthCompletionMetadata,
  kafkaHealthCoverageLabel,
  kafkaHealthCoverageState,
  kafkaHealthNeedsCoverageNotice,
  kafkaUsageContext,
} from "./kafka-view-model";

import type { CSSProperties, HTMLAttributes } from "react";

import type {
  BrokerConfigDto,
  BrokerConfigRequest,
  ClusterHealthRequest,
  ClusterOverviewDto,
  ClusterOverviewHealthDto,
  DiscoveredKafkaInfo,
  DiscoverRequest,
  KafkaConfigEntryDto,
  KafkaProgressEvent,
  OverviewRequest,
} from "../common/ipc";
import type { KafkaConnectionSettingsStore } from "./kafka-connection-settings";
import type { KafkaOverviewSettingsStore } from "./kafka-overview-settings";
import type { KafkaWriteSettingsStore } from "./kafka-write-settings";

export interface KafkaResourcePageDependencies {
  connectionSettings: KafkaConnectionSettingsStore;
  writeSettings: KafkaWriteSettingsStore;
  kubernetesClusterId?: string;
  resourceCache: KafkaResourceCache;
  discover: (request?: DiscoverRequest) => Promise<DiscoveredKafkaInfo[]>;
  overview: (request: OverviewRequest) => Promise<ClusterOverviewDto>;
  reachability: (bootstraps: string[]) => Promise<Record<string, boolean>>;
  subscribeProgress: (listener: (progress: KafkaProgressEvent) => void) => () => void;
}

export interface KafkaTargetPageProps extends KafkaResourcePageDependencies {
  health?: (request: ClusterHealthRequest) => Promise<ClusterOverviewHealthDto>;
  overviewSettings?: KafkaOverviewSettingsStore;
  params?: {
    target: Renderer.Navigation.PageParam<string>;
  };
}

export interface KafkaBrokersPageProps extends KafkaResourcePageDependencies {
  params?: {
    target: Renderer.Navigation.PageParam<string>;
    broker: Renderer.Navigation.PageParam<string>;
  };
  brokerConfig: (request: BrokerConfigRequest) => Promise<BrokerConfigDto>;
}

interface ClusterLoadState {
  cacheKey: string;
  loading: boolean;
  error?: string;
  clusters: DiscoveredKafkaInfo[];
  source?: "cache" | "network";
  updatedAt?: number;
}

interface MetadataState {
  cacheKey?: string;
  loading: boolean;
  error?: string;
  data?: ClusterOverviewDto;
  source?: "cache" | "network";
  targetId?: string;
  updatedAt?: number;
}

interface HealthState {
  cacheKey?: string;
  loading: boolean;
  error?: string;
  data?: ClusterOverviewHealthDto;
  source?: "cache" | "network" | "persisted";
  targetId?: string;
  updatedAt?: number;
}

export function useKafkaPageParam(param?: Renderer.Navigation.PageParam<string>) {
  const routeValue = param?.get() ?? "";
  const [value, setValue] = useState(routeValue);

  useEffect(() => setValue(routeValue), [routeValue]);
  useEffect(() => {
    const syncFromHistory = () => setValue(param?.get() ?? "");
    window.addEventListener("popstate", syncFromHistory);
    return () => window.removeEventListener("popstate", syncFromHistory);
  }, [param]);

  const update = useCallback(
    (targetId: string, replaceHistory = false) => {
      setValue(targetId);
      param?.set(targetId, { replaceHistory });
    },
    [param],
  );

  return [value, update] as const;
}

export function useKafkaResourcePage({
  connectionSettings,
  kubernetesClusterId,
  resourceCache,
  overview,
  health,
  reachability,
  subscribeProgress,
  params,
}: KafkaTargetPageProps) {
  const cacheKey = kubernetesClusterId ?? "active";
  const persistentState = kafkaPersistentStateStore();
  const [requestedTargetId, setRequestedTargetId] = useKafkaPageParam(params?.target);
  const [healthRefreshVersion, setHealthRefreshVersion] = useState(0);
  const [refreshVersion, setRefreshVersion] = useState(0);
  const [clusterState, setClusterState] = useState<ClusterLoadState>(() => {
    const cached = resourceCache.readDiscovery(cacheKey);
    const catalog = new KafkaClusterCatalogStore(persistentState).targets(cacheKey);
    const clusters = cached.data ?? catalog;
    return {
      cacheKey,
      loading: false,
      clusters: mergeKafkaClusters(clusters, loadManualKafkaEndpoints(persistentState)),
      source: cached.data ? "cache" : undefined,
      updatedAt: cached.updatedAt,
    };
  });
  const [metadataState, setMetadataState] = useState<MetadataState>({ loading: false });
  const [healthState, setHealthState] = useState<HealthState>({ loading: false });
  const [discoveryProgress, setDiscoveryProgress] = useState<KafkaProgressEvent>();
  const [metadataProgress, setMetadataProgress] = useState<KafkaProgressEvent>();
  const [healthProgress, setHealthProgress] = useState<KafkaProgressEvent>();
  const [pcReachable, setPcReachable] = useState<boolean>();
  const discoveryOperationId = useRef<string>();
  const lastNetworkMetadata = useRef<{ targetId: string; updatedAt?: number }>();
  const metadataOperationId = useRef<string>();
  const healthOperationId = useRef<string>();
  const forcedHealthRefresh = useRef<{ targetId?: string; version: number }>({ version: 0 });

  useEffect(
    () =>
      subscribeProgress((progress) => {
        if (progress.operation === "discovery" && progress.operationId === discoveryOperationId.current) {
          setDiscoveryProgress(progress);
        } else if (progress.operation === "overview" && progress.operationId === metadataOperationId.current) {
          setMetadataProgress(progress);
        } else if (progress.operation === "health" && progress.operationId === healthOperationId.current) {
          setHealthProgress(progress);
          if (progress.healthSnapshot) {
            setHealthState((current) => ({
              ...current,
              data: { ...current.data, ...progress.healthSnapshot } as ClusterOverviewHealthDto,
              source: progress.healthSource ?? (progress.phase === "cache" ? (current.source ?? "cache") : "network"),
              updatedAt: progress.healthUpdatedAt ?? current.updatedAt,
            }));
          }
        }
      }),
    [subscribeProgress],
  );

  useEffect(() => {
    const cached = resourceCache.readDiscovery(cacheKey);
    const catalog = new KafkaClusterCatalogStore(persistentState).targets(cacheKey);
    const clusters = cached.data ?? catalog;
    discoveryOperationId.current = undefined;
    setDiscoveryProgress(undefined);
    setClusterState({
      cacheKey,
      loading: false,
      clusters: mergeKafkaClusters(clusters, loadManualKafkaEndpoints(persistentState)),
      source: cached.data ? "cache" : undefined,
      updatedAt: cached.updatedAt,
    });
  }, [cacheKey, refreshVersion, resourceCache]);

  const activeClusterState: ClusterLoadState =
    clusterState.cacheKey === cacheKey ? clusterState : { cacheKey, loading: true, clusters: [] };

  const rememberedTargetId = kubernetesClusterId
    ? rememberedKafkaTargetId(persistentState, kubernetesClusterId)
    : undefined;
  const selection = resolveKafkaClusterSelection(activeClusterState.clusters, requestedTargetId, rememberedTargetId);
  const selectedCluster = selection.state === "selected" ? selection.target : undefined;
  const selectedTargetId = selectedCluster?.targetId;
  const selectedNamespace = selectedCluster?.namespace;
  const selectedName = selectedCluster?.name;
  const selectedSource = selectedCluster?.source;
  const selectedBootstrap = selectedCluster?.bootstrap;
  const selectedTls = selectedCluster?.tls;
  const locatorNamespace = selectedCluster?.sourceLocator?.namespace;
  const locatorKind = selectedCluster?.sourceLocator?.kind;
  const locatorName = selectedCluster?.sourceLocator?.name;
  const locatorContainer = selectedCluster?.sourceLocator?.container;

  useEffect(() => {
    if (selectedCluster && selectedCluster.targetId !== requestedTargetId) {
      setRequestedTargetId(selectedCluster.targetId, true);
    }
  }, [requestedTargetId, selectedCluster, setRequestedTargetId]);

  useEffect(() => {
    if (!selectedCluster) {
      setMetadataState({ loading: false });
      setMetadataProgress(undefined);
      setPcReachable(undefined);
      return;
    }

    let cancelled = false;
    const cached = resourceCache.readOverview(cacheKey, selectedCluster.targetId);
    const cachedReachability = resourceCache.readReachability(cacheKey, selectedCluster.targetId);
    if (cachedReachability.fresh) setPcReachable(cachedReachability.data);

    if (cached.fresh) {
      const loadedByThisPage =
        lastNetworkMetadata.current?.targetId === selectedCluster.targetId &&
        lastNetworkMetadata.current.updatedAt === cached.updatedAt;
      metadataOperationId.current = undefined;
      setMetadataProgress(undefined);
      setMetadataState({
        cacheKey,
        loading: false,
        data: cached.data,
        source: loadedByThisPage ? "network" : "cache",
        targetId: selectedCluster.targetId,
        updatedAt: cached.updatedAt,
      });
      if (!cachedReachability.fresh) {
        reachability([selectedCluster.bootstrap])
          .then((result) => {
            if (!cancelled) setPcReachable(result[selectedCluster.bootstrap]);
          })
          .catch(() => undefined);
      }
      return () => {
        cancelled = true;
      };
    }

    const operationId = cached.operationId ?? createOperationId("overview");
    metadataOperationId.current = operationId;
    setMetadataProgress({
      operationId,
      operation: "overview",
      value: 1,
      phase: "strategy",
      label: "Preparing Kafka metadata",
      detail: "Selecting a safe read-only connection path.",
    });
    setMetadataState({
      cacheKey,
      loading: true,
      data: cached.data,
      targetId: selectedCluster.targetId,
      updatedAt: cached.updatedAt,
    });
    if (!cachedReachability.fresh) setPcReachable(undefined);

    overview({
      operationId,
      targetId: selectedCluster.targetId,
      namespace: selectedCluster.namespace,
      clusterName: selectedCluster.name,
      source: selectedCluster.source,
      bootstrap: selectedCluster.bootstrap,
      tls: selectedCluster.tls,
      security: connectionSettings.get(cacheKey, selectedCluster.targetId),
      sourceLocator: selectedCluster.sourceLocator,
    })
      .then((data) => {
        if (!cancelled) {
          const snapshot = resourceCache.readOverview(cacheKey, selectedCluster.targetId);
          lastNetworkMetadata.current = { targetId: selectedCluster.targetId, updatedAt: snapshot.updatedAt };
          setMetadataState({
            cacheKey,
            loading: false,
            data,
            source: "network",
            targetId: selectedCluster.targetId,
            updatedAt: snapshot.updatedAt,
          });
        }
      })
      .catch((error: unknown) => {
        if (!cancelled) {
          setMetadataState({
            cacheKey,
            loading: false,
            error: error instanceof Error ? error.message : String(error),
            targetId: selectedCluster.targetId,
          });
        }
      });

    reachability([selectedCluster.bootstrap])
      .then((result) => {
        if (!cancelled) setPcReachable(result[selectedCluster.bootstrap]);
      })
      .catch(() => undefined);

    return () => {
      cancelled = true;
    };
  }, [cacheKey, connectionSettings, overview, reachability, refreshVersion, resourceCache, selectedCluster]);

  useEffect(() => {
    if (
      !health ||
      !selectedTargetId ||
      selectedNamespace === undefined ||
      !selectedName ||
      !selectedSource ||
      !selectedBootstrap ||
      selectedTls === undefined
    ) {
      healthOperationId.current = undefined;
      setHealthProgress(undefined);
      setHealthState({ loading: false });
      return;
    }

    let cancelled = false;
    let animationFrame: number | undefined;
    const cached = resourceCache.readHealth(cacheKey, selectedTargetId);
    if (cached.fresh) {
      healthOperationId.current = undefined;
      setHealthProgress(undefined);
      setHealthState({
        cacheKey,
        loading: false,
        data: cached.data,
        source: "cache",
        targetId: selectedTargetId,
        updatedAt: cached.updatedAt,
      });
      return;
    }

    const operationId = cached.operationId ?? createOperationId("health");
    const forceRefresh =
      forcedHealthRefresh.current.targetId === selectedTargetId &&
      forcedHealthRefresh.current.version === healthRefreshVersion;
    if (forceRefresh) forcedHealthRefresh.current = { version: healthRefreshVersion };
    const persisted = decodeAggregateHealthSnapshot(
      persistentState.getItem(aggregateHealthSnapshotKey(cacheKey, selectedTargetId)),
    );
    const previousData = cached.data ?? persisted?.data;
    const previousSource: HealthState["source"] = cached.data ? "cache" : persisted ? "persisted" : undefined;
    const previousUpdatedAt = cached.updatedAt ?? persisted?.updatedAt;
    healthOperationId.current = operationId;
    setHealthProgress({
      operationId,
      operation: "health",
      value: 1,
      phase: "strategy",
      label: "Preparing cluster health",
      detail: "Broker and partition values will appear before the longer consumer lag calculation.",
    });
    setHealthState({
      cacheKey,
      loading: true,
      data: previousData,
      source: previousSource,
      targetId: selectedTargetId,
      updatedAt: previousUpdatedAt,
    });
    const startHealth = () => {
      if (cancelled) return;
      health({
        operationId,
        targetId: selectedTargetId,
        namespace: selectedNamespace,
        clusterName: selectedName,
        source: selectedSource,
        bootstrap: selectedBootstrap,
        tls: selectedTls,
        security: connectionSettings.get(cacheKey, selectedTargetId),
        sourceLocator:
          locatorNamespace && locatorKind && locatorName
            ? { namespace: locatorNamespace, kind: locatorKind, name: locatorName, container: locatorContainer }
            : undefined,
        refresh: forceRefresh,
      })
        .then((data) => {
          if (!cancelled) {
            const completedUpdatedAt = resourceCache.readHealth(cacheKey, selectedTargetId).updatedAt;
            healthOperationId.current = undefined;
            setHealthProgress(undefined);
            setHealthState((current) => ({
              ...kafkaHealthCompletionMetadata(current, completedUpdatedAt),
              cacheKey,
              loading: false,
              data,
              targetId: selectedTargetId,
            }));
          }
        })
        .catch((error: unknown) => {
          if (!cancelled) {
            setHealthState((current) => ({
              cacheKey,
              loading: false,
              data: current.cacheKey === cacheKey && current.targetId === selectedTargetId ? current.data : undefined,
              error: error instanceof Error ? error.message : String(error),
              source:
                current.cacheKey === cacheKey && current.targetId === selectedTargetId ? current.source : undefined,
              targetId: selectedTargetId,
              updatedAt:
                current.cacheKey === cacheKey && current.targetId === selectedTargetId ? current.updatedAt : undefined,
            }));
          }
        });
    };
    if (previousData) animationFrame = window.requestAnimationFrame(startHealth);
    else startHealth();

    return () => {
      cancelled = true;
      if (animationFrame !== undefined) window.cancelAnimationFrame(animationFrame);
    };
  }, [
    cacheKey,
    connectionSettings,
    health,
    locatorContainer,
    locatorKind,
    locatorName,
    locatorNamespace,
    healthRefreshVersion,
    resourceCache,
    selectedBootstrap,
    selectedName,
    selectedNamespace,
    selectedSource,
    selectedTargetId,
    selectedTls,
  ]);

  const activeMetadataState: MetadataState =
    metadataState.cacheKey === cacheKey && metadataState.targetId === selectedCluster?.targetId
      ? metadataState
      : { cacheKey, loading: Boolean(selectedCluster), targetId: selectedCluster?.targetId };
  const cachedHealth = selectedCluster ? resourceCache.readHealth(cacheKey, selectedCluster.targetId) : undefined;
  const activeHealthState: HealthState =
    healthState.cacheKey === cacheKey && healthState.targetId === selectedCluster?.targetId
      ? healthState
      : {
          cacheKey,
          data: cachedHealth?.data,
          loading: cachedHealth?.loading ?? Boolean(health && selectedCluster && !cachedHealth?.fresh),
          source: cachedHealth?.data ? "cache" : undefined,
          targetId: selectedCluster?.targetId,
          updatedAt: cachedHealth?.updatedAt,
        };

  const selectCluster = useCallback(
    (targetId: string) => {
      const target = activeClusterState.clusters.find((cluster) => cluster.targetId === targetId);
      if (!target) return;
      if (targetId !== selectedCluster?.targetId) resourceCache.invalidateTarget(cacheKey, targetId);
      if (kubernetesClusterId) {
        rememberKafkaClusterSelection(persistentState, kubernetesClusterId, target);
      }
      setRequestedTargetId(targetId);
    },
    [
      activeClusterState.clusters,
      cacheKey,
      kubernetesClusterId,
      persistentState,
      resourceCache,
      selectedCluster?.targetId,
      setRequestedTargetId,
    ],
  );

  return {
    ...activeClusterState,
    selection,
    selectedCluster,
    metadataState: activeMetadataState,
    healthState: activeHealthState,
    healthProgress,
    discoveryProgress,
    metadataProgress,
    pcReachable,
    cacheStats: resourceCache.stats(cacheKey, selectedCluster?.targetId),
    selectCluster,
    refresh: () => {
      if (selectedCluster) {
        resourceCache.invalidateTarget(cacheKey, selectedCluster.targetId);
        setRefreshVersion((version) => {
          const next = version + 1;
          setHealthRefreshVersion((healthVersion) => {
            const nextHealth = healthVersion + 1;
            forcedHealthRefresh.current = { targetId: selectedCluster.targetId, version: nextHealth };
            return nextHealth;
          });
          return next;
        });
      }
    },
    refreshHealth: () => {
      if (!selectedCluster) return;
      resourceCache.invalidateHealth(cacheKey, selectedCluster.targetId);
      setHealthRefreshVersion((version) => {
        const next = version + 1;
        forcedHealthRefresh.current = { targetId: selectedCluster.targetId, version: next };
        return next;
      });
    },
  };
}

export function KafkaResourceActions({
  state,
  onSelectCluster = state.selectCluster,
}: {
  state: ReturnType<typeof useKafkaResourcePage>;
  onSelectCluster?: (targetId: string) => void;
}) {
  const [now, setNow] = useState(Date.now());
  const updatedAt = state.metadataState.updatedAt ?? state.updatedAt;
  const loading = state.loading || state.metadataState.loading;
  const source = state.metadataState.data ? state.metadataState.source : state.source;
  const cacheState = loading ? (updatedAt ? "refreshing" : "loading") : source === "cache" ? "cached" : "updated";

  useEffect(() => {
    if (!updatedAt || loading) return;
    const interval = window.setInterval(() => setNow(Date.now()), 15_000);
    return () => window.clearInterval(interval);
  }, [loading, updatedAt]);

  const cacheLabel =
    cacheState === "loading"
      ? "Loading"
      : cacheState === "refreshing"
        ? "Refreshing"
        : `${cacheState === "cached" ? "Cached" : "Updated"} ${formatKafkaCacheAge(updatedAt ?? now, now)}`;

  return (
    <>
      <KafkaClusterSelector
        clusters={state.clusters}
        selectedTargetId={state.selectedCluster?.targetId}
        onChange={onSelectCluster}
        disabled={state.loading}
      />
      <span
        className={`KafkaCacheStatus ${cacheState}`}
        data-testid="kafka-cache-status"
        data-cache-state={cacheState}
        data-cache-source={source ?? "none"}
        data-updated-at={updatedAt}
        data-discovery-requests={state.cacheStats.discoveryRequests}
        data-health-requests={state.cacheStats.healthRequests}
        data-overview-requests={state.cacheStats.overviewRequests}
        data-reachability-requests={state.cacheStats.reachabilityRequests}
        title={updatedAt ? `Last refreshed ${new Date(updatedAt).toLocaleString()}` : "Loading Kafka resources"}
        aria-live="polite"
      >
        <Renderer.Component.Icon material={loading ? "sync" : cacheState === "cached" ? "history" : "schedule"} />
        <span className="KafkaCacheStatusLabel">{cacheLabel}</span>
      </span>
      <Renderer.Component.Button
        outlined
        className="KafkaRefreshButton"
        waiting={state.loading || state.metadataState.loading}
        onClick={state.refresh}
        title="Refresh Kafka metadata"
      >
        <Renderer.Component.Icon material="refresh" />
        Refresh
      </Renderer.Component.Button>
    </>
  );
}

export function KafkaResourceState({ state }: { state: ReturnType<typeof useKafkaResourcePage> }) {
  if (state.loading && state.clusters.length === 0 && state.discoveryProgress) {
    return <OperationProgress progress={state.discoveryProgress} />;
  }
  if (state.error) {
    return (
      <div className="KafkaPageState error" role="alert">
        <Renderer.Component.Icon material="error_outline" />
        <div>
          <strong>Kafka discovery failed</strong>
          <span>{state.error}</span>
        </div>
        <Renderer.Component.Button outlined onClick={state.refresh}>
          Retry
        </Renderer.Component.Button>
      </div>
    );
  }
  if (state.selection.state === "empty") {
    return (
      <div className="KafkaPageState empty">
        <Renderer.Component.Icon material="hub" />
        <div>
          <strong>No Kafka clusters found</strong>
          <span>Open Clusters to discover or add a Kafka endpoint.</span>
        </div>
      </div>
    );
  }
  if (state.selection.state === "required") {
    return (
      <div className="KafkaPageState empty">
        <Renderer.Component.Icon material="rule" />
        <div>
          <strong>Select a Kafka cluster</strong>
          <span>Choose one from the Kafka cluster selector to load read-only metadata.</span>
        </div>
      </div>
    );
  }
  if (state.metadataState.loading && !state.metadataState.data && state.metadataProgress) {
    return <OperationProgress progress={state.metadataProgress} />;
  }
  if (state.metadataState.error) {
    return (
      <div className="KafkaPageState error" role="alert">
        <Renderer.Component.Icon material="error_outline" />
        <div>
          <strong>Kafka metadata failed</strong>
          <span>{state.metadataState.error}</span>
        </div>
        <Renderer.Component.Button outlined onClick={state.refresh}>
          Retry
        </Renderer.Component.Button>
      </div>
    );
  }
  return null;
}

function strategyLabel(cluster: DiscoveredKafkaInfo, pcReachable?: boolean): string {
  if (pcReachable === undefined && cluster.source !== "strimzi") return "Checking reachability";
  const strategy = chooseStrategy(cluster.source, pcReachable ?? false);
  if (strategy === "portForward") return "Kubernetes port-forward";
  if (strategy === "direct") return "Direct";
  return "Pod-only · relay unavailable";
}

function KafkaHealthCoverageNotice({ health }: { health: ClusterOverviewHealthDto | undefined }) {
  if (!kafkaHealthNeedsCoverageNotice(health) || !health) return null;
  const unavailableGroups = health.consumerGroupLagUnavailableGroups ?? 0;
  const unavailableTopics = health.consumerGroupLagUnavailableTopics ?? 0;
  const lowerBound = health.consumerGroupLag !== "Unavailable";
  const unavailableSummary =
    unavailableGroups > 0 || unavailableTopics > 0
      ? `${unavailableGroups} group(s) and ${unavailableTopics} topic(s) were unavailable. `
      : "Consumer-group coverage is incomplete. ";

  return (
    <div className="KafkaHealthNotice" role="status" data-testid="kafka-health-notice">
      <Renderer.Component.Icon material="info_outline" />
      <div>
        <strong>{lowerBound ? "Consumer lag is a minimum" : "Consumer lag unavailable"}</strong>
        <span>
          {lowerBound ? unavailableSummary : "Complete lag could not be calculated. "}
          Broker and partition health is complete; the displayed lag never overstates the measured value.
        </span>
      </div>
    </div>
  );
}

function KafkaHealthProgressSlot({
  health,
  healthState,
  progress,
}: {
  health: ClusterOverviewHealthDto | undefined;
  healthState: HealthState;
  progress: KafkaProgressEvent | undefined;
}) {
  const active = Boolean((healthState.loading || healthState.error) && progress);
  return (
    <div className="KafkaHealthProgressSlot" data-active={active}>
      {active && progress && (
        <HealthProgress progress={progress} error={healthState.error} hasSnapshot={Boolean(health)} />
      )}
    </div>
  );
}

function KafkaHealthFreshnessDetails({
  health,
  healthState,
  metadataLoading,
  metadataUpdatedAt,
  now,
}: {
  health: ClusterOverviewHealthDto | undefined;
  healthState: HealthState;
  metadataLoading: boolean;
  metadataUpdatedAt: number | undefined;
  now: number;
}) {
  const topologyUpdatedAt = health?.topologyMeasuredAt;
  const aggregateUpdatedAt = healthState.updatedAt;
  return (
    <>
      <div>
        <span>Metadata cache</span>
        <strong
          data-testid="kafka-metadata-freshness"
          data-updated-at={metadataUpdatedAt}
          title={metadataUpdatedAt ? new Date(metadataUpdatedAt).toLocaleString() : undefined}
        >
          {metadataUpdatedAt
            ? formatKafkaCacheAge(metadataUpdatedAt, now)
            : metadataLoading
              ? "Refreshing"
              : "Waiting for first result"}
        </strong>
      </div>
      <div>
        <span>Topology health</span>
        <strong
          data-testid="kafka-topology-freshness"
          data-updated-at={topologyUpdatedAt}
          title={topologyUpdatedAt ? new Date(topologyUpdatedAt).toLocaleString() : undefined}
        >
          {topologyUpdatedAt
            ? formatKafkaCacheAge(topologyUpdatedAt, now)
            : healthState.loading
              ? "Refreshing"
              : "Waiting for first result"}
        </strong>
      </div>
      <div>
        <span>Aggregate lag</span>
        <strong
          data-testid="kafka-aggregate-freshness"
          data-updated-at={aggregateUpdatedAt}
          title={aggregateUpdatedAt ? new Date(aggregateUpdatedAt).toLocaleString() : undefined}
        >
          {aggregateUpdatedAt
            ? formatKafkaCacheAge(aggregateUpdatedAt, now)
            : healthState.loading
              ? "Refreshing"
              : "Waiting for first result"}
        </strong>
      </div>
      <div>
        <span>Coverage</span>
        <strong data-testid="kafka-health-coverage" data-coverage={kafkaHealthCoverageState(health)}>
          {kafkaHealthCoverageLabel(health)}
        </strong>
      </div>
    </>
  );
}

export function KafkaClusterOverviewPage(props: KafkaTargetPageProps) {
  const state = useKafkaResourcePage(props);
  const cluster = state.selectedCluster;
  const data = state.metadataState.data;
  const overviewSettings = props.overviewSettings;
  const health = state.healthState.data;
  const metadataUpdatedAt = state.metadataState.updatedAt;
  const [autoRefreshEnabled, setAutoRefreshEnabled] = useState(() =>
    cluster ? (overviewSettings?.get(cluster.targetId)?.enabled ?? false) : false,
  );
  const [autoRefreshInterval, setAutoRefreshInterval] = useState(() =>
    cluster
      ? (overviewSettings?.get(cluster.targetId)?.intervalMs ?? DEFAULT_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS)
      : DEFAULT_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS,
  );
  const [freshnessNow, setFreshnessNow] = useState(Date.now());

  useEffect(() => {
    if (!cluster || !overviewSettings) return;
    const settings = overviewSettings.get(cluster.targetId);
    setAutoRefreshEnabled(settings.enabled);
    setAutoRefreshInterval(settings.intervalMs);
  }, [cluster, overviewSettings]);

  useEffect(() => {
    if (!cluster || !overviewSettings) return;
    if (!autoRefreshEnabled) {
      overviewSettings.set(cluster.targetId, { enabled: false, intervalMs: autoRefreshInterval });
      return;
    }
    overviewSettings.set(cluster.targetId, { enabled: true, intervalMs: autoRefreshInterval });
    const timer = window.setInterval(() => {
      if (!state.healthState.loading) state.refreshHealth();
    }, autoRefreshInterval);
    return () => window.clearInterval(timer);
  }, [autoRefreshEnabled, autoRefreshInterval, cluster, overviewSettings, state]);

  useEffect(() => {
    setFreshnessNow(Date.now());
    if (!metadataUpdatedAt && !health?.topologyMeasuredAt && !state.healthState.updatedAt) return;
    const interval = window.setInterval(() => setFreshnessNow(Date.now()), 15_000);
    return () => window.clearInterval(interval);
  }, [health?.topologyMeasuredAt, metadataUpdatedAt, state.healthState.updatedAt]);

  const healthMetrics = (
    <KafkaMetricStrip
      ariaLabel="Kafka cluster health"
      className="KafkaResourceMetricStrip"
      metrics={[
        {
          label: "Online brokers",
          value: clusterHealthMetricValue(state.healthState, health?.onlineBrokers, true),
        },
        {
          label: "Unavailable partitions",
          value: clusterHealthMetricValue(state.healthState, health?.unavailablePartitions, true),
          tone: health && health.unavailablePartitions > 0 ? "error" : undefined,
        },
        {
          label: "Under-replicated",
          value: clusterHealthMetricValue(state.healthState, health?.underReplicatedPartitions, true),
          tone: health && health.underReplicatedPartitions > 0 ? "warning" : undefined,
        },
        {
          label: "Consumer lag",
          value: clusterHealthMetricValue(state.healthState, health?.consumerGroupLag, true),
          tone:
            health?.consumerGroupLag === "Unavailable" ||
            health?.consumerGroupLagUnavailableTopics ||
            health?.consumerGroupLagUnavailableGroups
              ? "warning"
              : undefined,
        },
      ]}
    />
  );

  return (
    <KafkaPageShell
      title="Overview"
      subtitle={cluster ? cluster.name : "Health, identity and connection metadata"}
      actions={<KafkaResourceActions state={state} />}
    >
      <KafkaResourceState state={state} />
      {cluster && health && !data && (
        <main
          className="KafkaResourcePage"
          data-testid="kafka-overview-page"
          data-health-state={state.healthState.loading ? "updating" : "ready"}
          data-health-error={state.healthState.error ?? undefined}
          data-health-source={state.healthState.source}
          data-health-updated-at={state.healthState.updatedAt}
        >
          {healthMetrics}
          <KafkaHealthProgressSlot health={health} healthState={state.healthState} progress={state.healthProgress} />
          {state.healthState.error && (
            <div className="KafkaHealthNotice" role="status" data-testid="kafka-health-notice">
              <Renderer.Component.Icon material="info_outline" />
              <div>
                <strong>Health refresh failed</strong>
                <span>{state.healthState.error} Previous measured values remain visible.</span>
              </div>
            </div>
          )}
          <KafkaHealthCoverageNotice health={health} />
          <section className="KafkaResourceDetails" aria-label="Kafka health freshness">
            <KafkaHealthFreshnessDetails
              health={health}
              healthState={state.healthState}
              metadataLoading={state.metadataState.loading}
              metadataUpdatedAt={metadataUpdatedAt}
              now={freshnessNow}
            />
          </section>
        </main>
      )}
      {cluster && data && (
        <main
          className="KafkaResourcePage"
          data-testid="kafka-overview-page"
          data-health-state={state.healthState.loading ? "updating" : state.healthState.error ? "unavailable" : "ready"}
          data-health-source={state.healthState.source}
          data-health-updated-at={state.healthState.updatedAt}
        >
          <KafkaMetricStrip
            ariaLabel="Kafka cluster summary"
            className="KafkaResourceMetricStrip"
            metrics={[
              {
                label: "Online brokers",
                value: clusterHealthMetricValue(state.healthState, health?.onlineBrokers, true),
              },
              { label: "Controller", value: data.controller ?? "—" },
              { label: "Topics", value: data.topics.length },
              {
                label: "Unavailable partitions",
                value: clusterHealthMetricValue(state.healthState, health?.unavailablePartitions, true),
                tone: health && health.unavailablePartitions > 0 ? "error" : undefined,
              },
              {
                label: "Under-replicated",
                value: clusterHealthMetricValue(state.healthState, health?.underReplicatedPartitions, true),
                tone: health && health.underReplicatedPartitions > 0 ? "warning" : undefined,
              },
              {
                label: "Consumer lag",
                value: clusterHealthMetricValue(state.healthState, health?.consumerGroupLag, true),
                tone:
                  health?.consumerGroupLag === "Unavailable" ||
                  health?.consumerGroupLagUnavailableTopics ||
                  health?.consumerGroupLagUnavailableGroups
                    ? "warning"
                    : undefined,
              },
            ]}
          />
          <KafkaHealthProgressSlot health={health} healthState={state.healthState} progress={state.healthProgress} />
          <KafkaHealthCoverageNotice health={health} />
          <section className="KafkaResourceDetails" aria-label="Kafka connection summary">
            <div>
              <span>Source</span>
              <SourceBadge kafka={cluster} />
            </div>
            <div>
              <span>Security</span>
              <SecurityBadge security={data.security ?? cluster.securityHint ?? { tls: cluster.tls, auth: "none" }} />
            </div>
            <div>
              <span>From PC</span>
              <ReachabilityBadge value={state.pcReachable} label />
            </div>
            <div>
              <span>Connection</span>
              <strong>{strategyLabel(cluster, state.pcReachable)}</strong>
            </div>
            <div>
              <span>Bootstrap</span>
              <code title={cluster.bootstrap}>{cluster.bootstrap}</code>
            </div>
            <div>
              <span>Kubernetes usage</span>
              <strong>{kafkaUsageContext(cluster)}</strong>
            </div>
            <KafkaHealthFreshnessDetails
              health={health}
              healthState={state.healthState}
              metadataLoading={state.metadataState.loading}
              metadataUpdatedAt={metadataUpdatedAt}
              now={freshnessNow}
            />
            <div>
              <span>Auto-refresh</span>
              <div className="KafkaAutoRefreshControl">
                <Renderer.Component.Switch
                  checked={autoRefreshEnabled}
                  onChange={setAutoRefreshEnabled}
                  aria-label="Enable auto-refresh"
                />
                {autoRefreshEnabled && (
                  <Renderer.Component.Select
                    options={KAFKA_OVERVIEW_AUTO_REFRESH_OPTIONS}
                    value={String(autoRefreshInterval)}
                    onChange={(option) => {
                      const next = Number(option?.value ?? "30000");
                      setAutoRefreshInterval(next);
                    }}
                    aria-label="Refresh interval"
                  />
                )}
              </div>
            </div>
          </section>
        </main>
      )}
    </KafkaPageShell>
  );
}

const BROKER_CONFIG_NAME_STYLE: CSSProperties = { flex: "0 0 240px", minWidth: 240, width: 240 };
const BROKER_CONFIG_VALUE_STYLE: CSSProperties = { flex: "1 1 0", minWidth: 0, width: 0 };
const BROKER_CONFIG_SOURCE_STYLE: CSSProperties = { flex: "0 0 230px", minWidth: 230, width: 230 };
const BROKER_CONFIG_FLAGS_STYLE: CSSProperties = { flex: "0 0 140px", minWidth: 140, width: 140 };

interface BrokerConfigState {
  brokerId?: number;
  loading: boolean;
  data?: BrokerConfigDto;
  error?: string;
  progress?: KafkaProgressEvent;
}

function BrokerConfigView({ state, onRetry }: { state: BrokerConfigState; onRetry: () => void }) {
  if (state.loading && state.progress) return <OperationProgress progress={state.progress} />;
  if (state.loading) return <Renderer.Component.Spinner center />;
  if (state.error) {
    return (
      <div className="KafkaPageState error" role="alert">
        <Renderer.Component.Icon material="error_outline" />
        <div>
          <strong>Failed to load broker configuration</strong>
          <span>{state.error}</span>
        </div>
        <Renderer.Component.Button outlined onClick={onRetry}>
          Retry
        </Renderer.Component.Button>
      </div>
    );
  }
  if (!state.data || state.data.entries.length === 0) {
    return (
      <div className="KafkaPageState empty">
        <Renderer.Component.Icon material="tune" />
        <div>
          <strong>No configuration entries</strong>
          <span>Kafka returned no configuration entries for this broker.</span>
        </div>
      </div>
    );
  }

  return (
    <Renderer.Component.Table<KafkaConfigEntryDto>
      className="KafkaBrokerConfigTable"
      tableId="kafka-broker-configuration"
      autoSize={false}
      scrollable
      sortSyncWithUrl={false}
      sortByDefault={{ sortBy: "name", orderBy: "asc" }}
      sortable={{ name: (entry) => entry.name.toLowerCase() }}
    >
      <Renderer.Component.TableHead sticky={false} nowrap>
        <Renderer.Component.TableCell className="configNameCell" sortBy="name" style={BROKER_CONFIG_NAME_STYLE}>
          Name
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="configValueCell" style={BROKER_CONFIG_VALUE_STYLE}>
          Value
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="configSourceCell" style={BROKER_CONFIG_SOURCE_STYLE}>
          Source
        </Renderer.Component.TableCell>
        <Renderer.Component.TableCell className="configFlagsCell" style={BROKER_CONFIG_FLAGS_STYLE}>
          Flags
        </Renderer.Component.TableCell>
      </Renderer.Component.TableHead>
      {state.data.entries.map((entry) => (
        <Renderer.Component.TableRow key={entry.name} sortItem={entry} nowrap>
          <Renderer.Component.TableCell className="configNameCell" title={entry.name} style={BROKER_CONFIG_NAME_STYLE}>
            <span className="KafkaEllipsis">{entry.name}</span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell
            className="configValueCell"
            title={entry.value}
            style={BROKER_CONFIG_VALUE_STYLE}
          >
            <span className="KafkaEllipsis KafkaMono">{entry.value || "—"}</span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell className="configSourceCell" style={BROKER_CONFIG_SOURCE_STYLE}>
            <span className="KafkaConfigSource">{entry.source}</span>
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell className="configFlagsCell" style={BROKER_CONFIG_FLAGS_STYLE}>
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

export function KafkaBrokersPage(props: KafkaBrokersPageProps) {
  const state = useKafkaResourcePage(props);
  const [rawBrokerId, setRawBrokerId] = useKafkaPageParam(props.params?.broker);
  const cluster = state.selectedCluster;
  const data = state.metadataState.data;
  const parsedBrokerId = /^\d+$/.test(rawBrokerId) ? Number(rawBrokerId) : undefined;
  const broker = data?.brokers.find((candidate) => candidate.nodeId === parsedBrokerId);
  const workspaceTargetId = useRef<string>();
  const operationId = useRef<string>();
  const [configState, setConfigState] = useState<BrokerConfigState>({ loading: false });

  useEffect(
    () =>
      props.subscribeProgress((progress) => {
        if (progress.operation === "brokerConfig" && progress.operationId === operationId.current) {
          setConfigState((current) => ({ ...current, progress }));
        }
      }),
    [props.subscribeProgress],
  );

  useEffect(() => {
    const selectedTargetId = cluster?.targetId;
    if (!selectedTargetId) return;
    if (!rawBrokerId) {
      workspaceTargetId.current = selectedTargetId;
      return;
    }
    const previous = workspaceTargetId.current;
    workspaceTargetId.current = selectedTargetId;
    if (previous && previous !== selectedTargetId) setRawBrokerId("", true);
  }, [cluster?.targetId, rawBrokerId, setRawBrokerId]);

  const loadBrokerConfig = useCallback(() => {
    if (!cluster || !broker) {
      operationId.current = undefined;
      setConfigState({ loading: false });
      return;
    }
    const nextOperationId = createOperationId("brokerConfig");
    operationId.current = nextOperationId;
    const progress: KafkaProgressEvent = {
      operationId: nextOperationId,
      operation: "brokerConfig",
      value: 1,
      phase: "strategy",
      label: "Preparing broker configuration",
      detail: "Selecting the existing read-only connection path.",
    };
    setConfigState({ brokerId: broker.nodeId, loading: true, progress });
    props
      .brokerConfig({
        operationId: nextOperationId,
        targetId: cluster.targetId,
        namespace: cluster.namespace,
        clusterName: cluster.name,
        source: cluster.source,
        bootstrap: cluster.bootstrap,
        tls: cluster.tls,
        sourceLocator: cluster.sourceLocator,
        security: props.connectionSettings.get(props.kubernetesClusterId ?? "active", cluster.targetId),
        brokerId: broker.nodeId,
      })
      .then((result) => {
        if (operationId.current === nextOperationId) {
          setConfigState({ brokerId: broker.nodeId, loading: false, data: result });
        }
      })
      .catch((error: unknown) => {
        if (operationId.current !== nextOperationId) return;
        setConfigState({
          brokerId: broker.nodeId,
          loading: false,
          error: error instanceof Error ? error.message : String(error),
          progress,
        });
      });
  }, [broker, cluster, props.brokerConfig, props.connectionSettings, props.kubernetesClusterId]);

  useEffect(() => {
    if (!broker || configState.brokerId === broker.nodeId) return;
    loadBrokerConfig();
  }, [broker, configState.brokerId, loadBrokerConfig]);

  useEffect(
    () => () => {
      operationId.current = undefined;
    },
    [],
  );

  const selectWorkspaceCluster = (targetId: string): void => {
    state.selectCluster(targetId);
    setRawBrokerId("", true);
  };

  if (rawBrokerId) {
    return (
      <KafkaPageShell
        title={broker ? `Broker ${broker.nodeId}` : "Broker"}
        subtitle={cluster ? `Brokers / ${cluster.name}` : "Broker Workspace"}
        actions={
          <>
            <Renderer.Component.Button outlined onClick={() => setRawBrokerId("")}>
              <Renderer.Component.Icon material="arrow_back" />
              Brokers
            </Renderer.Component.Button>
            <KafkaResourceActions state={state} onSelectCluster={selectWorkspaceCluster} />
          </>
        }
      >
        <KafkaResourceState state={state} />
        {cluster && data && !broker && (
          <div className="KafkaPageState error" role="alert">
            <Renderer.Component.Icon material="error_outline" />
            <div>
              <strong>Broker not found</strong>
              <span>Broker {rawBrokerId} was not returned by this Kafka cluster.</span>
            </div>
          </div>
        )}
        {cluster && data && broker && (
          <main className="KafkaResourcePage KafkaBrokerWorkspace" data-testid="kafka-broker-workspace">
            <KafkaMetricStrip
              ariaLabel="Kafka broker detail summary"
              className="KafkaResourceMetricStrip"
              metrics={[
                { label: "Broker", value: broker.nodeId },
                { label: "Role", value: broker.nodeId === data.controller ? "Controller" : "Broker" },
                { label: "Config entries", value: configState.data?.entries.length ?? "—" },
              ]}
            />
            <section className="KafkaResourceDetails" aria-label="Kafka broker identity">
              <div>
                <span>Advertised host</span>
                <code title={broker.host}>{broker.host}</code>
              </div>
              <div>
                <span>Advertised port</span>
                <strong>{broker.port}</strong>
              </div>
            </section>
            <section className="KafkaBrokerConfigSection" aria-label={`Configuration for broker ${broker.nodeId}`}>
              <h2>Configuration</h2>
              <BrokerConfigView state={configState} onRetry={loadBrokerConfig} />
            </section>
          </main>
        )}
      </KafkaPageShell>
    );
  }

  return (
    <KafkaPageShell
      title="Brokers"
      subtitle={cluster ? cluster.name : "Broker identities and advertised endpoints"}
      actions={<KafkaResourceActions state={state} />}
    >
      <KafkaResourceState state={state} />
      {cluster && data && (
        <main className="KafkaResourcePage" data-testid="kafka-brokers-page">
          <KafkaMetricStrip
            ariaLabel="Kafka broker summary"
            className="KafkaResourceMetricStrip"
            metrics={[
              { label: "Brokers", value: data.brokers.length },
              { label: "Controller", value: data.controller ?? "—" },
              { label: "Advertised endpoints", value: data.brokers.length },
            ]}
          />
          {data.brokers.length === 0 ? (
            <div className="KafkaPageState empty">
              <Renderer.Component.Icon material="dns" />
              <div>
                <strong>No brokers reported</strong>
                <span>Kafka metadata returned no broker identities.</span>
              </div>
            </div>
          ) : (
            <Renderer.Component.Table autoSize={false} scrollable className="KafkaBrokerPageTable">
              <Renderer.Component.TableHead nowrap>
                <Renderer.Component.TableCell className="brokerIdCell">Broker</Renderer.Component.TableCell>
                <Renderer.Component.TableCell className="brokerRoleCell">Role</Renderer.Component.TableCell>
                <Renderer.Component.TableCell className="brokerHostCell">Advertised host</Renderer.Component.TableCell>
                <Renderer.Component.TableCell className="brokerPortCell">Port</Renderer.Component.TableCell>
              </Renderer.Component.TableHead>
              {data.brokers.map((broker) => {
                const open = (event: { stopPropagation: () => void }): void => {
                  event.stopPropagation();
                  setRawBrokerId(String(broker.nodeId));
                };
                const interaction: HTMLAttributes<HTMLDivElement> = {
                  role: "button",
                  tabIndex: 0,
                  "aria-label": `Open broker ${broker.nodeId}`,
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
                    key={broker.nodeId}
                    data-broker={broker.nodeId}
                    nowrap
                  >
                    <Renderer.Component.TableCell className="brokerIdCell">
                      <strong>{broker.nodeId}</strong>
                    </Renderer.Component.TableCell>
                    <Renderer.Component.TableCell className="brokerRoleCell">
                      <Renderer.Component.Badge
                        small
                        className={broker.nodeId === data.controller ? "KafkaControllerBadge" : ""}
                        label={broker.nodeId === data.controller ? "Controller" : "Broker"}
                      />
                    </Renderer.Component.TableCell>
                    <Renderer.Component.TableCell className="brokerHostCell" title={broker.host}>
                      <code className="KafkaEllipsis">{broker.host}</code>
                    </Renderer.Component.TableCell>
                    <Renderer.Component.TableCell className="brokerPortCell">
                      {broker.port}
                    </Renderer.Component.TableCell>
                  </Renderer.Component.TableRow>
                );
              })}
            </Renderer.Component.Table>
          )}
          <div className="KafkaResourceFootnote">
            Broker membership comes from cluster metadata; it is not an individual health probe.
          </div>
        </main>
      )}
    </KafkaPageShell>
  );
}
