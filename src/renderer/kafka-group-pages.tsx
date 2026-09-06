import { Renderer } from "@freelensapp/extensions";
import { useCallback, useEffect, useRef, useState } from "react";
import { kafkaPersistentStateStore } from "../common/kafka-persistent-state-store";
import { kafkaListWindow } from "./kafka-list-window";
import { rememberKafkaClusterSelection } from "./kafka-navigation";
import { KafkaMetricStrip, KafkaPageShell } from "./kafka-page-shell";
import {
  KafkaResourceActions,
  type KafkaResourcePageDependencies,
  KafkaResourceState,
  useKafkaPageParam,
  useKafkaResourcePage,
} from "./kafka-resource-pages";
import { createOperationId, kafkaDecimalSortKey } from "./kafka-view-model";
import { canSubmitWriteAction, getWriteConfirmationLabel } from "./kafka-write-policy";
import { useKafkaWriteMode } from "./kafka-write-settings";

import type { CSSProperties, HTMLAttributes } from "react";

import type {
  ConsumerGroupDetailDto,
  ConsumerGroupsDto,
  GroupDetailRequest,
  GroupsRequest,
  KafkaProgressEvent,
  ResetOffsetsRequest,
  ResetOffsetsResultDto,
} from "../common/ipc";

const GROUP_NAME_COLUMN_STYLE: CSSProperties = { flex: "1 1 0", minWidth: 0, width: 0 };
const GROUP_STATE_COLUMN_STYLE: CSSProperties = { flex: "0 0 136px", minWidth: 136, width: 136 };
const GROUP_TYPE_COLUMN_STYLE: CSSProperties = { flex: "0 0 100px", minWidth: 100, width: 100 };
const GROUP_MEMBERS_COLUMN_STYLE: CSSProperties = { flex: "0 0 80px", minWidth: 80, width: 80, textAlign: "right" };
const GROUP_ACTION_COLUMN_STYLE: CSSProperties = { flex: "0 0 44px", minWidth: 44, width: 44 };

interface KafkaGroupsPageParams {
  target: string;
  query: string;
  group: string;
  view: string;
}

export type KafkaGroupView = "offsets" | "members" | "topics";

export interface KafkaGroupsPageProps extends KafkaResourcePageDependencies {
  params?: {
    target: Renderer.Navigation.PageParam<KafkaGroupsPageParams["target"]>;
    query: Renderer.Navigation.PageParam<KafkaGroupsPageParams["query"]>;
    group: Renderer.Navigation.PageParam<KafkaGroupsPageParams["group"]>;
    view: Renderer.Navigation.PageParam<KafkaGroupsPageParams["view"]>;
  };
  groups: (request: GroupsRequest) => Promise<ConsumerGroupsDto>;
  groupDetail: (request: GroupDetailRequest) => Promise<ConsumerGroupDetailDto>;
  onOpenTopic: (targetId: string, topic: string) => void;
  resetOffsets: (request: ResetOffsetsRequest) => Promise<ResetOffsetsResultDto>;
}

function implementedGroupView(view?: string): KafkaGroupView {
  return view === "members" || view === "topics" ? view : "offsets";
}

export function KafkaGroupsPage({
  params,
  groups,
  groupDetail,
  onOpenTopic,
  resetOffsets,
  ...dependencies
}: KafkaGroupsPageProps) {
  const state = useKafkaResourcePage({ ...dependencies, params });
  const canWrite = useKafkaWriteMode(dependencies.writeSettings, state.selectedCluster?.targetId);
  const [query, setQuery] = useKafkaPageParam(params?.query);
  const [groupId, setGroupId] = useKafkaPageParam(params?.group);
  const [rawView, setRawView] = useKafkaPageParam(params?.view);
  const view = groupId ? implementedGroupView(rawView) : "offsets";
  const workspaceTargetId = useRef<string>();

  const [groupsState, setGroupsState] = useState<{
    loading: boolean;
    data?: ConsumerGroupsDto;
    error?: string;
    progress?: KafkaProgressEvent;
  }>({ loading: false });
  const [resetOpen, setResetOpen] = useState(false);
  const [resetMode, setResetMode] = useState<"earliest" | "latest" | "offset" | "timestamp">("earliest");
  const [resetOffset, setResetOffset] = useState("");
  const [resetTimestamp, setResetTimestamp] = useState("");
  const [resetConfirmed, setResetConfirmed] = useState(false);
  const [resetTypedName, setResetTypedName] = useState("");
  const [resetResult, setResetResult] = useState<ResetOffsetsResultDto>();
  const [resetError, setResetError] = useState<string>();

  const [detailState, setDetailState] = useState<{
    loading: boolean;
    data?: ConsumerGroupDetailDto;
    error?: string;
    progress?: KafkaProgressEvent;
  }>({ loading: false });

  const groupsOperationId = useRef<string>();
  const detailOperationId = useRef<string>();
  const [groupPage, setGroupPage] = useState(0);

  useEffect(() => setGroupPage(0), [query]);

  // Normalize view when no group is selected.
  useEffect(() => {
    if (!groupId && rawView !== "offsets") setRawView("offsets", true);
  }, [groupId, rawView, setRawView]);

  // Reset group + view on cluster switch from Group Workspace.
  useEffect(() => {
    const selectedTargetId = state.selectedCluster?.targetId;
    if (!selectedTargetId) return;
    if (!groupId) {
      workspaceTargetId.current = selectedTargetId;
      return;
    }
    const previous = workspaceTargetId.current;
    workspaceTargetId.current = selectedTargetId;
    if (previous && previous !== selectedTargetId) {
      params?.group?.clear?.();
      setGroupId("", true);
      params?.query?.clear?.();
      setQuery("", true);
    }
  }, [params?.group, params?.query, setGroupId, setQuery, state.selectedCluster?.targetId, groupId]);

  useEffect(
    () =>
      dependencies.subscribeProgress((progress) => {
        if (progress.operation === "groups" && progress.operationId === groupsOperationId.current) {
          setGroupsState((current) => ({ ...current, progress }));
        }
        if (progress.operation === "groupDetail" && progress.operationId === detailOperationId.current) {
          setDetailState((current) => ({ ...current, progress }));
        }
      }),
    [dependencies.subscribeProgress],
  );

  const loadGroups = useCallback(() => {
    const cluster = state.selectedCluster;
    if (!cluster) {
      groupsOperationId.current = undefined;
      setGroupsState({ loading: false });
      return;
    }
    const operationId = createOperationId("groups");
    groupsOperationId.current = operationId;
    const cacheKey = dependencies.kubernetesClusterId ?? "active";
    const cached = dependencies.resourceCache.readGroups(cacheKey, cluster.targetId);
    setGroupsState({ loading: true, data: cached.data });
    dependencies.resourceCache
      .loadGroups(cacheKey, cluster.targetId, () =>
        groups({
          operationId,
          targetId: cluster.targetId,
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
        if (groupsOperationId.current !== operationId) return;
        setGroupsState({ loading: false, data });
      })
      .catch((error: unknown) => {
        if (groupsOperationId.current !== operationId) return;
        setGroupsState((current) => ({
          loading: false,
          data: current.data,
          error: error instanceof Error ? error.message : String(error),
        }));
      });
  }, [
    dependencies.connectionSettings,
    dependencies.kubernetesClusterId,
    dependencies.resourceCache,
    groups,
    state.selectedCluster,
  ]);

  useEffect(() => {
    loadGroups();
    return () => {
      groupsOperationId.current = undefined;
    };
  }, [loadGroups]);

  const loadDetail = useCallback(() => {
    const cluster = state.selectedCluster;
    if (!groupId || !cluster) {
      detailOperationId.current = undefined;
      setDetailState({ loading: false });
      return;
    }
    const operationId = createOperationId("groupDetail");
    detailOperationId.current = operationId;
    setDetailState({ loading: true });
    groupDetail({
      operationId,
      targetId: cluster.targetId,
      namespace: cluster.namespace,
      clusterName: cluster.name,
      source: cluster.source,
      bootstrap: cluster.bootstrap,
      tls: cluster.tls,
      sourceLocator: cluster.sourceLocator,
      security: dependencies.connectionSettings.get(dependencies.kubernetesClusterId ?? "active", cluster.targetId),
      groupId,
    })
      .then((data) => {
        if (detailOperationId.current !== operationId) return;
        setDetailState({ loading: false, data });
      })
      .catch((error: unknown) => {
        if (detailOperationId.current !== operationId) return;
        setDetailState({ loading: false, error: error instanceof Error ? error.message : String(error) });
      });
  }, [dependencies.connectionSettings, dependencies.kubernetesClusterId, groupDetail, groupId, state.selectedCluster]);

  useEffect(() => {
    loadDetail();
    return () => {
      detailOperationId.current = undefined;
    };
  }, [loadDetail]);

  const selectWorkspaceCluster = (targetId: string): void => {
    const cluster = state.clusters.find((c) => c.targetId === targetId);
    if (!cluster) return;
    if (dependencies.kubernetesClusterId) {
      rememberKafkaClusterSelection(kafkaPersistentStateStore(), dependencies.kubernetesClusterId, cluster);
    }
    params?.group?.clear?.();
    setGroupId("", true);
    params?.query?.clear?.();
    setQuery("", true);
    state.selectCluster(targetId);
  };

  if (groupId) {
    return (
      <KafkaPageShell
        title={groupId}
        subtitle={state.selectedCluster ? `Consumer Groups / ${state.selectedCluster.name}` : "Group Workspace"}
        actions={
          <>
            <Renderer.Component.Button
              outlined
              onClick={() => {
                setGroupId("");
                setRawView("offsets", true);
              }}
            >
              <Renderer.Component.Icon material="arrow_back" />
              Consumer Groups
            </Renderer.Component.Button>
            {canWrite && groupId && (
              <Renderer.Component.Button
                outlined
                data-testid="kafka-reset-offsets-button"
                onClick={() => {
                  setResetConfirmed(false);
                  setResetTypedName("");
                  setResetOffset("");
                  setResetTimestamp("");
                  setResetOpen(true);
                }}
              >
                <Renderer.Component.Icon material="restart_alt" />
                Reset offsets
              </Renderer.Component.Button>
            )}
            <KafkaResourceActions state={state} onSelectCluster={selectWorkspaceCluster} />
          </>
        }
      >
        <KafkaResourceState state={state} />
        {state.selectedCluster && (
          <main className="KafkaResourcePage KafkaGroupWorkspace" data-testid="kafka-group-workspace">
            {resetOpen && groupId && (
              <section
                className="KafkaWriteDrawer"
                data-testid="kafka-reset-offsets-drawer"
                aria-label="Reset offsets confirmation"
              >
                <div className="KafkaPageState warning">
                  <Renderer.Component.Icon material="restart_alt" />
                  <div>
                    <strong>Reset offsets</strong>
                    <span>Destructive or recovery-like changes require explicit confirmation before execution.</span>
                  </div>
                </div>
                <div className="KafkaWriteForm" style={{ display: "grid", gap: 12 }}>
                  {resetResult && (
                    <div role="status">
                      Reset partition {resetResult.partition} to offset {resetResult.offset}
                    </div>
                  )}
                  {resetError && <div role="alert">{resetError}</div>}
                  <Renderer.Component.Select
                    options={[
                      { value: "earliest", label: "Earliest" },
                      { value: "latest", label: "Latest" },
                      { value: "offset", label: "Specific offset" },
                      { value: "timestamp", label: "Timestamp" },
                    ]}
                    value={resetMode}
                    onChange={(option) => setResetMode((option?.value as typeof resetMode) ?? "earliest")}
                    aria-label="Offset reset mode"
                  />
                  {resetMode === "offset" && (
                    <Renderer.Component.Input
                      value={resetOffset}
                      onChange={setResetOffset}
                      placeholder="Offset value"
                      aria-label="Offset number"
                    />
                  )}
                  {resetMode === "timestamp" && (
                    <Renderer.Component.Input
                      value={resetTimestamp}
                      onChange={setResetTimestamp}
                      placeholder="2026-08-17T00:00:00"
                      aria-label="Reset timestamp"
                    />
                  )}
                  <div className="KafkaWriteSummary">
                    <strong>Group</strong>
                    <span>{groupId}</span>
                    <strong>Cluster</strong>
                    <span>{state.selectedCluster.name}</span>
                  </div>
                  <Renderer.Component.Input
                    value={resetTypedName}
                    onChange={setResetTypedName}
                    placeholder={`Type ${groupId} to confirm`}
                    aria-label="Type the exact resource name"
                    data-testid="kafka-reset-confirmation-text"
                  />
                  <label style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                    <span>{getWriteConfirmationLabel({ destructive: true, resourceName: groupId })}</span>
                    <Renderer.Component.Switch
                      aria-label="Confirm reset offsets"
                      data-testid="kafka-reset-confirmation-switch"
                      checked={resetConfirmed}
                      onChange={setResetConfirmed}
                    />
                  </label>
                  <div style={{ display: "flex", gap: 8, justifyContent: "flex-end" }}>
                    <Renderer.Component.Button outlined onClick={() => setResetOpen(false)}>
                      Cancel
                    </Renderer.Component.Button>
                    <Renderer.Component.Button
                      primary
                      disabled={
                        !canSubmitWriteAction({
                          confirmationAccepted: resetConfirmed,
                          requiredResourceName: groupId,
                          enteredResourceName: resetTypedName,
                        })
                      }
                      onClick={() => {
                        if (
                          !canSubmitWriteAction({
                            confirmationAccepted: resetConfirmed,
                            requiredResourceName: groupId,
                            enteredResourceName: resetTypedName,
                          })
                        ) {
                          return;
                        }
                        const firstTopic = detailState.data?.topicOffsets[0];
                        const firstPartition = firstTopic?.partitions[0];
                        if (!state.selectedCluster || !firstTopic || !firstPartition) return;
                        setResetError(undefined);
                        void resetOffsets({
                          targetId: state.selectedCluster.targetId,
                          source: state.selectedCluster.source,
                          bootstrap: state.selectedCluster.bootstrap,
                          tls: state.selectedCluster.tls,
                          namespace: state.selectedCluster.namespace,
                          clusterName: state.selectedCluster.name,
                          groupId,
                          topic: firstTopic.topic,
                          partition: firstPartition.partition,
                          mode: resetMode,
                          offset: resetMode === "offset" ? resetOffset : undefined,
                          timestamp: resetMode === "timestamp" ? Date.parse(resetTimestamp) : undefined,
                        })
                          .then((result) => setResetResult(result))
                          .catch((error: unknown) =>
                            setResetError(error instanceof Error ? error.message : String(error)),
                          );
                      }}
                    >
                      Reset offsets
                    </Renderer.Component.Button>
                  </div>
                </div>
              </section>
            )}
            <nav className="KafkaEntityTabs" role="tablist" aria-label="Group sections">
              {(["offsets", "members", "topics"] as const).map((tab) => (
                <Renderer.Component.Button
                  key={tab}
                  plain
                  role="tab"
                  active={view === tab}
                  aria-selected={view === tab}
                  onClick={() => setRawView(tab)}
                >
                  {tab === "offsets" ? "Offsets & Lag" : tab === "members" ? "Members" : "Topics"}
                </Renderer.Component.Button>
              ))}
            </nav>
            {detailState.loading && (
              <div className="KafkaPageState loading">
                <Renderer.Component.Spinner />
                <span>Loading group detail…</span>
              </div>
            )}
            {!detailState.loading && detailState.error && (
              <div className="KafkaPageState error" role="alert">
                <Renderer.Component.Icon material="error_outline" />
                <div>
                  <strong>Failed to load group detail</strong>
                  <span>{detailState.error}</span>
                </div>
                <Renderer.Component.Button outlined onClick={loadDetail}>
                  Retry
                </Renderer.Component.Button>
              </div>
            )}
            {!detailState.loading && detailState.data && view === "offsets" && (
              <KafkaGroupOffsets detail={detailState.data} />
            )}
            {!detailState.loading && detailState.data && view === "members" && (
              <KafkaGroupMembers detail={detailState.data} />
            )}
            {!detailState.loading && detailState.data && view === "topics" && (
              <KafkaGroupTopics
                detail={detailState.data}
                onOpenTopic={(topic) => onOpenTopic(state.selectedCluster?.targetId ?? "", topic)}
              />
            )}
          </main>
        )}
      </KafkaPageShell>
    );
  }

  const allGroups = groupsState.data?.groups ?? [];
  const filteredGroups = allGroups.filter((g) =>
    query ? g.groupId.toLowerCase().includes(query.toLowerCase()) : true,
  );
  const groupWindow = kafkaListWindow(filteredGroups, groupPage);

  return (
    <KafkaPageShell
      title="Consumer Groups"
      subtitle={state.selectedCluster ? state.selectedCluster.name : "Browse consumer groups in a Kafka cluster"}
      actions={<KafkaResourceActions state={state} />}
    >
      <KafkaResourceState state={state} />
      {state.selectedCluster && (
        <main className="KafkaResourcePage KafkaGroupsPage" data-testid="kafka-groups-page">
          {groupsState.loading && !groupsState.data && (
            <div className="KafkaPageState loading">
              <Renderer.Component.Spinner />
              <span>Loading consumer groups…</span>
            </div>
          )}
          {!groupsState.loading && groupsState.error && (
            <div className="KafkaPageState error" role="alert">
              <Renderer.Component.Icon material="error_outline" />
              <div>
                <strong>Failed to load consumer groups</strong>
                <span>{groupsState.error}</span>
              </div>
              <Renderer.Component.Button outlined onClick={loadGroups}>
                Retry
              </Renderer.Component.Button>
            </div>
          )}
          {!groupsState.loading && groupsState.data && (
            <>
              <KafkaMetricStrip
                ariaLabel="Kafka consumer group summary"
                className="KafkaResourceMetricStrip"
                metrics={[{ label: "Groups", value: allGroups.length }]}
              />
              {allGroups.length === 0 ? (
                <div className="KafkaPageState empty">
                  <Renderer.Component.Icon material="group" />
                  <div>
                    <strong>No consumer groups</strong>
                    <span>Kafka returned no consumer groups for this cluster.</span>
                  </div>
                </div>
              ) : (
                <>
                  <Renderer.Component.Input
                    className="KafkaTopicSearch"
                    value={query}
                    onChange={(nextQuery) => setQuery(nextQuery, true)}
                    iconLeft="search"
                    placeholder="Filter groups"
                    aria-label="Filter consumer groups"
                  />
                  <Renderer.Component.Table<(typeof allGroups)[number]>
                    className="KafkaGroupTable KafkaGroupPageTable"
                    tableId="kafka-groups-page"
                    autoSize={false}
                    scrollable
                    sortSyncWithUrl={false}
                    sortByDefault={{ sortBy: "id", orderBy: "asc" }}
                    sortable={{ id: (g) => g.groupId.toLowerCase() }}
                    noItems={
                      <div className="KafkaTopicPrompt">
                        <Renderer.Component.Icon material="filter_alt_off" />
                        <span>No groups match the current filter.</span>
                      </div>
                    }
                  >
                    <Renderer.Component.TableHead sticky={false} nowrap>
                      <Renderer.Component.TableCell
                        className="groupNameCell"
                        sortBy="id"
                        style={GROUP_NAME_COLUMN_STYLE}
                      >
                        Group ID
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="groupStateCell" style={GROUP_STATE_COLUMN_STYLE}>
                        State
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="groupTypeCell" style={GROUP_TYPE_COLUMN_STYLE}>
                        Protocol
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="groupMembersCell" style={GROUP_MEMBERS_COLUMN_STYLE}>
                        Members
                      </Renderer.Component.TableCell>
                      <Renderer.Component.TableCell className="groupActionCell" style={GROUP_ACTION_COLUMN_STYLE} />
                    </Renderer.Component.TableHead>
                    {groupWindow.items.map((group) => {
                      const interaction: HTMLAttributes<HTMLDivElement> = {
                        role: "button",
                        tabIndex: 0,
                        "aria-label": `Open group ${group.groupId}`,
                        onClick: (event) => {
                          event.stopPropagation();
                          setRawView("offsets", true);
                          setGroupId(group.groupId);
                        },
                        onKeyDown: (event) => {
                          if (event.key === "Enter" || event.key === " ") {
                            event.preventDefault();
                            setRawView("offsets", true);
                            setGroupId(group.groupId);
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
                            className="groupNameCell"
                            title={group.groupId}
                            style={GROUP_NAME_COLUMN_STYLE}
                          >
                            <Renderer.Component.Icon material="group" />
                            <span className="KafkaEllipsis">{group.groupId}</span>
                          </Renderer.Component.TableCell>
                          <Renderer.Component.TableCell className="groupStateCell" style={GROUP_STATE_COLUMN_STYLE}>
                            <GroupStateBadge state={group.state} />
                          </Renderer.Component.TableCell>
                          <Renderer.Component.TableCell className="groupTypeCell" style={GROUP_TYPE_COLUMN_STYLE}>
                            <span className="KafkaGroupProtocol">{group.protocolType || "—"}</span>
                          </Renderer.Component.TableCell>
                          <Renderer.Component.TableCell className="groupMembersCell" style={GROUP_MEMBERS_COLUMN_STYLE}>
                            {group.memberCount}
                          </Renderer.Component.TableCell>
                          <Renderer.Component.TableCell className="groupActionCell" style={GROUP_ACTION_COLUMN_STYLE}>
                            <Renderer.Component.Icon material="chevron_right" />
                          </Renderer.Component.TableCell>
                        </Renderer.Component.TableRow>
                      );
                    })}
                  </Renderer.Component.Table>
                  {groupWindow.pageCount > 1 && (
                    <div className="KafkaListPagination" aria-label="Consumer group pages">
                      <Renderer.Component.Button
                        outlined
                        disabled={groupWindow.page === 0}
                        onClick={() => setGroupPage((page) => Math.max(0, page - 1))}
                        aria-label="Previous consumer group page"
                      >
                        Previous
                      </Renderer.Component.Button>
                      <span>
                        Page {groupWindow.page + 1} of {groupWindow.pageCount} ({groupWindow.total} groups)
                      </span>
                      <Renderer.Component.Button
                        outlined
                        disabled={groupWindow.page >= groupWindow.pageCount - 1}
                        onClick={() => setGroupPage((page) => Math.min(groupWindow.pageCount - 1, page + 1))}
                        aria-label="Next consumer group page"
                      >
                        Next
                      </Renderer.Component.Button>
                    </div>
                  )}
                </>
              )}
            </>
          )}
        </main>
      )}
    </KafkaPageShell>
  );
}

function GroupStateBadge({ state }: { state: string }) {
  const tone =
    state === "Stable"
      ? "KafkaHealthyBadge"
      : state === "Dead" || state === "Unknown"
        ? "KafkaErrorBadge"
        : "KafkaWarningBadge";
  return <Renderer.Component.Badge small className={tone} label={state || "Unknown"} />;
}

const OFFSET_PARTITION_STYLE: CSSProperties = { flex: "0 0 90px", minWidth: 90, width: 90 };
const OFFSET_VALUE_STYLE: CSSProperties = { flex: "0 0 140px", minWidth: 140, width: 140, textAlign: "right" };
const OFFSET_LAG_STYLE: CSSProperties = { flex: "0 0 100px", minWidth: 100, width: 100, textAlign: "right" };

function KafkaGroupOffsets({ detail }: { detail: ConsumerGroupDetailDto }) {
  if (detail.topicOffsets.length === 0) {
    return (
      <div className="KafkaPageState empty">
        <Renderer.Component.Icon material="rule" />
        <div>
          <strong>No committed offsets</strong>
          <span>This group has no committed offsets for any topic.</span>
        </div>
      </div>
    );
  }

  return (
    <div className="KafkaGroupOffsetSection" data-testid="kafka-group-offsets">
      {detail.topicOffsets.map((topicData) => (
        <section key={topicData.topic} className="KafkaGroupTopicSection">
          <header className="KafkaGroupTopicHeader">
            <Renderer.Component.Icon material="topic" />
            <span className="KafkaGroupTopicName">{topicData.topic}</span>
            <span className="KafkaGroupTopicLag">
              Total lag: <strong>{topicData.totalLag}</strong>
            </span>
          </header>
          <Renderer.Component.Table autoSize={false} scrollable className="KafkaOffsetTable">
            <Renderer.Component.TableHead nowrap>
              <Renderer.Component.TableCell className="partitionCell" style={OFFSET_PARTITION_STYLE}>
                Partition
              </Renderer.Component.TableCell>
              <Renderer.Component.TableCell className="committedCell" style={OFFSET_VALUE_STYLE}>
                Committed offset
              </Renderer.Component.TableCell>
              <Renderer.Component.TableCell className="hwmCell" style={OFFSET_VALUE_STYLE}>
                High watermark
              </Renderer.Component.TableCell>
              <Renderer.Component.TableCell className="lagCell" style={OFFSET_LAG_STYLE}>
                Lag
              </Renderer.Component.TableCell>
            </Renderer.Component.TableHead>
            {topicData.partitions.map((p) => (
              <Renderer.Component.TableRow key={p.partition} nowrap>
                <Renderer.Component.TableCell className="partitionCell" style={OFFSET_PARTITION_STYLE}>
                  <strong>{p.partition}</strong>
                </Renderer.Component.TableCell>
                <Renderer.Component.TableCell className="committedCell" style={OFFSET_VALUE_STYLE}>
                  <code>{p.committedOffset === "-1" ? "—" : p.committedOffset}</code>
                </Renderer.Component.TableCell>
                <Renderer.Component.TableCell className="hwmCell" style={OFFSET_VALUE_STYLE}>
                  <code>{p.highWatermark}</code>
                </Renderer.Component.TableCell>
                <Renderer.Component.TableCell className="lagCell" style={OFFSET_LAG_STYLE}>
                  <LagValue lag={p.lag} />
                </Renderer.Component.TableCell>
              </Renderer.Component.TableRow>
            ))}
          </Renderer.Component.Table>
        </section>
      ))}
    </div>
  );
}

function LagValue({ lag }: { lag: string }) {
  if (lag === "—") return <span className="KafkaLagUnknown">—</span>;
  const n = BigInt(lag);
  const tone = n === 0n ? "KafkaLagZero" : n < 1000n ? "KafkaLagLow" : "KafkaLagHigh";
  return <strong className={tone}>{lag}</strong>;
}

const GROUP_TOPIC_NAME_STYLE: CSSProperties = { flex: "1 1 0", minWidth: 0, width: 0 };
const GROUP_TOPIC_PARTITIONS_STYLE: CSSProperties = { flex: "0 0 100px", minWidth: 100, width: 100 };
const GROUP_TOPIC_LAG_STYLE: CSSProperties = { flex: "0 0 120px", minWidth: 120, width: 120 };
const GROUP_TOPIC_ACTION_STYLE: CSSProperties = { flex: "0 0 44px", minWidth: 44, width: 44 };

function KafkaGroupTopics({
  detail,
  onOpenTopic,
}: {
  detail: ConsumerGroupDetailDto;
  onOpenTopic: (topic: string) => void;
}) {
  if (detail.topicOffsets.length === 0) {
    return (
      <div className="KafkaPageState empty">
        <Renderer.Component.Icon material="topic" />
        <div>
          <strong>No consumed topics</strong>
          <span>This group has no committed offsets for any topic.</span>
        </div>
      </div>
    );
  }

  return (
    <div className="KafkaGroupTopicsSection" data-testid="kafka-group-topics">
      <KafkaMetricStrip
        ariaLabel="Group topic summary"
        className="KafkaResourceMetricStrip"
        metrics={[{ label: "Topics", value: detail.topicOffsets.length }]}
      />
      <Renderer.Component.Table<(typeof detail.topicOffsets)[number]>
        autoSize={false}
        scrollable
        className="KafkaGroupTopicsTable"
        tableId="kafka-group-topics"
        sortSyncWithUrl={false}
        sortByDefault={{ sortBy: "topic", orderBy: "asc" }}
        sortable={{
          topic: (topic) => topic.topic.toLowerCase(),
          lag: (topic) => kafkaDecimalSortKey(topic.totalLag),
        }}
      >
        <Renderer.Component.TableHead nowrap>
          <Renderer.Component.TableCell className="groupTopicNameCell" sortBy="topic" style={GROUP_TOPIC_NAME_STYLE}>
            Topic
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell className="groupTopicPartitionsCell" style={GROUP_TOPIC_PARTITIONS_STYLE}>
            Partitions
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell className="groupTopicLagCell" sortBy="lag" style={GROUP_TOPIC_LAG_STYLE}>
            Total lag
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell className="groupTopicActionCell" style={GROUP_TOPIC_ACTION_STYLE} />
        </Renderer.Component.TableHead>
        {detail.topicOffsets.map((topic) => {
          const open = (): void => onOpenTopic(topic.topic);
          const interaction: HTMLAttributes<HTMLDivElement> = {
            role: "button",
            tabIndex: 0,
            "aria-label": `Open topic ${topic.topic}`,
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
              key={topic.topic}
              sortItem={topic}
              data-topic={topic.topic}
              nowrap
            >
              <Renderer.Component.TableCell
                className="groupTopicNameCell"
                title={topic.topic}
                style={GROUP_TOPIC_NAME_STYLE}
              >
                <Renderer.Component.Icon material="topic" />
                <span className="KafkaEllipsis">{topic.topic}</span>
              </Renderer.Component.TableCell>
              <Renderer.Component.TableCell className="groupTopicPartitionsCell" style={GROUP_TOPIC_PARTITIONS_STYLE}>
                {topic.partitions.length}
              </Renderer.Component.TableCell>
              <Renderer.Component.TableCell className="groupTopicLagCell" style={GROUP_TOPIC_LAG_STYLE}>
                <LagValue lag={topic.totalLag} />
              </Renderer.Component.TableCell>
              <Renderer.Component.TableCell className="groupTopicActionCell" style={GROUP_TOPIC_ACTION_STYLE}>
                <Renderer.Component.Icon material="chevron_right" />
              </Renderer.Component.TableCell>
            </Renderer.Component.TableRow>
          );
        })}
      </Renderer.Component.Table>
    </div>
  );
}

const MEMBER_ID_STYLE: CSSProperties = { flex: "1 1 0", minWidth: 0, width: 0 };
const MEMBER_CLIENT_STYLE: CSSProperties = { flex: "0 0 200px", minWidth: 200, width: 200 };
const MEMBER_HOST_STYLE: CSSProperties = { flex: "0 0 180px", minWidth: 180, width: 180 };

function KafkaGroupMembers({ detail }: { detail: ConsumerGroupDetailDto }) {
  if (detail.members.length === 0) {
    return (
      <div className="KafkaPageState empty">
        <Renderer.Component.Icon material="person_off" />
        <div>
          <strong>No active members</strong>
          <span>This consumer group has no active members.</span>
        </div>
      </div>
    );
  }

  return (
    <div className="KafkaGroupMemberSection" data-testid="kafka-group-members">
      <KafkaMetricStrip
        ariaLabel="Group member summary"
        className="KafkaResourceMetricStrip"
        metrics={[{ label: "Members", value: detail.members.length }]}
      />
      <Renderer.Component.Table autoSize={false} scrollable className="KafkaMemberTable">
        <Renderer.Component.TableHead nowrap>
          <Renderer.Component.TableCell className="memberIdCell" style={MEMBER_ID_STYLE}>
            Member ID
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell className="clientIdCell" style={MEMBER_CLIENT_STYLE}>
            Client ID
          </Renderer.Component.TableCell>
          <Renderer.Component.TableCell className="clientHostCell" style={MEMBER_HOST_STYLE}>
            Client host
          </Renderer.Component.TableCell>
        </Renderer.Component.TableHead>
        {detail.members.map((m) => (
          <Renderer.Component.TableRow key={m.memberId} nowrap>
            <Renderer.Component.TableCell className="memberIdCell" title={m.memberId} style={MEMBER_ID_STYLE}>
              <code className="KafkaEllipsis">{m.memberId}</code>
            </Renderer.Component.TableCell>
            <Renderer.Component.TableCell className="clientIdCell" title={m.clientId} style={MEMBER_CLIENT_STYLE}>
              <span className="KafkaEllipsis">{m.clientId}</span>
            </Renderer.Component.TableCell>
            <Renderer.Component.TableCell className="clientHostCell" title={m.clientHost} style={MEMBER_HOST_STYLE}>
              <code className="KafkaEllipsis">{m.clientHost}</code>
            </Renderer.Component.TableCell>
          </Renderer.Component.TableRow>
        ))}
      </Renderer.Component.Table>
    </div>
  );
}
