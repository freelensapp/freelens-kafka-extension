# SPEC-007 — Topic and Broker Depth

| Field | Value |
| --- | --- |
| Status | Verified |
| Date | 2026-08-06 |
| Source | v1.0.0 parity gap analysis; accepted by user 2026-08-06 |
| Safety | Kafka reads only; governed by `TESTING-SAFETY.md` |

## Problem

The Topic Workspace and Brokers page show partition health but expose no configuration data and no
cross-references between topics and the consumer groups that consume them. Users must leave the
extension to inspect retention policy, compaction settings or which groups are lagging on a given
topic.

## Scope

- **Topic Configuration tab** — topic-level config key/value pairs with their source (default,
  dynamic topic, dynamic broker, static broker).
- **Topic → Consumers tab** — consumer groups subscribed to this topic with their aggregate lag for
  the topic.
- **Consumer Group → Topics tab** — cross-link from the Group Workspace to every topic the group has
  committed offsets on, with aggregate lag per topic. (Deferred from SPEC-006.)
- **Broker config** — per-broker configuration detail accessible from the Brokers page.

## Non-goals

- Modifying topic or broker configuration (write operation, SPEC-009+).
- Creating or deleting topics.
- Real-time configuration polling or change detection.
- Displaying internal broker metrics (JMX, MBeans).

## User Scenarios

### US-023 — Inspect topic retention without leaving the workspace (P1)

**Given** a selected topic, **when** the user opens the Configuration tab, **then** Freelens shows
all configuration entries (key, value, source) without fetching any messages.

### US-024 — Find which consumer groups are lagging on a topic (P1)

**Given** a selected topic, **when** the user opens the Consumers tab, **then** Freelens lists every
consumer group subscribed to that topic with its aggregate lag.

### US-025 — Navigate from a consumer group to its consumed topics (P1)

**Given** a selected consumer group, **when** the user opens the Topics tab in the Group Workspace,
**then** Freelens lists all topics with committed offsets and their aggregate lag.

### US-026 — Inspect per-broker configuration (P2)

**Given** the Brokers page, **when** the user selects a broker, **then** Freelens shows that
broker's configuration key/value pairs.

### US-027 — Cross-navigate from topic to group and back (P1)

**Given** the Topic Consumers tab, **when** the user clicks a consumer group row, **then** Freelens
opens the Group Workspace for that group. **Given** the Group Topics tab, **when** the user clicks a
topic row, **then** Freelens opens the Topic Workspace for that topic. Both preserve the selected
Kafka cluster.

## Functional Requirements

- **REQ-084** — The Topic Workspace MUST include a **Configuration** tab showing all configuration
  entries returned by `admin.describeConfigs` for the topic. Each entry MUST show key, value and
  source (DEFAULT, DYNAMIC_TOPIC_CONFIG, DYNAMIC_BROKER_CONFIG, STATIC_BROKER_CONFIG).
- **REQ-085** — Topic configuration MUST be fetched lazily when the Configuration tab is first
  opened; it MUST NOT be fetched during page load or when other tabs are active.
- **REQ-086** — The Topic Workspace MUST include a **Consumers** tab listing every consumer group
  that has committed offsets on that topic, with aggregate lag for the topic and the group's current
  state.
- **REQ-087** — Each row in the Topic Consumers tab MUST be clickable and MUST navigate to the Group
  Workspace for that group, preserving the selected Kafka cluster.
- **REQ-088** — The Consumer Group Workspace MUST include a **Topics** tab listing all topics on
  which the group has committed offsets, showing topic name and aggregate lag.
- **REQ-089** — Each row in the Consumer Group Topics tab MUST be clickable and MUST navigate to the
  Topic Workspace for that topic, preserving the selected Kafka cluster.
- **REQ-090** — The Brokers page MUST provide a per-broker configuration detail view showing all
  entries returned by `admin.describeConfigs` for that broker (resource type BROKER).
- **REQ-091** — All new tabs and detail views MUST follow the bounded operation pattern with a
  cancellation path and `disconnect()` exactly once in `finally`. Point reads MUST time out after
  30 seconds. The cluster-wide Topic Consumers scan MAY use up to 120 seconds, MUST limit concurrent
  OffsetFetch calls to 16 and MUST report completed/total consumer groups while running.
- **REQ-092** — All new tabs MUST show visually distinct loading, empty, error and populated states.
- **REQ-093** — Switching Kafka cluster from any new tab MUST follow the SPEC-004 cluster-switch
  contract: return to the list for the new cluster and clear the selected entity.

## Success Criteria

- **SC-047** — Opening the Topic Configuration tab shows at least `retention.ms` and
  `cleanup.policy` for a test topic without issuing any message fetch.
- **SC-048** — The Topic Consumers tab shows at least one consumer group for a topic with active
  consumers; clicking the row navigates to the correct Group Workspace.
- **SC-049** — The Consumer Group Topics tab shows all topics with committed offsets; clicking a
  topic row navigates to the correct Topic Workspace.
- **SC-050** — Broker config detail shows configuration entries for a selected broker, verified
  against a local Docker fixture.
- **SC-051** — Empty state is shown correctly when a topic has no consumer groups and when a broker
  returns an empty config list.

## Implementation Progress

| Slice | Status | Evidence |
| --- | --- | --- |
| Topic Configuration (REQ-084–085) | Done | `topic-config.test.ts`; packaged Topic Workspace E2E |
| Topic Consumers + Group cross-link (REQ-086–087) | Done | Optimized bounded scan; `group-fetch.test.ts`; `itest:groups`; packaged keyboard-navigation E2E |
| Consumer Group Topics + Topic cross-link (REQ-088–089) | Done | Existing group-detail protocol evidence; packaged tab/lag/keyboard-navigation E2E |
| Broker configuration detail (REQ-090) | Done | `broker-config.test.ts`; `itest:broker-config`; packaged Broker Workspace E2E |
| Shared bounded lifecycle and UI states (REQ-091–093) | Done | Bounded IPC review; error/empty unit coverage; packaged cluster-switch E2E |

## Traceability

| Requirement | Delivered code | Verification evidence |
| --- | --- | --- |
| REQ-084–REQ-085 | Topic config DTO/adapter/IPC and lazy Configuration tab | `topic-config.test.ts`; packaged assertions for `cleanup.policy` and `retention.ms` |
| REQ-086–REQ-087 | Topic Consumers aggregation describes only matching groups, scans offsets with concurrency 16, reports progress, and preserves the Group cross-link | Scale/call-count cases in `group-fetch.test.ts`; `TOPIC_CONSUMERS_OK`; packaged lag and keyboard navigation |
| REQ-088–REQ-089 | Group Topics tab using existing detail DTO and Topic cross-link | Group detail protocol evidence; packaged topic count/lag and keyboard navigation |
| REQ-090 | Broker config DTO/adapter/IPC, URL-backed Broker Workspace and masked values | `broker-config.test.ts`; `BROKER_CONFIG_OK`; packaged config table and back navigation |
| REQ-091 | 30-second point reads; 120-second bounded Topic Consumers scan; cancellation guards and final disconnect | Concurrency/progress unit tests; Main IPC review; local protocol cleanup; full packaged suite |
| REQ-092 | Loading, empty, error and populated states for all new views | Adapter error/empty unit tests; static UI review; populated packaged regressions |
| REQ-093 | Cluster switch clears Topic/Group/Broker entity state | Packaged Topic tail switch and Broker Workspace URL/workspace reset assertions |

## Verification Evidence

- **Unit:** 21 test files / 112 tests pass.
- **Static:** `type:check`, Biome/Prettier and Knip development/production pass.
- **Protocol:** disposable local Kafka reports `TOPIC_CONSUMERS_OK` and
  `BROKER_CONFIG_OK brokerId=1 entries=314`; sensitive config values remain masked.
- **Build/package:** production build, `smoke:main` and extension tarball pass.
- **Packaged app:** the fresh `0.1.1-0` artifact passes focused Topic Consumers in 24.169 seconds;
  all 6 Playwright/Jest scenarios pass against Freelens 1.10.3 in 43.286 seconds,
  including both cross-links, topic/broker configuration, keyboard navigation and cluster-switch reset.

## Decision Log

- **2026-08-06** — Spec accepted as part of v1.0.0 scope definition.
- **2026-08-06** — Consumer Group → Topics tab deferred from SPEC-006 is formally assigned here.
- **2026-08-06** — Broker config is read-only in this spec; write (config alter) is deferred to SPEC-009+.
- **2026-08-07** — Topic Configuration and Topic Consumers/Group cross-link slices implemented and
  verified; Consumer Group Topics and Broker config remain before SPEC-007 can become Verified.
- **2026-08-07** — Consumer Group Topics tab and keyboard cross-link to Topic Workspace implemented
  and verified without adding a Kafka request; Broker config is the final SPEC-007 feature slice.
- **2026-08-07** — Broker Workspace and read-only broker configuration implemented; exhaustive
  progress rendering and URL reset regressions added. All requirements and gates pass; SPEC-007 is Verified.
- **2026-08-17** — Manual testing exposed a 30-second Topic Consumers timeout on a large cluster.
  Reordered the scan to describe only groups with committed offsets, raised bounded OffsetFetch
  concurrency from 8 to 16, added `x/y` progress and allowed this cluster-wide operation up to 120 seconds.
