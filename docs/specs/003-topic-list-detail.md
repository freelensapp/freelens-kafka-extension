# SPEC-003 — Topic List and Detail

| Field | Value |
| --- | --- |
| Status | Verified |
| Date | 2026-07-22 |
| Source | Planned roadmap P3.1(c), accepted by the user |
| Safety | Kafka metadata reads only; governed by [`TESTING-SAFETY.md`](../../TESTING-SAFETY.md) |

## Problem

The cluster Drawer reports only a topic count. Operators cannot identify available topics or inspect
partition leadership and replica health without leaving Freelens. Large managed clusters may contain
hundreds of topics, so eagerly loading every partition topology would add avoidable latency and IPC
payload size to the common cluster-overview path.

## Scope

- Searchable, sortable topic names within the selected Kafka cluster Drawer.
- Explicit identification of Kafka internal topics.
- Lazy, read-only metadata retrieval for one selected topic.
- Partition leadership, replicas, in-sync replicas (ISR), offline replicas and health.
- Native loading, empty, error and keyboard interaction states.

## Non-goals

- Creating, deleting or configuring topics or partitions.
- Producing, consuming or tailing messages.
- Consumer groups, offsets or lag.
- Topic configuration inspection.
- Eagerly transferring partition metadata for every topic in a cluster.

## User Scenarios

### US-008 — Find a topic (P1)

**Given** a connected Kafka cluster, **when** the user opens the Topics view and searches by name,
**then** matching topics remain visible in deterministic order and internal topics are clearly marked.

### US-009 — Inspect partition health (P1)

**Given** a topic in the list, **when** the user selects it by pointer or keyboard, **then** Freelens
loads only that topic's metadata and displays partition, leader, replica, ISR, offline-replica and
health information.

### US-010 — Understand constrained or failed metadata reads (P1)

**Given** a slow or unavailable Kafka connection, **when** topic metadata is requested, **then** the
Drawer shows correlated progress or a retained error without losing the topic list or exposing
credentials.

## Functional Requirements

- **REQ-023** — The cluster Drawer MUST expose the topic names returned by the existing read-only
  overview and MUST support case-insensitive filtering and deterministic sorting.
- **REQ-024** — Kafka internal topics MUST be visually distinguishable without hiding them by default.
- **REQ-025** — Selecting a topic MUST fetch metadata only for that topic; cluster overview MUST NOT
  eagerly fetch all partition topology.
- **REQ-026** — Topic detail MUST show partition ID, leader, replica IDs, ISR IDs, offline replica IDs
  and an explicit healthy, under-replicated or unavailable state.
- **REQ-027** — Topic detail MUST summarize partition count, replication factor, under-replicated
  partition count and unavailable partition count.
- **REQ-028** — Topic metadata loading and failure MUST be correlated by operation ID; stale responses
  from an earlier topic selection MUST NOT replace the current selection.
- **REQ-029** — Topic rows MUST support pointer selection and Enter/Space keyboard activation with a
  visible focus state and accessible selected state.
- **REQ-030** — Empty topic lists, no search matches, loading and errors MUST each have a distinct,
  user-readable state while preserving access to cluster connection settings.
- **REQ-031** — Topic inspection MUST reuse the selected target's connection/security semantics and
  MUST remain metadata-only; credentials and Secret values MUST never enter response DTOs or logs.
- **REQ-032** — The topic UI MUST avoid horizontal Drawer overflow at desktop and compact viewports,
  and existing broker, authentication, manual-endpoint and navigation behavior MUST remain covered.

## Success Criteria

- **SC-013** — A topic search filters names case-insensitively and recovers when cleared.
- **SC-014** — Selecting one topic issues one topic-scoped metadata request and renders every returned
  partition in partition-ID order.
- **SC-015** — A fixture with one missing ISR member renders an under-replicated state; a negative
  leader or partition error renders unavailable.
- **SC-016** — Keyboard Enter/Space and pointer interaction select a topic without closing the cluster
  Drawer.
- **SC-017** — Topic list/detail and the cluster Drawer have no horizontal overflow at the packaged
  Electron desktop and compact viewports.
- **SC-018** — Unit/static gates, focused packaged-app E2E and the committed integration regression pass.

## Assumptions and Decisions

- Topic names come from the existing `admin.listTopics()` overview call.
- A name beginning with `__` is presented as an internal Kafka topic because KafkaJS does not expose
  the broker protocol's internal-topic flag in `ITopicMetadata`.
- `admin.fetchTopicMetadata({ topics: [name] })` is invoked only after explicit topic selection.
- The cluster Drawer uses one Brokers/Topics view switch; topic detail is in-place rather than a nested Drawer.

## Implementation Slices

1. Shared topic DTOs and pure KafkaJS metadata normalization.
2. Topic-scoped Main operation, correlated IPC progress and guaranteed connection teardown.
3. Native Brokers/Topics Drawer views, topic search and in-place partition detail.
4. Unit, protocol, MCP-assisted UI and packaged-app regression evidence.

## Traceability

| Requirement | Delivered code/evidence |
| --- | --- |
| REQ-023–REQ-024 | `ClusterOverviewDto.topics`; deterministic `filterTopicNames`; native searchable topic table and Internal badge; view-model unit tests |
| REQ-025–REQ-028 | `KafkaConnection.topicDetail`; `kafka:topic` IPC; operation-correlated progress and stale-response guard; local Kafka protocol test |
| REQ-026–REQ-027 | `TopicDetailDto`, `toTopicDetail`, partition-health badges/metrics; mapper unit tests and three-partition packaged E2E |
| REQ-029–REQ-030 | Pointer + Enter/Space topic rows, prompt/no-match/error/retry states; MCP and packaged-app assertions |
| REQ-031 | Existing `resolveConnection` security path plus metadata-only `fetchTopicMetadata`; no secret-bearing DTO fields |
| REQ-032 | Responsive SCSS, MCP desktop/compact measurement, anti-clipping/overflow committed E2E assertions |

## Verification Evidence

- **Unit/static:** 11 test files / 50 tests pass; typecheck, Biome/Prettier and build pass.
- **Bundle:** Main/Renderer build and `smoke:main` pass; packed artifact is 2,240,455 bytes.
- **Protocol:** disposable Kafka 3.9 returns the expected one-partition topology through
  `topicDetail`; the existing redirect/produce/consume flow remains green and is torn down.
- **Packaged Freelens 1.10.3:** diagnostic E2E passes in 18.8 s; topic search/no-match/recovery,
  lazy metadata progress, three partitions, health, keyboard activation, auth/manual flows and
  desktop layout are covered.
- **Committed integration:** exact committed suite passes 2/2 in 26.1 s, including desktop and
  760×700 Drawer/table/header overflow assertions; temporary test copy and fixtures are absent.
- **Official Playwright MCP:** `@playwright/mcp@0.0.78` attached through CDP to a one-context
  `kind-kind` app. Accessibility snapshots verified one reachable `127.0.0.1:19092` target,
  no-match/recovery, lazy `freelens-orders` detail, metrics `3/1/0/0`, partitions 0–2 Healthy,
  Space activation and zero error-level console messages.
- **MCP visual finding:** desktop/compact screenshots exposed a clipped `Partition` header. Column
  sizing was corrected and promoted into deterministic anti-clipping assertions before verification.
- **Safety/cleanup:** only local Docker and `kind-kind` fixtures were mutated; no real cluster was
  contacted. All Kafka containers, namespaces, ports, MCP runtimes and watchdogs were removed.

## Decision Log

- **2026-07-22:** User accepted P3.1(c) as the next roadmap increment.
- **2026-07-22:** Keep the overview list lightweight and lazy-load one topic topology at a time.
- **2026-07-22:** Verified after MCP, protocol, packaged-app and committed integration evidence;
  MCP-discovered header clipping is covered by regression assertions.