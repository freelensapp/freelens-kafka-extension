# SPEC-004 — Resource Navigation and Kafka Cluster Context

| Field | Value |
| --- | --- |
| Status | Verified |
| Date | 2026-07-27 |
| Verified | 2026-07-31 |
| Source | User-approved [`Kafka UX v3`](../design/kafka-ux-v3-proposal.md) |
| Safety | Kafka/Kubernetes reads only; governed by [`TESTING-SAFETY.md`](../../TESTING-SAFETY.md) |

## Problem

The delivered UI concentrates Kafka discovery, brokers and topics in one cluster page and a primary
Drawer. This makes a selected Kafka cluster difficult to carry across resource workflows, limits URL
navigation and does not scale cleanly to Consumer Groups, Schema Registry or Kafka Connect. The
accepted v3 design replaces the Drawer-first model with stable Freelens sidebar resource pages and
entity-specific workspaces.

## Scope

- Nested Apache Kafka resource navigation in the Freelens cluster sidebar.
- A stable, non-secret selected Kafka cluster context per active Kubernetes cluster.
- Typed page parameters with reload and browser-history behavior.
- Dedicated Clusters, Overview, Topics and Brokers pages.
- URL-backed Topic Workspace state owned by the Topics route, with entity tabs and list-return behavior.
- Migration of the verified topic list and partition detail out of the primary Drawer.
- Connection Settings retained as a secondary panel.
- Desktop and compact responsive behavior with no inert production controls.

## Non-goals

- Reading or tailing Kafka messages.
- Listing Consumer Groups, members, offsets or lag.
- Producing messages or any other Kafka write.
- Creating relay pods or mutating Kubernetes resources.
- Schema Registry, Kafka Connect, ACL or broker-configuration workflows.
- A Broker Workspace beyond the read-only broker inventory.
- Displaying mockup-only navigation controls or future placeholders in production.

## User Scenarios

### US-011 — Navigate Kafka resource areas (P1)

**Given** an open Freelens Kubernetes cluster, **when** the user expands Apache Kafka and selects an
implemented child entry, **then** the corresponding full resource page opens, remains highlighted
and exposes no inert or mockup-only controls.

### US-012 — Select and switch Kafka clusters (P1)

**Given** several Kafka clusters discovered from one Kubernetes cluster, **when** the user explicitly
selects one and moves between Overview, Topics and Brokers, **then** the same non-secret Kafka cluster
context is retained in page state and can be changed from each cluster-scoped page.

### US-013 — Understand cluster and broker metadata (P1)

**Given** a selected reachable Kafka cluster, **when** Overview or Brokers opens, **then** the user
sees truthful metadata, progress and errors without continuous polling or unverified per-broker
health claims.

### US-014 — Move between Topics and one Topic Workspace (P1)

**Given** a filtered Topics list, **when** the user opens one Topic and later returns, **then** its
partition metadata appears in a full workspace and harmless list state is preserved through
navigation and history.

### US-015 — Recover from stale, missing or constrained context (P1)

**Given** an unavailable Kafka cluster, a stale URL parameter or a cluster switch during an active
request, **when** the page resolves its context, **then** it presents a safe selection, loading,
empty or error state and never renders stale data from the previous cluster.

## Functional Requirements

- **REQ-033** — The extension MUST register one Apache Kafka sidebar parent using the existing
  Material `hub` icon and child entries for each implemented cluster-scoped resource page.
- **REQ-034** — Every visible sidebar entry and page control MUST perform its labeled action;
  mockup controls, future resources and unimplemented Consumer Group or write workflows MUST NOT be
  exposed as interactive production UI.
- **REQ-035** — The first delivered child pages MUST be Clusters, Overview, Topics and Brokers;
  entity workspaces MUST remain non-sidebar states whose owning resource entry stays highlighted.
- **REQ-036** — Clusters, Overview, Topics, Brokers and Topic Workspace MUST use registered
  `PageParam`s so navigation, reload and browser back/forward restore valid non-secret page state.
- **REQ-037** — User-facing text MUST use **Kafka cluster**; the internal stable `targetId` MUST
  distinguish connections within the active Kubernetes cluster without containing credentials,
  Secret data or mutable row indexes.
- **REQ-038** — Overview, Topics, Brokers and Topic Workspace MUST expose a Kafka cluster selector;
  Clusters MUST instead show all available Kafka cluster connections.
- **REQ-039** — Switching Kafka cluster on a resource-list page MUST keep the same resource page,
  clear entity-specific state and retain only harmless applicable filters or sorting.
- **REQ-040** — Switching Kafka cluster from Topic Workspace MUST return to Topics for the new
  cluster rather than assume that a topic with the same name exists there.
- **REQ-041** — Requests MUST be correlated to the selected `targetId`; responses or progress from a
  previous cluster or entity MUST be cancelled when possible or ignored before rendering.
- **REQ-042** — With one available Kafka cluster the UI MAY select it automatically; with multiple
  clusters and no valid remembered selection it MUST ask for an explicit choice rather than silently
  select the first discovery row.
- **REQ-043** — Clusters MUST preserve delivered discovery, filtering, reachability, provider,
  security, Kubernetes-usage, manual-endpoint and connection-settings behavior, and selecting an
  inspectable row MUST open Overview for that cluster.
- **REQ-044** — Overview MUST show only metadata supported by completed reads, including available
  broker/controller/topic/partition, replication and connection summaries; unavailable future
  metrics MUST be omitted or identified as unavailable, not fabricated.
- **REQ-045** — Overview MUST load only on entry, cluster change, retry or explicit refresh; it MUST
  expose correlated loading, partial and error states and MUST NOT continuously poll by default.
- **REQ-046** — Brokers MUST show broker node ID, controller role, advertised host and advertised
  port from Kafka metadata and MUST NOT infer individual broker health or reachability from cluster
  membership alone.
- **REQ-047** — Topics and Topic Workspace MUST preserve every verified `REQ-023`–`REQ-032`
  behavior while moving list, search and partition topology out of the primary cluster Drawer.
- **REQ-048** — Once page parity is verified, the primary resource Drawer MUST be removed; only
  secondary workflows such as Connection Settings MAY remain in a Drawer or panel.
- **REQ-049** — All new navigation and metadata behavior MUST remain Kafka/Kubernetes read-only,
  keyboard accessible and free of page-level horizontal overflow at packaged desktop and compact
  viewports; credentials and Secret values MUST remain absent from URLs, logs and Renderer DTOs.
- **REQ-050** — Cluster-level resource navigation MUST appear only in the Apache Kafka sidebar;
  Freelens sibling tabs MUST NOT duplicate Clusters, Overview, Topics or Brokers above the page.
  Page tabs MUST be reserved for implemented views of one selected entity.
- **REQ-051** — Shared metric strips MUST retain a stable horizontal desktop layout, adapt
  deliberately at compact widths, include spacing between values and labels and omit placeholder or
  unsupported metrics. Windows and Linux packaged-app evidence MUST cover computed layout and text.
- **REQ-052** — Discovery, reachability and cluster metadata results MUST be shared across Kafka
  resource pages per Kubernetes cluster and `targetId`, with in-flight request deduplication and a
  bounded freshness policy. Warm navigation MUST render cached non-secret DTOs without repeating the
  Kubernetes workload scan or Kafka metadata connection.
- **REQ-053** — Shared cache entries MUST contain no credentials or Secret values and MUST be
  invalidated by explicit Refresh, Kubernetes-cluster change, Kafka-cluster change where applicable,
  manual-endpoint mutation or Connection Settings mutation. The UI MUST expose refresh age/state and
  MUST preserve correlated stale-response guards.
- **REQ-054** — Resource tables MUST apply identical column geometry to headers and every data row;
  sortable labels and icons MUST remain readable, and variable-length names MUST truncate only
  inside their assigned column without moving sibling columns. This contract applies explicitly to
  Topic/Type/action columns and Partition/Leader/Replicas/ISR/State columns.

## Success Criteria

- **SC-019** — Packaged Freelens exposes only implemented Kafka child entries; each opens the named
  page and applies the correct active sidebar state.
- **SC-020** — Direct navigation, reload and back/forward restore selected cluster, resource page and
  Topic Workspace state without secrets in the URL.
- **SC-021** — Switching clusters on Overview, Topics and Brokers keeps the active resource page and
  updates all rendered provider, connection and metadata values without stale results.
- **SC-022** — Switching clusters in Topic Workspace returns to Topics for the new cluster and clears
  the selected topic.
- **SC-023** — A multi-cluster fixture with no remembered selection shows an explicit selection
  state; a one-cluster fixture may open it automatically.
- **SC-024** — Selecting a Clusters row opens Overview; Overview and Brokers render controller and
  broker metadata from the selected fixture and make no unsupported broker-health claim.
- **SC-025** — Existing topic search, lazy topic metadata, partition health and pointer/keyboard
  behavior pass unchanged on the page-based UI.
- **SC-026** — Desktop and compact checks report no page/workspace overflow, clipped controls or
  overlapping text; implemented navigation is fully keyboard operable.
- **SC-027** — Unit/static, Docker/KinD, focused packaged-app and committed integration gates pass,
  and any MCP visual finding is either covered by a deterministic regression or explicitly recorded.
- **SC-028** — Packaged Overview, Topics and Brokers show no top-level Clusters/Overview/Topics/Brokers
  tab strip while their nested sidebar entries remain visible, navigable and correctly active.
- **SC-029** — Metric strips render all supported metrics as equal non-overlapping columns at
  desktop and as a deliberate two-column grid at compact width; value and label text remain
  distinguishable and no placeholder metric is shown.
- **SC-030** — After one cold discovery/metadata load, navigating Overview → Topics → Brokers for the
  same target performs no additional workload discovery or Kafka overview request while cache data
  is fresh; explicit Refresh performs exactly one new cycle.
- **SC-031** — Read-only timing evidence on local fixtures and an explicitly authorized context
  records cold and warm durations, request counts and cache age without
  logging secrets or contacting any unapproved context.
- **SC-032** — At desktop and compact packaged viewports, Topics header/rows and Partitions
  header/rows have matching left positions and widths for every column within one CSS pixel. Topic,
  Partition, Leader, Replicas and ISR headers remain readable, the Topic sort icon is not clipped and
  neither table introduces horizontal overflow.

## Assumptions and Decisions

- Freelens remains the owner of Kubernetes-cluster selection; the extension selector chooses one
  Kafka cluster connection within that frame.
- `LensRendererExtension.navigate(pageId, params)` and registered `PageParam`s remain the only router.
- Stable IDs are derived from non-secret discovery identity, not display order or credential values.
- Consumer Groups becomes a sidebar entry only when its own accepted specification is implemented.
- Schema Registry, Kafka Connect and Produce Message remain roadmap concepts and do not appear as
  active production entries during SPEC-004.
- The existing `overview` and `topic` IPC paths are reused or evolved rather than duplicated per page.
- The primary resource Drawer is retired. Connection Settings remains a secondary, session-scoped
  panel and resource data is available only through its owning pages/workspace.

## Implementation Slices

1. Stable Kafka cluster identity, selected-cluster registry and pure route/state helpers. **Done.**
2. Freelens page/menu registration, typed page params, shared cluster selector and page shell. **Done.**
3. Clusters page migration with discovery/progress/filter/manual endpoint parity. **Done.**
4. Truthful Overview and Brokers pages using existing metadata with correlated operation state. **Done.**
5. Topics and Topic Workspace migration with preserved list state and `REQ-023`–`REQ-032` parity. **Done.**
6. Stabilization from manual review:
  - **6A:** remove duplicate Freelens sibling tabs. **Done.**
  - **6B:** repair shared metric-strip layout, supported content and table-column alignment.
    **Done; Linux packaged assertions and Windows visual confirmation pass.**
  - **6C:** add shared discovery/metadata cache, in-flight deduplication and timing evidence.
    **Done; local packaged and authorized read-only timing evidence pass.**
7. Drawer retirement, responsive/accessibility polish and full regression evidence. **Done.**

## Implementation Progress

### Slice 1 — Stable Identity and Selection — Done 2026-07-27

- Canonical bootstrap normalization produces a stable, route-safe, non-secret `targetId` independent
  of broker order, hostname case, URI scheme and implicit port 9092.
- Discovery DTOs and manual endpoints carry `targetId`; persisted manual endpoints migrate and
  deduplicate by canonical identity.
- The delivered Drawer UI now uses `targetId` for React identity, selection and volatile security
  overrides.
- Explicit selections persist only the target ID, isolated by active Kubernetes cluster ID.
- Pure selection helpers implement requested → remembered → single-cluster precedence and require an
  explicit choice for ambiguous multi-cluster state.
- Pure navigation helpers define Clusters/Overview/Topics/Brokers/Topic IDs and ensure a cluster
  switch from Topic returns to Topics without stale entity parameters.

Evidence: focused identity/navigation tests pass 8/8; the full suite passes 13 files / 58 tests;
typecheck, lint, build and `smoke:main` pass. Knip exits 0 with the pre-existing
`electron.vite.config.js` loader warning. No page registration or visible UX v3 behavior is claimed
by this slice.

### Slices 2–3 — Navigation Foundation and Clusters Page — Done 2026-07-27

- The extension registers a nested **Apache Kafka → Clusters** sidebar hierarchy using Material
  `hub`; the parent expands and the child navigates to `kafka-clusters`.
- Only Clusters is exposed. Overview, Topics, Brokers, Consumer Groups and future domains remain
  absent until their pages are functional.
- Clusters is a full extension page using the shared Kafka page shell while preserving discovery,
  progress, reachability, filtering, manual endpoints, connection settings and temporary Drawer
  behavior.
- The cluster filter is backed by a registered `query` `PageParam`, updates the URL with replace
  history semantics and remains responsive through local state synchronized on initial load and
  browser history navigation.
- A reusable Kafka cluster selector is implemented for the upcoming cluster-scoped pages but is not
  rendered on Clusters, whose purpose is to show all connections.

Evidence: page/menu manifest tests and navigation tests pass 9/9; the full suite passes 14 files / 62
tests; typecheck, lint (60 files), build and `smoke:main` pass; Knip exits 0 with the known Vite config
loader warning. Packaged Freelens 1.10.3 E2E passes 1/1 in 16.465 s, covering parent/child navigation,
the `kafka-clusters` route, URL-backed filter/no-match/clear behavior, manual endpoint parity,
existing Drawer/Topic metadata and no horizontal page overflow. Disposable `kind-kind`/Docker
fixtures were removed after the run.

### Slice 4 — Overview and Brokers — Done 2026-07-27

- Overview and Brokers are registered as functional Apache Kafka sidebar children with a typed,
  non-secret `target` page parameter and shared Kafka cluster selector.
- Clusters row pointer/keyboard activation now navigates to Overview; the trailing action remains the
  temporary secondary Drawer entry for Connection Settings and pre-migration Topic access.
- The target-scoped loader merges discovered and persisted manual endpoints, resolves
  requested/remembered/single selection, correlates discovery and metadata progress and ignores
  stale unmounted responses.
- Overview reports only available broker count, controller, topic count, provider, security,
  reachability, strategy, bootstrap and Kubernetes usage. It does not fabricate aggregate health or
  partition totals.
- Brokers reports node ID, controller role, advertised host and port. It explicitly states that
  cluster membership is not an individual broker health probe.
- Shared manual endpoint loading recalculates canonical target IDs and keeps discovered clusters
  authoritative over equivalent persisted endpoints.

Evidence: the full suite passes 15 files / 65 tests; typecheck, lint (63 files), build and
`smoke:main` pass; Knip exits 0 with the known Vite config loader warning. Packaged Freelens 1.10.3
E2E passes 1/1 in 17.17 s, covering Clusters row → Overview, target URL state, truthful metrics and
connection details, Brokers route/table/controller/host, absence of inferred Healthy text and
desktop/760×700 no-overflow checks. All disposable Docker/KinD fixtures were removed.

### Slice 5 — Topics and Topic Workspace — Done 2026-07-27

- Topics is a functional Apache Kafka sidebar page with typed `target`, `query`, `topic` and `view`
  URL parameters, a Kafka cluster selector, searchable deterministic topic list and Internal labels.
- Topic Workspace is represented by `topic/view` state on the same `kafka-topics` route. Freelens
  1.10.3 marks sidebar items active by exact route, so this preserves the required active Topics
  entry while still supporting direct URL, reload, back and list-return semantics.
- Pointer and Enter/Space open one Topic lazily; only that topic invokes the existing read-only
  `kafka:topic` metadata operation with correlated progress and stale-operation guards.
- The workspace exposes only functional Overview and Partitions tabs. Messages, Consumers and
  Configuration remain absent until their own behavior is implemented.
- Overview shows partition/replication/under-replicated/unavailable metrics; Partitions reuses the
  verified leader/replicas/ISR/health table. Switching Kafka cluster clears topic/view/filter and
  returns to the new cluster's Topics list.
- Back to Topics preserves the prior filter, and desktop/compact layouts avoid horizontal overflow.

Evidence: the full suite passes 15 files / 66 tests; typecheck, lint (64 files), build and
`smoke:main` pass. Two consecutive clean builds pass with explicit ESM named-export shims for host
React/MobX modules, replacing the nondeterministic CommonJS external shim. Packaged Freelens 1.10.3
E2E passes 1/1 in 17.313 s, covering Topics navigation,
URL-backed no-match/recovery, keyboard opening, active-route workspace state, Overview/Partitions
tabs, three Healthy partitions, 760×700 overflow and filter-preserving back navigation. Disposable
Docker/KinD fixtures were removed after every run.

### Slice 6A — Remove Duplicate Resource Tabs — Done 2026-07-28

- Freelens 1.10.3 automatically wraps nested extension pages in `SiblingsInTabLayout`, duplicating
  Clusters, Overview, Topics and Brokers above the page even though the accepted v3 UX assigns that
  navigation level exclusively to the sidebar.
- Invisible, non-parented route-owner menu registrations now precede the visible nested entries.
  They suppress the automatic sibling wrapper while the visible Apache Kafka hierarchy remains
  navigable and correctly active.
- `mobx@6.16.1` is declared as a dev-only host API type/runtime dependency and remains externalized to
  `global.Mobx`; no additional runtime package is bundled.

Evidence: manifest tests pass 4/4; the full suite passes 15 files / 67 tests; typecheck, clean build
and `smoke:main` pass. Packaged Freelens 1.10.3 E2E passes 1/1 in 16.744 s and asserts zero
`[data-testid="tab-layout"]` wrappers on Overview, Brokers and Topics while all nested sidebar routes
and prior Clusters/Topic regressions remain green. Docker/KinD fixtures were removed after the run.

### Slice 6B — Metric and Table Geometry — Implemented 2026-07-28

- All cluster/topic summaries use one structured `KafkaMetricStrip` with explicit value and label
  elements. Overview, Topics and Brokers show three supported metrics; placeholders such as
  `Available / Metadata` and `Lazy / Partition metadata` were removed.
- Desktop uses equal-width metric columns. Compact width deliberately uses two columns with an odd
  final metric spanning the full row. Values and labels have independent block layout and spacing.
- Topics applies the same explicit flex basis, minimum width and width to every header/data cell for
  Topic, Type and action columns. Long names truncate inside Topic without moving sibling columns.
- Partitions applies one shared geometry contract to Partition, Leader, Replicas, ISR and State.
  Desktop preserves wider identity/status columns; the compact contract keeps all five columns
  readable in the page-based Topic Workspace without horizontal overflow.
- The disposable Kafka fixture includes a deterministic long Topic name, so geometry regressions do
  not depend on a real MSK cluster.

Evidence: typecheck, 15 files / 67 tests, Sass clean build and `smoke:main` pass. Packaged Linux
Freelens 1.10.3 E2E passes 1/1 for the combined Slice 6B regression and measures three-column desktop
metrics, two-column compact metrics, value/label separation, no placeholder metrics, Topic
header/row alignment and Partition header/row alignment within one CSS pixel. The final focused run
passes in 16.692 s at desktop and 760×700, including Partition header readability and table/page
no-overflow assertions. Windows visual confirmation passed on 2026-07-30. All disposable Docker/KinD
fixtures were removed.

### Slice 6C — Shared Resource Cache — Done 2026-07-30

- One renderer-owned `KafkaResourceCache` shares discovery, reachability and overview DTOs across
  Overview, Topics and Brokers. Entries are isolated by active Kubernetes cluster and stable
  non-secret `targetId`, expire after 60 seconds and deduplicate concurrent Promise loaders.
- Cache entries contain only existing non-secret Renderer DTOs, timestamps, generations and request
  counts. Credentials, Secret values and connection-setting passwords remain outside the cache.
- Generation guards prevent an invalidated in-flight response from repopulating an entry. A
  Kubernetes-cluster change clears the store; explicit Refresh invalidates discovery plus the
  selected target; target switches, manual endpoint mutations and Connection Settings changes
  invalidate their applicable target entries.
- Resource headers expose `Loading`, `Refreshing`, `Updated … ago` or `Cached … ago`. Deterministic
  attributes expose aggregate request counts to the packaged test without logging endpoints,
  credentials or response payloads.
- The opt-in `itest:cache:real` probe fails closed unless `ALLOW_REAL_READ_ONLY=1` is present and
  `KUBE_CONTEXT` exactly matches `AUTHORIZED_KUBE_CONTEXT`. It performs only Kubernetes reads, TCP connect
  probes without bytes and Kafka `describeCluster`/`listTopics`; it emits aggregate cardinalities,
  durations, cache age and counts only.

Evidence: cache unit tests pass 7/7; the full suite passes 16 files / 74 tests. Typecheck, lint,
Knip, clean build and `smoke:main` pass. Packaged Freelens 1.10.3 E2E passes 1/1 in 19.013 s and proves
Overview → Topics → Brokers keeps discovery/overview/reachability counts unchanged, explicit Refresh
increments each exactly once, cache status changes from `Cached` to `Updated`, and all prior
desktop/compact Topic/Partition regressions remain green. Local aggregate timing records discovery
cold at 1,219 ms, Topics warm at 68 ms and Brokers warm at 110 ms with a 1,373 ms cache age and warm
request delta 0/0/0. Authorized read-only evidence found five targets and four reachable endpoints: discovery cold
20,028 ms / warm 0 ms, reachability cold 5,139 ms / warm 0 ms and metadata cold 4,818 ms / warm 0 ms;
request counts were 1/1/1 with warm delta 0/0/0. No real-cluster writes were performed.

### Slice 7 — Primary Drawer Retirement — Done 2026-07-31

- Clicking or pressing Enter/Space on an inspectable Clusters row opens Overview. The trailing
  `tune` command opens only a secondary Connection Settings Drawer and does not change the selected
  Kafka cluster or route.
- The primary `KafkaClusterDetail` resource Drawer and all duplicate broker/topic/partition loaders,
  tabs, tables and styles were removed. Broker and Topic resources now exist only on their dedicated
  pages and Topic Workspace.
- `KafkaConnectionSettingsStore` keeps TLS/auth/username/password overrides only in renderer memory,
  isolated by Kubernetes cluster and non-secret `targetId`. Overview and Topic requests read the
  applicable override; passwords never enter URLs, localStorage, logs or cache DTOs.
- Settings opens without network activity, including for unreachable manual endpoints. `Apply and
  reconnect` invalidates the target cache and explicitly verifies the connection; manual endpoints
  remain removable from the panel even when Overview is unavailable.
- The panel is a semantic form, supports Enter submission, announces validation errors, marks
  required credential inputs and disables editable controls during reconnect. Desktop and 760×700
  assertions cover no overflow, keyboard navigation and absence of resource content in the Drawer.
- Topic/Partition styles moved from the legacy Drawer scope to `.KafkaOverviewPage`; sort-icon
  padding is limited to the sortable Partition header so compact labels remain readable.

Evidence: volatile settings tests pass 3/3; the full suite passes 17 files / 77 tests. Typecheck,
Biome/Prettier, Knip development/production, clean build and `smoke:main` pass. Packaged Freelens
1.10.3 E2E passes 1/1 in 16.358 s and asserts settings-only Drawer content, keyboard submit,
session-only URL safety, explicit reconnect, unreachable manual endpoint removal, Enter → Overview,
warm cache/Refresh counts and all desktop/compact Topic/Partition geometry. Disposable Docker/KinD
fixtures were removed after the run.

## Delivered Traceability

| Requirement | Delivered code surface | Passing evidence |
| --- | --- | --- |
| REQ-033–REQ-036 | Renderer extension page/menu registrations, page IDs and typed params | Route/menu unit checks; packaged sidebar, reload and history E2E |
| REQ-037–REQ-042 | Stable target identity, selected-cluster store and request guards | Pure state tests; multi-target packaged selector/switch regression |
| REQ-043 | Clusters page and extracted discovery/manual endpoint components | Existing view-model tests plus discovery/manual packaged regressions |
| REQ-044–REQ-046 | Overview/Brokers view models, DTO evolution and cluster metadata IPC | DTO/view-model units; disposable Kafka protocol and packaged page assertions |
| REQ-047–REQ-048 | Topics/Topic Workspace pages and secondary Connection Settings panel | Existing SPEC-003 unit/protocol/E2E suite migrated to routes; Drawer absence assertion |
| REQ-049 | Shared page states and responsive styles | Keyboard E2E, desktop/compact pixel and overflow assertions, safety/static review |
| REQ-050 | Route-owner menu registrations and nested sidebar manifest | Manifest unit test; packaged no-sibling-tabs/sidebar-active assertions |
| REQ-051 | Shared metric-strip component/styles | Windows/Linux computed-style, text-separation and compact-layout assertions |
| REQ-052–REQ-053 | Renderer resource cache/store and invalidation policy | Cache unit tests, IPC call-count E2E, local + authorized read-only timing evidence |
| REQ-054 | Shared Topic/Partition table-column contracts | Header/row geometry, anti-clipping and no-overflow packaged assertions on short/long topic names and three partitions |

## Verification

- **Tier 0:** focused route/state/view-model unit tests, typecheck, Biome/Prettier, Knip and build.
- **Protocol:** disposable Docker Kafka metadata and topic-detail regressions.
- **KinD:** discovery plus Strimzi port-forward behavior on `kind-kind` only.
- **Tier 1:** isolated Playwright MCP exploration for sidebar, history, cluster switching and
  desktop/compact visual findings.
- **Tier 2:** packaged Freelens 1.10.3 focused navigation and page-parity E2E.
- **Tier 3:** committed integration workflow regression.
- **Real clusters:** only explicitly authorized read-only discovery/metadata evidence; never required
  for acceptance and never used for autonomous writes.

## Decision Log

- **2026-07-24:** User accepted Kafka UX v3 as the implementation basis.
- **2026-07-24:** Use sidebar entries for resource domains and tabs only for one selected entity.
- **2026-07-24:** Rename the user-facing connection collection from Targets to Clusters while
  retaining `targetId` as internal terminology.
- **2026-07-24:** Keep Material `hub`; do not use the official Apache Kafka trademark as product icon.
- **2026-07-24:** Overview and Brokers are functional pages; no visible production entry may be inert.
- **2026-07-24:** Produce Message remains a separately approved, opt-in future write workflow.
- **2026-07-27:** Implementation started; Slice 1 delivered stable target identity, per-Kubernetes-
  cluster selection persistence and pure navigation/state helpers. Page registration remains next.
- **2026-07-27:** Slices 2–3 delivered the functional Clusters route, nested sidebar entry, shared
  page shell/selector and URL-backed filter. Keep unimplemented resource entries hidden; Overview and
  Brokers are next.
- **2026-07-27:** Slice 4 delivered truthful Overview and Brokers pages. Keep the trailing Drawer
  action only as a temporary secondary bridge; Topics and Topic Workspace migration is next.
- **2026-07-27:** Slice 5 delivered Topics and Topic Workspace. Use one `kafka-topics` route with
  entity PageParams so Freelens keeps Topics active; expose only Overview and Partitions until later
  features exist. Drawer retirement and Connection Settings extraction are next.
- **2026-07-27:** Replace `vite-plugin-external` with explicit ESM global shims after clean builds
  intermittently lost React named exports; require consecutive clean-build success for this wiring.
- **2026-07-28:** Manual Freelens testing found duplicate sibling tabs, collapsed metric strips and
  repeated cold discovery/metadata work on every resource page. Add Slices 6A–6C before Drawer
  retirement; user explicitly authorized read-only timing on one approved context.
- **2026-07-28:** Slice 6A removed automatic sibling tabs using invisible route owners after packaged
  verification proved sidebar navigation remained active. Metric-strip stabilization is next.
- **2026-07-28:** Manual Topics testing found header/data column drift caused by content-sensitive
  flex sizing. Include explicit shared column geometry in Slice 6B and packaged pixel assertions.
- **2026-07-28:** Slice 6B implemented structured metrics and shared table geometry; Linux packaged
  pixel assertions pass. A subsequent Partition-table report exposed compact rules scoped only to
  the legacy Drawer; apply the same explicit geometry to the page-based Topic Workspace and retain
  packaged desktop/compact assertions. Keep Windows confirmation open before shared caching 6C.
- **2026-07-30:** Windows visual confirmation closed Slice 6B. Slice 6C added a 60-second
  renderer-owned cache with in-flight deduplication, generation-safe invalidation, visible age/state,
  packaged request-count assertions and aggregate authorized read-only timing evidence.
- **2026-07-31:** Slice 7 removed the primary resource Drawer, retained a settings-only secondary
  panel with context-isolated volatile overrides and completed responsive, keyboard, safety and
  packaged parity evidence. SPEC-004 is Verified.
