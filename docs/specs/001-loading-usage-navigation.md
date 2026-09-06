# SPEC-001 — Loading, Kubernetes Usage and Cluster Navigation

| Field | Value |
| --- | --- |
| Status | Verified |
| Date | 2026-07-22 |
| Source | User feedback from manual testing on an explicitly authorized read-only environment |
| Safety | Read-only behavior; governed by [`TESTING-SAFETY.md`](../../TESTING-SAFETY.md) |

## Problem

On Kubernetes clusters with many workloads, Kafka discovery and metadata loading can take long
enough to look stalled. The current spinner does not explain the work in progress. The `Namespace`
column is ambiguous for workload-discovered Kafka because it contains the first encountered usage
namespace, not an owning namespace. Finally, users expect an available table row to open its details,
not only the trailing arrow.

## Scope

- Meaningful, graphical, phase-based progress for discovery and cluster metadata loading.
- Clear Kubernetes resource-location versus workload-usage semantics.
- Whole-row pointer and keyboard navigation for inspectable Kafka targets.
- Progress/error behavior that remains truthful under slow APIs, VPNs and large EKS clusters.

## Non-goals

- Predicting an exact completion time.
- Topic list/detail, messages or consumer-group UI.
- Relay-pod connectivity.
- Any write to Kafka or Kubernetes.

## User Scenarios

### US-001 — Understand slow discovery (P1)

**Given** a connected Kubernetes cluster with many workloads, **when** the user opens Kafka,
**then** a graphical progress indicator appears promptly and names the current real phase, including
workload progress when a total is known, until the list or an error is shown.

### US-002 — Understand where Kafka is used (P1)

**Given** a Kafka bootstrap referenced by workloads in multiple namespaces, **when** the list is
shown, **then** the usage cell reports workload and namespace counts rather than presenting an
arbitrary first namespace as ownership. For Strimzi and Service sources, it reports the resource
namespace.

### US-003 — Open details naturally (P1)

**Given** a reachable Kafka row, **when** the user clicks anywhere on the row or focuses it and
presses Enter/Space, **then** its details open. An unavailable row remains disabled and explains why.

### US-004 — Understand slow metadata reads (P1)

**Given** a reachable Kafka cluster, **when** the user opens details, **then** the Drawer shows a
graphical progression through credential resolution, connection, broker metadata and topic metadata.
If a phase fails, the last phase and error remain visible.

## Functional Requirements

- **REQ-001** — Discovery progress MUST become visible within 500 ms of starting the operation.
- **REQ-002** — Discovery progress MUST be monotonic, graphical and identify the current real phase
  and why it is needed.
- **REQ-003** — During workload scanning, progress MUST show completed/total workloads when the total
  is known and MUST emit visible updates during long scans.
- **REQ-004** — Discovery completion MUST transition to list, empty or error state without leaving a
  stale progress indicator.
- **REQ-005** — A workload-discovered Kafka MUST display aggregate workload and unique namespace
  counts; it MUST NOT label the first encountered usage namespace as the Kafka namespace.
- **REQ-006** — Strimzi and Service candidates MUST display their Kubernetes resource namespace, and
  manual endpoints MUST display that they are manual rather than Kubernetes-owned.
- **REQ-007** — Every inspectable row MUST open from a click anywhere on the row and from Enter/Space;
  the trailing action icon MAY remain as an additional affordance.
- **REQ-008** — A non-inspectable row MUST remain non-actionable and expose the reason in accessible
  text or a tooltip.
- **REQ-009** — Detail progress MUST report credential/security resolution, connection setup, broker
  metadata and topic metadata as distinct real phases.
- **REQ-010** — Detail progress failure MUST preserve the failing phase and present the error without
  exposing credentials.
- **REQ-011** — Progress instrumentation and performance improvements MUST remain read-only and MUST
  not broaden Kubernetes Secret reads beyond containers relevant to Kafka discovery/connection.

## Success Criteria

- **SC-001** — In E2E, the first progress UI is visible within 500 ms and reaches 100% or an explicit
  error state.
- **SC-002** — Recorded percentages never decrease within one operation.
- **SC-003** — A fixture used by workloads in two namespaces renders both counts correctly.
- **SC-004** — Clicking the cluster-name cell and pressing Enter on a row each open the Drawer.
- **SC-005** — E2E observes all four detail phases before metadata is displayed.
- **SC-006** — Existing read-only MSK discovery and metadata behavior remains unchanged.

## Assumptions and Decisions

- Percentages are weighted phase completion, not time estimates.
- `referencedBy` is the source of truth for workload/namespace aggregation.
- Discovery sources may execute concurrently; the aggregate percentage remains monotonic.
- Progress events are correlated by operation ID so multiple Freelens frames do not cross-talk.

## Implementation Slices

1. Shared progress contract and Main→Renderer event stream.
2. Instrument discovery and bounded workload scanning.
3. Instrument connection and metadata phases.
4. Native progress UI, usage semantics and row keyboard behavior.
5. Unit, committed integration and real Freelens 1.10.3 E2E evidence.

## Traceability

| Requirement | Delivered code | Verification evidence |
| --- | --- | --- |
| REQ-001–REQ-004 | `KafkaProgressEvent`, monotonic reporter, discovery phases, native `LineProgress` | `progress.test.ts`; packaged Freelens E2E observes first event <500 ms, monotonic values and `complete=100` |
| REQ-003 | Bounded-concurrency workload scan with completed/total callbacks | `discovery.test.ts`; EKS read-only evidence shows `2/232` through `231/232` and 100% |
| REQ-005–REQ-006 | `kafkaUsageSummary` / `KafkaUsageCell` | `kafka-view-model.test.ts`; E2E renders `2 workloads / 2 namespaces` |
| REQ-007 | Click/keyboard row interaction with native Drawer propagation fix | E2E opens from `.nameCell`, closes, then reopens with Enter |
| REQ-008 | Disabled-row `aria-disabled`, label/title reason | Type/lint gates + renderer contract inspection |
| REQ-009–REQ-010 | Security/connection/broker/topic progress callbacks and retained failing phase | E2E observes `security`, `connection`, `connect`, `brokers`, `topics`, `complete`; UI error uses last correlated event |
| REQ-011 | Selective env resolver + Promise cache + concurrency limit 6 | unrelated-Secret regression in `external-credentials.test.ts`; authorized read-only EKS run |

## Verification Evidence

- **Unit:** 10 test files / 46 tests pass.
- **Static/build:** type, lint, knip (known config-loader warning, exit 0), build and `smoke:main` pass.
- **Packaged app:** Freelens 1.10.3 Playwright E2E passes in 16.9 s, including progress, usage,
  whole-row click, Enter, auth, manual endpoint and responsive regression.
- **Visual:** `/tmp/freelens-kafka-discovery-progress.png` and
  `/tmp/freelens-kafka-detail-progress.png` inspected at desktop viewport.
- **Authorized real target (read-only):** an approved environment scanned 232 workloads and discovered
  five Kafka targets in 21,244 ms; progress emitted 3% → 100%, with four VPN-reachable managed targets
  returning 3 brokers and 906–982 topics. No writes were performed.

## Decision Log

- **2026-07-22:** User feedback accepted; use real phase events instead of simulated timers.
- **2026-07-22:** Replace ambiguous `Namespace` with `Kubernetes usage` semantics.
- **2026-07-22:** Verified; native Drawer row clicks must stop propagation before mount or its
  global outside-click handler immediately closes the newly-opened Drawer.
