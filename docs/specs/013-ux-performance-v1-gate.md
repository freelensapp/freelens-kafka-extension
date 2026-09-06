# SPEC-013 — UX, Performance and v1.0.0 Quality Gate

| Field | Value |
| --- | --- |
| Status | Verified |
| Date | 2026-08-06 |
| Source | v1.0.0 scope; accepted by user 2026-08-06 |
| Safety | No write operations in this spec; governed by `TESTING-SAFETY.md` |

## Problem

Individual feature increments (SPEC-001–012) are each verified in isolation but cross-cutting
quality — consistent UX patterns, performance under concurrent load, keyboard accessibility and a
defined release gate — is not addressed by any single spec. SPEC-013 closes that gap and defines the
conditions under which the extension may be released as v1.0.0.

## Scope

- **Overview health dashboard** — unavailable partition count, under-replicated partition count,
  online broker count, aggregate consumer group lag.
- **UX consistency audit** — loading, empty and error states across all pages introduced in
  SPEC-007–012 must follow a uniform visual pattern; any divergence found during the audit is
  remediated in this increment.
- **Auto-refresh** — opt-in configurable refresh interval for cluster-scoped data, with a
  last-refreshed timestamp on every live-data page.
- **Keyboard navigation audit** — every interactive element on every new page is reachable and
  operable by keyboard alone.
- **IPC deduplication** — concurrent identical IPC requests from the same Renderer context are
  coalesced into a single in-flight call.
- **Constants extraction** — all hardcoded timeout and limit values in `src/` are moved to a shared
  constants module and surfaced in extension settings with documented defaults.
- **v1.0.0 release gate** — defined checklist that must be met before the version is tagged.

## Non-goals

- Dark/light theme customisation beyond CSS variables already in use.
- Custom fonts or brand theming.
- Feature flags or A/B testing infrastructure.
- Performance profiling against production-scale clusters (that requires a dedicated benchmark spec).

## User Scenarios

### US-052 — See cluster health at a glance (P1)

**Given** a selected Kafka cluster, **when** the user opens Overview, **then** a health summary
shows unavailable partitions, under-replicated partitions, broker availability and aggregate
consumer group lag, updating on each manual refresh.

### US-053 — Enable auto-refresh for a dashboard (P2)

**Given** the Overview page, **when** the user enables auto-refresh and sets a 30-second interval,
**then** the health summary updates automatically; disabling auto-refresh stops the updates.

### US-054 — Navigate all new pages by keyboard only (P1)

**Given** any page introduced in SPEC-007–012, **when** the user navigates using only Tab, Enter,
Space and Escape, **then** every action (open detail, trigger fetch, confirm write, close dialog) is
reachable and operable without a mouse.

### US-055 — Fresh install produces no console errors (P1)

**Given** the packaged v1.0.0 extension installed into Freelens, **when** the extension loads and
the user browses idle (no cluster selected), **then** no console errors or unhandled rejections are
produced.

## Functional Requirements

### Overview health dashboard

- **REQ-144** — The cluster Overview page MUST display a health summary showing: unavailable
  partition count, under-replicated partition count, online broker count and (when Consumer Groups
  is enabled) aggregate consumer group lag across all groups for the selected cluster.
- **REQ-145** — The health summary MUST include a last-refreshed timestamp and a manual refresh
  action. Auto-refresh MUST be opt-in, configurable to a minimum interval of 10 seconds, and stored
  in extension settings per cluster.

### UX consistency

- **REQ-146** — Loading, empty and error states across all pages introduced in SPEC-007–012 MUST
  follow the visual pattern established in SPEC-001–006. An audit pass MUST identify every
  divergence; all divergences found MUST be resolved before this spec is marked Verified.

### Keyboard accessibility

- **REQ-147** — Every interactive element introduced in SPEC-007–012 MUST be operable by keyboard.
  Tab/Enter/Space/Escape MUST behave consistently with the patterns established in SPEC-001–006.

### IPC deduplication

- **REQ-148** — When the same IPC channel is invoked concurrently with identical parameters from the
  same Renderer context (e.g. two components mounting simultaneously both requesting the same topic
  config), the Renderer client MUST coalesce them into a single in-flight call; the second caller
  awaits the result of the first rather than issuing a duplicate Admin connection.

### Constants and configuration

- **REQ-149** — All timeout values, page sizes and retry limits hardcoded in `src/` MUST be
  extracted to a shared `src/common/constants.ts` module. Each constant MUST have a documented
  default. User-facing constants MUST be exposable via extension settings.

### v1.0.0 release gate

- **REQ-150** — v1.0.0 MUST NOT be tagged until all of the following are true:
  - SPEC-001–014 are all in Verified state.
  - `type:check`, `lint:check`, `knip:check`, `test:unit`, `build` all pass on the release commit.
  - The packaged integration test suite (Tier 2/3) passes on the release commit.
  - A changelog entry exists for every user-visible change since the previous release.
  - The fresh-install smoke test (REQ-151) passes.
- **REQ-151** — The packaged extension installed into Freelens (current stable) MUST produce zero
  console errors and zero unhandled promise rejections when the extension is loaded but idle (no
  Kubernetes cluster connected, no Kafka cluster selected).

## Success Criteria

- **SC-082** — Overview health summary shows correct unavailable and under-replicated partition
  counts against a local fixture with a deliberately degraded partition (e.g. a topic with
  replication factor 3 and only 1 in-sync replica).
- **SC-083** — Auto-refresh at 30-second interval updates the Overview health summary without user
  interaction; disabling it stops the updates.
- **SC-084** — Audit report records zero pages with inconsistent loading/empty/error states after
  the remediation pass.
- **SC-085** — Keyboard-only navigation reaches every action on every page introduced in
  SPEC-007–012 without mouse interaction, verified via Playwright keyboard-navigation tests.
- **SC-086** — Two concurrent identical IPC calls from the same page result in exactly one Admin
  connection, verified by a spy in unit tests.
- **SC-087** — Fresh-install smoke test on the release build produces zero console errors and zero
  unhandled rejections for the idle extension.

## Decision Log

- **2026-08-06** — Spec accepted as part of v1.0.0 scope definition.
- **2026-08-06** — IPC deduplication added as a non-functional requirement to prevent unnecessary
  Admin connections when multiple components mount simultaneously on the same page.
- **2026-08-06** — v1.0.0 release gate formalised here so the condition is machine-readable and
  referenced by ARCHITECTURE.md.
- **2026-08-19** — Automated gates passed: 32 unit-test files (153 tests), type-check, Biome,
  Prettier, Knip, renderer build and main-process smoke test. Packaged extension was manually
  checked in Freelens; Overview health, refresh, keyboard navigation and cross-page UX passed.
  The auto-refresh interval select was corrected after manual verification identified a click
  propagation issue, then the package was rebuilt and rechecked successfully.
- **2026-08-20** — The user added SPEC-014 to v1.0.0 after production-scale timing exposed a
  separate performance architecture gap. REQ-150 now requires SPEC-014 Verified before tagging.
