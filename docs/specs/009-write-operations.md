# SPEC-009 — Write Operations: Policy, Produce Message and Offset Reset

| Field | Value |
| --- | --- |
| Status | Verified |
| Date | 2026-08-06 |
| Source | v1.0.0 scope; write policy accepted by user 2026-08-06 |
| Safety | **First write-capable spec.** Write policy defined here is normative for all subsequent write-capable SPEC (010, 011, 012). Governed by `TESTING-SAFETY.md`. |

## Problem

SPEC-001–008 are read-only. Users who need to send test messages or recover a stalled consumer group
must leave the extension and use command-line tooling with no guardrails. Both operations need a
write permission model that makes accidental production writes structurally impossible.

## Write Policy (normative for SPEC-009 and all subsequent write-capable specs)

The following rules apply to every write feature introduced in this extension, regardless of which
spec introduces it.

**Default state:** All write features are disabled by default for every cluster.

**Enabling write mode:** The user must explicitly enable write mode per cluster in Extension
Settings. Write mode is stored per `targetId`; enabling it for one cluster has no effect on others.
No credential, password or Secret is stored as part of write-mode configuration.

**Mandatory confirmation:** Every write operation must present a non-dismissible confirmation
dialog before the call is made. The dialog must show: cluster identity (non-sensitive bootstrap
reference or display name), resource (topic, group, subject…), operation name and expected
consequence. There is no "remember this decision" option.

**Reinforced confirmation for destructive operations:** Any operation that is irreversible or whose
consequence is difficult to quantify — including offset reset to earliest, schema deletion, ACL
deletion, connector deletion, and any operation that modifies or removes data that cannot be
reconstructed from the current state — must additionally require the user to type the exact resource
name (or a provided confirmation phrase) before the submit button becomes active.

**Locked context:** The destination cluster and resource are locked at compose time. If the active
Kafka cluster or page context changes before the user confirms, the pending operation is cancelled
without execution and the user is notified.

**No automatic retry:** After an ambiguous result (e.g. a produce call that timed out with unknown
delivery status), the extension reports the ambiguity with the known facts and stops. It does not
retry automatically.

**Development and automated testing:** Write operations in tests are permitted only against local
Docker or KinD Kafka instances created by the test harness for that test run. The test setup must
assert that the bootstrap address resolves to a local address (127.0.0.1 or ::1) before executing
any write. Any test that cannot verify a local bootstrap must abort rather than skip.

## Scope

- Write policy implementation as a shared infrastructure layer for all write-capable specs.
- **Produce Message** — compose key, value, headers and optional partition; confirm and send once.
- **Consumer Group offset reset** — reset a group's committed offset for a topic/partition to
  earliest, latest, a specific offset or a timestamp.
- **Topic deletion** (v1.1.0, issue #24) — delete one topic from its workspace after the
  reinforced confirmation.

## Non-goals

- Batch produce (multiple messages in one action).
- Transactional produce.
- Creating or deleting topics, groups or consumer instances (future spec).
- Modifying topic or broker configuration (future spec).
- Writing on clusters that do not have write mode explicitly enabled.

## User Scenarios

### US-032 — Write mode is invisible until explicitly enabled (P1)

**Given** a cluster with write mode disabled (default), **when** the user browses Topics, Consumer
Groups or any other page, **then** no produce button, reset control or other write element is
visible.

### US-033 — Enable write mode for one cluster only (P1)

**Given** two configured Kafka clusters, **when** the user enables write mode for cluster A in
Extension Settings, **then** cluster A shows write controls and cluster B does not.

### US-034 — Produce a message to a test topic (P1)

**Given** write mode enabled for a cluster, **when** the user opens a Topic Workspace and clicks
Produce, **then** a compose view opens with the cluster and topic locked; the user fills in key and
value, reviews the confirmation dialog and confirms; the resulting partition and offset are shown.

### US-035 — Cancelling produce leaves the topic unchanged (P1)

**Given** a compose view with a message ready to send, **when** the user dismisses the confirmation
dialog, **then** no message is produced and the topic is unchanged.

### US-036 — Reset a consumer group offset to earliest (P1)

**Given** write mode enabled for a cluster and a selected consumer group, **when** the user chooses
reset-to-earliest for a topic/partition, **then** a reinforced confirmation (type resource name) is
shown; after confirmation the committed offset is updated.

### US-037 — Context change during compose cancels the operation (P1)

**Given** a compose view open for cluster A, **when** the user switches to cluster B before
confirming, **then** the compose view closes and no message is produced.

## Functional Requirements

### Write policy infrastructure

- **REQ-102** — Write features MUST be disabled by default. No write-capable UI element (button,
  menu item, form) MUST be visible or reachable unless write mode has been explicitly enabled for
  the current cluster.
- **REQ-103** — Write mode is stored per `targetId` in extension settings. Enabling it for one
  cluster MUST NOT affect the write-mode state of any other cluster.
- **REQ-104** — Every write operation MUST present a confirmation dialog, shown before the IPC call
  is made, displaying: cluster identity (non-sensitive), resource, operation and expected
  consequence.
- **REQ-105** — Destructive or hard-to-reverse operations MUST use a reinforced confirmation: the
  submit button remains disabled until the user types the exact resource name or the provided
  confirmation phrase into an input field within the dialog.
- **REQ-106** — The destination cluster and resource MUST be locked at compose time. If the active
  cluster or page context changes before confirmation, the pending operation MUST be cancelled
  without execution and the user MUST be notified.
- **REQ-107** — After an ambiguous result, the extension MUST surface the known facts (operation
  attempted, cluster, resource, last known outcome) and stop. It MUST NOT retry automatically.

### Produce Message

- **REQ-108** — A Produce action MUST be accessible from the Topic Workspace header when write mode
  is enabled for the cluster. It MUST NOT be visible when write mode is disabled.
- **REQ-109** — The Produce compose view MUST allow editing key, value and headers using native
  input controls. Partition selection MUST be optional; when unspecified, the producer uses the
  default partitioner.
- **REQ-110** — Produce MUST use an idempotent KafkaJS producer where supported by the broker
  (enable.idempotence = true). This does not guarantee exactly-once to the user; REQ-107 applies.
- **REQ-111** — After a confirmed produce, the extension MUST show the resulting partition and offset
  on success, or the full error message on failure, without truncation.

### Consumer Group offset reset

- **REQ-112** — Offset reset controls MUST be accessible from the Offsets & Lag tab when write mode
  is enabled. They MUST NOT be visible when write mode is disabled.
- **REQ-113** — Offset reset MUST support: reset-to-earliest, reset-to-latest, reset-to-specific-
  offset (numeric input) and reset-to-timestamp. Reset-to-timestamp uses `offsetsForTimes`.
- **REQ-114** — The offset reset confirmation MUST show group ID, topic, partition and the resolved
  new offset before execution. Reset-to-earliest and reset-to-timestamp before the earliest
  available offset MUST use the reinforced confirmation gate (REQ-105).
- **REQ-115** — Automated tests for all write features MUST assert that the bootstrap address is a
  local loopback address (127.0.0.1 or ::1) before executing any write. Tests MUST abort with a
  clear diagnostic if this assertion fails; they MUST NOT skip silently.

### Topic deletion (v1.1.0)

- **REQ-194** — A Delete topic action MUST be accessible from the Topic Workspace header when
  write mode is enabled for the cluster. It MUST NOT be visible when write mode is disabled.
- **REQ-195** — Topic deletion MUST use the reinforced confirmation gate (REQ-105): the submit
  button stays disabled until the exact topic name is typed and the confirmation is accepted. The
  confirmation MUST show the cluster, the topic, its partition count and state that every record
  is lost. The cluster and topic are locked at compose time (REQ-106).
- **REQ-196** — After a confirmed deletion the extension MUST return to the topic list and refresh
  the target's metadata and health so the topic disappears, and MUST report the outcome. On
  failure the full error message MUST be shown without truncation and nothing is retried.

### Batch topic deletion (v1.3.0)

- **REQ-203** — When write mode is enabled for the cluster, the Topics list MUST offer a selection
  checkbox per topic and a page-level checkbox that selects the application topics of the visible
  page (internal `__` topics are never selected as a page and can only be ticked one by one). A
  "Delete N topics" action with the current count MUST appear next to the filter, disabled while
  nothing is selected. None of these controls is visible when write mode is disabled. The selection
  belongs to one cluster: it survives filter and page changes, is cleared on a cluster switch or a
  write-mode opt-out, and drops the names that a refresh no longer returns.
- **REQ-204** — Batch deletion MUST use the reinforced confirmation gate (REQ-105) with the number
  of selected topics as the confirmation phrase: the submit stays disabled until the exact count is
  typed and the switch is accepted. The confirmation MUST show the cluster and every locked topic
  name and state that every record is lost. The cluster and the names are locked at compose time
  (REQ-106); the checkboxes are frozen while the confirmation is open, and a cluster change, an
  opened topic or a write-mode opt-out cancels the pending batch with a notice.
- **REQ-205** — The main process MUST delete the locked topics through one Admin session with one
  `DeleteTopics` request per topic, so that a broker error on one name does not hide the outcome
  of the others, and MUST return the deleted names and every failure with its full error message.
  The renderer MUST report the outcome ("Deleted N topics", or the partial count), list every
  failure without truncation, keep the failed topics selected and refresh the list; nothing is
  retried. The call is guarded by REQ-197 like every other write.

### Main-process guard (v1.1.1)

- **REQ-197** — The main process MUST refuse every write IPC call (produce, topic deletion, offset
  reset, Schema Registry and Kafka Connect writes, ACL create and delete) for a Kafka target whose
  write mode has not been enabled in the current session. The renderer mirrors the per-target
  switch to main on activation and on every change; the guard is defence in depth against a
  renderer bug, not a substitute for the renderer policy.

## Success Criteria

- **SC-058** — With write mode disabled (default), no Produce button and no offset-reset control is
  visible on any page.
- **SC-059** — Enabling write mode for cluster A shows write controls for cluster A only; cluster B
  shows none.
- **SC-060** — Produce to a local Docker Kafka topic (bootstrap 127.0.0.1) creates a message
  retrievable via the message browser; the confirmation dialog was shown and the post-confirmation
  view shows partition and offset.
- **SC-061** — Dismissing the confirmation dialog before produce leaves the topic message count
  unchanged.
- **SC-062** — Switching Kafka cluster while the compose view is open cancels the produce without
  sending and shows a cancellation notice.
- **SC-063** — Offset reset to earliest (local Docker Kafka) requires typing the resource name;
  after confirmation the committed offset for the selected group+topic+partition is at the earliest
  available offset.
- **SC-064** — The test setup for all write tests asserts `bootstrap.includes("127.0.0.1") ||
  bootstrap.includes("::1")`; a test pointed at a non-local bootstrap fails at setup, not at the
  write call.
- **SC-112** — With write mode enabled, deleting a disposable local topic requires typing its exact
  name: a wrong name or the accepted switch alone leaves the submit disabled. After confirmation
  the topic is absent from the list and from the broker.
- **SC-113** — With write mode disabled, no Delete topic control is visible in the Topic Workspace.
- **SC-114** — A write IPC call for a target that was never enabled in the session is rejected by
  main with an explicit error, even if a renderer sent it.
- **SC-117** — With write mode enabled, ticking three disposable local topics in the list shows
  "Delete 3 topics"; the confirmation lists the three names and stays disabled with the switch
  alone or a wrong count; after typing `3` and confirming, the three rows and the broker topics
  are gone and the status reads "Deleted 3 topics".
- **SC-118** — With write mode disabled, the Topics list shows no selection column and no batch
  action.

## Decision Log

- **2026-08-06** — Spec accepted as part of v1.0.0 scope definition.
- **2026-08-06** — Write policy normalised here as the single authoritative source; SPEC-010–012
  reference this spec rather than repeating the rules.
- **2026-08-06** — Idempotent producer preferred but delivery guarantee is explicitly documented as
  at-least-once; no exactly-once claim is made in the UI.
- **2026-08-06** — Reset-to-timestamp uses the same `offsetsForTimes` admin call introduced in
  SPEC-008; no new API surface.
- **2026-09-10** — Topic deletion (REQ-194–REQ-196) added after the v1.0.0 release on user
  request (#24) under the same write policy; it uses the KafkaJS Admin `deleteTopics` call and the
  typed-name gate because the records cannot be reconstructed.
- **2026-09-10** — v1.1.1 adds the main-process guard (REQ-197): before it, the only gate was the
  renderer setting, so a renderer defect could have reached a broker with a write.
- **2026-09-11** — Batch deletion from the list (REQ-203–REQ-205) added on user request (#56).
  The typed count replaces the typed name because retyping every name defeats the purpose of a
  batch; the locked list of names in the confirmation keeps the operation explicit. One Admin
  request per topic was chosen over a single multi-topic `DeleteTopics` call because KafkaJS
  surfaces only the first per-topic error of a batch response.

## Verification Evidence

| Requirement range | Evidence |
| --- | --- |
| REQ-102–REQ-105, REQ-108–REQ-109, REQ-112, REQ-115 | Packaged Electron Playwright E2E: write mode is default-off, controls appear only after target opt-in, Produce confirmation and typed Reset confirmation are enforced, and the test asserts the loopback bootstrap. |
| REQ-103 | `src/renderer/kafka-write-settings.test.ts`: per-target isolation, persistence, and cross-instance synchronization. |
| REQ-110–REQ-111 | `src/main/kafka/kafka-connection.ts` plus packaged E2E: idempotent KafkaJS producer returns and renders partition/offset. |
| REQ-113–REQ-114 | `src/main/kafka/kafka-connection.ts` plus packaged E2E: earliest reset resolves and commits the selected partition after reinforced confirmation. |
| REQ-197, SC-114 | `src/main/write-mode.test.ts`: unknown, disabled and missing targets are refused, enabled ones pass; the packaged write E2E scenarios run through the renderer mirror. |
| REQ-194–REQ-196, SC-112 | `src/main/kafka/kafka-connection.test.ts` (`deleteTopic`) plus packaged E2E: a disposable loopback topic is deleted only after the typed name and the switch, then it is absent from the list and the broker. |
| REQ-203–REQ-205, SC-117 | `src/renderer/kafka-topic-selection.test.ts` (page selection without internal topics, pruning, outcome wording), `src/main/kafka/kafka-connection.test.ts` (`deleteTopics`: one session, one request per topic, per-topic failures) plus packaged E2E: three disposable loopback topics ticked in the list, gate enforced on the switch and on a wrong count, then absent from the list and the broker. |
| SC-058–SC-064 | Packaged Electron Playwright suite: **7/7 tests passed** on 2026-08-18 using only `127.0.0.1:19092` and the local KinD fixture; typecheck, 122 unit tests and Prettier also passed. |
