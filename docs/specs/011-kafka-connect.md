# SPEC-011 — Kafka Connect Integration

| Field | Value |
| --- | --- |
| Status | Verified |
| Date | 2026-08-06 |
| Source | v1.0.0 parity gap analysis; accepted by user 2026-08-06 |
| Safety | Reads are non-mutating. Lifecycle operations (restart, pause, resume, create, delete) follow the write policy defined in SPEC-009. Governed by `TESTING-SAFETY.md`. |

## Problem

Teams running Kafka Connect alongside Kafka have no way to inspect connector status, task health or
error details inside the extension. They must switch to external tooling or the Connect REST API
directly to diagnose pipeline failures.

## Scope

- **Endpoint configuration** per cluster: Connect REST URL, optional TLS, optional basic-auth
  username (password not stored).
- **Connectors list** — all connectors with name, type (source/sink), status and task count.
- **Connector detail** — full configuration, task list with individual task status and error trace.
- **Lifecycle write operations** (write mode required, SPEC-009 policy): restart connector, restart
  individual task, pause connector, resume connector.
- **Management write operations** (write mode required): create connector (JSON config), update
  connector configuration, delete connector.

## Non-goals

- Kafka Streams topology view.
- Vendor-specific connector UX (Debezium lineage, MirrorMaker offsets, etc.).
- Plugin installation or JAR/ZIP management on the Connect cluster.
- Distributed mode worker management.

## User Scenarios

### US-043 — Inspect connector status without leaving the extension (P1)

**Given** a Kafka cluster with a Connect endpoint configured, **when** the user opens Kafka Connect,
**then** all connectors are listed with their status and task count.

### US-044 — Diagnose a failed task (P1)

**Given** a connector with one or more FAILED tasks, **when** the user opens the connector detail,
**then** each failed task's error trace is shown in full.

### US-045 — Restart a failed connector (P2, write mode required)

**Given** write mode enabled and a FAILED connector, **when** the user clicks Restart and confirms,
**then** the connector transitions to RUNNING and the task list updates.

### US-046 — Create a new connector from JSON config (P2, write mode required)

**Given** write mode enabled, **when** the user opens the create view, pastes a valid connector JSON
and confirms, **then** the new connector appears in the list.

### US-047 — Delete a connector (P2, write mode required)

**Given** write mode enabled, **when** the user initiates deletion and types the connector name to
confirm, **then** the connector is removed from the list.

## Functional Requirements

- **REQ-126** — The extension MUST support optional Kafka Connect REST endpoint configuration per
  cluster, stored in extension settings keyed by `targetId`. Only URL, TLS flag and optional
  basic-auth username are stored; passwords MUST NOT be stored or logged.
- **REQ-127** — A **Kafka Connect** sidebar entry MUST appear under the Apache Kafka parent only
  when a Connect endpoint is configured for the selected cluster.
- **REQ-128** — The Connectors list MUST show connector name, type (source/sink), status
  (RUNNING, PAUSED, FAILED, UNASSIGNED) and task count. It MUST support text search by connector
  name.
- **REQ-129** — Selecting a connector MUST open a detail view showing its full configuration
  key/value pairs, task list with individual task status, and the full error trace for any FAILED
  task without truncation.
- **REQ-130** — Restart (connector and individual task), pause and resume MUST require write mode
  enabled and show the standard confirmation dialog (SPEC-009 REQ-104).
- **REQ-131** — Connector deletion MUST require write mode enabled and use the reinforced
  type-to-confirm gate (SPEC-009 REQ-105).
- **REQ-132** — Connector creation and configuration update MUST require write mode enabled; the
  compose view MUST allow editing the full JSON configuration with syntax validation before the
  confirmation dialog is shown.
- **REQ-133** — All Connect IPC operations MUST use the bounded operation pattern: 30-second
  timeout, cancellation path and HTTP abort in `finally`. They MUST NOT cache basic-auth credentials
  between calls.
- **REQ-134** — Connect endpoint configuration MUST survive cluster reconnects and Freelens restarts
  without requiring re-entry.
- **REQ-135** — The Connectors list MUST reflect the outcome of any write operation (restart, pause,
  delete, create) with a refresh after confirmation, without requiring a manual page reload.

## Success Criteria

- **SC-071** — Configuring a local Connect endpoint shows the sidebar entry and lists connectors
  from a local fixture.
- **SC-072** — A RUNNING connector shows its task count; a FAILED connector shows FAILED status and
  the error trace in its detail view.
- **SC-073** — Pausing a RUNNING connector (write mode, confirmation) transitions it to PAUSED;
  the list updates without a manual reload.
- **SC-074** — Deleting a connector (type-to-confirm) removes it from the list.
- **SC-075** — Creating a connector from valid JSON (write mode, confirmation) shows the new
  connector in the list.
- **SC-076** — Removing the Connect endpoint from settings causes the sidebar entry to disappear
  immediately.

## Decision Log

- **2026-08-06** — Spec accepted as part of v1.0.0 scope definition.
- **2026-08-06** — Write operations governed by the write policy in SPEC-009; not repeated here.
- **2026-08-06** — Vendor-specific UX (Debezium, MirrorMaker) is explicitly a non-goal to keep the
  spec bounded; general connector config editing covers most use cases.
- **2026-09-10** — v1.1.1: the basic-auth password is entered in the connection settings and kept
  in renderer memory for the session only; before this the Connect pages never sent the username
  either (the persisted `username` was not mapped to the request), so basic auth never worked. The
  packaged E2E fixture now requires basic auth.


## Verification Evidence

| Requirement range | Evidence |
| --- | --- |
| REQ-126–REQ-129, REQ-133–REQ-134 | Packaged Electron Playwright E2E against the local Connect REST fixture: endpoint settings, conditional menu, connector list/filter, detail, task status and full FAILED trace. |
| REQ-130–REQ-132, REQ-135 | Packaged Electron Playwright E2E: write mode, pause, restart, JSON update, typed delete and JSON create; fixture state is refreshed through the UI without page reload. |
| SC-071–SC-076 | Focused Connect E2E passed with the complete flow; full packaged run executed the Connect test successfully. Typecheck, build, Prettier and 137 unit tests passed. |
