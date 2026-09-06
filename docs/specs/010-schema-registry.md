# SPEC-010 — Schema Registry Integration

| Field | Value |
| --- | --- |
| Status | Verified |
| Date | 2026-08-06 |
| Source | v1.0.0 parity gap analysis; accepted by user 2026-08-06 |
| Safety | Reads are non-mutating. Write operations (register, delete) follow the write policy defined in SPEC-009. Governed by `TESTING-SAFETY.md`. |

## Problem

Messages serialised with Avro or Protobuf appear as unreadable binary in the message browser when no
schema context is available. Users must switch to external tooling to inspect schema definitions and
to decode message payloads.

## Scope

- **Endpoint configuration** per cluster: Schema Registry URL, optional TLS, optional basic-auth
  username (password not stored).
- **Subjects page** — list all subjects with latest version number and schema type.
- **Subject detail** — all versions of a subject, schema content per version, compatibility level.
- **Transparent deserialization** — when a topic message carries the Confluent wire format magic
  byte (0x00), the message browser decodes the payload using the registered schema and renders the
  decoded fields instead of raw bytes.
- **Write operations** (write mode required, SPEC-009 policy): register a new schema version,
  delete a schema version, delete a subject.

## Non-goals

- Schema editing inside the extension UI (users compose schemas in external tools and register the
  result).
- Automatic schema inference from unregistered messages.
- KsqlDB or stream-processing schema management.
- Schema migration or compatibility evolution tooling.

## User Scenarios

### US-038 — Configure a Schema Registry endpoint (P1)

**Given** a Kafka cluster with an associated Schema Registry, **when** the user adds the endpoint in
Extension Settings, **then** a Schema Registry sidebar entry appears and lists subjects.

### US-039 — Browse subjects and their schemas (P1)

**Given** a configured Schema Registry, **when** the user opens the Subjects page, **then** all
subjects are listed with their latest version and type (AVRO, PROTOBUF, JSON).

### US-040 — Inspect all versions of a subject (P1)

**Given** a subject in the list, **when** the user selects it, **then** all versions are shown with
their full schema content and compatibility level.

### US-041 — Decoded Avro message in the browser (P1)

**Given** a configured Schema Registry and a topic whose messages use the Confluent wire format,
**when** the user browses messages, **then** the decoded field/value tree is shown instead of raw
bytes; a fallback to raw display with a warning is shown if decoding fails.

### US-042 — Register a new schema version (P2, write mode required)

**Given** write mode enabled and an existing subject, **when** the user pastes a new schema
definition and confirms, **then** the new version appears in the subject detail.

## Functional Requirements

- **REQ-116** — The extension MUST support optional Schema Registry endpoint configuration per
  cluster. The configuration is stored in extension settings keyed by `targetId`. Only the URL and
  an optional TLS flag and basic-auth username are stored; passwords and tokens MUST NOT be stored,
  logged or sent to IPC payloads.
- **REQ-117** — A **Schema Registry** sidebar entry MUST appear under the Apache Kafka parent only
  when a Registry endpoint is configured for the selected cluster. It MUST NOT appear as a permanent
  placeholder.
- **REQ-118** — The Subjects page MUST list all subjects with latest version number, schema type
  (AVRO, PROTOBUF, JSON) and compatibility level. It MUST support text search by subject name.
- **REQ-119** — Selecting a subject MUST open a detail view showing all versions, the schema content
  of each version (formatted JSON or IDL) and the subject-level compatibility setting.
- **REQ-120** — When Schema Registry is configured and a topic message begins with magic byte 0x00
  (Confluent wire format), the message browser MUST deserialise the payload using the schema
  identified by the embedded schema ID and display the decoded field tree.
- **REQ-121** — Deserialisation failures MUST fall back to raw byte display and show an inline
  non-fatal warning. They MUST NOT crash the message browser, hide the message row or emit an
  unhandled rejection.
- **REQ-122** — Schema registration (new version) and schema/subject deletion MUST require write
  mode enabled for the cluster and follow the write policy defined in SPEC-009.
- **REQ-123** — Deleting a subject or a version that other subjects reference MUST use the
  reinforced type-to-confirm gate (SPEC-009 REQ-105).
- **REQ-124** — All Schema Registry IPC operations MUST use the bounded operation pattern: 30-second
  timeout, cancellation path and `disconnect` (or HTTP abort) in `finally`.
- **REQ-125** — Schema Registry configuration survives cluster reconnects and Freelens restarts
  without requiring re-entry.

## Success Criteria

- **SC-065** — Configuring a local Schema Registry endpoint shows the sidebar entry and lists at
  least one subject from a fixture.
- **SC-066** — An Avro-serialised message in the browser is decoded and its fields displayed when a
  matching subject exists; the raw bytes are not shown for that message.
- **SC-067** — A Protobuf-serialised message is decoded analogously to SC-066.
- **SC-068** — A message with an unknown magic byte or an unregistered schema ID shows raw bytes and
  an inline warning without error.
- **SC-069** — Registering a new schema version (write mode, confirmation) shows the new version in
  the subject detail.
- **SC-070** — Removing the Schema Registry endpoint from settings causes the sidebar entry to
  disappear immediately.

## Decision Log

- **2026-08-06** — Spec accepted as part of v1.0.0 scope definition.
- **2026-08-06** — Write operations (register, delete) governed by the write policy in SPEC-009;
  not repeated here.
- **2026-08-06** — Password not stored by design; users must re-enter the password after a Freelens
  restart if basic auth is required. This is consistent with the extension's no-credential-storage
  principle.
- **2026-08-06** — Confluent wire format (magic byte 0x00 + 4-byte schema ID) is the only
  auto-detection path. Custom serialisation without this header is not auto-detected; raw display is
  the fallback.

## Verification Evidence

| Requirement range | Evidence |
| --- | --- |
| REQ-116–REQ-119, REQ-125 | Per-target settings, conditional Schema Registry navigation, Subjects search/detail and persistence are covered by unit tests and the packaged Electron E2E fixture. |
| REQ-120–REQ-121 | Avro and Protobuf Confluent payloads decode in the packaged message-browser E2E; unknown schema ID remains raw with a non-fatal warning. |
| REQ-122–REQ-123 | Register and Delete Subject are write-mode gated; registration confirmation and reinforced typed deletion are exercised against the local HTTP Registry fixture in packaged E2E. |
| REQ-124 | Schema Registry client uses bounded timeout/AbortController and Main IPC cleanup paths; timeout and HTTP adapter tests pass. |
| SC-065–SC-070 | Packaged Freelens Electron Playwright suite: **8/8 tests passed** on 2026-08-18 using only local Docker Kafka, local HTTP Registry and local KinD discovery. Typecheck, build, Prettier and **133 unit tests** also passed. |
