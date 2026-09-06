# SPEC-012 — ACL and Security Views

| Field | Value |
| --- | --- |
| Status | Verified |
| Date | 2026-08-06 |
| Source | v1.0.0 parity gap analysis; accepted by user 2026-08-06 |
| Safety | DescribeAcls is read-only. CreateAcls and DeleteAcls follow the write policy defined in SPEC-009. Governed by `TESTING-SAFETY.md`. |

## Problem

Cluster ACL rules are invisible inside the extension. Operators auditing permissions or diagnosing
authorisation failures must use external command-line tooling with no consistent view of which
principals can perform which operations on which resources.

## Scope

- **ACL list** — all ACL rules with resource type, resource name, pattern type, principal, host,
  operation and permission type (ALLOW/DENY).
- **Filtering** — client-side filter by resource type, principal substring and operation.
- **Write operations** (write mode required, SPEC-009 policy): create ACL rule, delete ACL rule.
- **Graceful degradation** — brokers that do not support or authorise DescribeAcls show an
  explanatory state instead of an error crash.

## Non-goals

- Principal or user management beyond ACL scope.
- Quota views.
- Role-based access models that are not represented as standard Kafka ACLs.
- Wildcard ACL expansion or effective-permission simulation.

## User Scenarios

### US-048 — Audit ACL rules without leaving the extension (P1)

**Given** a cluster with ACLs configured, **when** the user opens the ACL page, **then** all rules
are listed with resource, principal, operation and permission.

### US-049 — Filter ACL rules by resource type (P1)

**Given** an ACL list with mixed resource types, **when** the user selects TOPIC as the resource
type filter, **then** only topic-scoped ACL entries are shown.

### US-050 — Create an ACL rule (P2, write mode required)

**Given** write mode enabled, **when** the user fills in the rule form and confirms, **then** the
new rule appears in the ACL list.

### US-051 — Delete an ACL rule (P2, write mode required)

**Given** write mode enabled, **when** the user selects a rule, initiates deletion and types the
resource name to confirm, **then** the rule is removed from the list.

## Functional Requirements

- **REQ-136** — The extension MUST expose ACL inspection as a cluster-scoped resource page
  accessible from the Kafka sidebar. The page MUST appear only after a probe confirms the broker
  supports and authorises DescribeAcls for the current principal; it MUST NOT appear as a permanent
  placeholder.
- **REQ-137** — The ACL list MUST show resource type, resource name, pattern type, principal, host,
  operation and permission (ALLOW/DENY) for every entry returned by `admin.describeAcls`.
- **REQ-138** — The ACL list MUST support client-side filtering by resource type (dropdown),
  principal substring and operation (dropdown); results MUST update without issuing a new API call.
- **REQ-139** — When a broker returns `CLUSTER_AUTHORIZATION_FAILED` or `UNSUPPORTED_VERSION` for
  the DescribeAcls call, the ACL page MUST show an explanatory state (e.g. "ACL inspection is not
  available for this cluster") without an error crash or an unhandled rejection.
- **REQ-140** — ACL rule creation MUST require write mode enabled and show the full rule preview
  (all fields) in the standard confirmation dialog (SPEC-009 REQ-104) before the CreateAcls call.
- **REQ-141** — ACL rule deletion MUST require write mode enabled and use the reinforced
  type-to-confirm gate (SPEC-009 REQ-105).
- **REQ-142** — All ACL IPC operations MUST use the bounded operation pattern: 30-second timeout,
  cancellation path and `disconnect` in `finally`. They MUST log only non-sensitive resource
  identifiers; principal credentials and secret values MUST NOT appear in logs or IPC payloads.
- **REQ-143** — The ACL list MUST refresh after any write operation (create, delete) without
  requiring a manual page reload.

## Success Criteria

- **SC-077** — Opening the ACL page on a local fixture cluster with ACLs configured shows the full
  list with correct resource type, principal and operation columns.
- **SC-078** — Applying the TOPIC resource type filter hides all non-topic entries without issuing a
  new API call.
- **SC-079** — On a broker configured without ACL support, the ACL page shows an explanatory state;
  no error banner and no console exception.
- **SC-080** — Creating an ACL rule (write mode, confirmation) shows the new entry in the list after
  the refresh.
- **SC-081** — Deleting an ACL rule (type-to-confirm) removes it from the list after the refresh.

## Decision Log

- **2026-08-06** — Spec accepted as part of v1.0.0 scope definition.
- **2026-08-06** — Write operations governed by the write policy in SPEC-009; not repeated here.
- **2026-08-06** — Graceful degradation for unsupported or unauthorised DescribeAcls is a first-
  class requirement because many managed Kafka clusters restrict this API.
- **2026-08-19** — Kafka reports ACL fields as numeric enums, so the adapter now maps names to and
  from `AclResourceTypes`, `ResourcePatternTypes`, `AclOperationTypes` and `AclPermissionTypes`
  instead of passing the DTO strings through. Unmappable names are rejected before any connection is
  opened, so a malformed rule can never reach the broker.
- **2026-08-19** — REQ-139 also covers `SECURITY_DISABLED`, which is what a broker without an
  authorizer actually returns. KafkaJS carries the code in the error `type`, not in its prose
  message, so degradation is detected from both.
- **2026-08-19** — REQ-136 is implemented as a DescribeAcls probe carried by the existing overview
  connection: no extra round-trip, and the menu is gated on the probe result per Kafka target.
- **2026-08-19** — ACL IPC calls now carry the active Kubernetes cluster id, so ACL inspection works
  for cluster-discovered brokers and not only for manual endpoints.

## Traceability

| Requirement | Implementation | Evidence |
| --- | --- | --- |
| REQ-136 | `src/renderer/kafka-acl-availability.ts`, `src/renderer/index.tsx`, `probeAclSupport` | Packaged E2E asserts the ACL menu is absent on the unauthorized broker and appears after the authorized one is opened |
| REQ-137 | `src/main/kafka/acl.ts`, `src/renderer/kafka-acl-pages.tsx` | `acl.test.ts` numeric-enum mapping; packaged E2E asserts resource, principal, host, operation and permission |
| REQ-138 | `src/renderer/kafka-acl-pages.tsx` | Packaged E2E filters by resource type and principal with no refetch |
| REQ-139 | `unavailableMessage` in `src/main/kafka/acl.ts` | `acl.test.ts` covers the three codes and a real KafkaJS protocol error; packaged E2E asserts the degraded state |
| REQ-140 | ACL rule form and `kafka-acl-rule-preview` | Packaged E2E asserts the full rule preview and the disabled-until-confirmed create button |
| REQ-141 | Type-to-confirm delete via `canSubmitWriteAction` | Packaged E2E types the resource name before deletion is enabled |
| REQ-142 | `withTimeout(..., 30_000, ...)` in `src/main/ipc.ts` | Bounded ACL read, create and delete handlers with `disconnect` in `finally` |
| REQ-143 | `runWrite` reloads before reporting status | Packaged E2E sees the created rule appear and the deleted rule disappear without a manual reload |

Evidence (2026-08-19): 147 unit tests, `type:check` and `build`/`smoke:main` pass. The packaged
Electron suite passes 11/11, including the ACL lifecycle against the disposable authorized broker on
`127.0.0.1:19095`. All ACL writes targeted that loopback fixture; no external cluster was contacted.
