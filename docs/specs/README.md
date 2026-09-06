# Spec-Driven Development

This directory is the product contract for the Freelens Kafka extension. Architecture explains the
system, feature docs explain delivered behavior, and specifications define **testable outcomes before
implementation**.

## Lifecycle

1. **Discover** — capture the user problem, evidence and constraints.
2. **Specify** — assign a `SPEC-NNN` document and globally unique `REQ-NNN` requirements; define
   scenarios, non-goals, assumptions and measurable success criteria.
3. **Plan** — record the implementation slices and the requirement-to-test strategy.
4. **Implement** — keep code and tests linked to requirement IDs where the link is not obvious.
5. **Verify** — attach executable evidence, including real Freelens E2E evidence for runtime contracts.
6. **Evolve** — update the specification and its decision log when user feedback changes behavior;
   never silently change an accepted requirement.

Specification states: `Draft` → `Accepted` → `Implementing` → `Verified` → `Superseded`.

## Quality bar

- Requirements describe observable **what/why**, not implementation details.
- Every requirement is independently testable and uses one globally unique `REQ-NNN` ID.
- User scenarios use Given/When/Then and include loading, empty, error and constrained-network states.
- Success criteria are measurable and user-facing.
- Security and cluster-safety constraints are explicit; `TESTING-SAFETY.md` remains authoritative.
- A specification is `Verified` only when its traceability table names passing tests/evidence.
- New requirement IDs continue from the highest allocated ID; IDs are never reused.

## Index

| Spec | Status | Requirements | Scope |
| --- | --- | --- | --- |
| [SPEC-001](./001-loading-usage-navigation.md) | Verified | REQ-001–REQ-011 | Meaningful progress, Kubernetes usage semantics and row navigation |
| [SPEC-002](./002-playwright-mcp-assisted-verification.md) | Verified | REQ-012–REQ-022 | Safe Playwright MCP assisted-verification layer |
| [SPEC-003](./003-topic-list-detail.md) | Verified | REQ-023–REQ-032 | Read-only topic list, partition topology and replica health |
| [SPEC-004](./004-resource-navigation-cluster-context.md) | Verified | REQ-033–REQ-054 | Resource navigation, Kafka cluster context, page migration and stabilization |
| [SPEC-005](./005-read-only-message-browser.md) | Verified | REQ-055–REQ-073 | Explicit bounded Browse/Tail and byte-safe record inspection |
| [SPEC-006](./006-consumer-groups-lag.md) | Verified | REQ-074–REQ-083 | Consumer group list, Offsets & Lag and Members workspace |
| [SPEC-007](./007-topic-broker-depth.md) | Verified | REQ-084–REQ-093 | Topic Configuration tab, Topic↔Consumer Group cross-links, Broker config |
| [SPEC-008](./008-message-browser-enhancements.md) | Verified | REQ-094–REQ-101 | Message browser filtering (key/value/header) and seek-by-timestamp |
| [SPEC-009](./009-write-operations.md) | Verified | REQ-102–REQ-115 | Write policy, Produce Message and Consumer Group offset reset |
| [SPEC-010](./010-schema-registry.md) | Verified | REQ-116–REQ-125 | Schema Registry integration: subjects, versions, Avro/Protobuf deserialization |
| [SPEC-011](./011-kafka-connect.md) | Verified | REQ-126–REQ-135 | Kafka Connect: connector list, detail, lifecycle and management |
| [SPEC-012](./012-acl-security-views.md) | Verified | REQ-136–REQ-143 | ACL list, filtering and write management |
| [SPEC-013](./013-ux-performance-v1-gate.md) | Verified | REQ-144–REQ-151 | UX/performance cross-cutting quality and v1.0.0 release gate |
| [SPEC-014](./014-production-scale-performance.md) | Verified | REQ-152–REQ-191 | Production-scale performance, lossless aggregate-health batching, backend/API/browser gates and explicit final approval |

The next available requirement ID is `REQ-192`. SPEC-007–014 allocate REQ-084–REQ-191 and define
the complete v1.0.0 scope. Future specs for v2.0.0 and beyond start from REQ-192.
