# SPEC-006 — Consumer Groups and Lag

| Field | Value |
| --- | --- |
| Status | Implementing |
| Date | 2026-08-06 |
| Source | User-approved next increment after verified SPEC-005; accepted UX v3 roadmap §UX3.5 |
| Safety | Kafka reads only — no group join, no offset commit; governed by `TESTING-SAFETY.md` |

## Problem

Operators using Freelens can inspect topics, partitions and messages but cannot see consumer-group
state or lag. Understanding which groups are active, how many members they have and how far behind
each partition is requires leaving the application and running command-line tooling with no
guarantee about write safety.

## Scope

- A `Consumer Groups` sidebar entry under the Apache Kafka parent, registered only when functional.
- A searchable cluster-level Consumer Groups list showing group state and member count.
- A Group Workspace with **Offsets & Lag** and **Members** tabs for one selected group.
- Read-only use of `admin.listGroups`, `admin.describeGroups`, `admin.fetchOffsets` and
  `admin.fetchTopicOffsets` only — no group join, no consumer creation, no offset commit or reset.

## Non-goals

- Resetting or committing consumer-group offsets.
- Creating, deleting or modifying consumer groups.
- Joining a consumer group or creating a consumer.
- A Topics tab inside the Group Workspace (future cross-link increment).
- ACL inspection or altering group configurations.

## User Scenarios

### US-020 — Browse consumer groups without side-effects (P1)

**Given** a selected Kafka cluster, **when** the user opens Consumer Groups, **then** Freelens shows
all groups with their current state and member count without joining any group.

### US-021 — Inspect lag for a selected group (P1)

**Given** a selected consumer group, **when** the user opens the Offsets & Lag tab, **then**
Freelens shows committed offset, high-watermark and computed lag per topic/partition.

### US-022 — Inspect active members of a group (P1)

**Given** a selected consumer group with members, **when** the user opens the Members tab, **then**
Freelens shows member ID, client ID and client host for each active member.

## Functional Requirements

- **REQ-074** — Consumer Groups MUST appear as a Kafka sidebar resource registered only when the
  page is functional; it MUST NOT appear as an empty placeholder.
- **REQ-075** — The Consumer Groups list MUST show group ID, state and member count. It MUST include
  all groups returned by `admin.listGroups()` (consumer and non-consumer protocol types).
- **REQ-076** — The Consumer Groups list MUST support text search by group ID and share the Kafka
  cluster selector with all other cluster-scoped resource pages.
- **REQ-077** — Selecting a group MUST open a Group Workspace on the same `kafka-groups` route with
  `group` and `view` URL parameters; the sidebar MUST remain active on `kafka-groups`.
- **REQ-078** — The Offsets & Lag tab MUST show committed offset, high-watermark and lag per
  topic/partition. Lag MUST be computed as `max(0, highWatermark − committedOffset)` using BigInt
  arithmetic; partitions with no committed offset (`−1`) MUST display `—` for lag.
- **REQ-079** — The Members tab MUST show member ID, client ID and client host for each active
  member. An empty state MUST be shown when the group has no active members.
- **REQ-080** — Main MUST NOT join, create, modify or commit to any consumer group in any code path.
  `admin.describeGroups`, `admin.fetchOffsets` and `admin.fetchTopicOffsets` are the only
  group-related Admin API calls permitted.
- **REQ-081** — Every consumer group IPC operation MUST have a 30-second bounded timeout, a
  cancellation path and a disconnect exactly once in `finally`.
- **REQ-082** — Switching Kafka cluster from the Group Workspace MUST return to the Consumer Groups
  list for the new cluster and clear the selected group (SPEC-004 cluster-switch behavior).
- **REQ-083** — Empty, loading, error and no-group-selected states MUST be visually distinct.

## Success Criteria

- **SC-044** — Opening Consumer Groups performs no record fetch or group-join and renders a list.
- **SC-045** — A local fixture group with committed offsets shows correct lag per partition in
  Offsets & Lag; partitions with no committed offset show `—`.
- **SC-046** — `admin.listGroups` then `admin.describeGroups` protocol evidence reports `groups≥1`
  and an unchanged committed-offset count after the IPC call returns.
- **SC-047** — Switching Kafka cluster from Group Workspace clears `group` URL param and returns
  to the list.

## Assumptions and Decisions

- `fetchOffsets({ groupId })` with no `topics` argument returns all committed topic-partitions for
  the group; topics with zero committed partitions do not appear.
- `fetchTopicOffsets(topic)` returns `{ partition, offset, high, low }` per partition; `high` is
  the high-watermark used for lag computation.
- Member assignment parsing (binary `memberAssignment` Buffer) is deferred; the Members tab shows
  only the three plaintext fields (`memberId`, `clientId`, `clientHost`).
- Groups with `protocolType !== "consumer"` are shown in the list but their offset/lag data may be
  empty (they have no consumer-protocol offsets).

## Implementation Slices

1. IPC contract, KafkaJS group adapter and Main handlers.
2. Consumer Groups list page with cluster selector and search.
3. Group Workspace: Offsets & Lag tab.
4. Group Workspace: Members tab.
5. Polish, tests, protocol evidence and packaged regression.

## Implementation Progress

### Slices 1–4 — Consumer Groups list + Workspace — Implementing 2026-08-06

## Verification Plan

- **Tier 0:** unit tests for group lag computation and navigation helpers.
- **Protocol:** `pnpm itest:groups` against disposable Docker Kafka; proves no group join, no offset
  change after calls.
- **Tier 2:** packaged Freelens E2E covering list load, workspace open, Offsets & Lag and Members.

## Decision Log

- **2026-08-06:** User accepted SPEC-006 as the next planned increment after SPEC-005 verification.
- **2026-08-06:** Member assignment decoding deferred (binary KafkaJS protocol); plaintext fields only.
- **2026-08-06:** Topics cross-link deferred to a later increment to keep Slice 1 focused.
