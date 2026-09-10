# SPEC-015 - Topic Size on Disk

| Field | Value |
| --- | --- |
| Status | Implemented |
| Date | 2026-09-10 |
| Source | Issue #30, requested by a user after v1.0.0 |
| Safety | Read-only (DescribeLogDirs); governed by `TESTING-SAFETY.md` |

## Problem

Operators judge topics by their footprint: which ones are big, which are empty, where the disk
goes. Other Kafka consoles show a size column; the extension showed none because KafkaJS has no
Admin call for `DescribeLogDirs`.

## Scope

- Size per topic in the Topics list, sortable, plus the sum of the listed topics in the summary.
- Size in the Topic Workspace overview and per partition in the Partitions tab.
- One `DescribeLogDirs` request per broker with bounded concurrency, sent through the same
  read-only raw transport as the batched group offsets (SPEC-014).

## Non-goals

- Per log directory or per broker disk views.
- Historical growth, retention forecasts, or write operations of any kind.

## Functional Requirements

- **REQ-198** — The extension MUST read partition sizes with `DescribeLogDirs` (API key 35,
  version 1), one request per broker, and MUST NOT use any other API or shell tooling for it.
- **REQ-199** — The size shown for a topic and for a partition is the leader replica size; the
  sum of every replica MUST be available on hover. A topic whose partition leaders were not all
  reported MUST be marked as a lower bound (`≥`) instead of showing a wrong exact value.
- **REQ-200** — A broker that fails or refuses the request MUST NOT fail the page: it is listed
  as unavailable and the affected topics become lower bounds; a cluster without the API shows
  `n/a`.
- **REQ-201** — The fetch MUST be bounded by the admin timeout, MUST start only after the
  metadata snapshot is available, and MUST NOT block the topic list rendering.

## Success Criteria

- **SC-115** — On the packaged E2E fixture the Topics list shows a byte value for
  `freelens-orders`, the summary strip shows a Size metric, and the Partitions tab shows a byte
  value per partition.
- **SC-116** — Unit tests cover the wire encoding and decoding, the aggregation (leader and
  replica sums, lower bounds, unavailable brokers) and the unsupported cluster path.

## Decision Log

- **2026-09-10** — Leader size chosen as the headline number because it matches what other
  consoles call the topic size; the replica sum is the disk footprint and stays on hover.
- **2026-09-10** — Version 1 of the API (non-flexible encoding) is enough for every supported
  broker and keeps the hand-written codec small.

## Verification Evidence

| Requirement range | Evidence |
| --- | --- |
| REQ-198, SC-116 | `src/main/kafka/log-dirs-protocol.test.ts` (encoding, decoding, read-only transport gate) |
| REQ-199–REQ-200, SC-116 | `src/main/kafka/log-dirs.test.ts` (aggregation, lower bounds, unavailable and unsupported brokers) |
| REQ-201, SC-115 | Packaged Electron E2E: Topics list, summary strip and Partitions tab show byte values on the loopback fixture |
