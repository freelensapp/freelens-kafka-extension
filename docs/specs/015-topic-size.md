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
  The request asks for every log directory of the broker (a null topic list, as `kafka-log-dirs`
  sends) and the reader keeps the partitions of the listed topics that the metadata knows.
- **REQ-199** — The size shown for a topic and for a partition is the leader replica size; the
  sum of every replica MUST be available on hover. A topic whose partition leaders were not all
  reported MUST be marked as a lower bound (`≥`) instead of showing a wrong exact value.
- **REQ-200** — A broker that fails, refuses or does not answer within 15 seconds MUST NOT fail
  the page: it is listed as unavailable with its reason and the affected topics become lower
  bounds. A broker that drops the connection mid-request is asked once more on a new connection,
  within the same 15 seconds. A broker whose log directories all answer an error has not
  reported. When no broker reports, the sizes are unknown: the pages show `n/a` with the reason
  of each broker, never `≥ 0 B`. A cluster without the API shows `n/a`.
- **REQ-201** — The fetch MUST be bounded by the admin timeout, MUST start only after the
  metadata snapshot is available, and MUST NOT block the topic list rendering. When the timeout
  fires, the read stops asking brokers. The read MUST end by itself three seconds before that
  timeout, which can only say that the sizes timed out: a step still waiting then (opening the
  read cluster, the metadata) fails with its name, and the brokers not asked yet are left out.
  Nothing is sent to a broker that was already left out.
- **REQ-221** — The read cluster that topic sizes, the message browser, the high watermarks and
  the batched group offsets share MUST NOT ask the brokers for metadata by topic name. It has no
  target topics, so KafkaJS asks for the metadata of all topics; a reader that needs a topic or a
  partition the metadata does not know yet reads it all again. The message browser and the high
  watermarks MUST NOT read it again because of its age: KafkaJS fails that read on one partition
  without a leader anywhere in the cluster, and the reader of a healthy topic must go on. The
  size read refreshes it every time the topic list is loaded.
- **REQ-222** — A size read that fails in a step (opening the read cluster, the metadata, the
  brokers) MUST tell the step and how long the attempt took. For a Kafka error it MUST tell the
  error with the broker and what was known of the connection before the request (already open or
  new and, with SASL, since when it was authenticated and the session lifetime the broker
  declared); for a step or a broker that did not answer in time, the time it was given. A
  metadata read that fails because the peer dropped the connection is done once more, and both
  attempts are reported. What a successful read had to work around (a second read, a broker left
  out with its reason) MUST be on the hover text of the topic size, in the Topics list and in the
  Topic Workspace. A failed read MUST show as `n/a` with its reason in the Topics list, in the
  Topic Workspace and in the Partitions tab. If the admin timeout of REQ-201 fires all the same,
  it is reported as it is, without these details.

## Success Criteria

- **SC-115** — On the packaged E2E fixture the Topics list shows a byte value for
  `freelens-orders`, the summary strip shows a Size metric, and the Partitions tab shows a byte
  value per partition.
- **SC-116** — Unit tests cover the wire encoding and decoding, the aggregation (leader and
  replica sums, lower bounds, unavailable brokers) and the unsupported cluster path.
- **SC-131** — `pnpm itest:read-cluster` on the loopback fixture. Through a pipe that ends the
  connection on every Metadata request naming a topic, the sizes, the message browser, the high
  watermarks and the health read of a seeded consumer group work. The failure names the step, the
  broker and the connection when the pipe ends the connection on `DescribeLogDirs` or on every
  Metadata request, a broker that never answers `DescribeLogDirs` is left out after 15 seconds,
  and a read whose Metadata request is never answered ends at its deadline with that step. After
  a topic is deleted, the sizes and the browser of another topic still work.

## Decision Log

- **2026-09-10** — Leader size chosen as the headline number because it matches what other
  consoles call the topic size; the replica sum is the disk footprint and stays on hover.
- **2026-09-10** — Version 1 of the API (non-flexible encoding) is enough for every supported
  broker and keeps the hand-written codec small.
- **2026-10-12** — The read cluster stopped naming topics (#82). On MSK Express brokers with IAM
  the Topics page showed `n/a` with "Closed connection": the broker ended the connection on the
  Metadata request that named every topic, on a new connection too, while the topic list, which
  asks for all topics without naming them, worked and `kafka-log-dirs`, which never names a
  topic, read the sizes. Why those brokers close on that request is not known. KafkaJS keeps
  every topic name a cluster was given and sends them all again at each metadata refresh, so the
  failed size read also left every name behind for the message browser on the same cluster.
  Behind a pipe that closes on such a request, v1.4.1 then fails to browse any topic with the
  same error when the sizes are the first read of the connection, and otherwise every topic
  outside the metadata the cluster already had, with "Partition 0 does not exist". The size
  reader therefore takes the leaders from the all-topics metadata and asks each broker for every
  log directory, and no reader gives the cluster a topic name any more.
- **2026-10-12** — A read cluster for the size reader alone was tried and dropped. On the shared
  cluster the size read of every Topics list load is also what refreshes the partition leaders
  for the other readers and, before this change, what removed the names of deleted topics: with
  the size reader elsewhere, browsing a topic after another one was deleted failed with
  "This server does not host this topic-partition". A cluster without names has no name to
  remove, and the size read keeps refreshing its metadata.
- **2026-10-12** — No broker reporting became `n/a` with the reasons: `≥ 0 B` on every topic said
  nothing about why. A broker that answers with no log directory counts as refusing, because
  version 1 of the API has no error code for a user that may not describe the cluster. The first
  broker is no longer special: its failure used to fail the whole read.
- **2026-10-12** — The 15 seconds of a broker are counted by the reader. KafkaJS only lets a
  request wait longer than the request timeout of the client, never less, and that timeout equals
  the 30 seconds the page waits: one silent broker would turn every size into a bare timeout.
  For the same reason the whole read has a deadline, set where the admin timeout is set and three
  seconds before it: the timeout cannot tell which step it interrupted, the reader can.
- **2026-10-12** — The message browser and the watermarks do not refresh the metadata because of
  its age. A refresh every five minutes was tried and dropped: with all topics in one request, a
  partition without a leader anywhere in the cluster fails it after the five retries of KafkaJS,
  and the browser of a healthy topic would stop working on a degraded cluster. Before this
  change those readers never refreshed a topic they already knew either. A leader that moved is
  picked up when the topic list is loaded again, as before; asking again when a broker answers
  that it is not the leader is left to a later change.
- **2026-10-12** — Not covered here: the KafkaJS Admin calls that name topics in requests of
  their own. The topic detail names one topic; the consumer group detail, and the lag of the
  health read when its batched watermark read fails, name one more topic at each topic they go
  through. Nothing shows that those brokers close on them.

## Verification Evidence

| Requirement range | Evidence |
| --- | --- |
| REQ-198, SC-116 | `src/main/kafka/log-dirs-protocol.test.ts` (encoding with the null topic list, decoding, read-only transport gate) |
| REQ-199–REQ-200, SC-116 | `src/main/kafka/log-dirs.test.ts` (aggregation, lower bounds, brokers left out with their reasons, a broker that does not answer in time, one time for both tries, log directories that all answer an error, no broker reporting, a cluster without the API, bounded concurrency) |
| REQ-201 | `src/main/kafka/log-dirs.test.ts` and `src/main/kafka/kafka-connection.test.ts` (an aborted read stops asking brokers and is not done again; at the deadline the metadata step or the opening of the read cluster fails with its name and the brokers not asked yet are left out, also when the timer fires before the clock; nothing is sent to a broker already left out); `src/main/operation-timeout.test.ts` (the deadline falls three seconds before the timeout, the signal aborts at the timeout, a timed-out operation runs its teardown) |
| REQ-221 | `src/main/kafka/message-fetch.test.ts` (a real KafkaJS cluster is never given a topic name and its metadata is read again for an unknown topic or partition, not for its age), `src/main/kafka/offset-batch.test.ts`, `src/main/kafka/log-dirs.test.ts` (one metadata read, every log directory asked, unlisted topics and partitions dropped) |
| REQ-222 | `src/main/kafka/log-dirs.test.ts` (step, broker and connection of a failure, read before the connection closes), `src/main/kafka/kafka-connection.test.ts` (second read, both attempts, timing, notes), `src/renderer/format-bytes.test.ts` (text and hover text of every state) |
| REQ-200, REQ-221–REQ-222, SC-131 | `pnpm itest:read-cluster` (`test/e2e/integration.read-cluster.local.ts`) on the loopback fixture |
| REQ-201, SC-115 | Packaged Electron E2E: Topics list, summary strip and Partitions tab show byte values on the loopback fixture |
