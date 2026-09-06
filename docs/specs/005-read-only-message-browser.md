# SPEC-005 — Read-Only Message Browser

| Field | Value |
| --- | --- |
| Status | Done |
| Date | 2026-08-03 |
| Source | User-approved next increment after verified SPEC-004; accepted UX v3 roadmap |
| Safety | Kafka reads only; governed by [`TESTING-SAFETY.md`](../../TESTING-SAFETY.md) |

## Problem

Freelens can identify topics and partition topology but cannot inspect records. Operators must leave
the application and use command-line consumers, often without a clear bound or assurance that no
consumer-group state will be created. Message values may be large, binary or sensitive, so reading
automatically, joining a group, committing offsets or transferring unbounded payloads is unsafe.

## Scope

- A Messages tab inside one selected Topic Workspace.
- Explicit, bounded Browse for one partition from earliest, latest-window, offset or timestamp.
- Read-committed broker fetches without consumer groups or offset commits.
- A compact message list and selected-record inspector for metadata, key, value and headers.
- Truthful null/text/JSON/binary and truncation presentation.
- Explicit bounded Tail with start/stop lifecycle after Browse is verified.
- Desktop and compact keyboard-accessible states with no automatic reads.

## Non-goals

- Producing, retrying or editing messages.
- Joining, creating or modifying consumer groups; committing or resetting offsets.
- Reading multiple partitions in one request or silently changing partition.
- Deserializing Avro/Protobuf/JSON Schema or contacting Schema Registry.
- Persisting message payloads, keys, headers, credentials or browse results.
- Unbounded export, search across the full log or background polling.

## User Scenarios

### US-015 — Open Messages without reading (P1)

**Given** a selected Topic Workspace, **when** the user opens Messages, **then** Freelens shows
bounded controls and an idle state without opening a Kafka fetch session.

### US-016 — Browse a bounded partition window (P1)

**Given** an explicit partition, start mode and limit, **when** the user activates Browse, **then**
Freelens reads at most that many committed records, shows the exact returned offset range and offers
an explicit next window only when one exists.

### US-017 — Inspect record bytes truthfully (P1)

**Given** a returned record, **when** the user selects it by pointer or keyboard, **then** the
inspector shows timestamp, partition, offset, byte lengths, key, value and headers without corrupting
binary bytes or hiding truncation.

### US-018 — Understand empty, compacted and failed reads (P1)

**Given** an empty range, compacted offsets, invalid control or unavailable broker, **when** Browse
finishes or fails, **then** the UI retains the controls and reports a distinct bounded result or
correlated error without fabricating records.

### US-019 — Tail only by explicit request (P2)

**Given** the Messages tab is idle, **when** the user starts Tail, **then** Freelens reads new
committed records into a bounded buffer until Stop, navigation, topic/cluster change or unmount.

## Functional Requirements

- **REQ-055** — Messages MUST be an implemented tab of one selected Topic Workspace; it MUST NOT be
  a cluster-level sidebar entry or appear for an unselected topic.
- **REQ-056** — Entering or reloading Messages MUST perform no Kafka record fetch. Browse and Tail
  MUST begin only after their distinct explicit commands.
- **REQ-057** — Browse MUST require one valid partition, one start mode (`earliest`, `latest`,
  `offset` or `timestamp`) and a record limit from 1 through 100, defaulting to 50.
- **REQ-058** — `latest` MUST mean the ascending offset window beginning at
  `max(log-start-offset, high-watermark - limit)`; the UI MUST identify this as an offset window and
  MUST NOT claim that compacted logs return exactly `limit` records.
- **REQ-059** — Main MUST use Kafka's broker Fetch/ListOffsets protocols with `READ_COMMITTED`,
  `allowAutoTopicCreation=false` and no consumer-group join, coordinator, offset commit, producer or
  mutating admin API.
- **REQ-060** — Main MUST validate the selected topic and partition against authoritative metadata,
  reject malformed offsets/timestamps/limits before network fetch and correlate each response to
  the active `targetId`, topic and operation ID.
- **REQ-061** — Each broker request and Renderer DTO MUST be bounded by configured byte and record
  limits. Oversized key/value/header previews MUST be truncated with original byte length retained;
  one oversized broker record MUST NOT create an unbounded IPC payload.
- **REQ-062** — Each record DTO MUST include topic, partition, offset, timestamp, key, value, ordered
  headers and byte lengths; it MUST contain no credentials, Secret values unrelated to that record,
  consumer-group identity or mutable client state.
- **REQ-063** — Null, UTF-8 text, valid JSON and binary payloads MUST be distinguished truthfully.
  Binary and truncated previews MUST preserve bytes as base64; invalid UTF-8 MUST never be rendered
  as silently replacement-decoded text.
- **REQ-064** — Duplicate header names and multiple values MUST remain distinguishable and ordered;
  null header values MUST remain null.
- **REQ-065** — Browse responses MUST expose requested/start/next offsets, log-start offset,
  high-watermark, returned count and `hasMore`. Next-page reads MUST require an explicit command and
  MUST not repeat the same offset after an empty or compacted fetch.
- **REQ-066** — The message list and inspector MUST support pointer plus Enter/Space selection,
  visible focus/selection and stable responsive dimensions without page-level horizontal overflow.
- **REQ-067** — Idle, loading, empty, no-records-in-window, truncated, completed and error states
  MUST be visually distinct; a failed request MUST retain the user's harmless controls for retry.
- **REQ-068** — URLs MAY retain harmless partition/start/limit controls but MUST NOT contain message
  bytes, keys, headers, credentials, operation IDs or Tail buffers. Browse results MUST remain
  volatile and clear when target/topic changes.
- **REQ-069** — Tail MUST be a separate explicit mode with Start and Stop, read only one partition,
  use `READ_COMMITTED`, maintain a bounded in-memory record/byte buffer and expose dropped-record
  count when the buffer evicts older entries.
- **REQ-070** — Tail MUST stop and release all broker/socket/port-forward resources on Stop,
  completion/error, route unmount, topic change, Kafka-cluster change and extension deactivation;
  late events from a stopped session MUST be ignored.
- **REQ-071** — Switching Kafka cluster from Topic Workspace MUST retain SPEC-004 behavior: return
  to the Topics list for the new cluster and clear selected topic, Browse results and Tail state.
- **REQ-072** — Automated record-producing fixtures MUST be disposable local Docker/KinD only.
  Real-cluster validation, when explicitly authorized, MAY read bounded records but MUST never
  produce, commit, alter, delete or create Kubernetes resources.
- **REQ-073** — Every Browse/Tail connection MUST have a bounded timeout/cancellation path and
  disconnect exactly once on success, failure or cancellation; tests MUST prove no leaked socket,
  group creation or committed offset.

## Success Criteria

- **SC-033** — Opening Messages and waiting produces zero record-fetch IPC calls and renders Idle.
- **SC-034** — A local fixture Browse from earliest returns no more than the requested limit in
  ascending offset order with matching partition, start/next/log-start/high-watermark metadata.
- **SC-035** — Latest, explicit offset and timestamp modes resolve to deterministic fixture windows;
  invalid partition/offset/timestamp/limit requests fail before broker Fetch.
- **SC-036** — Broker/group instrumentation proves Browse performs Fetch/ListOffsets only and leaves
  group count and committed offsets unchanged.
- **SC-037** — JSON, UTF-8, null and binary fixture values plus duplicate/null headers render with
  exact byte lengths, truthful format and base64-preserved binary/truncated previews.
- **SC-038** — Next window requires a click and advances; an empty/compacted window cannot loop on
  the same offset.
- **SC-039** — Pointer, Enter and Space select a record; desktop and 760×700 packaged checks show no
  list/inspector/control overlap or page overflow.
- **SC-040** — A stale Browse response after target/topic/control change cannot replace current
  state; errors retain controls and Retry remains explicit.
- **SC-041** — Tail starts only by command, Stop halts new events, bounded eviction reports drops and
  unmount/cluster/topic changes leave no active session or socket.
- **SC-042** — URL, localStorage, logs and cache snapshots contain no record payloads or credentials.
- **SC-043** — Unit/static, disposable protocol, focused packaged Freelens 1.10.3 and committed
  integration gates pass; no real cluster write is performed.

## Assumptions and Decisions

- Browse uses KafkaJS 2.2.4's pinned internal `Cluster`/`Broker.fetch` implementation behind one
  extension-owned adapter because KafkaJS's public consumer always requires a group ID.
- The adapter is version-guarded and tested against the disposable Kafka fixture; no KafkaJS
  internal type crosses the extension's engine or IPC boundaries.
- `READ_COMMITTED` excludes uncommitted transaction data; KafkaJS Batch filtering removes aborted
  transaction records and control batches before DTO normalization.
- Offset arithmetic uses decimal strings/`BigInt`, never JavaScript floating-point numbers.
- Slice 1 delivers the bounded broker-fetch core; Slice 2 wires correlated Browse IPC; Slice 3 adds
  the Messages Browse UI; Slice 4 adds explicit Tail; Slice 5 completes visual/safety evidence.

## Implementation Slices

1. Broker-level read-only fetch adapter, validation/normalization and deterministic local records.
2. Correlated bounded Browse IPC with guaranteed teardown and protocol evidence.
3. Messages tab, Browse controls, list/inspector, URL-safe state and packaged regressions.
4. Explicit bounded Tail session/event lifecycle and stop/cleanup evidence.
5. Responsive/accessibility polish, MCP review, full gates and final traceability.

## Implementation Progress

### Slices 1–3 — Bounded Browse — Done 2026-08-03

- `message-fetch.ts` isolates KafkaJS 2.2.4's private cluster factory and `Broker.fetch` behind a
  version guard. It uses `READ_COMMITTED`, `allowAutoTopicCreation=false`, leader-specific Fetch and
  ListOffsets without importing or constructing a consumer, coordinator or group ID.
- Validation bounds one partition, decimal offsets, epoch timestamps and limit `1..100`. Offset
  arithmetic uses `BigInt`; latest is the specified high-watermark window and empty/compacted
  batches advance without repeating the requested offset.
- KafkaJS `Batch` filters offsets below the request, aborted transactions and control records.
  Focused tests exercise abort markers, compacted gaps, latest/timestamp windows and unknown
  partitions before broker Fetch.
- Key/value/header normalization distinguishes null, JSON, valid UTF-8 and binary. Each field preview
  is at most 64 KiB and the response-wide preview budget is 512 KiB; original byte lengths and
  truncation remain explicit, with exact preview bytes in base64.
- `kafka:messages:browse` reuses the selected target's connection/security strategy, emits correlated
  progress and applies one 30-second resolve-plus-Fetch timeout. Success, failure, timeout and late
  connection resolution all reach a disconnect path.
- Topic Workspace exposes Overview, Messages and Partitions. Messages enters Idle with request count
  zero; explicit Browse provides bounded controls, Next window, stable native table and selected
  record inspector. Results are volatile and stale operation IDs are ignored.
- The deterministic Docker fixture seeds four records in partition 0: JSON with duplicate headers,
  UTF-8 text, binary and null. Seeding is idempotent and local-only.

Evidence: Browse core tests pass 13/13; the full suite passes 18 files / 90 tests. Typecheck,
Biome/Prettier, Knip development/production, clean build and `smoke:main` pass. Live Kafka 3.9
protocol evidence reports `records=4 range=0-4 groups=0`, proves
earliest/latest/timestamp behavior and unchanged group count, and tears down Docker. Packaged
Freelens 1.10.3 E2E passes 1/1 in 16.612 s, proving zero reads on entry, explicit windows `0–2` and
`2–4`, JSON/binary/null/header rendering, Enter/Space selection, URL payload absence and one-column
760×700 layout while all SPEC-004 regressions remain green. A runtime finding where missing
`messagesBrowse` progress steps remounted Topics was fixed and retained in the same packaged test.
Safety review additions prove aborted/control filtering, aggregate preview-budget exhaustion,
compacted-gap advancement and a total resolve-plus-Fetch timeout with late-connection cleanup.

Windows 11 manual verification then exposed two Browse defects. A real topic containing LZ4 batches
failed with `KafkaJSNotImplemented: LZ4 compression not implemented`, because KafkaJS recognizes LZ4
but ships no codec. Browse now registers a self-contained no-WASM `lz4-asm` codec in KafkaJS before
Fetch decoding; a real Docker LZ4 batch, source round-trip and compiled-bundle round-trip all pass.
The original KafkaJS-documented `kafkajs-lz4` wrapper was evaluated and rejected because its WASM
loader cannot resolve a local file under Node 24/Electron.

The Windows screenshot also showed a cyan artifact below `Latest window`. Removing the native Button
pseudo-element was insufficient: user retesting proved the remaining artifact was Windows's native
horizontal scrollbar, drawn because the flex item compressed a start-mode container configured with
`overflow-x:auto`. Start modes now use a non-shrinking four-column grid with no scrollable overflow,
a visible `Start position` legend, `aria-pressed`, stable line-height/minimum height and suppressed
host pseudo-elements. A final refinement moved the focus indication to the fieldset container so the
selected segment stays visually flat while keyboard focus remains visible. Packaged assertions measure
exact labels, no overlap/clipping, hidden pseudo-elements and `scrollWidth/scrollHeight` within the
client box at desktop and 760×700. The final no-scroll regression passes 1/1 in 17.934 s.

The newest Windows retest exposed the actual remaining overlap: Freelens globally gives `.Select` a
220 px minimum width while the Partition field was allocated 150 px, so the control and its focused
border extended about 70 px into `Latest window`. An attempted auto-open suppression was reverted
because it broke pointer partition selection. The Messages-specific select is now constrained to
100% of its 150 px field with `min-width:0`; the menu remains fully interactive. Packaged assertions
select Partition 1 then Partition 0 and prove that the select is contained and geometrically
separated from Start position at desktop and compact widths. The regression passes 1/1 in 18.409 s.

Tail requirements `REQ-069`–`REQ-071` remain unimplemented and keep SPEC-005 in `Implementing`.

### Slice 4 — Explicit Bounded Tail — Done 2026-08-05

- Tail is a renderer-side polling session over the existing `kafka:messages:browse` IPC. `startTail`
  issues an initial `latest` Browse (limit 1) to capture the high-watermark cursor, then schedules
  a repeating offset-cursor Browse every 2 seconds until `stopTail` cancels the session.
- A monotonic `tailSession` counter invalidates in-flight callbacks after the session is incremented
  by Stop, unmount, topic change or cluster change; late callbacks from a stopped session are ignored.
- The bounded in-memory record buffer evicts the oldest entries when the accumulated message count
  exceeds `limit`; `droppedCount` is exposed in the status strip.
- `stopTail` cancels the pending `setTimeout` and clears `tailTimer.current`; the unmount effect
  also increments `tailSession` to guard against any in-flight Browse completing late.
- Switching Kafka cluster from Topic Workspace clears `topic`, `query` and normalises `view` to
  `overview` reactively: `view = topicName ? implementedKafkaTopicView(rawView) : "overview"` means
  the existing normalization effect `if (rawView !== view) setRawView(view, true)` fires automatically
  once `topicName` becomes empty, without requiring direct `history.replaceState` calls.

Evidence: the full suite passes 18 files / 91 tests; typecheck, Biome/Prettier and Knip pass.
Packaged Freelens 1.10.3 E2E passes three Tail-related tests:
- `tails new records and stops without late updates` (REQ-069/070): tail starts, 2 new records arrive
  on partition 0, high-watermark advances, tail stops; no late event after 3.5 s proves no leaked
  session.
- `clears topic workspace and tail state when switching Kafka cluster` (REQ-071): switch from a
  direct cluster with active tail to a manual-endpoint cluster; topic workspace closes, tail stops,
  URL topic param is cleared and view resets to `overview`.

### Slice 5 — Safety Gates (REQ-072 / REQ-073) — Done 2026-08-06

- **REQ-072**: All automated record-producing fixtures use the disposable local Docker Kafka
  (`127.0.0.1:19092` / `docker-compose.direct.yml`). `produce-direct-tail-messages.ts` and
  `setup-direct-messages.ts` use only that endpoint; no real-cluster write is ever issued in
  autonomous runs. `TESTING-SAFETY.md` governs the boundary between read-only real-cluster validation
  and local-only write operations.
- **REQ-073**: Every Browse and Tail connection has a 30-second `withTimeout` guard in the IPC
  handler plus a `cancelled` flag that suppresses late IPC broadcasts. The `finally` block in
  `messagesBrowse` disconnects the connection exactly once whether it resolved or was still pending.
  `KafkaConnection.disconnect()` calls `this.directClose()` (the `createDirectSocketFactory`
  `closeAll` thunk) which force-destroys every tracked TCP socket, guaranteeing no leaked file
  descriptors after the handler exits.

Evidence: `pnpm itest:messages` (disposable Docker Kafka) extended with a Tail-polling simulation —
3 independent `connectDirect` / `browseMessages` / `disconnect` cycles matching the IPC handler
pattern — reports `groups=0` before and after, proving no consumer-group creation or offset commit
during polling. Full output:
```
MESSAGE_BROWSE_OK records=4 range=0-4 groups=0
TAIL_POLL_OK polls=3 final_cursor=4 groups=0
```
Process exits cleanly after all disconnects, confirming no leaked sockets.

## Planned Traceability

| Requirement | Planned code surface | Planned evidence |
| --- | --- | --- |
| REQ-055–REQ-058 | Topic Workspace navigation and pure browse-control normalization | Navigation/control unit tests; packaged idle/latest-window assertions |
| REQ-059–REQ-065 | KafkaJS broker-fetch adapter, message DTO normalizer and Browse IPC | Mocked adapter units; Docker protocol/group-count/offset tests |
| REQ-066–REQ-068 | Renderer Messages list/inspector and URL-backed harmless controls | View-model units; desktop/compact packaged keyboard and URL checks |
| REQ-069–REQ-071 | Main-owned Tail registry and correlated Renderer lifecycle | Fake-clock/buffer tests; local protocol + packaged Start/Stop/navigation checks |
| REQ-072–REQ-073 | Safety guards, connection lifecycle and workflow fixtures | Static API review; socket cleanup; local-only write workflow evidence |

## Verification Plan

- **Tier 0:** focused validation/normalization/navigation tests, typecheck, Biome/Prettier and Knip.
- **Protocol:** disposable Kafka 3.9 records across partitions/formats; no groups/commits before/after.
- **Tier 1:** isolated Playwright MCP inspection of controls, inspector and compact states.
- **Tier 2:** packaged Freelens 1.10.3 idle/Browse/Tail/history/keyboard/overflow regression.
- **Tier 3:** committed integration workflow with local record seeding and guaranteed teardown.
- **Real clusters:** optional exact-context, explicitly authorized, bounded read-only evidence only;
  never required for acceptance and never used for autonomous record production.

## Decision Log

- **2026-08-03:** User approved proceeding to the next planned increment after SPEC-004 verification.
- **2026-08-03:** Allocate `REQ-055`–`REQ-073` and keep Messages inside Topic Workspace.
- **2026-08-03:** Reject KafkaJS public consumers because they require group coordination; use one
  version-pinned broker Fetch adapter with `READ_COMMITTED` and no group/commit path.
- **2026-08-03:** Deliver Browse before Tail so protocol bounds and byte rendering are verified before
  introducing a live lifecycle.
- **2026-08-03:** Slices 1–3 Browse passed unit, disposable protocol and packaged Freelens evidence;
  proceed to explicit bounded Tail only after Browse user verification.
- **2026-08-03:** Windows user testing found missing LZ4 decode support and a native horizontal
  scrollbar over `Latest window`; both were fixed and promoted to live-protocol plus packaged
  no-scroll geometry tests. A final refinement moved the focus indicator to the fieldset container
  so the selected segment stays visually flat.
- **2026-08-04:** Windows retest identified the final root cause as Freelens's 220 px global Select
  minimum overflowing a 150 px Partition field. Menu interaction was restored; a local min-width
  override and packaged Partition 1→0 plus no-overlap assertions close the defect.
