# SPEC-008 — Message Browser Enhancements

| Field | Value |
| --- | --- |
| Status | Verified |
| Date | 2026-08-06 |
| Source | v1.0.0 parity gap analysis; accepted by user 2026-08-06 |
| Safety | Kafka reads only; `offsetsForTimes` is read-only and commits no offset; governed by `TESTING-SAFETY.md` |

## Problem

The message browser can page and tail but provides no way to narrow results or jump to a time window
of interest. Users with high-volume topics must page through large result sets to find specific
messages.

## Scope

- **Client-side filters** — filter the currently loaded page by key substring/regex, value
  substring/regex and header key=value pair.
- **Seek by timestamp** — let the user enter a point in time; Browse fetches records from the
  nearest available offset at or after that time on every partition.
- Filter state persisted in URL parameters, surviving reload without re-fetching.

## Non-goals

- Full-text indexing of historical messages.
- Filtering by deserialized Avro/Protobuf field values (requires Schema Registry, SPEC-010).
- Saving or exporting named filter presets.
- Server-side value scanning across all partitions (prohibitively expensive for large topics).

## User Scenarios

### US-028 — Find messages by key pattern (P1)

**Given** a loaded message page, **when** the user enters a key filter, **then** only rows whose key
matches the filter are shown; the page controls remain available.

### US-029 — Find messages by value pattern (P1)

**Given** a loaded message page, **when** the user enters a value filter, **then** only rows whose
value matches are shown.

### US-030 — Find messages by header (P1)

**Given** a loaded message page, **when** the user enters a header filter (key=value), **then** only
rows carrying that header with a matching value are shown.

### US-031 — Jump to messages from a specific time (P1)

**Given** a topic with messages spread over several hours, **when** the user enters a timestamp and
presses Browse, **then** the browser fetches records from the offset nearest to that time across all
partitions, with no offset commit.

## Functional Requirements

- **REQ-094** — The message browser MUST support real-time client-side filtering by key: substring
  match (default) or ECMAScript regex when the input is prefixed with `/`.
- **REQ-095** — The message browser MUST support real-time client-side filtering by value using the
  same substring/regex semantics as REQ-094.
- **REQ-096** — The message browser MUST support filtering by header: the user specifies a header
  key and optionally a value; only messages carrying that header (with the matching value, if given)
  are shown.
- **REQ-097** — Active filter state (key filter, value filter, header filter, timestamp) MUST be
  reflected in URL page parameters and MUST survive a full page reload without issuing a new fetch.
- **REQ-098** — The message browser MUST support seek-by-timestamp: a datetime input above the
  Browse controls sets the starting position; Browse fetches from the offset returned by
  `admin.offsetsForTimes` for each partition. If a partition has no offset at or after the given
  time, it is fetched from the latest available offset.
- **REQ-099** — Seek-by-timestamp MUST use `admin.offsetsForTimes` exclusively and MUST NOT commit,
  reset or otherwise mutate any offset.
- **REQ-100** — Filter controls MUST be visually distinct from Browse/Tail controls and MUST NOT
  trigger a new fetch on their own; they operate only on the currently loaded result set.
- **REQ-101** — When a filter is active and returns zero rows on the current page, the empty state
  MUST distinguish between "no messages match the current filter" and "no messages on this topic or
  partition range".

## Success Criteria

- **SC-053** — Key filter applied to a page with seeded messages shows only rows whose key contains
  the filter string; all other rows are hidden without a new IPC call.
- **SC-054** — Value regex filter `/error/i` applied to a seeded page shows only messages whose
  value matches the pattern.
- **SC-055** — Header filter `x-trace-id=abc` shows only messages carrying that header with the
  matching value.
- **SC-056** — Seek-by-timestamp with a past time returns records at or after that time; a fixture
  verifies the offset used matches the value returned by `offsetsForTimes`.
- **SC-057** — Reloading a URL with embedded filter params pre-populates all filter controls and
  does not issue a new fetch until Browse is explicitly pressed.

## Decision Log

- **2026-08-06** — Spec accepted as part of v1.0.0 scope definition.
- **2026-08-06** — Client-side filtering chosen to avoid additional per-message IPC round-trips;
  server-side scan is explicitly a non-goal.
- **2026-08-06** — Schema-aware filtering deferred to SPEC-010 where Schema Registry context is
  available.
- **2026-08-19** — A focused full-window reload test passed once, but the subsequent integrated
  packaged suite reproduced a route-restore failure. Reload evidence remains incomplete until the
  focused test and full suite both pass from a clean fixture state.
- **2026-08-19** — Reload restoration moved from the Overview page to an always-mounted cluster-frame
  component. Renderer-session-scoped snapshots prevent ordinary same-session navigation from
  restoring stale routes, while a fresh renderer restores and then clears the saved route.
- **2026-08-19** — Final local evidence passed both gates: the focused packaged reload test ran as
  `1/1` with ten intentional skips, and the integrated packaged Electron suite passed `11/11` in one
  session after all preceding Kafka workflows.

## Implementation Progress

| Slice | Status | Evidence |
| --- | --- | --- |
| Key/value/header client-side filters (REQ-094–REQ-096, REQ-100–REQ-101) | Done | Filter matcher unit tests; packaged Browse E2E with zero additional Browse requests |
| URL-backed filter and timestamp state (REQ-097) | Done | Route manifest/unit checks; packaged URL assertions |
| Seek by timestamp (REQ-098–REQ-099) | Done | Existing read-only timestamp adapter tests; packaged timestamp Browse E2E |
| Explicit reload-with-filters evidence (SC-057) | Done | Focused packaged reload E2E `1/1`; integrated packaged Electron suite `11/11` |

Final evidence (2026-08-19): typecheck, focused Biome/Prettier checks, build/smoke and 139 unit tests
pass. The packaged reload test passes in isolation and as the final case of the `11/11` integrated
Electron suite. All Kafka writes used the disposable loopback fixture; no non-local cluster was contacted.
