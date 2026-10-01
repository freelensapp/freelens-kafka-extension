# SPEC-018 - Durable Settings Across Freelens Restarts

| Field | Value |
| --- | --- |
| Status | Implemented |
| Date | 2026-10-01 |
| Source | Issue #76, opened after the reporter of #72 lost his settings when he reinstalled the extension |
| Safety | Read-only: nothing is sent to a cluster; passwords are still never written to disk; governed by `TESTING-SAFETY.md` |

## Problem

Freelens serves its windows from `https://<clusterId>.renderer.freelens.app:<port>`, where the port
belongs to the lens-proxy, which listens on a random port at every start of the app. The origin of
a cluster frame therefore changes at every launch, and with it the `localStorage` partition: whatever
an extension keeps there is left behind in the previous origin. The auto-refresh settings of the
Overview, the Schema Registry and Kafka Connect endpoints and the write-mode flags lived in
`localStorage`, so they were gone after every restart, which is also what a reinstall or an update
implies. The Security override of the Connection Settings drawer was kept in renderer memory only
(SPEC-004), which made the AWS IAM mode of SPEC-017 something to type again at every start. The
cluster catalog, the manual endpoints and the selected clusters had already moved to the host
extension store and did not have the problem.

## Scope

- Every setting of the extension that is meant to last lives in the host-managed extension store,
  the same `KafkaPersistentStateStore` the catalog uses.
- The Security override is kept across restarts without its password.
- Values written to `localStorage` by an earlier version during the same Freelens session are
  migrated once, so an in-place update loses nothing.

## Non-goals

- Persisting any password or access key: the renderer-memory rule of SPEC-004, SPEC-010, SPEC-011
  and SPEC-017 stands.
- Recovering settings lost in the origins of previous sessions: they are unreachable from the new
  origin.
- Changing how the host chooses its proxy port or where it keeps the extension store.

## Functional Requirements

- **REQ-218** — The Overview auto-refresh settings, the Schema Registry settings, the Kafka Connect
  settings, the write-mode flags and the Security override MUST be stored in the host-managed
  extension store (`<userData>/extension-store/<extension name>/`), never in `window.localStorage`
  or `sessionStorage`, and MUST be read back after a restart of Freelens with the same user data.
- **REQ-219** — The Security override MUST be persisted without its password: TLS mode,
  authentication mode, username, AWS region and AWS profile are durable, the password stays in
  renderer memory for the session that typed it. After a restart an `AWS IAM (MSK)` override
  reconnects by itself; a PLAIN or SCRAM override comes back with its username and asks for the
  password again through the existing error path of the main process.
- **REQ-220** — Each settings store MUST read the storage lazily, so that the durable state loaded
  by the host after the stores were built, and values written by another cluster frame, are seen
  on the next access; values persisted by a previous version in `localStorage` MUST be migrated
  once into the host store without overwriting durable values, and malformed persisted entries
  MUST be dropped rather than trusted.

## Success Criteria

- **SC-129** — Unit: every settings store persists through a shared storage and restores in a new
  instance; the persisted override contains no password; a store built before the storage holds
  data sees the data on the next access and after `reload()`; malformed entries are ignored; a
  storage that throws leaves the store usable in memory.
- **SC-130** — Packaged Freelens with the same user data across two launches: an `AWS IAM (MSK)`
  override, an auto-refresh interval and write mode set on a manual endpoint are still there after
  the restart, and a PLAIN override comes back with its username and an empty password.

## Decision Log

- **2026-10-01** — The host extension store rather than a fixed-port trick or `sessionStorage`:
  it is the storage Freelens documents for extensions, it is written by the main process, it
  survives restarts, updates and reinstalls, and the extension already used it for the catalog.
- **2026-10-01** — The override is persisted without the password instead of staying session-only:
  the IAM mode has no secret at all and typing the region at every start was the complaint of #76;
  for PLAIN and SCRAM the username is as safe to keep as the Schema Registry and Connect usernames
  already were.
- **2026-10-01** — Write mode is persisted as REQ-103 always said ("stored per `targetId` in
  extension settings"); the `localStorage` loss had been hiding that it never survived a restart.
  Every write still goes through its own confirmation (REQ-104), so a remembered switch cannot act
  by itself.
- **2026-10-01** — Lazy reads keyed on the raw stored string instead of eager loads in the
  constructors: the renderer builds its stores before `loadExtension` has read the file, and two
  cluster frames share one file through the host sync.

## Verification Evidence

| Requirement range | Evidence |
| --- | --- |
| REQ-218–REQ-220, SC-129 | `src/renderer/kafka-connection-settings.test.ts` (persisted without password, session password, restore, cross-instance read, malformed entries, throwing storage), `src/renderer/kafka-overview-settings.test.ts`, `src/renderer/kafka-schema-registry-settings.test.ts`, `src/renderer/kafka-connect-settings.test.ts`, `src/renderer/kafka-write-settings.test.ts` (late storage and `reload()`), `src/common/kafka-persistent-state-store.test.ts` (one-time migration) |
| SC-130 | Verified on 2026-10-01 in the packaged Freelens 1.10.3 with the single-bundle build of this branch and the same user data across two launches (renderer origins on ports 64183 and 64338): two unreachable manual endpoints, write mode, a Schema Registry URL and username, an `AWS IAM (MSK)` override with region and profile and a SASL/PLAIN override with username and password; the host wrote `extension-store/@freelensapp/kafka-extension/freelens-kafka-state-store.json` without the password, and after the restart the endpoints, the switch, the Schema Registry fields, the IAM mode with its region and profile and the PLAIN mode with its username were back, the password field empty (15 checks, driver and screenshots kept outside the repository) |
