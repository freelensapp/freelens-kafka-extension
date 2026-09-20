# SPEC-016 - Update Without Restart: Version Probe and Restart Notice

| Field | Value |
| --- | --- |
| Status | Implemented |
| Date | 2026-09-20 |
| Source | Pull request #61, a user who updated the extension without restarting Freelens |
| Safety | Read-only (one IPC call between the two halves of the extension); governed by `TESTING-SAFETY.md` |

## Problem

Freelens loads the main side of an extension through the Node module cache and never invalidates
it. After an in-place update from the Extensions page the main process keeps running the main side
of the previous version until the app restarts, while every cluster frame opened afterwards loads
the new renderer. The IPC prefix is derived from the extension id, so both versions share the same
channels: a new renderer can call a channel the old main never registered and the user sees a raw
`No handler registered` error (the batch topic deletion of v1.3.0, fixed for that one channel by
#61), or a main-side fix silently does not apply.

## Scope

- Each half of the extension knows the version it was built from.
- The renderer asks the main process for its version once per frame and tells the user to restart
  Freelens when the two differ, on every Kafka page.

## Non-goals

- Reloading the main side without a restart: the module cache belongs to the host.
- Restarting Freelens from the extension, or blocking any feature while the versions differ.
- Per-channel compatibility fallbacks: the one added by #61 stays, no further ones are planned.

## Functional Requirements

- **REQ-209** — Both bundles MUST carry the package version they were built from, baked in at
  build time. The manifest MUST NOT be used for this: after an in-place update the host hands the
  new manifest to the old main module.
- **REQ-210** — The main process MUST answer a read-only `kafka:meta:version` call with the
  version of the code that is running. The renderer MUST probe it once per frame at activation
  and MUST treat a missing handler for that channel as a main process that predates the probe.
- **REQ-211** — When the versions differ, or the main process predates the probe, every Kafka
  page MUST show a restart notice that names the renderer version, names the running main version
  when it is known, and says that Freelens must be restarted to finish the update. A probe that
  fails for any other reason proves nothing and MUST NOT show the notice. The notice MUST NOT
  block the pages.

## Success Criteria

- **SC-122** — Packaged Freelens: install v1.3.0, update in place to a build with the probe and
  open a cluster without restarting. Every Kafka page shows the notice with "a previous version"
  and the batch topic deletion still succeeds under it.
- **SC-123** — Packaged Freelens: update in place between two builds that both have the probe.
  The notice names the version the main process still runs; after an app restart with the same
  user data the notice is gone.
- **SC-124** — A fresh install shows no notice.

## Decision Log

- **2026-09-20** — A version probe instead of more per-channel fallbacks: every future channel
  would need its own, and a fallback cannot help when the fix itself lives in the main side.
- **2026-09-20** — The version is baked into the bundles because the manifest follows the files on
  disk, not the code in memory.
- **2026-09-20** — The notice is permanent and not dismissible while the versions differ: the
  state ends only with a restart and the text is the whole remedy. It is one compact row so that
  it costs little height.

## Verification Evidence

| Requirement range | Evidence |
| --- | --- |
| REQ-210–REQ-211 | `src/renderer/kafka-version-skew.test.ts` (same version, different version, main without the probe, wording, one probe per frame, silent on unrelated failures) and `src/renderer/kafka-ipc-renderer.test.ts` (version answer, missing handler of this channel only) |
| REQ-209, SC-122–SC-124 | Verified on 2026-09-20 in the packaged Freelens 1.10.3 against the loopback Docker fixture, with production builds of two distinct versions: main handlers read from the main process (no `kafka:meta:version` on v1.3.0, present on the new builds), notice wording in both cases, dark and light themes, batch deletion under the notice, no notice after the restart and on a fresh install |
