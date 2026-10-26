# SPEC-019 - Port to the Freelens v2 Extension API

| Field | Value |
| --- | --- |
| Status | Accepted |
| Date | 2026-10-26 |
| Source | Freelens 2.0 replaces the extension API of 1.x; the plan is the migration issue of this repository and its feature issues A-F, opened from the `port-freelens-extension-to-v2` skill of freelensapp/freelens-example-extension |
| Safety | Writes only on the labs the repository's scripts create; real clusters and real Kafka only read-only, on targets the maintainer names; governed by `TESTING-SAFETY.md` |

## Problem

Freelens 2 loads extensions through a new API: ESM entry points, host libraries from a global
instead of `require()`, React 19 and MobX 7, a renderer without Node, a sibling stylesheet linked by
the host, a reload of both processes on every rebuild. Version 1.4.3 of this extension is built for
Freelens 1.x (`engines.freelens: ^1.8.0`, CommonJS, electron-vite, React 17, an inline stylesheet)
and does not load on Freelens 2.

Beyond loading, v2 states how an extension reaches a cluster and the network. A client of a catalog
cluster built from the cluster's `kubeConfigPath`, which is how 1.4.3 opens its per-broker
port-forwards, works on a plain network and goes around the Freelens proxy: it ignores the cluster's
proxy setting and does not get what Freelens does for the cluster. HTTP from main through
`node:http` ignores the user's proxy and CA settings. And `Ipc.broadcast`, which carries the
progress of the extension's operations, reaches every frame of every window in v2, so the progress
of one cluster's Kafka reaches the frames of the other clusters.

## Scope

- The extension loads and works on Freelens 2.x, with the behaviour SPEC-001 to SPEC-018 verified
  on 1.x.
- The main process reaches Kubernetes through Freelens and HTTP through the host's client.
- What an operation reports reaches only the frame that started it.
- A deactivation or a reload releases everything the extension started.
- The tests and the scripts work on labs of their own and never on a cluster or kubeconfig of the
  developer.
- Improvements found during the port, each approved by the maintainer before it is built, are
  added to this specification with their requirements.

## Non-goals

- Supporting Freelens 1.x from the same release: 1.4.3 stays on npm for it.
- Replacing kafkajs, or moving Kafka's own protocol off TCP: Freelens has no client for it.
- Upgrading the bundled main-process libraries to new majors (client-node 2, protobufjs 8): separate
  changes after the port.
- New Kafka features unrelated to the port.

## Functional Requirements

- **REQ-222** — The extension MUST declare `engines.freelens: ^2.0.0` and activate in both processes
  of Freelens 2.x with no error in the main log or in DevTools.
- **REQ-223** — Every requirement of SPEC-001 to SPEC-018 MUST keep holding on Freelens 2, except
  where this specification changes it; a requirement the port has to change is changed in its own
  specification, with a decision log entry, never silently.
- **REQ-224** — Every Kubernetes request of the extension for a catalog cluster, the discovery
  reads and the per-broker port-forwards alike, MUST reach the cluster through Freelens, so that
  the cluster's kubeconfig with its credential plugins and the cluster's proxy setting apply as they
  do for Freelens itself. No production path may build a client from the cluster's raw
  `kubeConfigPath`.
- **REQ-225** — The port-forwards of a cluster MUST follow its connection: none is started for a
  cluster that is not connected, which the page shows as such; they close when the cluster
  disconnects, when their session closes (idle or evicted, as in 1.4.3) and when the extension is
  deactivated.
- **REQ-226** — HTTP outside the cluster (Schema Registry, Kafka Connect, and the AWS SDK's
  credential providers where the SDK accepts a request handler) MUST go through the host's client,
  so that the user's proxy and CA settings apply. Where the SDK does not accept one, the deviation
  is recorded in the decision log.
- **REQ-227** — The progress and the results of an operation MUST reach only the frame that
  started it.
- **REQ-228** — Main MUST refuse a request that names no Kubernetes cluster rather than choose the
  active or the first one of the catalog.
- **REQ-229** — Deactivating the extension, or a reload of the development loop, MUST leave no
  Kafka connection, port-forward, listening socket, health worker, timer or listener of the
  previous instance.
- **REQ-230** — The extension's styles MUST apply to its own pages only: the host's components look
  the same outside the Kafka pages, and every Kafka page is readable in both themes.
- **REQ-231** — The write policy of SPEC-009 MUST hold with several cluster frames open: write mode
  changed in one frame applies to the others, and main refuses a write for a target whose write
  mode is off.
- **REQ-232** — The repository's tests and scripts MUST use only the clusters and kubeconfig files
  they create themselves; they MUST refuse any other context and any API server that is not local,
  and MUST leave the developer's kubeconfig unchanged.
- **REQ-233** — Main MUST decide whether a write is allowed from the write mode persisted in its own
  copy of the extension store, which the host keeps in sync with the renderer, rather than from what a
  renderer told it in the current session: the guard of SPEC-009 holds before any cluster frame has
  started and does not depend on the frames agreeing.

## Success Criteria

- **SC-133** — Type-check of `src/main`, `src/renderer` and `src/common` as separate programs, with
  the environment tests proving the split; the unit suite green; SC-131 keeps its meaning with the
  scan of `kafka-request-connection.test.ts` rewritten off the TypeScript 5 compiler API.
- **SC-134** — The 15 integration scenarios of 1.4.3 green on the template's harness against
  freelensapp/freelens `main` (then `v2.0.0`), locally and in CI.
- **SC-135** — Every lab itest green on the labs the scripts create; the developer's kubeconfig
  byte-identical before and after `lab:up`, the itests, `demo:up`/`demo:down` and the integration
  suite.
- **SC-136** — In Freelens dev, a rebuild reloads the extension once, and after a reload or a
  deactivation with sessions, port-forwards and health workers active, no listening socket,
  connection or timer of the previous instance remains.
- **SC-137** — With two clusters open in two frames, each frame shows the progress of its own
  operations only, and the other frame receives nothing of them.
- **SC-138** — On the lab, disconnecting the cluster closes its port-forwards; reconnecting and
  reopening the page starts them again.
- **SC-139** — Screenshots of every page in both themes match the 1.4.3 baseline on Freelens
  v1.10.3, apart from the differences listed in the decision log.
- **SC-140** — Manual verification, read-only, on targets the maintainer names: a cluster whose
  kubeconfig uses an `exec` credential plugin (discovery and port-forward), and Amazon MSK with IAM
  (connect and metadata).
- **SC-141** — Unit: main refuses a write for a target whose persisted write mode is off and allows it
  when it is on, with no renderer message involved; a change of the store reaches the guard.

## Decision Log

- **2026-10-26** — Version 2.0.0-0 after the port (`pnpm version premajor`); 2.0.0 is released
  after Freelens 2.0.0. One PR, squash-merged, with the migration issue and feature issues A-F.
  Scope: parity with 1.4.3 plus the improvements the agent proposes, each approved by the
  maintainer first.
- **2026-10-26** — The port compiles against `@freelensapp/extensions@2.0.0-0-nightly-2026-10-25`
  and reads the Freelens docs at freelensapp/freelens@7801fb10, the commit that nightly was built
  from; the pin moves with the nightly.
- **2026-10-26** — Kafka's own TCP stays on kafkajs over `node:net` and `node:tls`.
- **2026-10-26** — SPEC-019 accepted by the maintainer, with the store-based write guard of
  REQ-233 proposed during the analysis: the renderer's mirror of the write mode over IPC goes.
- **2026-10-26** — The tests and scripts get labs of their own (REQ-232): until 1.4.3 they wrote to
  the context `kind-kind`, which is the developer's own kind cluster.

## Open Questions

- How the port-forward reaches the cluster through Freelens: the host's kubectl from `Main.Shell`,
  or client-node on the kubeconfig of `Main.Shell.getEnvironment` (spike B0, then
  ARCHITECTURE.md 6.2).
- How progress reaches its frame: polling over `invoke` (proposed), `broadcast` kept as a
  deviation, or a local HTTP server (spike C0, then ARCHITECTURE.md 6.1).

## Manual Verification

Not run yet (SC-140).

## Verification Evidence

| Requirement range | Evidence |
| --- | --- |
| REQ-222–REQ-233 | Not verified yet |
