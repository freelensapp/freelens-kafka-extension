# SPEC-019 - Port to the Freelens v2 Extension API

| Field  | Value                                                                                                                                                                                                                          |
| ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Status | Accepted                                                                                                                                                                                                                       |
| Date   | 2026-10-26                                                                                                                                                                                                                     |
| Source | Freelens 2.0 replaces the extension API of 1.x; the plan is the migration issue of this repository and its feature issues A-F, opened from the `port-freelens-extension-to-v2` skill of freelensapp/freelens-example-extension |
| Safety | Writes only on the labs the repository's scripts create; real clusters and real Kafka only read-only, on targets the maintainer names; governed by `TESTING-SAFETY.md`                                                         |

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
- **2026-10-26** — The port-forward runs the host's kubectl (`Main.Shell.getKubectlPath` and
  `Main.Shell.getEnvironment` of the request's cluster), one `kubectl port-forward` per broker pod
  and port, as freelensapp/freelens-velero-extension does for its tunnels. Spike B0 on the lab: it
  works through the Freelens proxy, kubectl exits by itself when the cluster disconnects, and the
  version-matched kubectl that the host downloads on first use is the only slow step (15 s, then
  about 0.3 s per tunnel). The other route, client-node on the kubeconfig that
  `Main.Shell.getEnvironment` names, fails: that kubeconfig authenticates with a credential plugin
  that reads `FREELENS_PROXY_TOKEN`, which Freelens sets only in the environment of the tools it runs,
  so main's own environment would have to carry the token.
- **2026-10-26** — Progress follows the pattern of freelensapp/freelens-velero-extension: data
  travels only over `invoke`, bound to the frame that asked, and `broadcast` carries only a signal
  without content. Main keeps the recent events of each operation and broadcasts its operation id, a
  random value; the frame that started the operation, the only one that knows the id, fetches the
  event over `invoke`. Spike C0: the events name topics, groups and the authentication mode, which a
  broadcast of the events themselves would hand to the frames of every cluster.
- **2026-10-27** — Progress refined after the integration suite on Freelens 2: a fetch of the latest
  event only, delivered with the others in one go, folded the counted steps of a fast discovery into
  its last event, and the answer of the request could reach the page before its last events. Main now
  keeps the events of an operation in order (64 at most) and answers from the frame's cursor; the
  renderer hands them to the page one task apart and delivers the remaining ones before the answer,
  the order 1.x gave. The frame binding of REQ-227 is unchanged.
- **2026-10-27** — The T022 check of the 1.x suite (a persisted partial health painted after a restart
  of the main process) is not ported: it closed and relaunched the packaged app, which the template's
  harness, attached over CDP to Freelens dev, cannot do. The snapshot's sanitization and coverage stay
  unit-tested.
- **2026-10-26** — The seven hidden menus of SPEC-004 stay: without them Freelens 2 still shows a
  tab bar of the sibling pages above every Kafka page (spike D0).
- **2026-10-26** — The tests and scripts get labs of their own (REQ-232): until 1.4.3 they wrote to
  the context `kind-kind`, which is the developer's own kind cluster.
- **2026-10-26** — REQ-226 deviation: of the AWS SDK credential chain, the clients made for STS
  (a profile's `role_arn`, a web identity token) and SSO take `clientConfig.requestHandler`, a
  handler over `Main.Util.fetch`; the instance metadata (IMDS) and container (ECS, EKS Pod
  Identity) providers take none and keep the SDK's own Node client. They reach link-local or
  container-local addresses, which a proxy is not meant to serve.

## Open Questions

None.

## Manual Verification

Not run yet (SC-140).

## Verification Evidence

| Requirement range             | Evidence                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| REQ-224 (Kubernetes)          | Discovery reads through `Main.K8s` (`createCatalogKubeReader`), every request names its cluster. Port-forward: unit `kubectl-tunnels.test.ts` (the host's kubectl, environment and folder; refusals in words), `tunnel-forwarder.test.ts`; Freelens 2 dev on the kind lab (2026-10-26): overview, brokers and topics of a Strimzi cluster through a `kubectl port-forward` child of the Freelens main process. No client-node in `dist/main.js`.                                                                                                                                             |
| REQ-225, SC-138               | Unit: kubectl killed on abort, on `closeAll`, on process exit and when the cluster leaves the connected state; `KafkaConnection.onDisconnect`. Freelens 2 dev on the kind lab (2026-10-26): no kubectl after the cluster disconnects, a new one after reconnecting and reopening the page, none after the extension is disabled.                                                                                                                                                                                                                                                             |
| REQ-226                       | Unit: both clients on fetch only; undici's failures on real sockets (a refused port said as `ECONNREFUSED`, one retry of a Connect read after a dropped connection); the SDK's own assume-role call to a local fake STS sent through the given fetch. Freelens 2 dev (2026-10-26) with the `httpsProxy` preference on a local logging proxy: the Schema Registry subjects and the Kafka Connect connectors of local fakes, every request seen by the proxy, sent by undici. AWS deviation in the decision log.                                                                               |
| REQ-228                       | `requireClusterId` in `src/main/ipc.ts`: no `activeClusterId` fallback, no kubeconfig or context in the requests.                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| REQ-227, SC-137               | Unit: the store hands an event to its frame only and never lets another take an operation over; the renderer pulls only its own ids, folds signals, keeps an id 5 s after its answer; `KafkaIpcMain` with a fake host broadcasts `{ operationId }` alone and refuses another frame. Freelens 2 dev (2026-10-26), two catalog clusters in two frames: each frame fetched only its own operations' events (36 and 5), the other was told 44 and 12 ids and fetched none; every signal was the id alone; main answered `null` to frame B for an operation of frame A, and the event to frame A. |
| REQ-229 (renderer IPC)        | One `KafkaIpcRenderer` per activation (`onActivate`/`onDeactivate`). Freelens 2 dev: after disable and enable, a scan reaches main and its progress is fetched.                                                                                                                                                                                                                                                                                                                                                                                                                              |
| REQ-230, SC-139               | One CSS module, `src/renderer/kafka.module.scss`: the `Kafka*` classes local, the host's classes and the modifiers `:global` under them; a unit test compiles it and finds every style rule under a `Kafka*` class, and every rule of the built `renderer.css` is under a hashed local one. The 18 views of the 1.4.3 baseline in both themes on Freelens 2 dev (2026-10-26): the same visible text (apart from data and times), at most 0.5% of the pixels of the page area different.                                                                                                      |
| REQ-223 (SPEC-004 navigation) | The reload route restorer keeps only the params a page declares (`declaredKafkaPageParams`), since Freelens 2's `navigate` throws on another; on Freelens 2 dev a reload with an undeclared param in the saved route comes back to the topic workspace and its tab; the sidebar follows a topic opened from its group.                                                                                                                                                                                                                                                                       |
| REQ-233, SC-141               | `WriteModeGuard` reads `freelens-kafka.write-mode.v1` from main's copy of the extension store at every write call; the `kafka:write-mode` mirror and its renderer side are gone (SPEC-009 REQ-197 changed, with its decision log entry). Unit: `write-mode.test.ts`, `ipc-write-guard.test.ts`. Freelens 2 dev (2026-10-26), on the local compose broker: off → refused, on → produced, off → refused at once.                                                                                                                                                                               |
| Other requirements            | Not verified yet                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
