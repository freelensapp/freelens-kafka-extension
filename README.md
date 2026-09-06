# Freelens Kafka Extension

A [Freelens](https://freelens.app) extension for **Apache Kafka**: discover the Kafka
clusters running in your Kubernetes cluster and inspect them — without leaving Freelens
and without manual `kubectl port-forward` gymnastics.

> **Status — SPEC-001–014 verified; v1.0.0 implementation sign-off complete.** The extension discovers Strimzi, generic in-cluster and
> workload-referenced external/managed Kafka (including MSK), probes PC reachability, connects via
> port-forward or Direct and reads broker/controller/topic metadata. The native Freelens UI is
> sortable/filterable, uses dedicated Clusters/Overview/Topics/Brokers/ConsumerGroups pages, supports manual endpoints and resolves TLS/no-auth,
> PLAIN, SCRAM and mTLS profiles from workload Secrets or session-only overrides. Its Topics view
> searches names and lazy-loads partition leadership, replicas, ISR/offline replicas and health.
> [SPEC-004](./docs/specs/004-resource-navigation-cluster-context.md) verifies stable target identity,
> **Clusters**, **Overview**, **Topics**, Topic Workspace, **Brokers**, shared caching and a secondary
> Connection Settings panel. The former primary resource Drawer is retired. 
> [SPEC-005](./docs/specs/005-read-only-message-browser.md) delivers explicit bounded read-only **Messages Browse** and **Tail**.
> [SPEC-006](./docs/specs/006-consumer-groups-lag.md) adds **Consumer Groups** list and **Offsets & Lag** workspace. 
> [SPEC-007](./docs/specs/007-topic-broker-depth.md) adds topic/broker configuration and bidirectional Topic/Consumer Group cross-links.
> [SPEC-008](./docs/specs/008-message-browser-enhancements.md) adds URL-backed message filters, timestamp seek and full-window reload restoration.
> [SPEC-009](./docs/specs/009-write-operations.md) adds the local-safe Produce and Consumer Group offset-reset workflow.
> [SPEC-012](./docs/specs/012-acl-security-views.md) adds ACL inspection, filtering and local-safe ACL create/delete on brokers that authorize it.
> SPEC-001–014 are verified with full test traceability. SPEC-014 meets its three-run packaged
> click-to-paint SLO and pinned open-source reference-console browser comparison. See
> [ARCHITECTURE.md](./ARCHITECTURE.md).

Slow Kubernetes/MSK operations expose real graphical phases: discovery shows resource checks and
`x/y` workload scanning; detail shows security, connection, broker and topic metadata. Workload
targets show aggregate Kubernetes usage rather than an arbitrary first namespace, and a reachable
row opens from any cell or the keyboard.

## Try it in Freelens 1.10.3 (fastest path)

The extension is not published to npm yet, so you install it from a locally built
tarball. It is a **self-contained bundle** — its runtime libraries (`kafkajs`,
`@kubernetes/client-node`) are compiled into `out/`, so Freelens loads it without
installing anything extra (Freelens does not npm-install an extension's dependencies).

1. **Toolchain** (Node is pinned to 24.15.0):

   ```sh
   nvm use 24.15.0    # or: mise install
   corepack enable
   corepack pnpm i
   ```

2. **Build + pack** a tarball:

   ```sh
   corepack pnpm pack:dev
   ```

   This bumps a throwaway prerelease version, builds, and writes a
   `freelensapp-kafka-extension-*.tgz` into the repo root. (The version bump makes
   Freelens treat each rebuild as an upgrade, so re-installing actually reloads your
   changes.) For a one-off you can also just run `corepack pnpm build && corepack pnpm pack`.

3. **Install into Freelens**: open Freelens → **Extensions**
   (`Ctrl`+`Shift`+`E` / `Cmd`+`Shift`+`E`) → **drag-and-drop the `.tgz`** into the Freelens
   window (or paste its absolute path and click *Install*). Enable it if prompted.

4. **Use it**: open a cluster. An **Apache Kafka** group with **Clusters**, **Overview**, **Topics**
  and **Brokers** children appears in the cluster's left sidebar.
   - It lists Strimzi, Kafka Services and external/managed endpoints referenced by workload config.
   - Reachable external Kafka use Direct; Strimzi uses per-broker port-forward.
   - **Add endpoint** connects to a bootstrap not referenced by Kubernetes (TLS optional).
  - Click or press Enter on a reachable row to open its full **Overview** page.
  - Use the row's `tune` action for session-only Connection Settings or manual endpoint removal.
  - Open **Topics**, filter by name and select one to inspect partition topology and replica health.
  - Open **Messages** inside Topic Workspace; no records are read until **Browse** is pressed.

> **Safety:** discovery, reachability, port-forward and metadata/topic reads are read-only. Never
> produce messages, alter topics/configs/ACLs, commit offsets or mutate Kubernetes resources on a
> real cluster. See [TESTING-SAFETY.md](./TESTING-SAFETY.md).

### Get something to look at (local KinD)

- **Discovery only** (populate the Overview table, no real brokers): deploy the Strimzi CRD
  and sample `Kafka` fixtures used by the tests:

  ```sh
  corepack pnpm kind:disc:up     # apply fake Strimzi CRD + Kafka CRs
  # ... test in Freelens ...
  corepack pnpm kind:disc:down   # tear down
  ```

  The Clusters table will list the fixtures. Opening Overview will fail to connect because the
  fixture pods are not real brokers; Connection Settings remains available independently.

- **Fastest Overview + Topics test** uses a disposable host-reachable Kafka and a zero-impact
  workload reference in `kind-kind`:

  ```sh
  corepack pnpm kafka:direct:up   # starts Kafka and creates freelens-orders (3 partitions)
  corepack pnpm kind:direct:up    # makes the bootstrap discoverable from kind-kind
  # Open kind-kind → Kafka → 127.0.0.1 → Topics → freelens-orders
  corepack pnpm kind:direct:down
  corepack pnpm kafka:direct:down
  ```

  Overview shows the Direct connection, broker metadata and the searchable topic/partition detail.
  For the internal-only port-forward path, install a real Strimzi Kafka in KinD and open its
  pod-DNS-advertised listener.

## Development

For fast frame-aware exploratory UI verification, start an isolated Freelens CDP session containing
only `kind-kind`:

```sh
corepack pnpm mcp:app
```

Then attach a locally configured, version-pinned Playwright MCP server. This is an optional assisted
loop; focused and committed integration tests remain mandatory. See
[Playwright MCP testing](./docs/playwright-mcp-testing.md) and
[ADR-001](./docs/decisions/001-playwright-mcp-assisted-verification.md).

Node 24.15.0 (`.nvmrc` / `mise.toml`) + `corepack pnpm`. Run the local gates after every
change:

```sh
corepack pnpm type:check
corepack pnpm lint:check     # biome  (lint:fix to auto-format)
corepack pnpm build
corepack pnpm knip:check
corepack pnpm test:unit
```

Engine integration tests run against Docker / KinD — see [test/e2e](./test/e2e) and the
`kafka:*` / `kind:*` / `itest*` scripts in [package.json](./package.json).

## Documentation

- [ARCHITECTURE.md](./ARCHITECTURE.md) — design, decisions, roadmap.
- [docs/connectivity-engine.md](./docs/connectivity-engine.md) — how the redirecting
  socket factory + port-forward manager reach in-cluster brokers.
- [docs/discovery.md](./docs/discovery.md) — Strimzi / Service discovery + credentials.
- [docs/overview-ui.md](./docs/overview-ui.md) — the renderer overview + drill-in UI and
  its IPC flow.
- [docs/message-browser.md](./docs/message-browser.md) — group-free bounded record Fetch and byte-safe inspector.
- [Kafka UX v3](./docs/design/kafka-ux-v3-proposal.md) — accepted page/navigation design and complete
  delivery sequence.
- [SPEC-004](./docs/specs/004-resource-navigation-cluster-context.md) — verified contract for
  resource pages and selected Kafka cluster context.
- [Spec index](./docs/specs/README.md) — globally unique requirements, lifecycle and verification
  status.

## License

[MIT](./LICENSE) — © 2025-2026 Freelens Authors.
