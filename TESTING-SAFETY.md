# Testing safety — non-negotiable rules for autonomous testing

> **Commandment (user, 2026-07-21, refined).** In autonomous testing the agent may perform
> **read-only** operations against explicitly user-authorized real clusters, including managed
> Kafka reachable over a private network, but must **never write or modify anything**: not messages, not topics,
> not configs, not any cluster data or characteristic. Any cluster may be **production**; when
> unsure, treat it as production and only read.

## Allowed — read-only, even on authorized real clusters

- **Discovery / detection** of clusters (Strimzi CRs, Services, workload-config → MSK / Confluent / …).
- **Reachability probing** — a short TCP connect to a broker `host:port` (no data written).
- **Port-forward** to reach a broker for reading.
- **Connecting** and reading **metadata**: brokers, controller, **topics** (count, partitions,
  replication, ISR, configs), consumer groups and **lag**.
- **Reading messages** — only **without committing offsets** (ephemeral / random group id, no
  commit) and without changing any server-side state.

## Forbidden — writes / mutations on any real cluster

- **Producing** messages; **creating / deleting / altering** topics or partitions; **altering
  configs**; **deleting records**; **committing** consumer-group offsets; creating / altering **ACLs**.
- Creating / modifying / deleting **any Kubernetes resource** on a real cluster — including the
  **relay-pod** strategy (P3.2 i), which creates a Pod → **the lab only** in autonomous runs.
- **Changing the active kube context** to a real cluster for a mutating step.

## Practice

- Anything that **writes** is verified only on the **labs the repository's scripts create**
  (SPEC-019, REQ-232):
  - the kind cluster `freelens-kafka-lab`, whose kubeconfig is the file `.lab/kubeconfig`
    (`pnpm lab:up`, `pnpm lab:apply <profile>`, `pnpm lab:down`; `scripts/lab.sh`);
  - the demo cluster `freelens-kafka-demo`, with `.demo/kubeconfig` (`pnpm demo:up` / `demo:down`);
  - the docker-compose brokers of `test/e2e/` on loopback ports.

  The scripts and tests read only those kubeconfig files, refuse any other context and any API
  server that is not local, and never read or change the developer's kubeconfig. The developer's
  own clusters, the `kind` cluster included, are never used. Test-created fixtures are torn down
  afterwards; `lab:down` and `demo:down` leave no cluster, container, network or volume behind.
- Read-only features (discovery, reachability, topic / group listing, metadata) may be validated
  against a real cluster or managed Kafka service **when the user points to a specific target** — never by
  guessing which context is safe.
- Real-target performance probes additionally require caller-supplied SHA-256 pins in
  `AUTHORIZED_KUBE_CONTEXT_SHA256` and `AUTHORIZED_KAFKA_TARGET_HOST_SHA256`. No environment-derived
  identity or pin may be committed.
- If a needed check would write to a real cluster, **stop and ask the user first.**

## Playwright MCP / CDP

- Playwright MCP is **not a security boundary** and can control every page/context visible to the
  attached Freelens process.
- Autonomous MCP sessions MUST use `pnpm mcp:app`, which gives Freelens a one-context copy of a
  kubeconfig in a private runtime. By default it is the lab's (`.lab/kubeconfig`, context
  `kind-freelens-kafka-lab`); `MCP_KUBECONFIG=.demo/kubeconfig` with `kind-freelens-kafka-demo`
  selects the demo cluster. Any other file, or a context whose API server is not local, is refused.
- A user-approved real context requires both its exact name and `ALLOW_REAL_READ_ONLY=1` (the context
  is then copied, read-only, from `MCP_KUBECONFIG` or the developer's kubeconfig, which is never
  written); all actions remain read-only under this document.
- Cluster rows MUST be selected by exact allowlisted text/id. Never click the first row or use broad
  matching when multiple contexts are visible.
- Temporary MCP kubeconfig/app data MUST remain private and be removed by the launcher watchdog;
  retaining it requires an explicit `FREELENS_MCP_RUNTIME_DIR` override.
- Do not export storage state, Secret values, credentials or sensitive request bodies into MCP output.
- MCP findings do not replace deterministic tests; promote them before closing the iteration.

## Why

Developer kubeconfigs can hold multiple real EKS, AKS and private-network contexts, and any of them
may be **production**. Reading explicitly authorized discovery, reachability, topic and group metadata
is safe and useful; **writing** — produce, topic/config changes, offset commits, or creating a pod —
is not. A reachable managed Kafka target may be used for authorized **read** validation; it is never
a place to write.
