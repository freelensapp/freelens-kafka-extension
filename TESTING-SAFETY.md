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
  **relay-pod** strategy (P3.2 i), which creates a Pod → **`kind` only** in autonomous runs.
- **Changing the active kube context** to a real cluster for a mutating step.

## Practice

- Anything that **writes** is verified on the local **`kind`** cluster only. Test-created fixtures
  are disposable and must be torn down afterwards, but the `kind` / `kind-kind` cluster itself is
  persistent user-owned infrastructure. Never run `kind delete cluster --name kind` or remove its
  context unless the user explicitly requests it. Cleanup may stop `kind-control-plane`, but must
  preserve unrelated add-ons such as Metrics Server and kube-prometheus-stack.
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
- Autonomous MCP sessions MUST use `pnpm mcp:app`, which creates a one-context kubeconfig. Default is
  exactly `kind-kind`.
- A user-approved real context requires both its exact name and `ALLOW_REAL_READ_ONLY=1`; all actions
  remain read-only under this document.
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
