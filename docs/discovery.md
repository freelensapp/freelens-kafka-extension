# Feature: Discovery & Credentials (P2)

> **Status:** built & validated (unit tests + real kind API reads); **universal discovery** (non-Strimzi
> Services + external/managed Kafka from workload config) wired into the Overview and verified in
> Freelens 1.10.3. Main-side, framework-agnostic. In [`../src/main/kafka/`](../src/main/kafka).
> Architecture: [`../ARCHITECTURE.md`](../ARCHITECTURE.md) §6.4–§6.5.

## What it does

Turns "a connected Kubernetes cluster" into the inputs the P1 engine needs —
**automatically**, instead of hand-written `BrokerRef[]`/credentials:

- **Discovery** finds Kafka clusters: Strimzi `Kafka` CRs (with their broker pods) and, best-effort, generic Services exposing Kafka ports.
- **Workload-config discovery** finds **external/managed** Kafka (MSK, Confluent, Aiven, Redpanda, …) that the cluster's own workloads reference in their env / `ConfigMap`s / `Secret`s.
- **Merge** (`discoverAllKafkas`) unions every source for the Overview and de-dups in-cluster overlaps.
- **Credential resolution** reads SASL/SCRAM and TLS/mTLS material from Kubernetes `Secret`s.

## Module map

| File                                                                                  | Role                                                                                                                                |
| ------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| [`src/main/kafka/kube-reader.ts`](../src/main/kafka/kube-reader.ts)                   | `KubeReader` seam (`listCustomResources`/`listPods`/`listServices`/`listWorkloads`/`getConfigMap`/`getSecret`) + client-node impl.  |
| [`src/main/kafka/discovery.ts`](../src/main/kafka/discovery.ts)                       | `discoverStrimziKafkas`, `discoverKafkaServices`, `discoverWorkloadKafkas`, `discoverAllKafkas`, `classifyProvider` + pure parsers. |
| [`src/main/kafka/credentials.ts`](../src/main/kafka/credentials.ts)                   | `resolveStrimziCredentials` + pure `parseClusterCa`/`parseScram`/`parseMtls`.                                                       |
| [`src/main/kafka/external-credentials.ts`](../src/main/kafka/external-credentials.ts) | External workload env/Secret TLS, PLAIN, SCRAM and mTLS resolver + override merger.                                                 |
| [`src/main/kafka/workload-environment.ts`](../src/main/kafka/workload-environment.ts) | Cached/selective env, `valueFrom` and `envFrom` resolver; keeps Secret values in Main.                                              |

## The `KubeReader` seam

```ts
interface KubeReader {
  listCustomResources(ref: CustomResourceRef, namespace?): Promise<KubeObject[]>;
  listPods(namespace, labelSelector?): Promise<KubeObject[]>;
  listServices(namespace?): Promise<KubeObject[]>;
  listWorkloads(namespace?): Promise<KubeObject[]>; // Deployments + StatefulSets + DaemonSets
  getConfigMap(namespace, name): Promise<KubeConfigMap | null>;
  getSecret(namespace, name): Promise<KubeSecret | null>;
}
```

Two implementations satisfy the seam (unit tests inject an in-memory fake):

- **`createCatalogKubeReader(clusterId)`** ([`src/main/catalog-kube-reader.ts`](../src/main/catalog-kube-reader.ts))
  — the default inside Freelens. Reads through the **connected** cluster via `Main.K8s.queryCluster` /
  `Main.K8s.getResource`. This is required because the catalog's raw kubeconfig points at the Freelens
  proxy, which raw `@kubernetes/client-node` cannot reach (`ECONNREFUSED 127.0.0.1:<proxy>`).
- **`createKubeReader({ kubeConfigPath?, context? })`** — direct `@kubernetes/client-node` against a real
  kubeconfig. Used by the KinD e2e tests and when the request carries an explicit kubeconfig.

`CustomResourceRef` is `{ group, version, plural, kind }`: client-node needs the `plural`, `Main.K8s`
needs the `kind`. All parsing is pure and separated from I/O for testability.


## Strimzi discovery

`discoverStrimziKafkas(reader, namespace?)` → `DiscoveredKafka[]`:

- Lists `kafka.strimzi.io/v1beta2` `Kafka` CRs.
- Chooses an **internal** listener (plaintext first, else TLS); `bootstrap` from `status.listeners[].bootstrapServers`.
- Lists broker pods via `strimzi.io/cluster=<name>`, **excludes** `strimzi.io/broker-role=false` (controllers), and derives `brokerId` from the pod-name suffix — sorted.

Result: `{ source, name, namespace, bootstrap, tls, port, listeners, brokerPods: [{ brokerId, pod }] }`.

## Service discovery (best-effort, precision-tuned)

`discoverKafkaServices(reader, namespace?)` → `KafkaServiceCandidate[]`. A port qualifies **only with a
Kafka signal**: a kafka/`tcp-clients`-named port, or 9092/9093 on a kafka-named service. Port 9093 alone is
**not** enough — it collides with Prometheus Alertmanager (a real false positive caught on kind).

## Universal discovery (workload-config)

`discoverWorkloadKafkas(reader, namespace?)` → `WorkloadKafkaRef[]`: scans `Deployment`/`StatefulSet`/
`DaemonSet` containers for Kafka **bootstrap** configuration and resolves it to endpoints — the way to
surface **external/managed** Kafka (MSK, Confluent, …) that has no in-cluster object of its own:

- **Keys** (`isBootstrapKey`): anything ending `bootstrap[._-]servers?` (`KAFKA_BOOTSTRAP_SERVERS`,
  `bootstrap.servers`, `spring.kafka.bootstrap-servers`, …) or `kafka[._-]brokers?`.
- **Sources**: inline `env.value`, `env.valueFrom.configMapKeyRef`/`secretKeyRef`, and whole-object
  `envFrom.configMapRef`/`secretRef` (Secret values base64-decoded; lookups cached per scan).
- **Classification** (`classifyProvider`): MSK `*.kafka.*.amazonaws.com`, Confluent `*.confluent.cloud`,
  Aiven `*.aivencloud.com`, Redpanda, Upstash → **external**; `.svc`/`.local`/bare names → **in-cluster**.
- **Aggregation**: entries sharing a bootstrap collapse into one row listing the referencing workloads
  (`referencedBy: ["<ns>/<kind>/<name>"]`).

`discoverAllKafkas(reader, namespace?)` unions Strimzi + Service + workload results, dropping in-cluster
duplicates (a Strimzi cluster's own `Service`s by owner-name prefix, and any candidate whose bootstrap
`host:port` is already listed). The `discover` IPC returns this and the Overview renders a **Source**
column. `listWorkloads`/`listServices` list **cluster-wide** (namespace omitted) via `Main.K8s`.

> **Verified on Freelens 1.10.3.** A kind cluster with pods referencing MSK (via `env` **and** an
> `envFrom` `ConfigMap`) plus a non-Strimzi `redpanda` `Service` surfaced both an `MSK · external` row and
> an `In-cluster` row in the real app, with no Strimzi CR installed — reproduce with `pnpm kind:univ:up`
> ([`test/e2e/fixtures/workload-external-kind.yaml`](../test/e2e/fixtures/workload-external-kind.yaml)).

## Credentials

`resolveStrimziCredentials(reader, { namespace, clusterName, tls?, user?, mechanism? })` → `{ sasl?, ssl? }`:

- **TLS** → cluster CA from `Secret <cluster>-cluster-ca-cert` (`ca.crt`) → `ssl.ca`.
- **SCRAM user** → `KafkaUser` `Secret` `password` → `sasl { mechanism, username, password }`.
- **mTLS user** → `Secret` `user.crt` + `user.key` (+ `ca.crt`) → `ssl { cert, key, ca }`.

### External workload credentials (P3.2 h)

`resolveExternalCredentials(reader, { bootstrap })` first resolves **only bootstrap keys** across
workloads; full env/ConfigMap/Secret values are read only for the matching container. This avoids
touching unrelated explicit Secret refs and keeps the cluster-wide scan bounded.

Recognized profiles: no-auth PLAINTEXT, TLS without auth, SASL/PLAIN (incl. Confluent API key/secret),
SCRAM-SHA-256/512, Spring `sasl.jaas.config`, and mTLS with inline PEM CA/cert/key. Discovery returns
only the non-secret `{ tls, auth }` hint. Connection Settings can override TLS/auth for one connection; the
password is internal IPC payload and volatile renderer state, never `localStorage`.

Limitations: MSK IAM/OAUTHBEARER is deferred. Certificate **file paths inside pods** are not read
(that would require pod exec); PEM exposed through Secret-backed env is supported.

## Usage

```ts
import { resolveStrimziCredentials } from "../src/main/kafka/credentials";
import { discoverStrimziKafkas } from "../src/main/kafka/discovery";
import { createKubeReader } from "../src/main/kafka/kube-reader";

const reader = createKubeReader({ kubeConfigPath, context }); // from Main.Catalog
const [kafka] = await discoverStrimziKafkas(reader, "kafka");
const creds = await resolveStrimziCredentials(reader, {
  namespace: kafka.namespace,
  clusterName: kafka.name,
  tls: kafka.tls,
  user: "my-user", // optional
});
// kafka.bootstrap, kafka.brokerPods and creds.{sasl,ssl} feed the connect flow (see §6.5).
```

## Testing

```bash
cd engine
pnpm test                 # vitest: discovery + credentials parsers (fakes, no cluster)

# Real-API reads on kind (fake Strimzi CRD + fixtures; pods need not run):
pnpm kind:disc:up         # apply CRD (wait Established) + fixtures
pnpm itest:disc:kind      # createKubeReader → discover + resolve, asserted against the fixtures
pnpm kind:disc:down       # remove everything immediately
```

- **Unit** (`test/discovery.test.ts`, `test/credentials.test.ts`): parser correctness incl. the Alertmanager `:9093` exclusion.
- **Real-API e2e** (`test/integration.discovery.kind.ts`): validates the client-node reads (CustomObjects/Pods/Secrets/Services) against a live API server.

## Connecting the discovered cluster (P2.5)

`connectDiscovered({ discovered, forwarder, sasl?, ssl? })` ([`src/main/kafka/connect-discovered.ts`](../src/main/kafka/connect-discovered.ts))
turns a `DiscoveredKafka` into a live `KafkaConnection` in two phases, without guessing advertised DNS:

1. **Metadata** — port-forward one seed broker pod and `describeCluster` to learn each broker's authoritative advertised `host:port`.
2. **Connect** — `matchBrokersToPods` maps each broker to its pod (advertised-host prefix, else `nodeId == brokerId`), opens a per-broker forward, and connects via the exact redirect.

Validated on kind (`pnpm kind:p25:up && pnpm itest:p25:kind`) against a broker advertising pod-based DNS.

## Status & limitations

- ✅ Strimzi discovery, Service discovery and credential resolution validated (unit + kind).
- ✅ **Two-phase connect** (`connectDiscovered`) validated (unit + kind e2e) — metadata-driven broker→pod matching + per-broker forwards.
- ✅ **Universal discovery** — workload-config (env/`ConfigMap`/`Secret`) discovery of external/managed Kafka (MSK, Confluent, …) plus Service discovery, merged by `discoverAllKafkas`, wired into the Overview and verified in Freelens 1.10.3 (P3.2 f).
- ✅ **Reachability + Direct connect** — a `reachability` TCP probe marks each cluster reachable-from-PC, and PC-reachable non-Strimzi clusters (MSK public / VPN) connect via the **Direct** strategy (`kafkajs` straight to the bootstrap, no port-forward); Overview is enabled for them (P3.2 g, verified 1.10.3).
- ✅ **External credentials** — automatic Main-side workload/Secret resolution + session-only Connection Settings override for TLS/no-auth, PLAIN, SCRAM-256/512 and mTLS (P3.2 h); real protocol + Freelens UI E2E verified.
- ✅ **Manual endpoints** — bootstrap + TLS/SASL endpoints that Kubernetes does not reference can be added, persisted without passwords, probed and Direct-connected from the Overview (P3.2 j core + h).
- ⛔ **Relay** connect for pods-only external Kafka (P3.2 i), MSK IAM/OAUTHBEARER and pod-internal credential files are not implemented yet.
