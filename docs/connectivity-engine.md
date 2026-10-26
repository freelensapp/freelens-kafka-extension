# Feature: Connectivity Engine (P1)

> **Status:** built & validated (unit tests + local end-to-end). Main-side,
> framework-agnostic. Lives in [`../src/main/kafka/`](../src/main/kafka). See the architecture in
> [`../ARCHITECTURE.md`](../ARCHITECTURE.md) (§4–§6).

## What it does

The connectivity engine is the Main-process core that lets the extension talk to
a Kafka cluster reachable only from _inside_ Kubernetes. It opens a per-broker
port-forward, builds an `advertised → local` address map, and wires a kafkajs
client through a redirect `socketFactory` so that Kafka's bootstrap →
`advertised.listeners` handshake resolves to reachable local endpoints — **without
rewriting the Kafka wire protocol**.

It is deliberately independent of Freelens APIs so it can be unit-tested in
isolation and reused by the extension's Main process in P3.

## Module map

| File                                                                                  | Role                                                                                                                       |
| ------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| [`src/main/kafka/types.ts`](../src/main/kafka/types.ts)                               | `BrokerRef`, `PodPort`, `AddressMap`.                                                                                      |
| [`src/main/kafka/redirect.ts`](../src/main/kafka/redirect.ts)                         | `resolveTarget` (pure) + `createRedirectSocketFactory` for kafkajs.                                                        |
| [`src/main/kafka/port-forward-manager.ts`](../src/main/kafka/port-forward-manager.ts) | `PortForwardManager` + the `Forwarder` seam. Opens one local listener per broker, builds the map.                          |
| [`src/main/kafka/kubectl-tunnels.ts`](../src/main/kafka/kubectl-tunnels.ts)           | `KubectlTunnels` — one `kubectl port-forward` per pod and port, run with the host's kubectl and environment (Freelens v2). |
| [`src/main/kafka/tunnel-forwarder.ts`](../src/main/kafka/tunnel-forwarder.ts)         | `createTunnelForwarder` — the extension's `Forwarder`: pipes each socket to the kubectl tunnel of its pod.                 |
| [`src/main/tunnel-host.ts`](../src/main/tunnel-host.ts)                               | What the tunnels take of Freelens: `Main.Shell` and the catalog status of the cluster.                                     |
| [`test/e2e/kube-forwarder.ts`](../test/e2e/kube-forwarder.ts)                         | `createKubeForwarder` — the itests' `Forwarder` over `@kubernetes/client-node` SPDY port-forward.                          |
| [`src/main/kafka/kafka-connection.ts`](../src/main/kafka/kafka-connection.ts)         | `KafkaConnection` — orchestrates forwards + socketFactory + kafkajs; lifecycle owner.                                      |
| [`src/main/kafka/external-credentials.ts`](../src/main/kafka/external-credentials.ts) | Applies automatic/override TLS, PLAIN, SCRAM and mTLS profiles to Direct connections.                                      |
| [`src/main/ipc.ts`](../src/main/ipc.ts)                                               | Wires the engine to the renderer over `Main.Ipc`.                                                                          |

## Public API

- **`KafkaConnection.connect(options): Promise<KafkaConnection>`** — opens port-forwards and returns a live connection.
  - `options.brokers: BrokerRef[]` — brokers to reach, each mapped to its backing pod.
  - `options.bootstrap: string` — advertised `"host:port"` used to bootstrap.
  - `options.forwarder: Forwarder` — how local sockets reach pods (`createTunnelForwarder(...).forward` in the extension).
  - `options.sasl?`, `options.ssl?` — passed to kafkajs.
  - `.overview()` → `{ brokers, controller, topics }`; `.admin()`; `.client` (kafkajs); `.disconnect()`;
    `.onDisconnect(listener)` (the extension closes the session's tunnels there).
- **`createTunnelForwarder({ tunnels, clusterId, onPhase?, onError? })`** — `{ forward, open, close() }`: one kubectl
  per pod and port while it runs, a new one after it ends; `close()` kills them all.
- **`new KubectlTunnels(host)`** — `.open(clusterId, pod, signal, onPhase?)`, `.closeAll()`; kubectl is killed when the
  signal aborts, on `closeAll` (deactivation), on process exit and when the cluster leaves the connected state.
- **`createKubeForwarder({ kubeConfigPath?, context? })`** (itests only) — client-node on an explicit kubeconfig.
- **`PortForwardManager(forwarder, host?)`** — `.open(brokers) → AddressMap`, `.closeAll()`.
- **`createRedirectSocketFactory(map, onRedirect?)`** / **`resolveTarget(map, host, port)`**.

### The `Forwarder` seam

```ts
type Forwarder = (target: PodPort, socket: Duplex) => void;
```

`PortForwardManager` depends on a `Forwarder`, not on Kubernetes directly. The extension uses
`createTunnelForwarder` (kubectl through `Main.Shell`, SPEC-019); the kind itests use `createKubeForwarder` (SPDY);
the unit tests and the local e2e inject a TCP pipe. This keeps the manager unit-testable
without a cluster and is the single place the real port-forward is performed.

## Data flow

`connect` → open one port-forward per broker pod → build `advertised → 127.0.0.1:localPort`
map → create kafkajs client whose `socketFactory` redirects via that map → operations.
Full sequence in [`../ARCHITECTURE.md`](../ARCHITECTURE.md) §5.

## Usage (production, inside the extension Main process)

```ts
import { KafkaConnection } from "../src/main/kafka/kafka-connection";
import { KubectlTunnels } from "../src/main/kafka/kubectl-tunnels";
import { createTunnelForwarder } from "../src/main/kafka/tunnel-forwarder";
import { createTunnelHost } from "../src/main/tunnel-host";

// One KubectlTunnels for the extension (closeAll on deactivation), one forwarder per session.
const tunnels = new KubectlTunnels(createTunnelHost(() => extension.manifestPath));
const forwarder = createTunnelForwarder({ tunnels, clusterId });

const connection = await KafkaConnection.connect({
  clientId: "freelens-kafka",
  brokers: [
    {
      advertisedHost: "my-cluster-broker-0.my-cluster-kafka-brokers.kafka.svc",
      advertisedPort: 9092,
      namespace: "kafka",
      pod: "my-cluster-broker-0",
      containerPort: 9092,
    },
  ],
  bootstrap: "my-cluster-broker-0.my-cluster-kafka-brokers.kafka.svc:9092",
  forwarder: forwarder.forward,
});
connection.onDisconnect(() => forwarder.close());

const overview = await connection.overview(); // { brokers, controller, topics }
// ... producer/consumer via connection.client ...
await connection.disconnect(); // closes the local listeners and, through onDisconnect, the kubectl tunnels
```

> Broker→pod mapping (`BrokerRef`) is supplied by the caller in P1. Auto-discovery
> (Strimzi `Kafka` CR / `Service` / app env) and credentials-from-`Secret` arrive in **P2**.

## Testing

```bash
cd engine
pnpm install
pnpm typecheck            # tsc --noEmit
pnpm test                 # vitest: resolveTarget + PortForwardManager (no cluster)

# Local end-to-end (Docker; the kube SPDY tunnel is replaced by a TCP pipe):
pnpm kafka:up             # starts the ../poc broker advertising kafka-internal:9092
pnpm itest                # PortForwardManager → map → socketFactory → kafkajs (produce+consume)
pnpm kafka:down

# Real-cluster end-to-end (kind; the actual @kubernetes/client-node SPDY port-forward):
pnpm lab:up               # once: the lab cluster freelens-kafka-lab (.lab/kubeconfig), image loaded
pnpm kind:up              # apply test/e2e/fixtures/kafka-kind.yaml on the lab + wait Ready
pnpm itest:kind           # createKubeForwarder → real port-forward → kafkajs (produce+consume)
pnpm kind:down

# Local security matrix (metadata reads only):
pnpm kafka:auth:up
pnpm itest:security       # PLAINTEXT, TLS, PLAIN, SCRAM-256, SCRAM-512
pnpm kafka:auth:down
```

- **Unit** (`test/redirect.test.ts`, `test/port-forward-manager.test.ts`): pure logic + manager wiring.
- **Local e2e** (`test/integration.local.ts`): the full chain against a real broker that advertises an
  unreachable internal address; the port-forward is dependency-injected as a TCP pipe.
- **Real-cluster e2e** (`test/integration.kind.ts`): `createKubeForwarder` SPDY port-forward to an in-cluster pod on
  kind, then produce+consume. The extension's kubectl path is verified on Freelens v2 dev against the same kind lab.

## Status & limitations

- ✅ Manager, address map, socketFactory and `KafkaConnection` validated (unit + local e2e).
- ✅ The kubectl tunnels (`Main.Shell`) validated on Freelens v2 dev against kind: overview, brokers and topics through
  the tunnel; no kubectl left after the cluster disconnects or the extension is disabled; a new one after reconnecting.
- ✅ `createKubeForwarder` (real `@kubernetes/client-node` SPDY, itests) **validated against a live kind cluster**
  — produce+consume through an in-cluster pod advertising an unreachable internal address (`pnpm kind:up && pnpm itest:kind`).
- ✅ Discovery, Strimzi + external credentials, Direct/port-forward strategy and two-phase connect are wired into Freelens and verified (see [discovery.md](./discovery.md)).
