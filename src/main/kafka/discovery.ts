import { firstBrokerAddress, splitBootstrap } from "../../common/reachability";
import { parseWorkloadSecurityEnvironment } from "./external-credentials";
import {
  createWorkloadEnvironmentResolver,
  forEachConcurrent,
  isBootstrapKey,
  workloadContainers,
  workloadLabel,
} from "./workload-environment";

import type { KafkaSecurityHint, KafkaWorkloadSourceLocator } from "../../common/ipc";
import type { KubeObject, KubeReader } from "./kube-reader";
import type { KafkaProgressReporter } from "./progress";

export interface KafkaListenerInfo {
  name: string;
  port: number;
  tls: boolean;
  type?: string;
}

/** A Kafka cluster discovered from Strimzi resources in the connected cluster. */
export interface DiscoveredKafka {
  source: "strimzi";
  name: string;
  namespace: string;
  /** Advertised internal bootstrap `"host:port"` (from the Kafka CR status). */
  bootstrap: string;
  tls: boolean;
  /** The internal listener port chosen for the bootstrap. */
  port: number;
  listeners: KafkaListenerInfo[];
  /** Broker pods backing this cluster, sorted by broker id. */
  brokerPods: { brokerId: number; pod: string }[];
}

/** A generic (non-Strimzi) Kafka candidate found via a Service exposing 9092/9093. */
export interface KafkaServiceCandidate {
  source: "service";
  name: string;
  namespace: string;
  bootstrap: string;
  port: number;
  tls: boolean;
}

const STRIMZI_GROUP = "kafka.strimzi.io";
const STRIMZI_VERSION = "v1beta2";
const STRIMZI_KIND = "Kafka";
const STRIMZI_PLURAL = "kafkas";
const KAFKA_PORTS = new Set([9092, 9093]);

function securityHintStrength(hint?: KafkaSecurityHint): number {
  if (!hint) return 0;
  if (hint.auth === "mtls") return 4;
  if (hint.auth !== "none") return 3;
  return hint.tls ? 2 : 1;
}

/** Broker id = trailing numeric suffix of the pod name (`my-cluster-kafka-3` -> 3). */
export function extractBrokerId(podName: string): number | null {
  const match = /-(\d+)$/.exec(podName);
  return match ? Number(match[1]) : null;
}

/** Parse a Strimzi `Kafka` CR + its pods into a {@link DiscoveredKafka}. Pure. */
export function parseStrimziKafka(cr: KubeObject, pods: KubeObject[]): DiscoveredKafka | null {
  const name = cr.metadata?.name;
  const namespace = cr.metadata?.namespace;
  if (!name || !namespace) return null;

  const specListeners: KafkaListenerInfo[] = (cr.spec?.kafka?.listeners ?? []).map((l: any) => ({
    name: String(l.name),
    port: Number(l.port),
    tls: Boolean(l.tls),
    type: l.type != null ? String(l.type) : undefined,
  }));

  const statusListeners: any[] = cr.status?.listeners ?? [];
  const bootstrapFor = (listenerName: string): string | undefined =>
    statusListeners.find((s) => (s.name ?? s.type) === listenerName)?.bootstrapServers;

  // Prefer an internal listener, plaintext first (simplest), then TLS.
  const internal = specListeners.filter((l) => (l.type ?? "internal") === "internal");
  const chosen = internal.find((l) => !l.tls) ?? internal[0] ?? specListeners.find((l) => !l.tls) ?? specListeners[0];
  if (!chosen) return null;
  const bootstrap = bootstrapFor(chosen.name);
  if (!bootstrap) return null;

  const brokerPods = pods
    .filter((p) => p.metadata?.labels?.["strimzi.io/broker-role"] !== "false")
    .map((p) => ({
      pod: p.metadata?.name ?? "",
      brokerId: extractBrokerId(p.metadata?.name ?? ""),
    }))
    .filter((b): b is { pod: string; brokerId: number } => b.pod !== "" && b.brokerId !== null)
    .sort((a, b) => a.brokerId - b.brokerId);

  return {
    source: "strimzi",
    name,
    namespace,
    bootstrap,
    tls: chosen.tls,
    port: chosen.port,
    listeners: specListeners,
    brokerPods,
  };
}

/** Discover all Strimzi Kafka clusters (optionally within a namespace). */
export async function discoverStrimziKafkas(reader: KubeReader, namespace?: string): Promise<DiscoveredKafka[]> {
  const crs = await reader.listCustomResources(
    { group: STRIMZI_GROUP, version: STRIMZI_VERSION, plural: STRIMZI_PLURAL, kind: STRIMZI_KIND },
    namespace,
  );
  const discovered: DiscoveredKafka[] = [];
  for (const cr of crs) {
    const ns = cr.metadata?.namespace;
    const name = cr.metadata?.name;
    if (!ns || !name) continue;
    const pods = await reader.listPods(ns, `strimzi.io/cluster=${name}`);
    const parsed = parseStrimziKafka(cr, pods);
    if (parsed) discovered.push(parsed);
  }
  return discovered;
}

/** Parse Services into generic Kafka candidates. Pure.
 *
 * Heuristic tuned for precision: a port qualifies only with a Kafka signal —
 * a kafka/`tcp-clients`-named port, or port 9092/9093 on a kafka-named service.
 * (Port 9093 alone is ambiguous, e.g. Prometheus Alertmanager.)
 */
export function parseKafkaServices(services: KubeObject[]): KafkaServiceCandidate[] {
  const candidates: KafkaServiceCandidate[] = [];
  for (const svc of services) {
    const name = svc.metadata?.name;
    const namespace = svc.metadata?.namespace;
    if (!name || !namespace) continue;
    const serviceIsKafka = /kafka/i.test(name);
    for (const p of (svc.spec?.ports ?? []) as any[]) {
      const port = Number(p.port);
      const portName = typeof p.name === "string" ? p.name : "";
      const portNameIsKafka = /kafka|tcp-clients/i.test(portName);
      if (portNameIsKafka || (KAFKA_PORTS.has(port) && serviceIsKafka)) {
        candidates.push({
          source: "service",
          name,
          namespace,
          bootstrap: `${name}.${namespace}.svc:${port}`,
          port,
          tls: port === 9093,
        });
      }
    }
  }
  return candidates;
}

/** Discover generic Kafka candidates from Services (optionally within a namespace). */
export async function discoverKafkaServices(reader: KubeReader, namespace?: string): Promise<KafkaServiceCandidate[]> {
  return parseKafkaServices(await reader.listServices(namespace));
}

/** A Kafka referenced by a workload's config (env / ConfigMap / Secret) — typically external/managed. */
export interface WorkloadKafkaRef {
  source: "workload";
  /** The first bootstrap host (without port) — a stable, human-readable identity. */
  name: string;
  /** Namespace of the referencing workload (the first one, when referenced from several). */
  namespace: string;
  bootstrap: string;
  port: number;
  tls: boolean;
  /** `"MSK"` | `"Confluent"` | `"Aiven"` | `"Redpanda"` | `"Upstash"` | `"In-cluster"` | `"External"`. */
  provider: string;
  /** Whether the bootstrap host is outside the cluster (a managed/remote Kafka). */
  external: boolean;
  /** Workloads that reference it, e.g. `["apps/deploy/orders"]`. */
  referencedBy: string[];
  /** Non-secret TLS/auth mode inferred from the same workload container. */
  securityHint?: KafkaSecurityHint;
  /** Non-secret coordinates for targeted credential refresh. */
  sourceLocator?: KafkaWorkloadSourceLocator;
}

/** Any Kafka surfaced by discovery, regardless of source. */
export type AnyDiscoveredKafka = DiscoveredKafka | KafkaServiceCandidate | WorkloadKafkaRef;

export { isBootstrapKey } from "./workload-environment";

/** Classify a bootstrap endpoint into a provider label + whether it is external to the cluster. Pure. */
export function classifyProvider(bootstrap: string): { provider: string; external: boolean } {
  const h = firstBrokerAddress(bootstrap).host.toLowerCase();
  if (/\.amazonaws\.com$/.test(h) && h.includes("kafka")) return { provider: "MSK", external: true };
  if (/\.confluent\.cloud$/.test(h)) return { provider: "Confluent", external: true };
  if (/\.aivencloud\.com$/.test(h)) return { provider: "Aiven", external: true };
  if (/\.redpanda\.com$/.test(h)) return { provider: "Redpanda", external: true };
  if (/\.upstash\.io$/.test(h)) return { provider: "Upstash", external: true };
  const inCluster = h === "" || !h.includes(".") || /\.svc(\.cluster\.local)?$/.test(h) || h.endsWith(".local");
  return inCluster ? { provider: "In-cluster", external: false } : { provider: "External", external: true };
}

/** TLS is not stated by a bootstrap string alone; infer from the provider and the common port conventions. */
function guessTls(provider: string, port: number): boolean {
  if (provider === "Confluent" || provider === "Aiven" || provider === "Upstash") return true;
  return port === 9093 || port === 9094 || port === 9096 || port === 9098;
}

/**
 * Discover external/managed (and other non-Strimzi) Kafka referenced by workload configuration:
 * scan Deployment/StatefulSet/DaemonSet containers for bootstrap keys in inline env, `valueFrom`
 * ConfigMap/Secret refs, and whole-ConfigMap/Secret `envFrom` imports. Entries sharing a bootstrap
 * are collapsed, aggregating the referencing workloads.
 */
export interface WorkloadScanProgress {
  completed: number;
  total: number;
}

export async function discoverWorkloadKafkas(
  reader: KubeReader,
  namespace?: string,
  onProgress?: (progress: WorkloadScanProgress) => void,
): Promise<WorkloadKafkaRef[]> {
  const workloads = await reader.listWorkloads(namespace);
  const environments = createWorkloadEnvironmentResolver(reader);
  let completed = 0;
  onProgress?.({ completed, total: workloads.length });

  const byKey = new Map<string, WorkloadKafkaRef>();
  const record = (
    raw: string,
    workload: KubeObject,
    containerName: string | undefined,
    securityHint?: KafkaSecurityHint,
  ): void => {
    const bootstrap = raw.trim();
    if (!bootstrap) return;
    const { host, port } = firstBrokerAddress(bootstrap);
    const key = `${host}:${port}`.toLowerCase();
    const label = workloadLabel(workload);
    const sourceLocator: KafkaWorkloadSourceLocator = {
      namespace: workload.metadata?.namespace ?? "default",
      kind: workload.kind ?? "Workload",
      name: workload.metadata?.name ?? "?",
      ...(containerName ? { container: containerName } : {}),
    };
    const existing = byKey.get(key);
    if (existing) {
      if (!existing.referencedBy.includes(label)) existing.referencedBy.push(label);
      if (securityHint && securityHintStrength(securityHint) > securityHintStrength(existing.securityHint)) {
        existing.securityHint = securityHint;
        existing.tls = securityHint.tls;
        existing.sourceLocator = sourceLocator;
      }
      return;
    }
    const { provider, external } = classifyProvider(bootstrap);
    byKey.set(key, {
      source: "workload",
      name: host || bootstrap,
      namespace: workload.metadata?.namespace ?? "",
      bootstrap,
      port,
      tls: securityHint?.tls ?? guessTls(provider, port),
      provider,
      external,
      referencedBy: [label],
      securityHint,
      sourceLocator,
    });
  };

  await forEachConcurrent(workloads, 6, async (workload) => {
    for (const container of workloadContainers(workload)) {
      const bootstrapEnvironment = await environments.resolve(workload, container, isBootstrapKey);
      const bootstraps = Object.values(bootstrapEnvironment).filter(Boolean);
      if (bootstraps.length === 0) continue;

      const environment = await environments.resolve(workload, container);
      const parsedSecurity = parseWorkloadSecurityEnvironment(environment);
      const securityHint = parsedSecurity.detected ? parsedSecurity.hint : undefined;
      for (const bootstrap of bootstraps) record(bootstrap, workload, container.name, securityHint);
    }
    completed += 1;
    onProgress?.({ completed, total: workloads.length });
  });

  return [...byKey.values()];
}

/** Normalized `host:port` keys of a bootstrap, for cross-source de-duplication. */
function bootstrapKeySet(bootstrap: string): Set<string> {
  return new Set(splitBootstrap(bootstrap).map((hostPort) => hostPort.toLowerCase()));
}

function overlaps(bootstrap: string, known: Set<string>): boolean {
  for (const k of bootstrapKeySet(bootstrap)) if (known.has(k)) return true;
  return false;
}

/**
 * Discover every Kafka reachable from the connected cluster across all sources — Strimzi CRs,
 * in-cluster Services, and external/managed clusters referenced by workload config. In-cluster
 * duplicates (a Strimzi cluster's own Services, or a workload pointing at an already-listed
 * bootstrap) are collapsed. Secondary sources fail soft so a partial RBAC grant still lists what
 * it can; the primary Strimzi read still surfaces real cluster errors.
 */
export async function discoverAllKafkas(
  reader: KubeReader,
  namespace?: string,
  report?: KafkaProgressReporter,
): Promise<AnyDiscoveredKafka[]> {
  let strimziWeight = 0;
  let serviceWeight = 0;
  let workloadWeight = 0;
  const emit = (phase: string, label: string, detail?: string, progress?: WorkloadScanProgress): void => {
    report?.({
      value: 3 + strimziWeight + serviceWeight + workloadWeight,
      phase,
      label,
      detail,
      completed: progress?.completed,
      total: progress?.total,
    });
  };

  emit("starting", "Connecting to Kubernetes", "Using the active Freelens cluster connection.");
  const [strimzi, services, workloads] = await Promise.all([
    discoverStrimziKafkas(reader, namespace).then((result) => {
      strimziWeight = 10;
      emit("strimzi", "Checking Strimzi resources", `Found ${result.length} Strimzi Kafka cluster(s).`);
      return result;
    }),
    discoverKafkaServices(reader, namespace)
      .catch(() => [] as KafkaServiceCandidate[])
      .then((result) => {
        serviceWeight = 10;
        emit("services", "Checking Kubernetes Services", `Found ${result.length} Kafka Service candidate(s).`);
        return result;
      }),
    discoverWorkloadKafkas(reader, namespace, (progress) => {
      workloadWeight = progress.total === 0 ? 72 : Math.round((progress.completed / progress.total) * 72);
      emit(
        "workloads",
        "Scanning workload configuration",
        "Reading Kafka bootstrap settings and relevant ConfigMap/Secret references.",
        progress,
      );
    })
      .catch(() => [] as WorkloadKafkaRef[])
      .then((result) => {
        workloadWeight = 72;
        emit("workloads", "Workload scan complete", `Found ${result.length} distinct workload endpoint(s).`);
        return result;
      }),
  ]);

  report?.({
    value: 97,
    phase: "merge",
    label: "Merging Kafka endpoints",
    detail: "Removing duplicate Strimzi, Service and workload references.",
  });

  const known = new Set<string>();
  const strimziNames = new Map<string, string[]>();
  const result: AnyDiscoveredKafka[] = [];

  for (const s of strimzi) {
    result.push(s);
    for (const k of bootstrapKeySet(s.bootstrap)) known.add(k);
    const names = strimziNames.get(s.namespace) ?? [];
    names.push(s.name);
    strimziNames.set(s.namespace, names);
  }

  for (const svc of services) {
    const owners = strimziNames.get(svc.namespace) ?? [];
    if (owners.some((n) => svc.name === n || svc.name.startsWith(`${n}-`))) continue;
    if (overlaps(svc.bootstrap, known)) continue;
    result.push(svc);
    for (const k of bootstrapKeySet(svc.bootstrap)) known.add(k);
  }

  for (const w of workloads) {
    if (overlaps(w.bootstrap, known)) continue;
    result.push(w);
    for (const k of bootstrapKeySet(w.bootstrap)) known.add(k);
  }

  report?.({
    value: 100,
    phase: "complete",
    label: "Discovery complete",
    detail: `${result.length} Kafka target(s) available.`,
  });
  return result;
}
