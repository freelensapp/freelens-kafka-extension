import { EXTENSION_VERSION } from "../../common/extension-version";
import {
  type CoordinatorBatchResult,
  findCoordinatorBatchProtocol,
  offsetFetchBatchProtocol,
  sendKafkaProtocol,
} from "./group-offset-batch-protocol";
import { answerWithin, describeError, isAnswerTimeout } from "./log-dirs";
import { type DescribeLogDirsResult, describeLogDirsProtocol } from "./log-dirs-protocol";
import { fetchHighWatermarks, type OffsetBatchPartition } from "./offset-batch";

import type { Admin } from "kafkajs";

import type { KafkaReadCluster } from "./message-fetch";

/*
 * ISSUE #82 DIAGNOSTIC BUILD ONLY (pre-release 1.4.2-2, never merged into main).
 *
 * On an MSK Express cluster with IAM the size read dies with "Closed connection" while the topic list,
 * the messages and the consumer groups work. A local measurement showed that the size read shares its
 * connection with the batched FindCoordinator v4 and OffsetFetch v8 of the lag reader when Topics opens
 * while the Overview is still reading the lag. When the broker closes the connection of a size read,
 * this probe asks the cluster each request the readers send, one at a time and each on a read cluster
 * of its own with the KafkaJS retries off, then two of them together, and writes which ones the broker
 * answers and which ones it closes the connection on, and in which phase of the step (connecting,
 * the metadata a step needs first, the request itself). Read-only requests only. The report names no
 * host, topic, consumer group or AWS identity: brokers appear by id, bootstrap addresses by position,
 * the rest by counts, and the messages of errors that do not come from Kafka are withheld.
 */

const STEP_MS = 15_000;
const IDLE_MS = 30_000;
/** A breath between steps, in case the brokers limit how fast new IAM connections may come. */
const PAUSE_MS = 1_000;
const MAX_BOOTSTRAPS = 6;
const MAX_GROUPS = 500;
const MAX_GROUPS_PER_OFFSET_FETCH = 100;
const MAX_WATERMARK_TOPICS = 20;
/** Group id used when the cluster lists no group: FindCoordinator and OffsetFetch only read. */
const PLACEHOLDER_GROUP = "freelens-kafka-probe";
/** Request keys whose advertised versions the report shows. */
const SHOWN_APIS: ReadonlyArray<[number, string]> = [
  [3, "Metadata"],
  [9, "OffsetFetch"],
  [10, "FindCoordinator"],
  [35, "DescribeLogDirs"],
  [36, "SaslAuthenticate"],
];

type Outcome = "answered" | "closed by the broker" | "connection reset" | "no answer in time" | "error" | "skipped";

interface ProbeStep {
  key: string;
  title: string;
  outcome: Outcome;
  /** Where the step stopped when it is not the request it is about: "connect", "metadata", "broker 2"... */
  phase?: string;
  ms: number;
  detail: string;
}

/** KafkaJS internals the probe reads, all optional: a fake cluster in a test has few of them. */
interface ProbeConnection {
  authenticatedAt?: unknown;
  sessionLifetime?: { toString(): string };
  connectionStatus?: string;
  isConnected?(): boolean;
}

interface ProbeBroker {
  nodeId?: number;
  versions?: Record<number, { minVersion: number; maxVersion: number } | undefined>;
  connectionPool?: { pool?: Array<ProbeConnection | undefined> };
}

export type ProbeCluster = KafkaReadCluster & {
  brokerPool?: {
    seedBroker?: ProbeBroker;
    brokers?: Record<string, ProbeBroker | undefined>;
    metadata?: {
      brokers?: Array<{ nodeId: number; host: string; port: number }>;
      topicMetadata?: Array<{ partitionMetadata?: unknown[] }>;
    };
  };
};

export interface SizeProbeContext {
  /** The message of the failed size read, as the page shows it. */
  failure: string;
  /** The topics the page asked the sizes of: only counted, and used to pick partitions for the watermarks. */
  topics: readonly string[];
  /** The bootstrap addresses: each gets steps of its own, and they are hidden in the report. */
  bootstrap: readonly string[];
  /** SASL mechanism, TLS and strategy of the connection, without any address or secret. */
  settings: string;
  /** What the readers of the shared read cluster were doing when the size read failed. */
  activity: readonly string[];
  /** The connections of the shared read cluster when the size read failed (`describeConnections`). */
  sharedConnections: readonly string[];
  /** Other topic and group names the connection knows: only used to hide them. */
  names?: readonly string[];
}

export interface SizeProbeDeps {
  /** A read cluster on the given bootstrap addresses (all of them when omitted), KafkaJS retries off. */
  createCluster(bootstrap?: readonly string[]): ProbeCluster;
  /** An admin with the KafkaJS retries off. */
  createAdmin(): Pick<Admin, "connect" | "disconnect" | "listGroups">;
  /** Writes the whole report again; called after every step, so a closed app leaves what was done. */
  write(text: string): Promise<void>;
  /** True once the Kafka connection is closed: the steps left are skipped. */
  stopped?(): boolean;
  now?(): number;
  sleep?(ms: number): Promise<void>;
  stepMs?: number;
  idleMs?: number;
  pauseMs?: number;
  /** The home folder: shown as "~" in the report. */
  home?: string;
}

/**
 * What the readers of the shared read cluster are doing: the ones running and the latest events,
 * including the errors the lag reader swallows. The report shows it as it was at the failure.
 */
export class ReaderActivity {
  private nextId = 0;
  private readonly running = new Map<number, { reader: string; startedAt: number }>();
  private readonly events: Array<{ at: number; reader: string; text: string }> = [];

  constructor(
    private readonly now: () => number = Date.now,
    private readonly keep = 40,
  ) {}

  note(reader: string, text: string): void {
    this.events.push({ at: this.now(), reader, text });
    if (this.events.length > this.keep) this.events.splice(0, this.events.length - this.keep);
  }

  async track<T>(reader: string, run: () => Promise<T>, summary: (value: T) => string = () => "answered"): Promise<T> {
    const id = ++this.nextId;
    const startedAt = this.now();
    this.running.set(id, { reader, startedAt });
    try {
      const value = await run();
      this.note(reader, `${summary(value)} after ${this.now() - startedAt} ms`);
      return value;
    } catch (error) {
      this.note(reader, `failed after ${this.now() - startedAt} ms: ${describeError(error)}`);
      throw error;
    } finally {
      this.running.delete(id);
    }
  }

  /** The readers still running, then the latest events, oldest first. */
  describe(): string[] {
    const now = this.now();
    return [
      ...[...this.running.values()].map(({ reader, startedAt }) => `running for ${now - startedAt} ms: ${reader}`),
      ...this.events.map(({ at, reader, text }) => `${now - at} ms before: ${reader}: ${text}`),
    ];
  }
}

/** The errors of a KafkaJS wrapper chain, outermost first (KafkaJSNumberOfRetriesExceeded keeps its cause). */
function errorChain(error: unknown): unknown[] {
  const chain: unknown[] = [];
  for (let current = error; current !== undefined && current !== null && chain.length < 4; ) {
    chain.push(current);
    current = (current as { cause?: unknown }).cause;
  }
  return chain;
}

function classify(error: unknown): Outcome {
  const chain = errorChain(error);
  const named = (name: string) => chain.some((item) => (item as { name?: unknown }).name === name);
  if (named("KafkaJSConnectionClosedError")) return "closed by the broker";
  if (
    chain.some((item) => {
      const { name, code } = item as { name?: unknown; code?: unknown };
      return name === "KafkaJSConnectionError" && (code === "ECONNRESET" || code === "EPIPE");
    })
  ) {
    return "connection reset";
  }
  if (chain.some((item) => isAnswerTimeout(item))) return "no answer in time";
  return "error";
}

/** Messages of the probe itself and of the protocol helpers: they name nothing of the cluster. */
const OWN_MESSAGES = [
  /^no answer after \d+ ms$/,
  /^no time left to ask$/,
  /^the read cluster has no bootstrap broker$/,
  /^Kafka broker does not support /,
  /^KafkaJS broker request adapter is unavailable$/,
  /^Refusing non-read-only /,
  /^\w[\w ]* answered; /,
  /^\w[\w ]* (closed by the broker|connection reset|no answer in time|error): /,
];

/**
 * An error for the report: KafkaJS errors and the probe's own keep their message (scrubbed later),
 * any other error (the AWS credential chain, the file system) only its name and code, because its
 * message may carry a profile, an account or a path. A KafkaJS wrapper is followed to its cause.
 */
function describeProbeError(error: unknown): string {
  return errorChain(error)
    .map((item) => {
      if (!(item instanceof Error)) return "a value that is not an error";
      const fromKafka = item.name.startsWith("KafkaJS");
      const own = item.name === "TimeoutError" || OWN_MESSAGES.some((pattern) => pattern.test(item.message));
      if (fromKafka || own) return describeError(item);
      const code = (item as { code?: unknown }).code;
      return `${item.name}${typeof code === "string" ? ` (${code})` : ""}: message withheld, it does not come from Kafka`;
    })
    .join("; caused by ");
}

/** What KafkaJS knows of connection 0 of a broker: whether it authenticated and the session lifetime the broker declared. */
function connectionFacts(broker: ProbeBroker | undefined): string {
  const connection = broker?.connectionPool?.pool?.[0];
  if (!connection) return "no connection facts";
  const lifetime = connection.sessionLifetime?.toString() ?? "?";
  const declared = lifetime === "0" ? " (none declared)" : "";
  return `${connection.authenticatedAt ? "authenticated" : "not authenticated"}, SASL session lifetime ${lifetime} ms${declared}`;
}

function advertisedVersions(broker: ProbeBroker | undefined): string {
  const versions = broker?.versions;
  if (!versions) return "versions unknown";
  return SHOWN_APIS.map(([key, name]) => {
    const range = versions[key];
    return `${name} ${range ? `${range.minVersion}-${range.maxVersion}` : "not offered"}`;
  }).join(", ");
}

/**
 * The connections of a KafkaJS cluster, brokers by id: open or closed, and for an open one how long
 * ago it authenticated and the session lifetime the broker declared. An open connection that is not
 * authenticated is still authenticating, or is closing.
 */
export function describeConnections(
  cluster: ProbeCluster | undefined,
  hrtime: (start?: [number, number]) => [number, number] = process.hrtime,
): string[] {
  const pool = cluster?.brokerPool;
  if (!pool) return ["no read cluster was open"];
  const entries: Array<[string, ProbeBroker | undefined]> = [
    [
      `bootstrap broker${pool.seedBroker?.nodeId === undefined ? "" : ` (broker ${pool.seedBroker.nodeId})`}`,
      pool.seedBroker,
    ],
    ...Object.entries(pool.brokers ?? {}).map(([nodeId, broker]): [string, ProbeBroker | undefined] => [
      `broker ${nodeId}`,
      broker,
    ]),
  ];
  return entries.map(([label, broker]) => {
    const states = (broker?.connectionPool?.pool ?? []).map((connection, index) => {
      const name = index === 1 ? "connection 1 (Fetch)" : `connection ${index}`;
      if (!connection?.isConnected?.()) return `${name} closed`;
      const status = connection.connectionStatus === "disconnecting" ? "closing" : "open";
      const at = connection.authenticatedAt;
      if (!Array.isArray(at)) return `${name} ${status}, not authenticated`;
      const [seconds] = hrtime(at as [number, number]);
      return `${name} ${status}, authenticated ${seconds} s ago, SASL session lifetime ${connection.sessionLifetime?.toString() ?? "?"} ms`;
    });
    return `${label}: ${states.join("; ") || "no connection"}`;
  });
}

function logDirsSummary(response: DescribeLogDirsResult): string {
  let replicas = 0;
  let failed = 0;
  for (const result of response.results) {
    if (result.errorCode !== 0) failed += 1;
    for (const topic of result.topics) replicas += topic.partitions.length;
  }
  return `${response.results.length} log dir(s), ${replicas} partition replica(s)${failed ? `, ${failed} log dir(s) with an error code` : ""}`;
}

function coordinatorSummary(response: CoordinatorBatchResult): string {
  const nodes = new Set<number>();
  const codes = new Map<number, number>();
  for (const coordinator of response.coordinators) {
    if (coordinator.errorCode === 0) nodes.add(coordinator.nodeId);
    else codes.set(coordinator.errorCode, (codes.get(coordinator.errorCode) ?? 0) + 1);
  }
  const found = response.coordinators.length - [...codes.values()].reduce((sum, count) => sum + count, 0);
  const errors = [...codes].map(([code, count]) => `${count} x error code ${code}`).join(", ");
  return `${found} coordinator(s) on broker(s) ${[...nodes].sort((a, b) => a - b).join(", ") || "none"}${errors ? `, ${errors}` : ""}`;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * The scrubber of the dynamic lines of the report (error messages, the failure, the activity):
 * - the known hosts get their label ("broker 2", "bootstrap 1"), through placeholders that no name can touch;
 * - the home folder becomes "~";
 * - AWS identities (ARNs, account ids, access key ids, profile names) and any other IP address or host
 *   get a placeholder;
 * - topic and group names become "<name>" as whole tokens, all in one regular expression.
 */
export function createScrubber(
  hosts: ReadonlyMap<string, string>,
  names: readonly string[],
  home?: string,
): (text: string) => string {
  const byLength = [...hosts].sort((left, right) => right[0].length - left[0].length);
  const hostPattern = byLength.length
    ? new RegExp(byLength.map(([host]) => escapeRegExp(host)).join("|"), "gi")
    : undefined;
  const labelOf = new Map(byLength.map(([host, label]) => [host.toLowerCase(), label]));
  const unique = [...new Set(names)]
    .filter((name) => name.length >= 2)
    .sort((left, right) => right.length - left.length);
  const namePattern = unique.length
    ? new RegExp(`(?<![\\w])(?:${unique.map(escapeRegExp).join("|")})(?![\\w])`, "g")
    : undefined;
  return (text: string) => {
    const labels: string[] = [];
    let result = text;
    if (hostPattern) {
      result = result.replace(hostPattern, (match) => {
        labels.push(labelOf.get(match.toLowerCase()) ?? "<host>");
        return `\u0001${"\u0002".repeat(labels.length)}\u0001`;
      });
    }
    // KafkaJS errors say "(broker host:port)": the label already says "broker 2", and a topic named
    // "broker" must not turn the word into "<name>" next to it.
    result = result.replace(/\bbroker (?=\u0001)/g, "");
    if (home && home.length > 1) result = result.split(home).join("~");
    result = result.replace(/\barn:aws[\w-]*:[^\s,;)"'\]]+/gi, "<arn>");
    result = result.replace(/\b(?:AKIA|ASIA|AROA|AIDA|ANPA|AGPA)[A-Z0-9]{12,}\b/g, "<key id>");
    result = result.replace(/\b\d{12}\b/g, "<account>");
    result = result.replace(/\b(profile:?\s*)(\[[^\]]*\]|"[^"]*"|[^\s,;)]+)/gi, "$1<profile>");
    // IPv6, compressed ("::") or not, with brackets and port; a time like 11:23:14 has no letter and no "::".
    result = result.replace(/\[?(?:[0-9a-f]{0,4}:){2,7}[0-9a-f]{0,4}(?:%\w+)?\]?(?::\d{1,5})?(?![\w])/gi, (match) =>
      /::|[a-f]/i.test(match) || (match.match(/:/g) ?? []).length >= 4 ? "<address>" : match,
    );
    result = result.replace(/\b(?:\d{1,3}\.){3}\d{1,3}(?::\d{1,5})?\b/g, "<address>");
    result = result.replace(/\b(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}(?::\d{1,5})?\b/gi, "<host>");
    result = result.replace(/\b[a-z][a-z0-9-]*[a-z0-9]:\d{2,5}\b/gi, "<host>");
    if (namePattern) result = result.replace(namePattern, "<name>");
    result = result.replace(/\u0001(\u0002+)\u0001/g, (_, marks: string) => labels[marks.length - 1] ?? "<host>");
    // KafkaJS errors say "(broker host:port)": once the host is a label, "broker broker 2" reads "broker 2".
    return result.replace(/\bbroker (broker \d+|bootstrap \d+)/g, "$1");
  };
}

function addHost(hosts: Map<string, string>, address: string, label: string): void {
  hosts.set(address, label);
  const colon = address.lastIndexOf(":");
  if (colon > 0) hosts.set(address.slice(0, colon), label);
}

/** A step's handle: the phase it is in, and how to close what it opened (done when the step ends, even on a timeout). */
interface StepScope {
  phase: string;
  closers: Set<() => Promise<void>>;
}

/**
 * Ask the cluster, one request at a time and each on a new read cluster, what the size read and
 * the lag reader send, and write the report through `deps.write`. Returns a one-line summary for
 * the hover text of the next failed read.
 */
export async function probeTopicSizes(context: SizeProbeContext, deps: SizeProbeDeps): Promise<string> {
  // Leave the caller first: the hover text of the failed read must not wait for the first report.
  await new Promise<void>((resolve) => setImmediate(resolve));
  const now = deps.now ?? Date.now;
  const sleep = deps.sleep ?? ((ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms)));
  const stepMs = deps.stepMs ?? STEP_MS;
  const idleMs = deps.idleMs ?? IDLE_MS;
  const pauseMs = deps.pauseMs ?? PAUSE_MS;
  const startedAt = now();
  const steps: ProbeStep[] = [];
  const hosts = new Map<string, string>();
  const bootstraps = context.bootstrap.slice(0, MAX_BOOTSTRAPS);
  context.bootstrap.forEach((address, index) => addHost(hosts, address, `bootstrap ${index + 1}`));
  let groupIds: string[] = [];

  const render = (done: boolean): string => {
    const scrub = createScrubber(hosts, [...context.topics, ...(context.names ?? []), ...groupIds], deps.home);
    const lines = [
      `Freelens Kafka extension ${EXTENSION_VERSION}: topic sizes diagnostic report (issue #82)`,
      `${new Date(now()).toISOString()}, ${process.platform} ${process.arch}, Node ${process.versions.node}${
        process.versions.electron ? `, Electron ${process.versions.electron}` : ""
      }`,
      "No host, topic, consumer group or AWS identity is named here: brokers appear by id, bootstrap addresses by",
      "position, the rest by counts, and the messages of errors that do not come from Kafka are withheld.",
      `${done ? "Probe finished" : "Probe still running"} after ${Math.round((now() - startedAt) / 1000)} s.`,
      "",
      `Connection: ${context.settings}, ${context.bootstrap.length} bootstrap address(es)`,
      "",
      "The size read that started the probe:",
      `  ${scrub(context.failure)}`,
      "",
      "The connections of the shared read cluster at that moment:",
      ...(context.sharedConnections.length
        ? context.sharedConnections.map((line) => `  - ${scrub(line)}`)
        : ["  - none"]),
      "",
      "The readers of the shared read cluster at that moment (running ones first, then the latest events):",
      ...(context.activity.length ? context.activity.map((line) => `  - ${scrub(line)}`) : ["  - nothing recorded"]),
      "",
      "Probe: each request on a new connection of its own with the KafkaJS retries off, one after the other,",
      "then two of them together. A step that stopped before its own request says where: [... during <phase>].",
      ...steps.map((step, index) => {
        const where = step.phase && step.phase !== "request" ? ` during ${step.phase}` : "";
        const time = step.outcome === "skipped" ? "" : ` after ${step.ms} ms`;
        return `${String(index + 1).padStart(2)}. [${step.outcome}${where}${time}] ${step.title}: ${scrub(step.detail)}`;
      }),
    ];
    if (done) lines.push("", `Summary: ${summarize(steps)}`);
    return `${lines.join("\n")}\n`;
  };
  const flush = async (done = false) => {
    await deps.write(render(done)).catch(() => undefined);
  };
  const skip = async (key: string, title: string, why: string) => {
    steps.push({ key, title, outcome: "skipped", ms: 0, detail: why });
    await flush();
  };
  const step = async (key: string, title: string, run: (scope: StepScope) => Promise<string>, ms = stepMs) => {
    if (deps.stopped?.()) {
      await skip(key, title, "the Kafka connection of the page was closed");
      return;
    }
    if (steps.some((done) => done.outcome !== "skipped")) await sleep(pauseMs);
    const scope: StepScope = { phase: "request", closers: new Set() };
    const began = now();
    try {
      const detail = await answerWithin(ms, () => run(scope));
      steps.push({ key, title, outcome: "answered", ms: now() - began, detail });
    } catch (error) {
      steps.push({
        key,
        title,
        outcome: classify(error),
        phase: scope.phase,
        ms: now() - began,
        detail: describeProbeError(error),
      });
    } finally {
      // A step that ran out of time keeps going inside KafkaJS: close what it opened before the next one.
      await Promise.all([...scope.closers].map((close) => close().catch(() => undefined)));
    }
    await flush();
  };
  /** A new read cluster for the step: `connect` is its first phase; the cluster closes with the step. */
  const withCluster = async <T>(
    scope: StepScope,
    use: (cluster: ProbeCluster) => Promise<T>,
    bootstrap?: readonly string[],
  ): Promise<T> => {
    scope.phase = "connect";
    const cluster = deps.createCluster(bootstrap);
    const close = () => cluster.disconnect();
    scope.closers.add(close);
    try {
      await cluster.connect();
      return await use(cluster);
    } finally {
      scope.closers.delete(close);
      await close().catch(() => undefined);
    }
  };
  const learnBrokers = (cluster: ProbeCluster) => {
    for (const broker of cluster.brokerPool?.metadata?.brokers ?? []) {
      addHost(hosts, `${broker.host}:${broker.port}`, `broker ${broker.nodeId}`);
    }
  };
  const loadMetadata = async (scope: StepScope, cluster: ProbeCluster) => {
    scope.phase = "metadata";
    await cluster.refreshMetadata();
    learnBrokers(cluster);
  };
  const brokerOf = async (scope: StepScope, cluster: ProbeCluster, nodeId: string) => {
    scope.phase = `connecting to broker ${nodeId}`;
    return cluster.findBroker({ nodeId });
  };
  const seedOf = (cluster: ProbeCluster): ProbeBroker => {
    const seed = cluster.brokerPool?.seedBroker;
    if (!seed) throw new Error("the read cluster has no bootstrap broker");
    return seed;
  };

  await flush();

  await step(
    "groups",
    "Consumer groups listed through a new admin connection, as the Consumer Groups page does",
    async (scope) => {
      scope.phase = "connect";
      const admin = deps.createAdmin();
      const close = () => admin.disconnect();
      scope.closers.add(close);
      try {
        await admin.connect();
        scope.phase = "request";
        const { groups } = await admin.listGroups();
        groupIds = groups.map((group) => group.groupId);
        return `${groupIds.length} group(s)`;
      } finally {
        scope.closers.delete(close);
        await close().catch(() => undefined);
      }
    },
  );
  const probeGroups = groupIds.length > 0 ? groupIds.slice(0, MAX_GROUPS) : [PLACEHOLDER_GROUP];

  await step("connect", "Open a new read cluster: connection, ApiVersions and SASL with bootstrap 1", (scope) =>
    withCluster(scope, async (cluster) => {
      const seed = cluster.brokerPool?.seedBroker;
      return `bootstrap connection ${connectionFacts(seed)}; advertised versions: ${advertisedVersions(seed)}`;
    }),
  );

  for (const [index, address] of bootstraps.entries()) {
    await step(
      `metadata-b${index + 1}`,
      `Metadata of all topics, alone, asked to bootstrap ${index + 1} on a new read cluster (the first request of the size read)`,
      (scope) =>
        withCluster(
          scope,
          async (cluster) => {
            scope.phase = "request";
            await cluster.refreshMetadata();
            learnBrokers(cluster);
            const topics = cluster.brokerPool?.metadata?.topicMetadata ?? [];
            const partitions = topics.reduce((sum, topic) => sum + (topic.partitionMetadata?.length ?? 0), 0);
            return `${cluster.getNodeIds().length} broker(s), ${topics.length} topic(s), ${partitions} partition(s); connection ${connectionFacts(cluster.brokerPool?.seedBroker)}`;
          },
          [address],
        ),
    );
  }

  for (const [index, address] of bootstraps.entries()) {
    await step(
      `logdirs-b${index + 1}`,
      `DescribeLogDirs v1 for every partition, alone, to bootstrap ${index + 1} of a new read cluster, before any metadata`,
      (scope) =>
        withCluster(
          scope,
          async (cluster) => {
            const seed = seedOf(cluster);
            scope.phase = "request";
            return logDirsSummary(await sendKafkaProtocol(seed, describeLogDirsProtocol(null)));
          },
          [address],
        ),
    );
  }

  const groupsByCoordinator = new Map<string, string[]>();
  await step(
    "findcoordinator",
    `FindCoordinator v4 for ${probeGroups.length} group(s) in one request, alone, to bootstrap 1 of a new read cluster, before any metadata (the lag reader's FindCoordinator)`,
    (scope) =>
      withCluster(scope, async (cluster) => {
        const seed = seedOf(cluster);
        scope.phase = "request";
        const response = await sendKafkaProtocol(seed, findCoordinatorBatchProtocol(probeGroups));
        for (const coordinator of response.coordinators) {
          if (coordinator.errorCode !== 0) continue;
          const nodeId = String(coordinator.nodeId);
          groupsByCoordinator.set(nodeId, [...(groupsByCoordinator.get(nodeId) ?? []), coordinator.key]);
        }
        return coordinatorSummary(response);
      }),
  );

  const coordinators = [...groupsByCoordinator].sort(([left], [right]) => Number(left) - Number(right));
  if (coordinators.length === 0) {
    await skip("offsetfetch", "OffsetFetch v8 to each coordinator", "the FindCoordinator step found no coordinator");
  }
  for (const [nodeId, groups] of coordinators) {
    const chunk = groups.slice(0, MAX_GROUPS_PER_OFFSET_FETCH);
    await step(
      `offsetfetch-${nodeId}`,
      `OffsetFetch v8 for ${chunk.length} group(s) in one request, alone, to coordinator broker ${nodeId} of a new read cluster after the metadata (the lag reader's OffsetFetch)`,
      (scope) =>
        withCluster(scope, async (cluster) => {
          await loadMetadata(scope, cluster);
          const broker = await brokerOf(scope, cluster, nodeId);
          scope.phase = "request";
          const response = await sendKafkaProtocol(broker, offsetFetchBatchProtocol(chunk));
          const failed = response.groups.filter((group) => group.errorCode !== 0).length;
          return `${response.groups.length} group(s) answered${failed ? `, ${failed} with an error code` : ""}`;
        }),
    );
  }

  let brokerIds: string[] = [];
  await step(
    "brokers",
    "Metadata of all topics on a new read cluster, to learn the broker ids for the next steps",
    (scope) =>
      withCluster(scope, async (cluster) => {
        scope.phase = "request";
        await cluster.refreshMetadata();
        learnBrokers(cluster);
        brokerIds = cluster.getNodeIds();
        return `broker(s) ${brokerIds.join(", ") || "none"}`;
      }),
  );
  if (brokerIds.length === 0) {
    await skip(
      "logdirs",
      "DescribeLogDirs v1 to each broker after the metadata",
      "the broker ids are unknown: the metadata failed",
    );
  }
  for (const nodeId of brokerIds) {
    await step(
      `logdirs-${nodeId}`,
      `DescribeLogDirs v1 for every partition, alone, to broker ${nodeId} of a new read cluster after the metadata (the second request of the size read)`,
      (scope) =>
        withCluster(scope, async (cluster) => {
          await loadMetadata(scope, cluster);
          const broker = await brokerOf(scope, cluster, nodeId);
          scope.phase = "request";
          return logDirsSummary(await sendKafkaProtocol(broker, describeLogDirsProtocol(null)));
        }),
    );
  }

  await step(
    "watermarks",
    `ListOffsets (latest) for the partitions of up to ${MAX_WATERMARK_TOPICS} listed topics, internal ones left out, on a new read cluster (the watermarks of the Overview)`,
    (scope) =>
      withCluster(scope, async (cluster) => {
        await loadMetadata(scope, cluster);
        const partitions: OffsetBatchPartition[] = [];
        for (const topic of context.topics.filter((name) => !name.startsWith("__")).slice(0, MAX_WATERMARK_TOPICS)) {
          for (const { partitionId, leader } of cluster.findTopicPartitionMetadata(topic)) {
            if (leader !== null && leader >= 0) partitions.push({ topic, partition: partitionId, leaderId: leader });
          }
        }
        scope.phase = "request";
        const watermarks = await fetchHighWatermarks(cluster, partitions, 500);
        const answered = [...watermarks.values()].reduce((sum, byPartition) => sum + byPartition.size, 0);
        return `${partitions.length} partition(s) asked, ${answered} answered`;
      }),
  );

  await step(
    "together-metadata-findcoordinator",
    "Metadata of all topics and FindCoordinator v4 sent together on the connection to bootstrap 1 of a new read cluster (Topics opened while the Overview reads the lag)",
    (scope) =>
      withCluster(scope, async (cluster) => {
        const seed = seedOf(cluster);
        scope.phase = "request";
        const settled = await Promise.allSettled([
          cluster.refreshMetadata(),
          sendKafkaProtocol(seed, findCoordinatorBatchProtocol(probeGroups)),
        ]);
        learnBrokers(cluster);
        return settledSummary(["Metadata", "FindCoordinator v4"], settled);
      }),
  );

  const firstCoordinator = coordinators[0]?.[0];
  if (firstCoordinator === undefined) {
    await skip(
      "together-logdirs-offsetfetch",
      "DescribeLogDirs v1 and OffsetFetch v8 sent together",
      "no coordinator was found",
    );
  } else {
    const chunk = (groupsByCoordinator.get(firstCoordinator) ?? []).slice(0, MAX_GROUPS_PER_OFFSET_FETCH);
    await step(
      "together-logdirs-offsetfetch",
      `DescribeLogDirs v1 and OffsetFetch v8 sent together to coordinator broker ${firstCoordinator} of a new read cluster after the metadata (the size read and the lag reader overlapping)`,
      (scope) =>
        withCluster(scope, async (cluster) => {
          await loadMetadata(scope, cluster);
          const broker = await brokerOf(scope, cluster, firstCoordinator);
          scope.phase = "request";
          const settled = await Promise.allSettled([
            sendKafkaProtocol(broker, describeLogDirsProtocol(null)),
            sendKafkaProtocol(broker, offsetFetchBatchProtocol(chunk)),
          ]);
          return settledSummary(["DescribeLogDirs v1", "OffsetFetch v8"], settled);
        }),
    );
  }

  await step(
    "idle",
    `A new read cluster left idle for ${Math.round(idleMs / 1000)} s, then the metadata of all topics (a connection that stays open, like the shared one; the IAM token itself lives longer)`,
    (scope) =>
      withCluster(scope, async (cluster) => {
        const before = connectionFacts(cluster.brokerPool?.seedBroker);
        scope.phase = "the idle wait";
        await sleep(idleMs);
        scope.phase = "request";
        await cluster.refreshMetadata();
        learnBrokers(cluster);
        return `before the wait: ${before}; after it: ${connectionFacts(cluster.brokerPool?.seedBroker)}`;
      }),
    idleMs + stepMs,
  );

  await flush(true);
  return summarize(steps);
}

/** Both outcomes of two requests sent together; throws the first failure so that the step shows it. */
function settledSummary(names: readonly string[], settled: ReadonlyArray<PromiseSettledResult<unknown>>): string {
  const parts = settled.map((result, index) =>
    result.status === "fulfilled"
      ? `${names[index]} answered`
      : `${names[index]} ${classify(result.reason)}: ${describeProbeError(result.reason)}`,
  );
  const failed = settled.find((result): result is PromiseRejectedResult => result.status === "rejected");
  if (failed) {
    const error = new Error(parts.join("; "));
    error.name = classify(failed.reason) === "closed by the broker" ? "KafkaJSConnectionClosedError" : "Error";
    throw Object.assign(error, { cause: undefined });
  }
  return parts.join("; ");
}

/** One short line per step for the hover text: `metadata-b1 ok, findcoordinator CLOSED, offsetfetch-2 CLOSED@metadata, ...`. */
function summarize(steps: readonly ProbeStep[]): string {
  const short: Record<Outcome, string> = {
    answered: "ok",
    "closed by the broker": "CLOSED",
    "connection reset": "RESET",
    "no answer in time": "TIMEOUT",
    error: "ERROR",
    skipped: "skipped",
  };
  return steps
    .map((step) => {
      const where =
        step.phase && step.phase !== "request" && step.outcome !== "answered"
          ? `@${step.phase.replace(/ /g, "-")}`
          : "";
      return `${step.key} ${short[step.outcome]}${where}`;
    })
    .join(", ");
}
