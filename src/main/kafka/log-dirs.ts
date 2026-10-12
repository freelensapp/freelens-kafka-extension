import { type KafkaApiVersions, sendKafkaProtocol, supportsDescribeLogDirs } from "./group-offset-batch-protocol";
import { describeLogDirsProtocol } from "./log-dirs-protocol";

import type { PartitionSizeDto, TopicSizeDto, TopicSizesDto } from "../../common/ipc";

const DEFAULT_CONCURRENCY = 8;
/**
 * How long one broker may take to connect and answer, both tries together: half the time the page
 * waits for the sizes, so that a broker that never answers is left out and the others still
 * report. KafkaJS cannot do it: a request can only be given more time than the request timeout of
 * the client, never less.
 */
const DEFAULT_BROKER_TIMEOUT_MS = 15_000;
/** A breath before a broker that dropped the connection is asked again. */
const DEFAULT_RETRY_PAUSE_MS = 100;

/** KafkaJS internals of a broker, all optional: a fake cluster in a test has none of them. */
interface KafkaJsBrokerInternals {
  disconnect?(): Promise<void>;
  connectionPool?: {
    host?: string;
    port?: number;
    /** Connection 0 carries every request but Fetch. */
    pool?: Array<
      { isConnected?(): boolean; authenticatedAt?: [number, number] | null; sessionLifetime?: unknown } | undefined
    >;
  };
}

/**
 * The part of the KafkaJS read cluster the size reader needs (see `KafkaReadCluster`). The cluster
 * has no target topics, so `refreshMetadata` asks for all topics without naming them: a broker may
 * close the connection on a Metadata request that names every topic, as seen on MSK Express
 * brokers with IAM (#82), and `kafka-log-dirs` never names a topic either.
 */
export interface LogDirsCluster {
  refreshMetadata(): Promise<void>;
  getNodeIds(): string[];
  findTopicPartitionMetadata(topic: string): Array<{ partitionId: number; leader: number | null }>;
  findBroker(request: { nodeId: string }): Promise<object>;
  /** KafkaJS internals of a real cluster: which connections are open, and the handle to drop one before a retry. */
  brokerPool?: { seedBroker?: KafkaJsBrokerInternals; brokers?: Record<string, KafkaJsBrokerInternals | undefined> };
}

/**
 * The peer dropped a connection that was up: the broker closed it with a request in flight, or it
 * was reset. Asking again on a new connection may work; a connect timeout, a refused connection or
 * a request that timed out would not be cured by asking again at once.
 */
export function isDroppedConnection(error: unknown): boolean {
  const { name, code } = (error ?? {}) as { name?: unknown; code?: unknown };
  return (
    name === "KafkaJSConnectionClosedError" ||
    (name === "KafkaJSConnectionError" && (code === "ECONNRESET" || code === "EPIPE"))
  );
}

/** `Name: message (broker host:port)`, the way the page and the log show a Kafka error. */
export function describeError(error: unknown): string {
  if (!(error instanceof Error)) return String(error);
  const broker = (error as { broker?: unknown }).broker;
  const where = typeof broker === "string" && broker && !error.message.includes(broker) ? ` (broker ${broker})` : "";
  return `${error.name}: ${error.message}${where}`;
}

/**
 * The connections a cluster has open, by `host:port`, with what is known of each: for SASL, how
 * long ago it authenticated and the session lifetime the broker declared (0 when it declared
 * none, and then KafkaJS never authenticates again). Read before the requests go out, because
 * KafkaJS clears the authentication time of a connection as soon as it closes.
 */
function openConnections(cluster: LogDirsCluster): Map<string, string> {
  const open = new Map<string, string>();
  const pool = cluster.brokerPool;
  for (const broker of [pool?.seedBroker, ...Object.values(pool?.brokers ?? {})]) {
    const connections = broker?.connectionPool;
    const connection = connections?.pool?.[0];
    if (!connections?.host || !connection?.isConnected?.()) continue;
    let facts = "a connection already open";
    if (connection.authenticatedAt) {
      const [seconds] = process.hrtime(connection.authenticatedAt);
      facts += `, authenticated ${seconds} s ago, SASL session lifetime ${String(connection.sessionLifetime ?? "?")} ms`;
    }
    open.set(`${connections.host}:${connections.port}`, facts);
  }
  return open;
}

/**
 * The size fetch failed in a named step (the metadata, every broker): the page shows the step with
 * the reason, a KafkaJS error or a sentence, and what is known of the connection it failed on.
 */
export class TopicSizesStepError extends Error {
  readonly step: string;
  readonly reason: unknown;

  constructor(step: string, reason: unknown, connection = "") {
    super(`${step}: ${describeError(reason)}${connection}`);
    this.name = "TopicSizesError";
    this.step = step;
    this.reason = reason;
  }

  /** True when the step failed because the peer dropped the connection: reading again may work. */
  get dropped(): boolean {
    return isDroppedConnection(this.reason);
  }
}

export interface FetchTopicSizesOptions {
  concurrency?: number;
  retryPauseMs?: number;
  /** How long one broker may take, both tries together. */
  brokerTimeoutMs?: number;
  /**
   * When the read must be over (`Date.now()` milliseconds): a step still waiting by then fails with
   * its name, and the brokers not asked yet are left out. It is meant to fall before the page
   * stops waiting, whose timeout can only say that the sizes timed out. Without it only the
   * brokers are timed.
   */
  deadline?: number;
  signal?: AbortSignal;
  /** One line per notable event (a retry, a broker left out), for the page and the process output. */
  log?: (line: string) => void;
}

function sleep(ms: number): Promise<void> {
  return ms > 0 ? new Promise((resolve) => setTimeout(resolve, ms)) : Promise.resolve();
}

/** The longest delay a timer takes: a longer one would fire at once. */
const MAX_TIMER_MS = 2_147_483_647;
const answerTimeouts = new WeakSet<object>();

function timeoutError(message: string): Error {
  const error = Object.assign(new Error(message), { name: "TimeoutError" });
  answerTimeouts.add(error);
  return error;
}

/** True for the `TimeoutError` of `answerWithin`, not for a timeout KafkaJS or a broker reports. */
export function isAnswerTimeout(error: unknown): boolean {
  return typeof error === "object" && error !== null && answerTimeouts.has(error);
}

/**
 * Ask and wait for the answer at most the given time: a `TimeoutError` when it takes longer (a
 * late answer is dropped) and when no time is left, without asking at all. A time without a
 * limit waits as long as a timer can count.
 */
export function answerWithin<T>(ms: number, ask: () => Promise<T>): Promise<T> {
  if (!(ms > 0)) return Promise.reject(timeoutError("no time left to ask"));
  const wait = Math.min(Math.ceil(ms), MAX_TIMER_MS);
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(timeoutError(`no answer after ${wait} ms`)), wait);
    ask().then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

/** One replica of one partition as reported by the broker that hosts it. */
export interface ReplicaSizeEntry {
  topic: string;
  partition: number;
  nodeId: number;
  bytes: bigint;
  offsetLag: bigint;
}

export interface TopicLeaders {
  /** topic -> partition -> leader node id (null when the partition has no leader). */
  leaders: Map<string, Map<number, number | null>>;
}

/**
 * Sum the replica sizes per partition and per topic. `leaderBytes` follows the leader replica,
 * which is what most consoles show as the topic size; `replicaBytes` sums every replica, which
 * is the disk footprint across the cluster. A topic is `exact` only when every partition has a
 * leader entry: otherwise its leader size is a lower bound (a broker was unavailable or refused).
 */
export function aggregateTopicSizes(
  entries: readonly ReplicaSizeEntry[],
  { leaders }: TopicLeaders,
  unavailableBrokers: readonly number[],
  supported = true,
): TopicSizesDto {
  const byTopic = new Map<string, Map<number, PartitionSizeDto & { leaderSeen: boolean }>>();
  const partitionsOf = (topic: string) => {
    let partitions = byTopic.get(topic);
    if (!partitions) {
      partitions = new Map();
      byTopic.set(topic, partitions);
    }
    return partitions;
  };
  for (const [topic, topicLeaders] of leaders) {
    const partitions = partitionsOf(topic);
    for (const partition of topicLeaders.keys()) {
      partitions.set(partition, {
        partition,
        leaderBytes: "0",
        replicaBytes: "0",
        replicas: [],
        leaderSeen: false,
      });
    }
  }
  const replicaTotals = new Map<string, Map<number, bigint>>();
  const leaderTotals = new Map<string, Map<number, bigint>>();
  for (const entry of entries) {
    const partitions = partitionsOf(entry.topic);
    const record = partitions.get(entry.partition) ?? {
      partition: entry.partition,
      leaderBytes: "0",
      replicaBytes: "0",
      replicas: [],
      leaderSeen: false,
    };
    partitions.set(entry.partition, record);
    record.replicas.push({
      nodeId: entry.nodeId,
      bytes: entry.bytes.toString(),
      offsetLag: entry.offsetLag.toString(),
    });
    const totals = replicaTotals.get(entry.topic) ?? new Map<number, bigint>();
    replicaTotals.set(entry.topic, totals);
    totals.set(entry.partition, (totals.get(entry.partition) ?? 0n) + entry.bytes);
    const leader = leaders.get(entry.topic)?.get(entry.partition);
    if (leader !== undefined && leader !== null && leader === entry.nodeId) {
      record.leaderSeen = true;
      const leaderMap = leaderTotals.get(entry.topic) ?? new Map<number, bigint>();
      leaderTotals.set(entry.topic, leaderMap);
      leaderMap.set(entry.partition, entry.bytes);
    }
  }
  const topics: Record<string, TopicSizeDto> = {};
  for (const [topic, partitions] of byTopic) {
    let leaderBytes = 0n;
    let replicaBytes = 0n;
    let exact = true;
    const list: PartitionSizeDto[] = [];
    for (const record of [...partitions.values()].sort((a, b) => a.partition - b.partition)) {
      const leaderSize = leaderTotals.get(topic)?.get(record.partition) ?? 0n;
      const replicaSize = replicaTotals.get(topic)?.get(record.partition) ?? 0n;
      if (!record.leaderSeen) exact = false;
      leaderBytes += leaderSize;
      replicaBytes += replicaSize;
      record.replicas.sort((a, b) => a.nodeId - b.nodeId);
      list.push({
        partition: record.partition,
        leaderBytes: leaderSize.toString(),
        replicaBytes: replicaSize.toString(),
        replicas: record.replicas,
      });
    }
    topics[topic] = {
      leaderBytes: leaderBytes.toString(),
      replicaBytes: replicaBytes.toString(),
      exact,
      partitions: list,
    };
  }
  return { supported, unavailableBrokers: [...unavailableBrokers].sort((a, b) => a - b), topics };
}

function toBigInt(value: string): bigint {
  try {
    const parsed = BigInt(value);
    return parsed < 0n ? 0n : parsed;
  } catch {
    return 0n;
  }
}

/**
 * A broker that answers with no log directory at all. That is how it refuses a user that may not
 * describe the cluster: version 1 of the API has no error code to say so.
 */
const NO_LOG_DIRECTORY =
  "answered with no log directory, as a broker does for a user that may not describe the cluster";
const NO_API = "does not offer DescribeLogDirs version 1";
const NOT_ASKED = "not asked: the read ran out of time";

/**
 * Ask every broker for the sizes of the log directories it hosts (one DescribeLogDirs per broker,
 * bounded concurrency), keep the given topics and aggregate them per topic and partition. Neither
 * request names a topic: the partition leaders come from the metadata of all topics, and each
 * broker is asked for every log directory, as `kafka-log-dirs` does. A broker that drops the
 * connection mid-request is asked once more on a new connection; brokers that still fail, that
 * take too long or that the read has no time left to ask are listed in `unavailableBrokers` and
 * reported through `log` with the reason; a cluster that does not offer the API returns
 * `supported: false` instead of failing the page. A failure of the metadata read, or of every
 * broker, is raised as a `TopicSizesStepError`.
 */
export async function fetchTopicSizes(
  cluster: LogDirsCluster,
  topics: readonly string[],
  options: FetchTopicSizesOptions = {},
): Promise<TopicSizesDto> {
  const log = options.log ?? (() => undefined);
  const retryPause = options.retryPauseMs ?? DEFAULT_RETRY_PAUSE_MS;
  const brokerTimeout = options.brokerTimeoutMs ?? DEFAULT_BROKER_TIMEOUT_MS;
  const deadline = options.deadline ?? Number.POSITIVE_INFINITY;
  const names = [...new Set(topics)].filter(Boolean);
  const leaders = new Map<string, Map<number, number | null>>();
  if (names.length === 0) return aggregateTopicSizes([], { leaders }, []);
  const open = openConnections(cluster);
  /** Whether a Kafka error came from a connection that was already open or from a new one. */
  const onConnection = (error: unknown): string => {
    const broker = (error as { broker?: unknown } | null)?.broker;
    return typeof broker === "string" && broker ? `, on ${open.get(broker) ?? "a new connection"}` : "";
  };
  try {
    await answerWithin(deadline - Date.now(), () => cluster.refreshMetadata());
  } catch (error) {
    throw new TopicSizesStepError("reading the cluster metadata", error, onConnection(error));
  }
  for (const topic of names) {
    const partitions = cluster.findTopicPartitionMetadata(topic);
    // A topic the metadata does not know was deleted after the page listed it: it gets no size.
    if (partitions.length === 0) continue;
    leaders.set(topic, new Map(partitions.map((partition) => [partition.partitionId, partition.leader])));
  }
  const nodeIds = cluster.getNodeIds();
  if (nodeIds.length === 0) return aggregateTopicSizes([], { leaders }, [], false);

  const entries: ReplicaSizeEntry[] = [];
  /** Broker id -> why it did not report. */
  const leftOut = new Map<number, string>();
  const withoutApi: string[] = [];
  const queue = [...nodeIds];
  /**
   * Set when a wait that the deadline cut short times out. The clock may still be a millisecond
   * short of the deadline then, because a timer can fire that early: the read is over all the same.
   */
  let outOfTime = false;
  /**
   * The sizes a broker reports, undefined when it does not offer the API. The connection may come
   * after the broker was given up: nothing is sent to a broker nobody waits for.
   */
  const ask = async (nodeId: string, waitedFor: () => boolean) => {
    const broker = await cluster.findBroker({ nodeId });
    if (!waitedFor()) throw new Error(`broker ${nodeId} is not waited for any more`);
    if (!supportsDescribeLogDirs((broker as { versions?: KafkaApiVersions }).versions)) return undefined;
    return sendKafkaProtocol(broker, describeLogDirsProtocol(null));
  };
  /** Keep the sizes of the wanted partitions; returns the reason when the broker reported nothing. */
  const collect = (
    response: NonNullable<Awaited<ReturnType<typeof ask>>>,
    numericNodeId: number,
  ): string | undefined => {
    if (response.results.length === 0) return NO_LOG_DIRECTORY;
    const failed = response.results.filter((result) => result.errorCode !== 0);
    if (failed.length === response.results.length) {
      const codes = [...new Set(failed.map((result) => result.errorCode))].sort((left, right) => left - right);
      return `every log directory answered an error (code ${codes.join(", ")})`;
    }
    for (const result of response.results) {
      if (result.errorCode !== 0) {
        log(`broker ${numericNodeId}: log directory ${result.logDir} answered error code ${result.errorCode}`);
        continue;
      }
      for (const topic of result.topics) {
        const wanted = leaders.get(topic.topic);
        if (!wanted) continue;
        for (const partition of topic.partitions) {
          // A partition the metadata does not list (just created, or being deleted) would only make the topic a lower bound.
          if (partition.isFuture || !wanted.has(partition.partition)) continue;
          entries.push({
            topic: topic.topic,
            partition: partition.partition,
            nodeId: numericNodeId,
            bytes: toBigInt(partition.size),
            offsetLag: toBigInt(partition.offsetLag),
          });
        }
      }
    }
    return undefined;
  };
  const worker = async () => {
    for (let nodeId = queue.shift(); nodeId !== undefined; nodeId = queue.shift()) {
      if (options.signal?.aborted) throw new Error("Topic size fetch aborted");
      const id = nodeId;
      const numericNodeId = Number(id);
      const leaveOut = (reason: string) => {
        log(`broker ${id} left out: ${reason}`);
        leftOut.set(numericNodeId, reason);
      };
      const startedAt = Date.now();
      if (outOfTime || !(deadline - startedAt > 0)) {
        leaveOut(NOT_ASKED);
        continue;
      }
      /** The broker has until then for both tries, and never longer than the read has left. */
      const until = Math.min(startedAt + brokerTimeout, deadline);
      const waitedFor = () => !leftOut.has(numericNodeId) && !options.signal?.aborted;
      /** Ask the broker and keep what it reports; true when it answered with sizes. */
      const read = async (ms: number, asking: () => ReturnType<typeof ask>): Promise<boolean> => {
        const response = await answerWithin(ms, asking).catch((error: unknown) => {
          if (until === deadline && isAnswerTimeout(error)) outOfTime = true;
          throw error;
        });
        if (!response) {
          withoutApi.push(id);
          return false;
        }
        const refusal = collect(response, numericNodeId);
        if (refusal) leaveOut(refusal);
        return !refusal;
      };
      try {
        await read(until - startedAt, () => ask(id, waitedFor));
      } catch (error) {
        if (!isDroppedConnection(error) || options.signal?.aborted) {
          leaveOut(`${describeError(error)}${onConnection(error)}`);
          continue;
        }
        const dropped = `${describeError(error)}${onConnection(error)}`;
        log(`broker ${id}: ${dropped}; asking again on a new connection`);
        let askedAgain = false;
        const askAgain = async () => {
          // KafkaJS closes a dropped connection by itself, but the connection still counts as
          // connected while it is closing: wait for that, so that the request opens a new one.
          await cluster.brokerPool?.brokers?.[id]?.disconnect?.().catch(() => undefined);
          await sleep(retryPause);
          if (!waitedFor()) throw new Error(`broker ${id} is not waited for any more`);
          askedAgain = true;
          return ask(id, waitedFor);
        };
        try {
          if (await read(until - Date.now(), askAgain)) log(`broker ${id}: answered on the new connection`);
        } catch (again) {
          leaveOut(
            askedAgain
              ? `${dropped}; then on a new connection: ${describeError(again)}`
              : `${dropped}; no time was left to ask again`,
          );
        }
      }
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(options.concurrency ?? DEFAULT_CONCURRENCY, nodeIds.length) }, worker),
  );
  if (withoutApi.length === nodeIds.length) return aggregateTopicSizes([], { leaders }, [], false);
  for (const nodeId of withoutApi) {
    log(`broker ${nodeId} left out: ${NO_API}`);
    leftOut.set(Number(nodeId), NO_API);
  }
  if (leftOut.size === nodeIds.length) {
    // No broker reported: the sizes are unknown, not zero. The page shows why instead of lower bounds.
    throw new TopicSizesStepError(
      "asking the brokers",
      [...leftOut]
        .sort(([left], [right]) => left - right)
        .map(([nodeId, reason]) => `broker ${nodeId}: ${reason}`)
        .join("; "),
    );
  }
  return aggregateTopicSizes(entries, { leaders }, [...leftOut.keys()]);
}
