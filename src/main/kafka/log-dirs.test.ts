import { afterEach, describe, expect, it, vi } from "vitest";
import {
  aggregateTopicSizes,
  answerWithin,
  fetchTopicSizes,
  isAnswerTimeout,
  type LogDirsCluster,
  TopicSizesStepError,
} from "./log-dirs";

interface KafkaEncoder {
  readonly buffer: Buffer;
  writeInt16(value: number): KafkaEncoder;
  writeInt32(value: number): KafkaEncoder;
  writeInt64(value: number | string): KafkaEncoder;
  writeBoolean(value: boolean): KafkaEncoder;
  writeString(value: string): KafkaEncoder;
  writeArray(value: KafkaEncoder[]): KafkaEncoder;
}

// eslint-disable-next-line @typescript-eslint/no-require-imports
const Encoder = require("kafkajs/src/protocol/encoder") as new () => KafkaEncoder;

type SizeEntry = [topic: string, partition: number, size: number];

function logDir(entries: SizeEntry[], errorCode = 0, path = "/data"): KafkaEncoder {
  const byTopic = new Map<string, KafkaEncoder[]>();
  for (const [topic, partition, size] of entries) {
    const list = byTopic.get(topic) ?? [];
    list.push(new Encoder().writeInt32(partition).writeInt64(size).writeInt64(0).writeBoolean(false));
    byTopic.set(topic, list);
  }
  const topics = [...byTopic].map(([topic, partitions]) => new Encoder().writeString(topic).writeArray(partitions));
  return new Encoder().writeInt16(errorCode).writeString(path).writeArray(topics);
}

function response(logDirs: KafkaEncoder[]): Buffer {
  return new Encoder().writeInt32(0).writeArray(logDirs).buffer;
}

function brokerResponse(entries: SizeEntry[]): Buffer {
  return response([logDir(entries)]);
}

interface FakeProtocol {
  request: { encode(): Promise<{ buffer: Buffer }> };
  response: { decode(buffer: Buffer): unknown };
}

type SendRequest = (protocol: FakeProtocol) => Promise<unknown>;

interface FakeCluster extends LogDirsCluster {
  /** What the cluster was asked, in order: `metadata` or the encoded DescribeLogDirs body of a broker. */
  asked: Array<"metadata" | { nodeId: string; body: Buffer }>;
}

const WITH_API = { 35: { minVersion: 0, maxVersion: 4 } };
const WITHOUT_API = { 35: { minVersion: 2, maxVersion: 4 } };

/** A cluster whose brokers answer from a queue, so that a broker can fail once and then answer. */
function fakeCluster(
  leaders: Record<string, Array<{ partitionId: number; leader: number | null }>>,
  answers: Record<string, Array<Buffer | Error>>,
  options: {
    onRequest?: (nodeId: string) => Promise<void>;
    versions?: Record<string, object>;
  } = {},
): FakeCluster {
  const asked: FakeCluster["asked"] = [];
  return {
    asked,
    refreshMetadata: async () => {
      asked.push("metadata");
    },
    getNodeIds: () => Object.keys(answers),
    findTopicPartitionMetadata: (topic) => leaders[topic] ?? [],
    findBroker: async ({ nodeId }) => ({
      versions: options.versions?.[nodeId] ?? WITH_API,
      [Symbol("private:Broker:sendRequest")]: (async (protocol) => {
        asked.push({ nodeId, body: (await protocol.request.encode()).buffer });
        await options.onRequest?.(nodeId);
        const answer = answers[nodeId].shift();
        if (answer instanceof Error) throw answer;
        if (!answer) throw new Error(`no answer left for broker ${nodeId}`);
        return protocol.response.decode(answer);
      }) as SendRequest,
    }),
  };
}

const askedBrokers = (cluster: FakeCluster): string[] =>
  cluster.asked.flatMap((entry) => (entry === "metadata" ? [] : [entry.nodeId]));

function kafkaError(name: string, message: string, extra: object = {}): Error {
  return Object.assign(new Error(message), { name, ...extra });
}

const closedConnection = (broker = "b-2.example:9098") =>
  kafkaError("KafkaJSConnectionClosedError", "Closed connection", { broker });

/** The KafkaJS internals of a broker with an open connection, without SASL. */
const openConnectionTo = (host: string, port: number) => ({
  disconnect: async () => undefined,
  connectionPool: { host, port, pool: [{ isConnected: () => true, authenticatedAt: null, sessionLifetime: 0 }] },
});

const fast = { retryPauseMs: 0 };

describe("aggregateTopicSizes", () => {
  it("sums leader and replica bytes per partition and per topic", () => {
    const leaders = new Map([
      [
        "orders",
        new Map([
          [0, 1],
          [1, 2],
        ]),
      ],
    ]);
    const sizes = aggregateTopicSizes(
      [
        { topic: "orders", partition: 0, nodeId: 1, bytes: 100n, offsetLag: 0n },
        { topic: "orders", partition: 0, nodeId: 2, bytes: 90n, offsetLag: 3n },
        { topic: "orders", partition: 1, nodeId: 2, bytes: 50n, offsetLag: 0n },
        { topic: "orders", partition: 1, nodeId: 1, bytes: 50n, offsetLag: 0n },
      ],
      { leaders },
      [],
    );
    expect(sizes.supported).toBe(true);
    expect(sizes.topics.orders).toEqual({
      leaderBytes: "150",
      replicaBytes: "290",
      exact: true,
      partitions: [
        {
          partition: 0,
          leaderBytes: "100",
          replicaBytes: "190",
          replicas: [
            { nodeId: 1, bytes: "100", offsetLag: "0" },
            { nodeId: 2, bytes: "90", offsetLag: "3" },
          ],
        },
        {
          partition: 1,
          leaderBytes: "50",
          replicaBytes: "100",
          replicas: [
            { nodeId: 1, bytes: "50", offsetLag: "0" },
            { nodeId: 2, bytes: "50", offsetLag: "0" },
          ],
        },
      ],
    });
  });

  it("marks a topic inexact when a partition leader did not report", () => {
    const leaders = new Map([
      [
        "orders",
        new Map([
          [0, 1],
          [1, 3],
        ]),
      ],
    ]);
    const sizes = aggregateTopicSizes(
      [{ topic: "orders", partition: 0, nodeId: 1, bytes: 100n, offsetLag: 0n }],
      { leaders },
      [3],
    );
    expect(sizes.topics.orders.exact).toBe(false);
    expect(sizes.topics.orders.leaderBytes).toBe("100");
    expect(sizes.topics.orders.partitions[1]).toMatchObject({ partition: 1, leaderBytes: "0", replicas: [] });
    expect(sizes.unavailableBrokers).toEqual([3]);
  });
});

describe("fetchTopicSizes", () => {
  it("asks every broker, leaves the failing one out with its reason and aggregates by leader", async () => {
    const lines: string[] = [];
    const cluster = fakeCluster(
      {
        orders: [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 2 },
        ],
      },
      {
        "1": [
          brokerResponse([
            ["orders", 0, 1000],
            ["orders", 1, 999],
          ]),
        ],
        "2": [
          brokerResponse([
            ["orders", 1, 2000],
            ["orders", 0, 1001],
          ]),
        ],
        "3": [new Error("boom")],
      },
    );
    const sizes = await fetchTopicSizes(cluster, ["orders", "orders"], { ...fast, log: (line) => lines.push(line) });
    expect(sizes.supported).toBe(true);
    expect(sizes.unavailableBrokers).toEqual([3]);
    expect(sizes.topics.orders.leaderBytes).toBe("3000");
    expect(sizes.topics.orders.replicaBytes).toBe("5000");
    expect(sizes.topics.orders.exact).toBe(true);
    expect(lines).toEqual(["broker 3 left out: Error: boom"]);
  });

  it("reads the metadata of all topics once and asks each broker for every log directory, naming no topic", async () => {
    const cluster = fakeCluster(
      { orders: [{ partitionId: 0, leader: 1 }] },
      {
        "1": [
          brokerResponse([
            ["orders", 0, 10],
            ["__consumer_offsets", 0, 500],
            ["not-requested", 0, 70],
          ]),
        ],
        "2": [brokerResponse([["not-requested", 1, 30]])],
      },
    );
    const sizes = await fetchTopicSizes(cluster, ["orders"], fast);
    // A null topic array on the wire: every partition the broker hosts, as kafka-log-dirs asks.
    const allLogDirs = new Encoder().writeInt32(-1).buffer;
    expect(cluster.asked[0]).toBe("metadata");
    expect(cluster.asked.slice(1)).toEqual(
      expect.arrayContaining([
        { nodeId: "1", body: allLogDirs },
        { nodeId: "2", body: allLogDirs },
      ]),
    );
    expect(cluster.asked).toHaveLength(3);
    expect(Object.keys(sizes.topics)).toEqual(["orders"]);
    expect(sizes.topics.orders.leaderBytes).toBe("10");
    expect(sizes.unavailableBrokers).toEqual([]);
  });

  it("gives no size to a topic the metadata does not know and ignores the partitions it does not list", async () => {
    const cluster = fakeCluster(
      { orders: [{ partitionId: 0, leader: 1 }] },
      {
        "1": [
          brokerResponse([
            ["orders", 0, 10],
            // On disk but not in the metadata: just created, or being deleted.
            ["orders", 7, 99],
            ["deleted-meanwhile", 0, 5],
          ]),
        ],
      },
    );
    const sizes = await fetchTopicSizes(cluster, ["orders", "deleted-meanwhile"], fast);
    expect(Object.keys(sizes.topics)).toEqual(["orders"]);
    expect(sizes.topics.orders).toMatchObject({ leaderBytes: "10", replicaBytes: "10", exact: true });
    expect(sizes.topics.orders.partitions.map(({ partition }) => partition)).toEqual([0]);
  });

  it("reports a cluster without the API instead of failing, and says nothing more", async () => {
    const lines: string[] = [];
    const cluster = fakeCluster(
      { orders: [{ partitionId: 0, leader: 1 }] },
      { "1": [], "2": [] },
      { versions: { "1": WITHOUT_API, "2": WITHOUT_API } },
    );
    const sizes = await fetchTopicSizes(cluster, ["orders"], { ...fast, log: (line) => lines.push(line) });
    expect(sizes.supported).toBe(false);
    expect(sizes.topics.orders.exact).toBe(false);
    expect(askedBrokers(cluster)).toEqual([]);
    expect(lines).toEqual([]);
  });

  it("leaves out a broker without the API when the others have it", async () => {
    // A guard more than a case: KafkaJS gives every broker the API versions of the first one it
    // connected to, so the brokers of a real cluster all give the same answer.
    const lines: string[] = [];
    const cluster = fakeCluster(
      {
        orders: [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 2 },
        ],
      },
      { "1": [brokerResponse([["orders", 0, 10]])], "2": [] },
      { versions: { "2": WITHOUT_API } },
    );
    const sizes = await fetchTopicSizes(cluster, ["orders"], { ...fast, log: (line) => lines.push(line) });
    expect(sizes.supported).toBe(true);
    expect(sizes.unavailableBrokers).toEqual([2]);
    expect(sizes.topics.orders.exact).toBe(false);
    expect(lines).toEqual(["broker 2 left out: does not offer DescribeLogDirs version 1"]);
  });

  it("returns an empty result for no topics without touching the cluster", async () => {
    const cluster = fakeCluster({}, {});
    expect(await fetchTopicSizes(cluster, [])).toEqual({ supported: true, unavailableBrokers: [], topics: {} });
    expect(cluster.asked).toEqual([]);
  });

  it("skips a log directory that answers an error code and says so", async () => {
    const lines: string[] = [];
    const cluster = fakeCluster(
      {
        orders: [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 1 },
        ],
      },
      { "1": [response([logDir([["orders", 0, 10]]), logDir([["orders", 1, 99]], 56, "/data-2")])] },
    );
    const sizes = await fetchTopicSizes(cluster, ["orders"], { ...fast, log: (line) => lines.push(line) });
    expect(sizes.unavailableBrokers).toEqual([]);
    expect(sizes.topics.orders.leaderBytes).toBe("10");
    expect(sizes.topics.orders.exact).toBe(false);
    expect(lines).toEqual(["broker 1: log directory /data-2 answered error code 56"]);
  });

  it("leaves out a broker whose log directories all answer an error, and fails when no other reports", async () => {
    const lines: string[] = [];
    const cluster = fakeCluster(
      {
        orders: [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 2 },
        ],
      },
      {
        "1": [brokerResponse([["orders", 0, 10]])],
        "2": [response([logDir([["orders", 1, 99]], 57), logDir([], 56, "/data-2"), logDir([], 57, "/data-3")])],
      },
    );
    const sizes = await fetchTopicSizes(cluster, ["orders"], { ...fast, log: (line) => lines.push(line) });
    expect(sizes.unavailableBrokers).toEqual([2]);
    expect(sizes.topics.orders).toMatchObject({ leaderBytes: "10", exact: false });
    expect(lines).toEqual(["broker 2 left out: every log directory answered an error (code 56, 57)"]);

    // Lower bounds of zero on every topic would say nothing: the read fails with the reason.
    const alone = fakeCluster(
      { orders: [{ partitionId: 0, leader: 1 }] },
      { "1": [response([logDir([["orders", 0, 99]], 56)])] },
    );
    await expect(fetchTopicSizes(alone, ["orders"], fast)).rejects.toThrow(
      "asking the brokers: broker 1: every log directory answered an error (code 56)",
    );
  });

  it("leaves out a broker that answers with no log directory", async () => {
    const lines: string[] = [];
    const cluster = fakeCluster(
      {
        orders: [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 2 },
        ],
      },
      { "1": [brokerResponse([["orders", 0, 10]])], "2": [response([])] },
    );
    const sizes = await fetchTopicSizes(cluster, ["orders"], { ...fast, log: (line) => lines.push(line) });
    expect(sizes.unavailableBrokers).toEqual([2]);
    expect(sizes.topics.orders.exact).toBe(false);
    expect(lines).toEqual([
      "broker 2 left out: answered with no log directory, as a broker does for a user that may not describe the cluster",
    ]);
  });

  it("goes on with the other brokers when the first one cannot be reached", async () => {
    const lines: string[] = [];
    const cluster = fakeCluster(
      {
        orders: [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 2 },
        ],
      },
      { "1": [], "2": [brokerResponse([["orders", 1, 20]])] },
    );
    const reachable = cluster.findBroker;
    cluster.findBroker = async (request) => {
      if (request.nodeId === "1") {
        throw kafkaError("KafkaJSConnectionError", "Connection timeout", { broker: "b-1.example:9098" });
      }
      return reachable(request);
    };
    const sizes = await fetchTopicSizes(cluster, ["orders"], { ...fast, log: (line) => lines.push(line) });
    expect(sizes.unavailableBrokers).toEqual([1]);
    expect(sizes.topics.orders).toMatchObject({ leaderBytes: "20", exact: false });
    // A connect timeout is not asked again: the second try would only wait as long.
    expect(lines).toEqual([
      "broker 1 left out: KafkaJSConnectionError: Connection timeout (broker b-1.example:9098), on a new connection",
    ]);
  });

  it("fails with the reason of each broker when no broker reports, instead of showing lower bounds", async () => {
    const cluster: LogDirsCluster = {
      ...fakeCluster(
        { orders: [{ partitionId: 0, leader: 1 }] },
        { "1": [new Error("boom")], "2": [closedConnection(), closedConnection()], "3": [response([])] },
      ),
      brokerPool: { brokers: { 2: openConnectionTo("b-2.example", 9098) } },
    };
    const failure = await fetchTopicSizes(cluster, ["orders"], fast).catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(TopicSizesStepError);
    const stepError = failure as TopicSizesStepError;
    expect(stepError.step).toBe("asking the brokers");
    expect(stepError.message).toBe(
      "asking the brokers: broker 1: Error: boom; broker 2: KafkaJSConnectionClosedError: Closed connection (broker b-2.example:9098), on a connection already open; then on a new connection: KafkaJSConnectionClosedError: Closed connection (broker b-2.example:9098); broker 3: answered with no log directory, as a broker does for a user that may not describe the cluster",
    );
    // Every broker that dropped the connection was already asked again: reading it all again would not help.
    expect(stepError.dropped).toBe(false);
  });

  it("leaves out a broker that does not answer in time and keeps what the others report", async () => {
    const lines: string[] = [];
    const cluster = fakeCluster(
      {
        orders: [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 2 },
        ],
      },
      { "1": [brokerResponse([["orders", 0, 10]])], "2": [brokerResponse([["orders", 1, 20]])] },
      {
        // Broker 2 gets the request and never answers.
        onRequest: (nodeId) => (nodeId === "2" ? new Promise<void>(() => undefined) : Promise.resolve()),
      },
    );
    const sizes = await fetchTopicSizes(cluster, ["orders"], {
      ...fast,
      brokerTimeoutMs: 20,
      log: (line) => lines.push(line),
    });
    expect(sizes.unavailableBrokers).toEqual([2]);
    expect(sizes.topics.orders).toMatchObject({ leaderBytes: "10", exact: false });
    expect(askedBrokers(cluster).filter((nodeId) => nodeId === "2")).toHaveLength(1);
    expect(lines).toEqual(["broker 2 left out: TimeoutError: no answer after 20 ms"]);
  });

  it("goes on with the brokers still to ask when one used up its own time, not the time of the read", async () => {
    const cluster = fakeCluster(
      {
        orders: [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 2 },
        ],
      },
      { "1": [brokerResponse([["orders", 0, 10]])], "2": [brokerResponse([["orders", 1, 20]])] },
      {
        // Broker 1, asked first, never answers.
        onRequest: (nodeId) => (nodeId === "1" ? new Promise<void>(() => undefined) : Promise.resolve()),
      },
    );
    const sizes = await fetchTopicSizes(cluster, ["orders"], {
      ...fast,
      concurrency: 1,
      brokerTimeoutMs: 20,
      deadline: Date.now() + 60_000,
    });
    expect(askedBrokers(cluster)).toEqual(["1", "2"]);
    expect(sizes.unavailableBrokers).toEqual([1]);
    expect(sizes.topics.orders).toMatchObject({ leaderBytes: "20", exact: false });
  });

  it("gives a broker one time for both tries", async () => {
    const lines: string[] = [];
    let requests = 0;
    const cluster = fakeCluster(
      { orders: [{ partitionId: 0, leader: 1 }] },
      { "1": [closedConnection("b-1.example:9098"), brokerResponse([["orders", 0, 10]])] },
      {
        // The first request is dropped after 60 ms, the second one never gets an answer.
        onRequest: () =>
          ++requests === 1 ? new Promise((resolve) => setTimeout(resolve, 60)) : new Promise<void>(() => undefined),
      },
    );
    const failure = (await fetchTopicSizes(cluster, ["orders"], {
      ...fast,
      brokerTimeoutMs: 600,
      log: (line) => lines.push(line),
    }).catch((error: unknown) => error)) as Error;
    expect(requests).toBe(2);
    const secondTry = /; then on a new connection: TimeoutError: no answer after (\d+) ms$/.exec(failure.message);
    // What was left of the 600 ms after the first try, not 600 ms again.
    expect(Number(secondTry?.[1])).toBeGreaterThan(0);
    expect(Number(secondTry?.[1])).toBeLessThanOrEqual(545);
    expect(lines).toHaveLength(2);
  });

  it("waits for the dropped connection to close within the time of the broker", async () => {
    const cluster = fakeCluster(
      {
        orders: [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 2 },
        ],
      },
      { "1": [brokerResponse([["orders", 0, 10]])], "2": [closedConnection(), brokerResponse([["orders", 1, 20]])] },
    );
    // The connection never finishes closing.
    cluster.brokerPool = { brokers: { 2: { disconnect: () => new Promise<void>(() => undefined) } } };
    const lines: string[] = [];
    const sizes = await fetchTopicSizes(cluster, ["orders"], {
      ...fast,
      brokerTimeoutMs: 30,
      log: (line) => lines.push(line),
    });
    expect(sizes.unavailableBrokers).toEqual([2]);
    expect(sizes.topics.orders).toMatchObject({ leaderBytes: "10", exact: false });
    expect(askedBrokers(cluster).filter((nodeId) => nodeId === "2")).toHaveLength(1);
    // No request went out on a new connection, so none is blamed.
    expect(lines[1]).toBe(
      "broker 2 left out: KafkaJSConnectionClosedError: Closed connection (broker b-2.example:9098), on a new connection; no time was left to ask again",
    );
  });

  it("sends nothing to a broker it stopped waiting for", async () => {
    const leaders = {
      orders: [
        { partitionId: 0, leader: 1 },
        { partitionId: 1, leader: 2 },
        { partitionId: 2, leader: 3 },
      ],
    };
    const later = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

    // Broker 2 takes longer to connect than it is given: the connection comes when it is already left out.
    const slowToConnect = fakeCluster(leaders, {
      "1": [brokerResponse([["orders", 0, 10]])],
      "2": [brokerResponse([["orders", 1, 20]])],
    });
    const connect = slowToConnect.findBroker;
    slowToConnect.findBroker = async (request) => {
      if (request.nodeId === "2") await later(80);
      return connect(request);
    };
    const first = await fetchTopicSizes(slowToConnect, ["orders"], { ...fast, brokerTimeoutMs: 20 });
    expect(first.unavailableBrokers).toEqual([2]);

    // Broker 3 drops the connection, and closing it takes longer than the broker is given.
    const slowToClose = fakeCluster(leaders, {
      "1": [brokerResponse([["orders", 0, 10]])],
      "3": [closedConnection("b-3.example:9098"), brokerResponse([["orders", 2, 30]])],
    });
    slowToClose.brokerPool = { brokers: { 3: { disconnect: () => later(80) } } };
    const connected: string[] = [];
    const reconnect = slowToClose.findBroker;
    slowToClose.findBroker = async (request) => {
      connected.push(request.nodeId);
      return reconnect(request);
    };
    const second = await fetchTopicSizes(slowToClose, ["orders"], { ...fast, brokerTimeoutMs: 20 });
    expect(second.unavailableBrokers).toEqual([3]);

    // The read is abandoned during the pause before the second try.
    const abandoned = new AbortController();
    const paused = fakeCluster(leaders, {
      "3": [closedConnection("b-3.example:9098"), brokerResponse([["orders", 2, 30]])],
    });
    paused.brokerPool = { brokers: { 3: { disconnect: async () => abandoned.abort() } } };
    await fetchTopicSizes(paused, ["orders"], { retryPauseMs: 20, signal: abandoned.signal }).catch(() => undefined);

    await later(120);
    expect(askedBrokers(slowToConnect)).toEqual(["1"]);
    expect(askedBrokers(slowToClose).sort()).toEqual(["1", "3"]);
    // Not even a connection is opened towards a broker that was given up.
    expect(connected.sort()).toEqual(["1", "3"]);
    expect(askedBrokers(paused)).toEqual(["3"]);
  });

  it("ends at its deadline: a broker gets the time that is left and the next ones are not asked", async () => {
    const lines: string[] = [];
    const cluster = fakeCluster(
      {
        orders: [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 2 },
          { partitionId: 2, leader: 3 },
        ],
      },
      {
        "1": [brokerResponse([["orders", 0, 10]])],
        "2": [brokerResponse([["orders", 1, 20]])],
        "3": [brokerResponse([["orders", 2, 30]])],
      },
      {
        // Broker 2 gets the request and never answers.
        onRequest: (nodeId) => (nodeId === "2" ? new Promise<void>(() => undefined) : Promise.resolve()),
      },
    );
    const sizes = await fetchTopicSizes(cluster, ["orders"], {
      ...fast,
      concurrency: 1,
      brokerTimeoutMs: 10_000,
      deadline: Date.now() + 250,
      log: (line) => lines.push(line),
    });
    expect(sizes.unavailableBrokers).toEqual([2, 3]);
    expect(sizes.topics.orders).toMatchObject({ leaderBytes: "10", exact: false });
    expect(askedBrokers(cluster)).toEqual(["1", "2"]);
    expect(lines).toHaveLength(2);
    const waited = /^broker 2 left out: TimeoutError: no answer after (\d+) ms$/.exec(lines[0]);
    expect(Number(waited?.[1])).toBeGreaterThan(0);
    expect(Number(waited?.[1])).toBeLessThanOrEqual(250);
    expect(lines[1]).toBe("broker 3 left out: not asked: the read ran out of time");
  });

  it("ends at its deadline although the timer fires before the clock gets there", async () => {
    // A timer may fire a moment early. Here the clock never moves, so the wait of broker 2 ends
    // with the clock still short of the deadline: broker 3 must not be asked all the same.
    const frozen = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(frozen);
    const lines: string[] = [];
    const cluster = fakeCluster(
      {
        orders: [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 2 },
          { partitionId: 2, leader: 3 },
        ],
      },
      {
        "1": [brokerResponse([["orders", 0, 10]])],
        "2": [brokerResponse([["orders", 1, 20]])],
        "3": [brokerResponse([["orders", 2, 30]])],
      },
      { onRequest: (nodeId) => (nodeId === "2" ? new Promise<void>(() => undefined) : Promise.resolve()) },
    );
    try {
      const sizes = await fetchTopicSizes(cluster, ["orders"], {
        ...fast,
        concurrency: 1,
        brokerTimeoutMs: 10_000,
        deadline: frozen + 30,
        log: (line) => lines.push(line),
      });
      expect(sizes.unavailableBrokers).toEqual([2, 3]);
    } finally {
      clock.mockRestore();
    }
    expect(askedBrokers(cluster)).toEqual(["1", "2"]);
    expect(lines).toEqual([
      "broker 2 left out: TimeoutError: no answer after 30 ms",
      "broker 3 left out: not asked: the read ran out of time",
    ]);
  });

  it("says the time a broker was given, whatever the clock does between two looks", async () => {
    // Every look at the clock finds it one millisecond later.
    let now = Date.now();
    const clock = vi.spyOn(Date, "now").mockImplementation(() => now++);
    const lines: string[] = [];
    const cluster = fakeCluster(
      { orders: [{ partitionId: 0, leader: 1 }] },
      { "1": [brokerResponse([["orders", 0, 10]])] },
      { onRequest: () => new Promise<void>(() => undefined) },
    );
    try {
      await fetchTopicSizes(cluster, ["orders"], {
        ...fast,
        brokerTimeoutMs: 20,
        log: (line) => lines.push(line),
      }).catch(() => undefined);
    } finally {
      clock.mockRestore();
    }
    expect(lines).toEqual(["broker 1 left out: TimeoutError: no answer after 20 ms"]);
  });

  it("names the metadata step when the metadata does not come before the deadline", async () => {
    const cluster: FakeCluster = {
      ...fakeCluster({ orders: [{ partitionId: 0, leader: 1 }] }, { "1": [brokerResponse([["orders", 0, 10]])] }),
      refreshMetadata: () => new Promise<void>(() => undefined),
    };
    const failure = await fetchTopicSizes(cluster, ["orders"], { ...fast, deadline: Date.now() + 30 }).catch(
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(TopicSizesStepError);
    const stepError = failure as TopicSizesStepError;
    expect(stepError.step).toBe("reading the cluster metadata");
    expect(stepError.message).toMatch(/^reading the cluster metadata: TimeoutError: no answer after \d+ ms$/);
    // Nothing was dropped: reading it all again would only wait as long.
    expect(stepError.dropped).toBe(false);
    expect(askedBrokers(cluster)).toEqual([]);
  });

  it("asks nothing when its deadline has already passed", async () => {
    const cluster = fakeCluster(
      { orders: [{ partitionId: 0, leader: 1 }] },
      { "1": [brokerResponse([["orders", 0, 10]])] },
    );
    await expect(fetchTopicSizes(cluster, ["orders"], { ...fast, deadline: Date.now() - 1 })).rejects.toThrow(
      "reading the cluster metadata: TimeoutError: no time left to ask",
    );
    expect(cluster.asked).toEqual([]);
  });

  it("asks eight brokers at a time", async () => {
    let active = 0;
    let maxActive = 0;
    const answers = Object.fromEntries(
      Array.from({ length: 12 }, (_, index) => [String(index + 1), [brokerResponse([["orders", 0, 1]])]]),
    );
    const cluster = fakeCluster({ orders: [{ partitionId: 0, leader: 1 }] }, answers, {
      onRequest: async () => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, 2));
        active -= 1;
      },
    });
    const sizes = await fetchTopicSizes(cluster, ["orders"], fast);
    expect(maxActive).toBe(8);
    expect(askedBrokers(cluster)).toHaveLength(12);
    expect(sizes.unavailableBrokers).toEqual([]);
  });

  it("stops asking brokers once the read is aborted", async () => {
    const abandoned = new AbortController();
    const cluster = fakeCluster(
      { orders: [{ partitionId: 0, leader: 1 }] },
      { "1": [brokerResponse([["orders", 0, 1]])], "2": [brokerResponse([])], "3": [brokerResponse([])] },
      {
        onRequest: async () => {
          abandoned.abort();
        },
      },
    );
    await expect(
      fetchTopicSizes(cluster, ["orders"], { ...fast, concurrency: 1, signal: abandoned.signal }),
    ).rejects.toThrow("Topic size fetch aborted");
    expect(askedBrokers(cluster)).toEqual(["1"]);
  });
});

describe("answerWithin", () => {
  afterEach(() => vi.useRealTimers());

  it("gives the answer that comes in time, and the failure", async () => {
    await expect(answerWithin(1_000, async () => "sizes")).resolves.toBe("sizes");
    await expect(answerWithin(1_000, () => Promise.reject(new Error("boom")))).rejects.toThrow("boom");
  });

  it("stops waiting after the given time and drops the late answer", async () => {
    let answer: (value: string) => void = () => undefined;
    const waiting = answerWithin(19.2, () => new Promise<string>((resolve) => (answer = resolve)));
    const failure = (await waiting.catch((error: unknown) => error)) as Error;
    expect(failure.name).toBe("TimeoutError");
    expect(failure.message).toBe("no answer after 20 ms");
    answer("too late");
    await expect(waiting).rejects.toBe(failure);
  });

  it("does not ask when no time is left", async () => {
    let asked = 0;
    const ask = async () => ++asked;
    for (const ms of [0, -5, Number.NaN]) {
      const failure = (await answerWithin(ms, ask).catch((error: unknown) => error)) as Error;
      expect(failure.name).toBe("TimeoutError");
      expect(failure.message).toBe("no time left to ask");
    }
    expect(asked).toBe(0);
  });

  it("waits for the answer when the time is not limited", async () => {
    await expect(
      answerWithin(Number.POSITIVE_INFINITY, () => new Promise((resolve) => setTimeout(() => resolve("late"), 30))),
    ).resolves.toBe("late");
  });

  it("keeps waiting when the time is longer than a timer can count", async () => {
    vi.useFakeTimers();
    let settled = false;
    // A timer armed with more than 2^31 - 1 ms fires after one.
    void answerWithin(2 ** 31 + 5, () => new Promise<never>(() => undefined)).catch(() => {
      settled = true;
    });
    await vi.advanceTimersByTimeAsync(60_000);
    expect(settled).toBe(false);
  });

  it("tells its own timeout from a timeout somebody else reports", async () => {
    const own = await answerWithin(0, async () => "never asked").catch((error: unknown) => error);
    expect(isAnswerTimeout(own)).toBe(true);
    expect(isAnswerTimeout(Object.assign(new Error("Request timed out"), { name: "TimeoutError" }))).toBe(false);
    expect(isAnswerTimeout(undefined)).toBe(false);
  });
});

describe("fetchTopicSizes on a dropped connection", () => {
  it("names the step and what was known of the connection before the broker closed it", async () => {
    const connection = {
      isConnected: () => true,
      authenticatedAt: process.hrtime() as [number, number] | null,
      sessionLifetime: { toString: () => "899000" },
    };
    const cluster: LogDirsCluster = {
      ...fakeCluster({ orders: [{ partitionId: 0, leader: 1 }] }, { 1: [brokerResponse([["orders", 0, 10]])] }),
      brokerPool: { seedBroker: { connectionPool: { host: "b-2.example", port: 9098, pool: [connection] } } },
      refreshMetadata: async () => {
        // What KafkaJS does to a connection the broker closed, before the failure is described.
        connection.authenticatedAt = null;
        connection.isConnected = () => false;
        throw closedConnection();
      },
    };
    const failure = await fetchTopicSizes(cluster, ["orders"], fast).catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(TopicSizesStepError);
    const stepError = failure as TopicSizesStepError;
    expect(stepError.message).toBe(
      "reading the cluster metadata: KafkaJSConnectionClosedError: Closed connection (broker b-2.example:9098), on a connection already open, authenticated 0 s ago, SASL session lifetime 899000 ms",
    );
    expect(stepError.step).toBe("reading the cluster metadata");
    expect(stepError.dropped).toBe(true);
  });

  it("says a new connection when the cluster had none open towards that broker", async () => {
    const plain = { isConnected: () => true, authenticatedAt: null, sessionLifetime: 0 };
    const cluster: LogDirsCluster = {
      ...fakeCluster({ orders: [{ partitionId: 0, leader: 1 }] }, { 1: [brokerResponse([["orders", 0, 10]])] }),
      // Open, without SASL, towards another broker.
      brokerPool: { brokers: { 1: { connectionPool: { host: "b-1.example", port: 9092, pool: [plain] } } } },
      refreshMetadata: async () => {
        throw closedConnection("b-2.example:9092");
      },
    };
    const first = (await fetchTopicSizes(cluster, ["orders"], fast).catch((error: unknown) => error)) as Error;
    expect(first.message).toBe(
      "reading the cluster metadata: KafkaJSConnectionClosedError: Closed connection (broker b-2.example:9092), on a new connection",
    );

    cluster.refreshMetadata = async () => {
      throw closedConnection("b-1.example:9092");
    };
    const second = (await fetchTopicSizes(cluster, ["orders"], fast).catch((error: unknown) => error)) as Error;
    expect(second.message).toBe(
      "reading the cluster metadata: KafkaJSConnectionClosedError: Closed connection (broker b-1.example:9092), on a connection already open",
    );
  });

  it("does not take a timeout or a plain error for a dropped connection", async () => {
    const timedOut = kafkaError("KafkaJSRequestTimeoutError", "Request Metadata(key: 3, version: 6) timed out");
    for (const reason of [new Error("boom"), timedOut, kafkaError("KafkaJSConnectionError", "Connection timeout")]) {
      const cluster: LogDirsCluster = {
        ...fakeCluster({ orders: [{ partitionId: 0, leader: 1 }] }, { 1: [brokerResponse([["orders", 0, 10]])] }),
        refreshMetadata: async () => {
          throw reason;
        },
      };
      const failure = await fetchTopicSizes(cluster, ["orders"], fast).catch((error: unknown) => error);
      expect(failure).toBeInstanceOf(TopicSizesStepError);
      expect((failure as TopicSizesStepError).dropped).toBe(false);
    }
  });

  it("asks a broker once more on a new connection when it closes or resets one mid-request", async () => {
    const disconnected: string[] = [];
    const lines: string[] = [];
    const reset = kafkaError("KafkaJSConnectionError", "Connection error: read ECONNRESET", {
      broker: "b-3.example:9098",
      code: "ECONNRESET",
    });
    const dropHandle = (nodeId: string) => ({
      disconnect: async () => {
        disconnected.push(nodeId);
      },
    });
    const cluster: LogDirsCluster = {
      ...fakeCluster(
        {
          orders: [
            { partitionId: 0, leader: 1 },
            { partitionId: 1, leader: 2 },
            { partitionId: 2, leader: 3 },
          ],
        },
        {
          1: [brokerResponse([["orders", 0, 10]])],
          2: [closedConnection(), brokerResponse([["orders", 1, 20]])],
          3: [reset, brokerResponse([["orders", 2, 5]])],
        },
      ),
      brokerPool: { brokers: { 2: dropHandle("2"), 3: dropHandle("3") } },
    };
    const sizes = await fetchTopicSizes(cluster, ["orders"], {
      ...fast,
      concurrency: 1,
      log: (line) => lines.push(line),
    });
    expect(sizes.unavailableBrokers).toEqual([]);
    expect(sizes.topics.orders.leaderBytes).toBe("35");
    expect(sizes.topics.orders.exact).toBe(true);
    expect(disconnected).toEqual(["2", "3"]);
    expect(lines).toEqual([
      "broker 2: KafkaJSConnectionClosedError: Closed connection (broker b-2.example:9098), on a new connection; asking again on a new connection",
      "broker 2: answered on the new connection",
      "broker 3: KafkaJSConnectionError: Connection error: read ECONNRESET (broker b-3.example:9098), on a new connection; asking again on a new connection",
      "broker 3: answered on the new connection",
    ]);
  });

  it("leaves a broker out when the new connection is dropped too", async () => {
    const lines: string[] = [];
    const cluster = fakeCluster(
      {
        orders: [
          { partitionId: 0, leader: 1 },
          { partitionId: 1, leader: 2 },
          { partitionId: 2, leader: 3 },
        ],
      },
      {
        1: [brokerResponse([["orders", 0, 10]])],
        2: [closedConnection(), closedConnection()],
        3: [brokerResponse([["orders", 2, 5]])],
      },
    );
    cluster.brokerPool = { brokers: { 2: openConnectionTo("b-2.example", 9098) } };
    const sizes = await fetchTopicSizes(cluster, ["orders"], {
      ...fast,
      concurrency: 1,
      log: (line) => lines.push(line),
    });
    expect(sizes.unavailableBrokers).toEqual([2]);
    expect(sizes.topics.orders.exact).toBe(false);
    expect(sizes.topics.orders.leaderBytes).toBe("15");
    expect(askedBrokers(cluster)).toEqual(["1", "2", "2", "3"]);
    expect(lines).toEqual([
      "broker 2: KafkaJSConnectionClosedError: Closed connection (broker b-2.example:9098), on a connection already open; asking again on a new connection",
      "broker 2 left out: KafkaJSConnectionClosedError: Closed connection (broker b-2.example:9098), on a connection already open; then on a new connection: KafkaJSConnectionClosedError: Closed connection (broker b-2.example:9098)",
    ]);
  });
});
