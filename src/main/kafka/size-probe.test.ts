import { describe, expect, it, vi } from "vitest";
import {
  createScrubber,
  describeConnections,
  type ProbeCluster,
  probeTopicSizes,
  ReaderActivity,
  type SizeProbeDeps,
} from "./size-probe";

import type { KafkaProtocol } from "./group-offset-batch-protocol";

const HOST_1 = "b-1.secret-cluster.abc123.c5.kafka.us-east-1.amazonaws.com";
const HOST_2 = "b-2.secret-cluster.abc123.c5.kafka.us-east-1.amazonaws.com";
const TOPICS = ["orders-events", "payments-ledger", "__amazon_msk_canary"];
const GROUPS = ["orders-consumers", "ledger-writer"];

function closed(host: string): Error {
  return Object.assign(new Error("Closed connection"), {
    name: "KafkaJSConnectionClosedError",
    broker: `${host}:9098`,
  });
}

type Behaviour = (protocol: KafkaProtocol<unknown>) => Promise<unknown>;

/** A KafkaJS broker as `sendKafkaProtocol` sees it: versions and the private request transport. */
function broker(host: string, nodeId: number, behaviour: Behaviour): object {
  const sendSymbol = Symbol("private:Broker:sendRequest");
  return {
    nodeId,
    versions: {
      3: { minVersion: 0, maxVersion: 12 },
      9: { minVersion: 0, maxVersion: 9 },
      10: { minVersion: 0, maxVersion: 6 },
      35: { minVersion: 0, maxVersion: 4 },
    },
    connectionPool: {
      host,
      port: 9098,
      pool: [{ authenticatedAt: [1, 0], sessionLifetime: { toString: () => "899000" } }],
    },
    [sendSymbol]: vi.fn(behaviour),
  };
}

const LOG_DIRS = {
  throttleTime: 0,
  results: [
    {
      errorCode: 0,
      logDir: "/kafka/datalogs/logs",
      topics: [{ topic: TOPICS[0], partitions: [{ partition: 0, size: "10", offsetLag: "0", isFuture: false }] }],
    },
  ],
};

/** The broker as we suppose MSK Express behaves: it closes the connection on FindCoordinator v4 only. */
function closesOnFindCoordinator(host: string): Behaviour {
  return async (protocol) => {
    switch (protocol.request.apiName) {
      case "FindCoordinator":
        throw closed(host);
      case "OffsetFetch":
        return { throttleTime: 0, groups: protocol.groupIds.map((groupId) => ({ groupId, topics: [], errorCode: 0 })) };
      default:
        return LOG_DIRS;
    }
  };
}

/** A broker that answers everything: FindCoordinator puts every group on broker 1. */
function answersEverything(host: string): Behaviour {
  return async (protocol) => {
    switch (protocol.request.apiName) {
      case "FindCoordinator":
        return {
          throttleTime: 0,
          coordinators: protocol.groupIds.map((key) => ({
            key,
            nodeId: 1,
            host,
            port: 9098,
            errorCode: 0,
            errorMessage: null,
          })),
        };
      case "OffsetFetch":
        return { throttleTime: 0, groups: protocol.groupIds.map((groupId) => ({ groupId, topics: [], errorCode: 0 })) };
      default:
        return LOG_DIRS;
    }
  };
}

interface FakeOptions {
  behaviour?: (host: string) => Behaviour;
  /** The metadata request fails with this error, when set. */
  metadataError?: (host: string) => Error;
  connect?: () => Promise<void>;
}

function clusterFactory(options: FakeOptions = {}) {
  const created: Array<{ bootstrap?: readonly string[]; disconnect: ReturnType<typeof vi.fn> }> = [];
  const factory = (bootstrap?: readonly string[]): ProbeCluster => {
    const behaviour = options.behaviour ?? closesOnFindCoordinator;
    const brokers = { "1": broker(HOST_1, 1, behaviour(HOST_1)), "2": broker(HOST_2, 2, behaviour(HOST_2)) };
    const seed = bootstrap?.[0]?.startsWith(HOST_2) ? brokers["2"] : brokers["1"];
    let metadataLoaded = false;
    const disconnect = vi.fn(async () => undefined);
    created.push({ bootstrap, disconnect });
    const fake = {
      connect: vi.fn(options.connect ?? (async () => undefined)),
      disconnect,
      refreshMetadata: vi.fn(async () => {
        if (options.metadataError) throw options.metadataError(HOST_1);
        metadataLoaded = true;
        fake.brokerPool.metadata = {
          brokers: [
            { nodeId: 1, host: HOST_1, port: 9098 },
            { nodeId: 2, host: HOST_2, port: 9098 },
          ],
          topicMetadata: TOPICS.map(() => ({ partitionMetadata: [{}, {}] })),
        };
      }),
      getNodeIds: () => ["1", "2"],
      findTopicPartitionMetadata: (topic: string) => {
        if (!metadataLoaded) {
          throw Object.assign(new Error("Topic metadata not loaded"), { name: "KafkaJSTopicMetadataNotLoaded" });
        }
        return TOPICS.includes(topic)
          ? [
              { partitionId: 0, leader: 1 },
              { partitionId: 1, leader: 2 },
            ]
          : [];
      },
      fetchTopicsOffset: vi.fn(async (topics: Array<{ topic: string; partitions: Array<{ partition: number }> }>) =>
        topics.map(({ topic, partitions }) => ({
          topic,
          partitions: partitions.map(({ partition }) => ({ partition, offset: "42" })),
        })),
      ),
      findBroker: vi.fn(async ({ nodeId }: { nodeId: string }) => brokers[nodeId as "1" | "2"]),
      brokerPool: { seedBroker: seed, metadata: undefined as unknown },
    };
    return fake as unknown as ProbeCluster;
  };
  return { factory, created };
}

function admin(listGroups?: () => Promise<{ groups: Array<{ groupId: string; protocolType: string }> }>) {
  return () => ({
    connect: vi.fn(async () => undefined),
    disconnect: vi.fn(async () => undefined),
    listGroups: vi.fn(
      listGroups ?? (async () => ({ groups: GROUPS.map((groupId) => ({ groupId, protocolType: "consumer" })) })),
    ),
  });
}

async function runProbe(overrides: Partial<SizeProbeDeps> = {}, fake = clusterFactory()) {
  const written: string[] = [];
  const summary = await probeTopicSizes(
    {
      failure: `TopicSizesError: reading the cluster metadata: KafkaJSConnectionClosedError: Closed connection (broker ${HOST_2}:9098), on a new connection (after 626 ms)`,
      topics: TOPICS,
      bootstrap: [`${HOST_1}:9098`, `${HOST_2}:9098`],
      settings: "SASL oauthbearer, TLS, direct connection",
      activity: [
        `running for 1200 ms: lag of consumer groups: FindCoordinator v4 failed: Closed connection (broker ${HOST_1}:9098) for ${GROUPS[0]}`,
      ],
      sharedConnections: ["broker 1: connection 0 open, not authenticated"],
      names: GROUPS,
    },
    {
      createCluster: fake.factory,
      createAdmin: admin(),
      write: async (text) => {
        written.push(text);
      },
      sleep: async () => undefined,
      idleMs: 1,
      pauseMs: 0,
      home: "/Users/janedoe",
      ...overrides,
    },
  );
  return { summary, report: written.at(-1) ?? "", writes: written.length, created: fake.created };
}

describe("probeTopicSizes", () => {
  it("names the request the broker closes the connection on, and answers the others", async () => {
    const { summary, report } = await runProbe();
    expect(summary).toBe(
      [
        "groups ok",
        "connect ok",
        "metadata-b1 ok",
        "metadata-b2 ok",
        "logdirs-b1 ok",
        "logdirs-b2 ok",
        "findcoordinator CLOSED",
        "offsetfetch skipped",
        "brokers ok",
        "logdirs-1 ok",
        "logdirs-2 ok",
        "watermarks ok",
        "together-metadata-findcoordinator CLOSED",
        "together-logdirs-offsetfetch skipped",
        "idle ok",
      ].join(", "),
    );
    expect(report).toMatch(/\[closed by the broker after \d+ ms\] FindCoordinator v4 for 2 group\(s\)/);
    expect(report).toContain("Metadata answered; FindCoordinator v4 closed by the broker");
    expect(report).toContain("SASL session lifetime 899000 ms");
    expect(report).toContain(
      "advertised versions: Metadata 0-12, OffsetFetch 0-9, FindCoordinator 0-6, DescribeLogDirs 0-4, SaslAuthenticate not offered",
    );
    expect(report).toContain("broker 1: connection 0 open, not authenticated");
    expect(report).toContain("Probe finished");
  });

  it("asks each bootstrap address on a cluster of its own, and closes every cluster it opened", async () => {
    const { created } = await runProbe();
    expect(created.filter(({ bootstrap }) => bootstrap?.[0] === `${HOST_2}:9098`)).toHaveLength(2);
    expect(created.every(({ disconnect }) => disconnect.mock.calls.length > 0)).toBe(true);
  });

  it("says when a step stopped before its own request", async () => {
    const { summary, report } = await runProbe(
      {},
      clusterFactory({ metadataError: closed, behaviour: answersEverything }),
    );
    expect(summary).toContain("metadata-b1 CLOSED,");
    expect(summary).toContain("findcoordinator ok");
    expect(summary).toContain("offsetfetch-1 CLOSED@metadata");
    expect(summary).toContain("logdirs skipped");
    expect(summary).toContain("watermarks CLOSED@metadata");
    expect(summary).toContain("together-logdirs-offsetfetch CLOSED@metadata");
    expect(report).toMatch(/\[closed by the broker during metadata after \d+ ms\] OffsetFetch v8/);
  });

  it("follows a KafkaJS wrapper to the closed connection it hides", async () => {
    const retries = (host: string) =>
      Object.assign(new Error("Closed connection"), {
        name: "KafkaJSNumberOfRetriesExceeded",
        cause: closed(host),
      });
    const { summary } = await runProbe({}, clusterFactory({ metadataError: retries, behaviour: answersEverything }));
    expect(summary).toContain("metadata-b1 CLOSED,");
  });

  it("names no host, topic, consumer group or AWS identity", async () => {
    const credentials = Object.assign(
      new Error(
        "Could not resolve credentials using profile: [acme-prod] in configuration/credentials file(s); arn:aws:iam::123456789012:user/jdoe; /Users/janedoe/.aws/sso/cache/abc.json",
      ),
      { name: "CredentialsProviderError" },
    );
    const { report } = await runProbe({
      createAdmin: admin(async () => {
        throw credentials;
      }),
    });
    expect(report).toContain("CredentialsProviderError: message withheld, it does not come from Kafka");
    for (const secret of [
      "secret-cluster",
      "amazonaws",
      "acme-prod",
      "123456789012",
      "arn:aws",
      "janedoe",
      ...TOPICS,
      ...GROUPS,
    ]) {
      expect(report).not.toContain(secret);
    }
    expect(report).toContain("(broker 2)");
    expect(report).toContain("(broker 1)");
  });

  it("writes the report after every step, so that a closed app leaves what was done", async () => {
    const { writes } = await runProbe();
    expect(writes).toBeGreaterThan(10);
  });

  it("closes what a step opened when the step runs out of time", async () => {
    const fake = clusterFactory({ connect: () => new Promise<void>(() => undefined) });
    const disconnects: Array<ReturnType<typeof vi.fn>> = [];
    const hangingAdmin = () => {
      const disconnect = vi.fn(async () => undefined);
      disconnects.push(disconnect);
      return { connect: () => new Promise<void>(() => undefined), disconnect, listGroups: vi.fn() };
    };
    const { summary, created } = await runProbe({ createAdmin: hangingAdmin as never, stepMs: 5 }, fake);
    expect(summary).toMatch(/^groups TIMEOUT@connect, connect TIMEOUT@connect, metadata-b1 TIMEOUT@connect/);
    expect(disconnects[0]).toHaveBeenCalled();
    expect(created.every(({ disconnect }) => disconnect.mock.calls.length > 0)).toBe(true);
  });

  it("skips what is left once the Kafka connection is closed", async () => {
    const { summary } = await runProbe({
      createAdmin: () => {
        throw new Error("must not be called");
      },
      stopped: () => true,
    });
    expect(summary).toMatch(/^groups skipped, connect skipped, metadata-b1 skipped/);
  });
});

describe("createScrubber", () => {
  const hosts = new Map([
    [`${HOST_1}:9098`, "broker 1"],
    [HOST_1, "broker 1"],
    ["boot-xyz.example.net:9098", "bootstrap 1"],
  ]);

  it("replaces the known hosts with their labels, whatever their case", () => {
    const scrub = createScrubber(hosts, []);
    expect(scrub(`Closed connection (broker ${HOST_1.toUpperCase()}:9098), then ${HOST_1}`)).toBe(
      "Closed connection (broker 1), then broker 1",
    );
  });

  it("hides any other host, IPv4 or IPv6 address, single-label host and AWS identity", () => {
    const scrub = createScrubber(new Map(), [], "/Users/janedoe");
    expect(
      scrub(
        "kafka.example.org:9092, 10.0.12.7:9098, [2600:1f18:abcd:12::7]:9098, kafka-0:9092, arn:aws:iam::123456789012:role/x, AKIAABCDEFGHIJKLMNOP, profile: [acme-prod], /Users/janedoe/.aws",
      ),
    ).toBe("<host>, <address>, <address>, <host>, <arn>, <key id>, profile: <profile>, ~/.aws");
  });

  it("hides topic and group names as whole tokens, without touching the labels", () => {
    const scrub = createScrubber(hosts, ["orders", "orders-events", "broker", "t1"]);
    expect(scrub(`orders failed, ordersX kept, orders-events gone, t1 too, (broker ${HOST_1}:9098)`)).toBe(
      "<name> failed, ordersX kept, <name> gone, <name> too, (broker 1)",
    );
  });
});

describe("describeConnections", () => {
  it("tells open, closing and closed connections apart, brokers by id", () => {
    const cluster = {
      brokerPool: {
        seedBroker: {
          nodeId: 1,
          connectionPool: {
            pool: [
              {
                isConnected: () => true,
                connectionStatus: "connected",
                authenticatedAt: [5, 0],
                sessionLifetime: { toString: () => "899000" },
              },
              { isConnected: () => false },
            ],
          },
        },
        brokers: {
          "2": {
            connectionPool: {
              pool: [{ isConnected: () => true, connectionStatus: "disconnecting", authenticatedAt: null }],
            },
          },
        },
      },
    } as unknown as ProbeCluster;
    expect(describeConnections(cluster, () => [12, 0])).toEqual([
      "bootstrap broker (broker 1): connection 0 open, authenticated 12 s ago, SASL session lifetime 899000 ms; connection 1 (Fetch) closed",
      "broker 2: connection 0 closing, not authenticated",
    ]);
    expect(describeConnections(undefined)).toEqual(["no read cluster was open"]);
  });
});

describe("ReaderActivity", () => {
  it("lists the running readers first, then the latest events", async () => {
    let now = 1_000;
    const activity = new ReaderActivity(() => now);
    let finish: () => void = () => undefined;
    const running = activity.track("lag", () => new Promise<string>((resolve) => (finish = () => resolve("done"))));
    activity.note("lag", "FindCoordinator v4 failed");
    now = 1_500;
    expect(activity.describe()).toEqual(["running for 500 ms: lag", "500 ms before: lag: FindCoordinator v4 failed"]);
    finish();
    await running;
    expect(activity.describe()).toEqual([
      "500 ms before: lag: FindCoordinator v4 failed",
      "0 ms before: lag: answered after 500 ms",
    ]);
  });
});
