/**
 * Evidence that the read cluster never names a topic, against a broker that closes the connection
 * on a Metadata request naming topics. That is the strictest reading of what MSK Express brokers
 * with IAM do (#82), where only "a request naming every topic" is known to be closed.
 *
 * 1. Topic sizes, the message browser and the high watermarks all work behind such a broker.
 * 2. When a broker closes on DescribeLogDirs, the failure names the step, the broker and what was
 *    known of the connection; when it closes on every Metadata request, both attempts are named;
 *    when it never answers DescribeLogDirs, it is left out before the page stops waiting; when it
 *    never answers the Metadata request, the read ends at its deadline and names that step.
 * 3. A deleted topic does not break the readers afterwards (KafkaJS keeps every topic name it is
 *    given, so one deleted name used to fail the next request that added another one).
 *
 * The broker is the disposable loopback fixture. In 1 and 2 every KafkaJS connection goes through
 * the port-forward chain with a local pipe that plays the closing broker: it reads the request
 * frames and ends the socket on the first one it is told to refuse. Step 3 creates and deletes one
 * scratch topic on the fixture, and the npm script first seeds the consumer group whose lag the
 * health read needs watermarks for, so the run refuses any other broker.
 *
 * Not in this run: the admin calls that name one topic in a request of their own (the detail of a
 * topic or of a consumer group). Behind this pipe they would be closed; nothing is known of them
 * on those brokers.
 *
 * Run:
 *   pnpm kafka:direct:up
 *   pnpm itest:read-cluster
 *   pnpm kafka:direct:down
 */
import { execFileSync } from "node:child_process";
import net from "node:net";
import { connectDirect, KafkaConnection } from "../../src/main/kafka/kafka-connection";
import { createKafkaJsReadCluster } from "../../src/main/kafka/message-fetch";
import { fetchHighWatermarks } from "../../src/main/kafka/offset-batch";

import type { Forwarder } from "../../src/main/kafka/port-forward-manager";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";
const [brokerHost, brokerPortText] = BROKER.split(":");
const brokerPort = Number(brokerPortText);
const TOPIC = "freelens-orders";
const SCRATCH_TOPIC = `freelens-read-cluster-scratch-${Date.now()}`;
const METADATA_API_KEY = 3;
const DESCRIBE_LOG_DIRS_API_KEY = 35;
/** Metadata is flexible from version 9: KafkaJS 2.2.4 stops at 6, so a newer one must fail the run. */
const LAST_NON_FLEXIBLE_METADATA_VERSION = 8;

const timeout = setTimeout(() => {
  process.stderr.write("read cluster integration timed out\n");
  process.exit(2);
}, 90_000);
timeout.unref();

function expect(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

interface DockerContainerInspect {
  Config?: { Image?: string; Labels?: Record<string, string> };
  HostConfig?: { PortBindings?: Record<string, Array<{ HostIp?: string; HostPort?: string }> | null> };
  State?: { Running?: boolean };
}

function requireDisposableBroker(): void {
  if (process.env.ALLOW_LOCAL_MUTATING_KAFKA_FIXTURE !== "1" || BROKER !== "127.0.0.1:19092") {
    throw new Error("Refusing a mutating fixture outside the loopback Kafka broker");
  }
  const inspected = JSON.parse(
    execFileSync("docker", ["inspect", "freelens-kafka-direct"], { encoding: "utf8" }),
  ) as DockerContainerInspect[];
  const container = inspected[0];
  const labels = container?.Config?.Labels ?? {};
  const bindings = container?.HostConfig?.PortBindings?.["19092/tcp"] ?? [];
  if (
    !container?.State?.Running ||
    container.Config?.Image !== "apache/kafka:3.9.0" ||
    labels["com.docker.compose.project"] !== "freelens-kafka-e2e-direct" ||
    labels["com.docker.compose.service"] !== "kafka-direct" ||
    !bindings.some(({ HostIp, HostPort }) => HostIp === "127.0.0.1" && HostPort === "19092")
  ) {
    throw new Error("Disposable Kafka fixture identity is unavailable");
  }
}

/** A request frame: size, api key, api version, correlation id, client id, body. */
type Refuses = (frame: Buffer) => boolean;

const apiKey = (frame: Buffer): number => frame.readInt16BE(4);
const isMetadata: Refuses = (frame) => apiKey(frame) === METADATA_API_KEY;
const isDescribeLogDirs: Refuses = (frame) => apiKey(frame) === DESCRIBE_LOG_DIRS_API_KEY;

/** A Metadata request whose topic array names at least one topic. */
const namesTopics: Refuses = (frame) => {
  if (!isMetadata(frame)) return false;
  const version = frame.readInt16BE(6);
  if (version > LAST_NON_FLEXIBLE_METADATA_VERSION) throw new Error(`unexpected Metadata v${version} request`);
  const clientIdLength = frame.readInt16BE(12);
  const body = 14 + Math.max(clientIdLength, 0);
  // A null topic array (-1) asks for every topic; so does an empty one in version 0.
  return frame.readInt32BE(body) > 0;
};

/**
 * A connection through a pipe to the fixture that ends the client socket on the requests it
 * refuses and swallows the ones it ignores, so that they never get an answer.
 */
async function connectThroughClosingBroker(refuses: Refuses, ignores: Refuses = () => false) {
  let closed = 0;
  const forwarder: Forwarder = (_target, socket) => {
    const upstream = net.connect(brokerPort, brokerHost);
    let pending = Buffer.alloc(0);
    upstream.on("data", (chunk) => socket.write(chunk));
    upstream.on("error", () => socket.destroy());
    upstream.on("close", () => socket.end());
    socket.on("error", () => upstream.destroy());
    socket.on("close", () => upstream.destroy());
    socket.on("data", (chunk: Buffer) => {
      pending = Buffer.concat([pending, chunk]);
      while (pending.length >= 4) {
        const size = 4 + pending.readInt32BE(0);
        if (pending.length < size) return;
        const frame = pending.subarray(0, size);
        pending = pending.subarray(size);
        if (refuses(frame)) {
          closed += 1;
          socket.end();
          upstream.destroy();
          return;
        }
        if (!ignores(frame)) upstream.write(frame);
      }
    });
  };
  const connection = await KafkaConnection.connect({
    clientId: "freelens-kafka-read-cluster",
    bootstrap: BROKER,
    brokers: [
      {
        advertisedHost: brokerHost,
        advertisedPort: brokerPort,
        namespace: "default",
        pod: "kafka-direct",
        containerPort: brokerPort,
      },
    ],
    forwarder,
  });
  return { connection, closed: () => closed };
}

/** The failure of a size read; a read that succeeds fails the run. */
async function sizesFailure(connection: KafkaConnection, topics: string[], deadline?: number): Promise<Error> {
  const outcome = await connection.topicSizes(topics, undefined, deadline).then(
    () => undefined,
    (error: unknown) => error as Error,
  );
  expect(outcome, "the size read was expected to fail");
  return outcome;
}

const CLOSED = String.raw`KafkaJSConnectionClosedError: Closed connection \(broker 127\.0\.0\.1:19092\)`;
const TIMING = String.raw`\(after \d+ ms\)`;

/** Sizes, messages and watermarks arrive although the broker closes on a Metadata request naming topics. */
async function readsWithoutNamingTopics(): Promise<string[]> {
  const { connection, closed } = await connectThroughClosingBroker(namesTopics);
  try {
    // The "all topics" form goes through: this is what the topic list sends.
    const overview = await connection.overview();
    expect(overview.topics.includes(TOPIC), `the fixture has no ${TOPIC} topic`);
    expect(closed() === 0, "the pipe closed a connection on the all-topics metadata");

    // Control: the pipe does close on a Metadata request naming a topic. KafkaJS sends one as soon
    // as a cluster is given a target topic, which the readers never do.
    const probe = createKafkaJsReadCluster(connection.client);
    await probe.connect();
    const refused = await (probe as unknown as { addMultipleTargetTopics(topics: string[]): Promise<void> })
      .addMultipleTargetTopics([TOPIC])
      .then(
        () => undefined,
        (error: unknown) => error as Error,
      );
    await probe.disconnect();
    expect(refused?.name === "KafkaJSConnectionClosedError", "the pipe did not close on a metadata request by name");
    expect(closed() === 1, "the pipe did not count the closed connection");

    for (const round of ["first", "second"]) {
      const sizes = await connection.topicSizes(overview.topics);
      expect(sizes.supported, `${round} read: the fixture does not offer DescribeLogDirs`);
      expect(sizes.unavailableBrokers.length === 0, `${round} read: a broker did not report`);
      expect(sizes.notes === undefined, `${round} read: unexpected notes: ${sizes.notes?.join("; ")}`);
      const size = sizes.topics[TOPIC];
      expect(size?.exact, `${round} read: ${TOPIC} is not exact`);
      expect(size.partitions.length === 3, `${round} read: expected 3 partitions, got ${size.partitions.length}`);
      expect(BigInt(size.leaderBytes) > 0n, `${round} read: ${TOPIC} has no bytes`);
      expect(
        Object.keys(sizes.topics).length === overview.topics.length,
        `${round} read: expected ${overview.topics.length} topics, got ${Object.keys(sizes.topics).length}`,
      );
    }

    const browsed = await connection.browseMessages({ topic: TOPIC, partition: 0, startMode: "earliest", limit: 10 });
    expect(browsed.returnedCount > 0, "the message browser returned no record");

    // The health read takes the watermarks of the consumer group lag on the same read cluster. It
    // reads none without a consumer group: the npm script seeds one before this run.
    const health = await connection.health();
    expect(
      (health.consumerGroupLagCoverage?.totalGroups ?? 0) > 0,
      "the fixture has no consumer group: run pnpm itest:read-cluster, which seeds one",
    );
    expect(/^\d+$/.test(health.consumerGroupLag), `the health read has no exact lag: ${health.consumerGroupLag}`);

    const watermarkCluster = createKafkaJsReadCluster(connection.client);
    await watermarkCluster.connect();
    try {
      const watermarks = await fetchHighWatermarks(watermarkCluster, [{ topic: TOPIC, partition: 0, leaderId: 1 }]);
      expect(
        watermarks.get(TOPIC)?.get(0) === browsed.highWatermark,
        `the high watermark of ${TOPIC}-0 is ${watermarks.get(TOPIC)?.get(0)}, the browser saw ${browsed.highWatermark}`,
      );
    } finally {
      await watermarkCluster.disconnect();
    }

    expect(closed() === 1, "a reader sent a metadata request naming topics");
    return overview.topics;
  } finally {
    await connection.disconnect();
  }
}

/** Every broker closes on DescribeLogDirs: no lower bounds, the reason of each broker instead. */
async function namesTheBrokersThatClose(topics: string[]): Promise<string> {
  const { connection, closed } = await connectThroughClosingBroker(isDescribeLogDirs);
  try {
    const failure = await sizesFailure(connection, topics);
    expect(failure.name === "TopicSizesError", `unexpected error name ${failure.name}`);
    expect(
      new RegExp(
        `^asking the brokers: broker 1: ${CLOSED}, on a connection already open; then on a new connection: ${CLOSED} ${TIMING}$`,
      ).test(failure.message),
      `unexpected failure when the broker closes on DescribeLogDirs: ${failure.message}`,
    );
    expect(closed() === 2, `expected the request and one more on a new connection, got ${closed()} closes`);
    return failure.message;
  } finally {
    await connection.disconnect();
  }
}

/** The broker closes on every Metadata request: both attempts are named, with the connection each one used. */
async function namesBothAttempts(topics: string[]): Promise<string> {
  const { connection, closed } = await connectThroughClosingBroker(isMetadata);
  try {
    const failure = await sizesFailure(connection, topics);
    const attempt = (connection: string) => `reading the cluster metadata: ${CLOSED}, on ${connection} ${TIMING}`;
    expect(
      new RegExp(`^${attempt("a new connection")}; first attempt: ${attempt("a connection already open")}$`).test(
        failure.message,
      ),
      `unexpected failure when the broker closes on Metadata: ${failure.message}`,
    );
    expect(closed() === 2, `expected one close per attempt, got ${closed()}`);
    return failure.message;
  } finally {
    await connection.disconnect();
  }
}

/**
 * Close a connection the pipe swallowed a request of, without waiting: KafkaJS waits for that
 * request until its own timeout before it closes, half a minute after the request went out.
 */
function closeWithoutWaiting(connection: KafkaConnection): void {
  void connection.disconnect().catch(() => undefined);
}

/** The broker never answers DescribeLogDirs: it is left out with the reason before the 30 s of the page are over. */
async function leavesOutTheBrokerThatNeverAnswers(topics: string[]): Promise<string> {
  const { connection } = await connectThroughClosingBroker(() => false, isDescribeLogDirs);
  try {
    const failure = await sizesFailure(connection, topics);
    expect(
      /^asking the brokers: broker 1: TimeoutError: no answer after 15000 ms \(after 15\d{3} ms\)$/.test(
        failure.message,
      ),
      `unexpected failure when the broker never answers DescribeLogDirs: ${failure.message}`,
    );
    return failure.message;
  } finally {
    closeWithoutWaiting(connection);
  }
}

/**
 * The broker never answers the Metadata request: the read ends at the deadline its caller gave it,
 * four seconds here, and names the step. Without one KafkaJS would wait for its request timeout,
 * which is as long as the page waits.
 */
async function endsAtItsDeadline(topics: string[]): Promise<string> {
  const { connection } = await connectThroughClosingBroker(() => false, isMetadata);
  try {
    const startedAt = Date.now();
    const failure = await sizesFailure(connection, topics, startedAt + 4_000);
    const elapsed = Date.now() - startedAt;
    expect(
      new RegExp(`^reading the cluster metadata: TimeoutError: no answer after \\d{4} ms ${TIMING}$`).test(
        failure.message,
      ),
      `unexpected failure when the broker never answers Metadata: ${failure.message}`,
    );
    expect(elapsed >= 3_900 && elapsed < 6_000, `the read ended after ${elapsed} ms, not at its deadline`);
    return failure.message;
  } finally {
    closeWithoutWaiting(connection);
  }
}

/** The flow of the delete feature: sizes, browse a topic, delete it, sizes again, browse another topic. */
async function survivesADeletedTopic(): Promise<void> {
  requireDisposableBroker();
  const connection = await connectDirect({ bootstrap: BROKER });
  const withAdmin = async <T>(run: (admin: ReturnType<KafkaConnection["admin"]>) => Promise<T>): Promise<T> => {
    const admin = connection.admin();
    await admin.connect();
    try {
      return await run(admin);
    } finally {
      await admin.disconnect();
    }
  };
  const topics = async (): Promise<string[]> => (await connection.overview()).topics;
  try {
    await withAdmin((admin) =>
      admin.createTopics({ topics: [{ topic: SCRATCH_TOPIC, numPartitions: 1 }], waitForLeaders: true }),
    );
    expect((await connection.topicSizes(await topics())).topics[SCRATCH_TOPIC], "the scratch topic has no size");
    await connection.browseMessages({ topic: SCRATCH_TOPIC, partition: 0, startMode: "latest", limit: 10 });
    await connection.deleteTopic(SCRATCH_TOPIC);
    const remaining = await topics();
    expect(!remaining.includes(SCRATCH_TOPIC), "the scratch topic was not deleted");
    const sizes = await connection.topicSizes([...remaining, SCRATCH_TOPIC]);
    expect(sizes.topics[SCRATCH_TOPIC] === undefined, "the deleted topic still has a size");
    expect(sizes.topics[TOPIC]?.exact, `${TOPIC} lost its size after the deletion`);
    const browsed = await connection.browseMessages({ topic: TOPIC, partition: 0, startMode: "latest", limit: 10 });
    expect(browsed.returnedCount > 0, "the message browser returned no record after the deletion");
  } finally {
    await withAdmin((admin) => admin.deleteTopics({ topics: [SCRATCH_TOPIC] })).catch(() => undefined);
    await connection.disconnect();
  }
}

async function main(): Promise<void> {
  expect(brokerHost === "127.0.0.1" && Number.isInteger(brokerPort), "the fixture must be a loopback broker");
  const topics = await readsWithoutNamingTopics();
  const describeLogDirsClosed = await namesTheBrokersThatClose(topics);
  const metadataClosed = await namesBothAttempts(topics);
  const neverAnswered = await leavesOutTheBrokerThatNeverAnswers(topics);
  const metadataNeverAnswered = await endsAtItsDeadline(topics);
  await survivesADeletedTopic();
  process.stdout.write(
    [
      `READ_CLUSTER_OK topics=${topics.length} namedMetadataRequests=0 deletedTopic=survived`,
      `closing on DescribeLogDirs: ${describeLogDirsClosed}`,
      `closing on Metadata: ${metadataClosed}`,
      `never answering DescribeLogDirs: ${neverAnswered}`,
      `never answering Metadata: ${metadataNeverAnswered}`,
      "",
    ].join("\n"),
  );
}

main()
  .then(() => process.exit(0))
  .catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(1);
  });
