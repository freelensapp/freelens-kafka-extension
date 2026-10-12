import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { KafkaConnection } from "./kafka-connection";
import { fetchTopicSizes, TopicSizesStepError } from "./log-dirs";
import { createKafkaJsReadCluster, type KafkaReadCluster } from "./message-fetch";

vi.mock("./log-dirs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./log-dirs")>();
  return { ...actual, fetchTopicSizes: vi.fn(actual.fetchTopicSizes) };
});

vi.mock("./message-fetch", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./message-fetch")>();
  return { ...actual, createKafkaJsReadCluster: vi.fn(actual.createKafkaJsReadCluster) };
});

describe("KafkaConnection read cluster lifecycle", () => {
  it("waits for a pending read cluster and disconnects it exactly once", async () => {
    const connection = await KafkaConnection.connect({ bootstrap: "127.0.0.1:1" });
    let releaseCluster!: (cluster: KafkaReadCluster) => void;
    const pendingCluster = new Promise<KafkaReadCluster>((resolve) => {
      releaseCluster = resolve;
    });
    const disconnect = vi.fn(async () => undefined);
    const cluster = { disconnect } as unknown as KafkaReadCluster;
    const internals = connection as unknown as {
      messageCluster?: KafkaReadCluster;
      messageClusterConnect?: Promise<KafkaReadCluster>;
    };
    internals.messageClusterConnect = pendingCluster;

    let completed = false;
    const closing = connection.disconnect().then(() => {
      completed = true;
    });
    await Promise.resolve();
    expect(completed).toBe(false);
    expect(disconnect).not.toHaveBeenCalled();

    releaseCluster(cluster);
    await closing;

    expect(disconnect).toHaveBeenCalledOnce();
    expect(internals.messageCluster).toBeUndefined();
    expect(internals.messageClusterConnect).toBeUndefined();
  });
});

describe("KafkaConnection.deleteTopic", () => {
  it("deletes exactly the named topic through a connected admin client", async () => {
    const connection = await KafkaConnection.connect({ bootstrap: "127.0.0.1:1" });
    const admin = {
      connect: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
      deleteTopics: vi.fn(async () => undefined),
    };
    (connection as unknown as { kafka: unknown }).kafka = { admin: () => admin };

    await expect(connection.deleteTopic("orders")).resolves.toEqual({ topic: "orders" });

    expect(admin.connect).toHaveBeenCalledOnce();
    expect(admin.deleteTopics).toHaveBeenCalledWith({ topics: ["orders"] });
    expect(admin.disconnect).toHaveBeenCalledOnce();
  });

  it("refuses an empty topic name and still disconnects after a broker error", async () => {
    const connection = await KafkaConnection.connect({ bootstrap: "127.0.0.1:1" });
    const admin = {
      connect: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
      deleteTopics: vi.fn(async () => {
        throw new Error("This server does not host this topic-partition");
      }),
    };
    (connection as unknown as { kafka: unknown }).kafka = { admin: () => admin };

    await expect(connection.deleteTopic("")).rejects.toThrow("a topic name is required");
    expect(admin.connect).not.toHaveBeenCalled();

    await expect(connection.deleteTopic("missing")).rejects.toThrow("does not host");
    expect(admin.disconnect).toHaveBeenCalledOnce();
  });
});

describe("KafkaConnection.deleteTopics", () => {
  it("deletes the topics one by one in a single admin session and reports each outcome", async () => {
    const connection = await KafkaConnection.connect({ bootstrap: "127.0.0.1:1" });
    const admin = {
      connect: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
      deleteTopics: vi.fn(async ({ topics }: { topics: string[] }) => {
        if (topics[0] === "missing") throw new Error("This server does not host this topic-partition");
      }),
    };
    (connection as unknown as { kafka: unknown }).kafka = { admin: () => admin };

    await expect(connection.deleteTopics(["orders", "missing", "orders", "", "payments"])).resolves.toEqual({
      deleted: ["orders", "payments"],
      failed: [{ topic: "missing", error: "This server does not host this topic-partition" }],
    });

    expect(admin.connect).toHaveBeenCalledOnce();
    expect(admin.deleteTopics.mock.calls.map(([call]) => call)).toEqual([
      { topics: ["orders"] },
      { topics: ["missing"] },
      { topics: ["payments"] },
    ]);
    expect(admin.disconnect).toHaveBeenCalledOnce();
  });

  it("refuses an empty selection without connecting", async () => {
    const connection = await KafkaConnection.connect({ bootstrap: "127.0.0.1:1" });
    const admin = { connect: vi.fn(async () => undefined), disconnect: vi.fn(async () => undefined) };
    (connection as unknown as { kafka: unknown }).kafka = { admin: () => admin };

    await expect(connection.deleteTopics([])).rejects.toThrow("at least one topic name is required");
    await expect(connection.deleteTopics([""])).rejects.toThrow("at least one topic name is required");
    expect(admin.connect).not.toHaveBeenCalled();
  });
});

describe("KafkaConnection.topicSizes", () => {
  const closed = () =>
    Object.assign(new Error("Closed connection"), { name: "KafkaJSConnectionClosedError", broker: "b-2:9098" });
  const sizes = { supported: true, unavailableBrokers: [], topics: {} };

  /** A connection whose shared read cluster is already open. */
  async function connectionWithReadCluster() {
    const connection = await KafkaConnection.connect({ bootstrap: "127.0.0.1:1" });
    const cluster = { disconnect: vi.fn(async () => undefined) } as unknown as KafkaReadCluster;
    (connection as unknown as { messageCluster?: KafkaReadCluster }).messageCluster = cluster;
    return { connection, cluster };
  }

  /** A failure of the size reader that takes the given time to show up. */
  function failAfter(ms: number, error: unknown) {
    vi.mocked(fetchTopicSizes).mockImplementationOnce(async () => {
      vi.advanceTimersByTime(ms);
      throw error;
    });
  }

  /** The error a read fails with; a read that succeeds fails the test. */
  const failureOf = (read: Promise<unknown>): Promise<Error> =>
    read.then(
      () => {
        throw new Error("the read was expected to fail");
      },
      (error: unknown) => error as Error,
    );

  let warn: ReturnType<typeof vi.spyOn>;
  const warned = () => warn.mock.calls.map(([line]) => String(line));

  beforeEach(() => {
    vi.useFakeTimers();
    warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    vi.mocked(fetchTopicSizes).mockClear();
    vi.mocked(createKafkaJsReadCluster).mockClear();
  });

  afterEach(() => {
    vi.useRealTimers();
    warn.mockRestore();
    vi.mocked(fetchTopicSizes).mockReset();
    vi.mocked(createKafkaJsReadCluster).mockReset();
  });

  it("reads on the read cluster the other readers share and returns the plain result", async () => {
    const { connection, cluster } = await connectionWithReadCluster();
    const abandoned = new AbortController();
    vi.mocked(fetchTopicSizes).mockResolvedValueOnce(sizes);

    await expect(connection.topicSizes(["orders"], abandoned.signal)).resolves.toBe(sizes);

    expect(createKafkaJsReadCluster).not.toHaveBeenCalled();
    expect(vi.mocked(fetchTopicSizes).mock.calls).toHaveLength(1);
    const [usedCluster, topics, options] = vi.mocked(fetchTopicSizes).mock.calls[0];
    expect(usedCluster).toBe(cluster);
    expect(topics).toEqual(["orders"]);
    expect(options?.signal).toBe(abandoned.signal);
    expect(warned()).toEqual([]);
  });

  it("carries what the size reader reports while it reads in the notes of a read that succeeds", async () => {
    const { connection } = await connectionWithReadCluster();
    vi.mocked(fetchTopicSizes).mockImplementationOnce(async (_cluster, _topics, options) => {
      options?.log?.("broker 2 left out: Error: boom");
      return sizes;
    });

    await expect(connection.topicSizes(["orders"])).resolves.toEqual({
      ...sizes,
      notes: ["broker 2 left out: Error: boom"],
    });
    expect(warned()).toEqual(["[kafka] topic sizes: broker 2 left out: Error: boom"]);
  });

  it("gives both reads the deadline of the caller", async () => {
    const { connection } = await connectionWithReadCluster();
    const deadline = Date.now() + 27_000;
    failAfter(212, new TopicSizesStepError("reading the cluster metadata", closed()));
    vi.mocked(fetchTopicSizes).mockResolvedValueOnce(sizes);

    await connection.topicSizes(["orders"], undefined, deadline);

    expect(vi.mocked(fetchTopicSizes).mock.calls.map(([, , options]) => options?.deadline)).toEqual([
      deadline,
      deadline,
    ]);
  });

  it("does not start a read when no time is left, and blames no step", async () => {
    const connection = await KafkaConnection.connect({ bootstrap: "127.0.0.1:1" });

    const failure = await failureOf(connection.topicSizes(["orders"], undefined, Date.now() - 1));

    expect(failure.name).toBe("TopicSizesError");
    expect(failure.message).toBe("not read: no time was left when the read could start");
    expect(createKafkaJsReadCluster).not.toHaveBeenCalled();
    expect(vi.mocked(fetchTopicSizes)).not.toHaveBeenCalled();
  });

  it("names the step when the read cluster does not open before the deadline", async () => {
    const connection = await KafkaConnection.connect({ bootstrap: "127.0.0.1:1" });
    vi.mocked(createKafkaJsReadCluster).mockReturnValueOnce({
      connect: vi.fn(() => new Promise<void>(() => undefined)),
      disconnect: vi.fn(async () => undefined),
    } as unknown as KafkaReadCluster);

    const failure = failureOf(connection.topicSizes(["orders"], undefined, Date.now() + 27_000));
    await vi.advanceTimersByTimeAsync(27_000);

    expect((await failure).message).toBe(
      "opening the read cluster: TimeoutError: no answer after 27000 ms (after 27000 ms)",
    );
    expect(vi.mocked(fetchTopicSizes)).not.toHaveBeenCalled();
  });

  it("reads once more when the peer dropped the connection during the metadata read, and tells the page", async () => {
    const { connection, cluster } = await connectionWithReadCluster();
    failAfter(212, new TopicSizesStepError("reading the cluster metadata", closed(), ", on a connection already open"));
    vi.mocked(fetchTopicSizes).mockResolvedValueOnce(sizes);
    const detail =
      "reading the cluster metadata: KafkaJSConnectionClosedError: Closed connection (broker b-2:9098), on a connection already open (after 212 ms)";

    await expect(connection.topicSizes(["orders"])).resolves.toEqual({
      ...sizes,
      notes: [`${detail}; reading once more`, "the second read succeeded"],
    });

    // KafkaJS reconnects by itself: the cluster the other readers use is neither closed nor replaced.
    expect(cluster.disconnect).not.toHaveBeenCalled();
    expect(createKafkaJsReadCluster).not.toHaveBeenCalled();
    expect(vi.mocked(fetchTopicSizes).mock.calls.map(([usedCluster]) => usedCluster)).toEqual([cluster, cluster]);
    expect(warned()).toEqual([
      `[kafka] topic sizes: ${detail}; reading once more`,
      "[kafka] topic sizes: the second read succeeded",
    ]);
  });

  it("gives up after the second read and names both attempts", async () => {
    const { connection } = await connectionWithReadCluster();
    failAfter(212, new TopicSizesStepError("reading the cluster metadata", closed(), ", on a connection already open"));
    failAfter(3, new TopicSizesStepError("reading the cluster metadata", closed(), ", on a new connection"));

    const failure = await failureOf(connection.topicSizes(["orders"]));

    expect(failure.name).toBe("TopicSizesError");
    expect(failure.message).toBe(
      "reading the cluster metadata: KafkaJSConnectionClosedError: Closed connection (broker b-2:9098), on a new connection (after 3 ms); first attempt: reading the cluster metadata: KafkaJSConnectionClosedError: Closed connection (broker b-2:9098), on a connection already open (after 212 ms)",
    );
    expect(vi.mocked(fetchTopicSizes)).toHaveBeenCalledTimes(2);
  });

  it("does not read again after a failure that is not a dropped connection, and still says how long it took", async () => {
    const { connection } = await connectionWithReadCluster();
    failAfter(40, new TopicSizesStepError("asking the brokers", "broker 1: Error: boom"));

    const failure = await failureOf(connection.topicSizes(["orders"]));

    expect(failure.name).toBe("TopicSizesError");
    expect(failure.message).toBe("asking the brokers: broker 1: Error: boom (after 40 ms)");
    expect(vi.mocked(fetchTopicSizes)).toHaveBeenCalledOnce();
    expect(warned()).toEqual([]);
  });

  it("names the step when the read cluster cannot be opened", async () => {
    const connection = await KafkaConnection.connect({ bootstrap: "127.0.0.1:1" });
    // What KafkaJS raises once it has retried the connection by itself.
    const exhausted = Object.assign(new Error("Connection timeout"), { name: "KafkaJSNumberOfRetriesExceeded" });
    vi.mocked(createKafkaJsReadCluster).mockReturnValueOnce({
      connect: vi.fn(() => {
        vi.advanceTimersByTime(70_000);
        return Promise.reject(exhausted);
      }),
      disconnect: vi.fn(async () => undefined),
    } as unknown as KafkaReadCluster);

    const failure = await failureOf(connection.topicSizes(["orders"]));

    expect(failure.message).toBe(
      "opening the read cluster: KafkaJSNumberOfRetriesExceeded: Connection timeout (after 70000 ms)",
    );
    expect(vi.mocked(fetchTopicSizes)).not.toHaveBeenCalled();
  });

  it("does not read again once the read was aborted or the connection closed", async () => {
    const dropped = () => new TopicSizesStepError("reading the cluster metadata", closed());

    const aborted = await connectionWithReadCluster();
    const abandoned = new AbortController();
    const whileAborting = dropped();
    vi.mocked(fetchTopicSizes).mockImplementationOnce(async () => {
      abandoned.abort();
      throw whileAborting;
    });
    await expect(aborted.connection.topicSizes(["orders"], abandoned.signal)).rejects.toBe(whileAborting);

    const closing = await connectionWithReadCluster();
    const whileClosing = dropped();
    vi.mocked(fetchTopicSizes).mockImplementationOnce(async () => {
      await closing.connection.disconnect();
      throw whileClosing;
    });
    await expect(closing.connection.topicSizes(["orders"])).rejects.toBe(whileClosing);

    expect(vi.mocked(fetchTopicSizes)).toHaveBeenCalledTimes(2);
    expect(warned()).toEqual([]);
  });

  it("passes on an error that is not a step of the size reader", async () => {
    const { connection } = await connectionWithReadCluster();
    const stopped = new Error("Topic size fetch aborted");
    vi.mocked(fetchTopicSizes).mockRejectedValueOnce(stopped);

    await expect(connection.topicSizes(["orders"])).rejects.toBe(stopped);
    expect(vi.mocked(fetchTopicSizes)).toHaveBeenCalledOnce();
  });
});
