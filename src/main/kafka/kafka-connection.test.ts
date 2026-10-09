import { describe, expect, it, vi } from "vitest";
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

  async function connectionWithStaleCluster() {
    const connection = await KafkaConnection.connect({ bootstrap: "127.0.0.1:1" });
    const stale = { disconnect: vi.fn(async () => undefined) } as unknown as KafkaReadCluster;
    const fresh = {
      connect: vi.fn(async () => undefined),
      disconnect: vi.fn(async () => undefined),
    } as unknown as KafkaReadCluster;
    const internals = connection as unknown as { messageCluster?: KafkaReadCluster; messageClusterOpenedAt?: number };
    internals.messageCluster = stale;
    internals.messageClusterOpenedAt = Date.now() - 5_000;
    vi.mocked(createKafkaJsReadCluster).mockReturnValueOnce(fresh);
    return { connection, stale, fresh, internals };
  }

  it("reopens the read cluster and retries once when a broker closed the connection", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const { connection, stale, fresh, internals } = await connectionWithStaleCluster();
      const sizes = { supported: true, unavailableBrokers: [], topics: {} };
      vi.mocked(fetchTopicSizes)
        .mockRejectedValueOnce(new TopicSizesStepError("refreshing the metadata", closed()))
        .mockResolvedValueOnce(sizes);

      await expect(connection.topicSizes(["orders"])).resolves.toBe(sizes);

      expect(stale.disconnect).toHaveBeenCalledOnce();
      expect(fresh.connect).toHaveBeenCalledOnce();
      expect(vi.mocked(fetchTopicSizes).mock.calls.map(([cluster]) => cluster)).toEqual([stale, fresh]);
      expect(internals.messageCluster).toBe(fresh);
      expect(warn.mock.calls.map(([line]) => String(line))).toEqual([
        "[kafka] topic sizes: refreshing the metadata: KafkaJSConnectionClosedError: Closed connection (broker b-2:9098) (read cluster opened 5 s ago); reopening the read cluster and retrying once",
        "[kafka] topic sizes: the retry on a fresh read cluster succeeded",
      ]);
    } finally {
      warn.mockRestore();
      vi.mocked(fetchTopicSizes).mockClear();
    }
  });

  it("gives up after the retry and names both failures", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    try {
      const { connection, stale, fresh } = await connectionWithStaleCluster();
      const reset = Object.assign(new Error("Connection error: read ECONNRESET"), { name: "KafkaJSConnectionError" });
      vi.mocked(fetchTopicSizes)
        .mockRejectedValueOnce(new TopicSizesStepError("refreshing the metadata", closed()))
        .mockRejectedValueOnce(new TopicSizesStepError("connecting to broker 1", reset));

      await expect(connection.topicSizes(["orders"])).rejects.toThrow(
        "connecting to broker 1: KafkaJSConnectionError: Connection error: read ECONNRESET on a fresh read cluster; first attempt: refreshing the metadata: KafkaJSConnectionClosedError: Closed connection (broker b-2:9098) (read cluster opened 5 s ago)",
      );
      expect(stale.disconnect).toHaveBeenCalledOnce();
      expect(fresh.connect).toHaveBeenCalledOnce();
    } finally {
      warn.mockRestore();
      vi.mocked(fetchTopicSizes).mockClear();
    }
  });

  it("does not retry a failure that is not about the connection", async () => {
    const { connection, stale } = await connectionWithStaleCluster();
    const failure = new TopicSizesStepError(
      "refreshing the metadata",
      new Error("This server does not host this topic"),
    );
    vi.mocked(fetchTopicSizes).mockRejectedValueOnce(failure);

    await expect(connection.topicSizes(["orders"])).rejects.toBe(failure);
    expect(stale.disconnect).not.toHaveBeenCalled();
    expect(vi.mocked(fetchTopicSizes)).toHaveBeenCalledOnce();
    vi.mocked(createKafkaJsReadCluster).mockReset();
    vi.mocked(fetchTopicSizes).mockClear();
  });
});
