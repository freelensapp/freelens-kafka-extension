import { describe, expect, it, vi } from "vitest";
import { KafkaConnection } from "./kafka-connection";

import type { KafkaReadCluster } from "./message-fetch";

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
