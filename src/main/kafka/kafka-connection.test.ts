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
