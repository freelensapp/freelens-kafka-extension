// SC-141: main decides a write from the write mode persisted in its own copy of the extension store, before any
// frame has said anything (REQ-233). The handlers run over a fake host IPC; the store is a stand-in.

import { describe, expect, it, vi } from "vitest";

const host = vi.hoisted(() => ({
  handlers: new Map<string, (event: unknown, ...args: unknown[]) => Promise<unknown>>(),
  values: {} as Record<string, string>,
}));

vi.mock("@freelensapp/extensions", async (original) => ({
  ...(await original<object>()),
  Main: {
    Ipc: class {
      handle(channel: string, handler: (event: unknown, ...args: unknown[]) => Promise<unknown>) {
        host.handlers.set(channel, handler);
      }
      broadcast() {}
    },
  },
}));

vi.mock("../common/kafka-persistent-state-store", () => ({
  kafkaPersistentStateStore: () => ({
    getItem: (key: string) => host.values[key] ?? null,
    removeItem: () => undefined,
  }),
}));

import { KAFKA_IPC } from "../common/ipc";
import { KAFKA_WRITE_MODE_KEY } from "../common/write-mode-key";
import { KafkaIpcMain } from "./ipc";

import type { TunnelHost } from "./kafka/kubectl-tunnels";

const FRAME = { processId: 3, frameId: 1 };
const produce = { targetId: "kafka-1", namespace: "kafka", clusterName: "demo", topic: "orders", value: "v" };

describe("KafkaIpcMain write guard", () => {
  it("refuses a write while the persisted write mode of its target is off", async () => {
    new KafkaIpcMain({} as ConstructorParameters<typeof KafkaIpcMain>[0], {} as TunnelHost);
    const handler = host.handlers.get(KAFKA_IPC.produce);

    expect(host.handlers.has("kafka:write-mode")).toBe(false);
    await expect(handler?.(FRAME, produce)).rejects.toThrow("Produce message refused: write mode is not enabled");

    host.values[KAFKA_WRITE_MODE_KEY] = JSON.stringify(["kafka-2"]);
    await expect(handler?.(FRAME, produce)).rejects.toThrow("Produce message refused");
  });

  it("lets a write past the guard once the store has its target on", async () => {
    new KafkaIpcMain({} as ConstructorParameters<typeof KafkaIpcMain>[0], {} as TunnelHost);
    host.values[KAFKA_WRITE_MODE_KEY] = JSON.stringify(["kafka-1"]);

    // Past the guard, the request names no cluster: the next check refuses it.
    await expect(host.handlers.get(KAFKA_IPC.produce)?.(FRAME, produce)).rejects.toThrow(
      "no Kubernetes cluster was named",
    );
  });
});
