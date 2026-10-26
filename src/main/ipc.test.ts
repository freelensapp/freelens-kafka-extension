// The progress of KafkaIpcMain over a fake host IPC: a discovery that reports a topic-naming event, started by one
// frame; every frame is told the operation id only, and only the starting frame gets the event (REQ-227, SC-137).

import { describe, expect, it, vi } from "vitest";

const host = vi.hoisted(() => ({
  handlers: new Map<string, (event: unknown, ...args: unknown[]) => Promise<unknown>>(),
  broadcasts: [] as unknown[][],
}));

vi.mock("@freelensapp/extensions", async (original) => ({
  ...(await original<object>()),
  Main: {
    Ipc: class {
      handle(channel: string, handler: (event: unknown, ...args: unknown[]) => Promise<unknown>) {
        host.handlers.set(channel, handler);
      }
      broadcast(channel: string, ...args: unknown[]) {
        host.broadcasts.push([channel, ...args]);
      }
    },
  },
}));

vi.mock("./catalog-kube-reader", () => ({ createCatalogKubeReader: () => ({}) }));

vi.mock("./kafka/discovery", async (original) => ({
  ...(await original<typeof import("./kafka/discovery")>()),
  discoverAllKafkas: vi.fn(async (_reader: unknown, _namespace: unknown, report: (update: object) => void) => {
    report({ value: 50, phase: "workloads", label: "Reading ConfigMap secret-orders-config" });
    return [];
  }),
}));

import { KAFKA_IPC, type KafkaProgressEvent } from "../common/ipc";
import { KafkaIpcMain } from "./ipc";

import type { TunnelHost } from "./kafka/kubectl-tunnels";

const FRAME_A = { processId: 7, frameId: 1 };
const FRAME_B = { processId: 7, frameId: 2 };
const tunnelHost = {} as TunnelHost;

function handler(channel: string) {
  const found = host.handlers.get(channel);
  if (!found) throw new Error(`no handler for ${channel}`);
  return found;
}

describe("KafkaIpcMain progress", () => {
  it("broadcasts the operation id only and hands the event to the frame that started it", async () => {
    const ipc = new KafkaIpcMain({} as ConstructorParameters<typeof KafkaIpcMain>[0], tunnelHost);

    await handler(KAFKA_IPC.discover)(FRAME_A, { clusterId: "cluster-a", operationId: "op-a" });

    expect(host.broadcasts).toEqual([[KAFKA_IPC.progress, { operationId: "op-a" }]]);
    expect(JSON.stringify(host.broadcasts)).not.toContain("secret-orders");

    const pull = handler(KAFKA_IPC.progressEvent);
    const mine = (await pull(FRAME_A, { operationId: "op-a" })) as KafkaProgressEvent;
    expect(mine).toMatchObject({
      operationId: "op-a",
      operation: "discovery",
      label: expect.stringContaining("secret"),
    });
    expect(await pull(FRAME_B, { operationId: "op-a" })).toBeNull();
    expect(await pull({}, { operationId: "op-a" })).toBeNull();
    expect(await pull(FRAME_A, undefined)).toBeNull();

    await ipc.shutdown();
    expect(await pull(FRAME_A, { operationId: "op-a" })).toBeNull();
  });

  it("refuses a request that names no cluster", async () => {
    new KafkaIpcMain({} as ConstructorParameters<typeof KafkaIpcMain>[0], tunnelHost);

    await expect(handler(KAFKA_IPC.discover)(FRAME_A, { operationId: "op-b" })).rejects.toThrow(
      "no Kubernetes cluster was named",
    );
  });
});
