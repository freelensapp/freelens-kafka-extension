import { describe, expect, it, vi } from "vitest";
import { createIpcRequestDeduper } from "./kafka-ipc-deduper";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

describe("Kafka IPC request deduper", () => {
  it("coalesces concurrent calls with equivalent object parameters", async () => {
    const dedupe = createIpcRequestDeduper();
    const pending = deferred<string>();
    const invoke = vi.fn(() => pending.promise);

    const first = dedupe("kafka:topic:config", { topic: "orders", clusterId: "kube-a", operationId: "one" }, invoke);
    const second = dedupe("kafka:topic:config", { clusterId: "kube-a", topic: "orders", operationId: "two" }, invoke);

    await Promise.resolve();
    expect(invoke).toHaveBeenCalledTimes(1);
    expect(first).toBe(second);
    pending.resolve("ready");
    await expect(first).resolves.toBe("ready");
  });

  it("allows a later call after the shared request settles", async () => {
    const dedupe = createIpcRequestDeduper();
    const invoke = vi.fn(async () => "ready");

    await dedupe("kafka:groups", { clusterId: "kube-a" }, invoke);
    await dedupe("kafka:groups", { clusterId: "kube-a" }, invoke);

    expect(invoke).toHaveBeenCalledTimes(2);
  });
});
