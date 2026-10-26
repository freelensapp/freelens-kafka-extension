import { afterEach, describe, expect, it, vi } from "vitest";
import { KafkaProgressStore, PROGRESS_BOUNDS, senderKey } from "./progress-store";

import type { KafkaProgressEvent } from "../common/ipc";

function event(operationId: string, label = "Reading topic orders"): KafkaProgressEvent {
  return {
    operationId,
    operation: "topic",
    value: 40,
    phase: "metadata",
    label,
    elapsedMs: 1,
    stageDurationMs: 1,
    phaseElapsedMs: 1,
    phasePercent: 40,
    etaStatus: "ready",
  } as KafkaProgressEvent;
}

afterEach(() => {
  vi.useRealTimers();
});

describe("senderKey", () => {
  it("names a frame by its process and frame ids, and nothing that names no frame", () => {
    expect(senderKey({ processId: 4, frameId: 9 })).toBe("4:9");
    expect(senderKey({ processId: 4 })).toBeUndefined();
    expect(senderKey({ processId: 4, frameId: 1.5 })).toBeUndefined();
    expect(senderKey(undefined)).toBeUndefined();
  });
});

describe("KafkaProgressStore", () => {
  it("hands the latest event of an operation to the frame that started it, and to no other", () => {
    const store = new KafkaProgressStore();

    expect(store.put("1:1", event("op-a", "first"))).toBe(true);
    expect(store.put("1:1", event("op-a", "second"))).toBe(true);

    expect(store.get("1:1", "op-a")?.label).toBe("second");
    expect(store.get("1:2", "op-a")).toBeUndefined();
    expect(store.get(undefined, "op-a")).toBeUndefined();
    expect(store.get("1:1", 42)).toBeUndefined();
  });

  it("never lets another frame take over an operation", () => {
    const store = new KafkaProgressStore();

    store.put("1:1", event("op-a", "mine"));
    expect(store.put("1:2", event("op-a", "theirs"))).toBe(false);
    expect(store.get("1:1", "op-a")?.label).toBe("mine");
  });

  it("forgets an operation idle for its bound", () => {
    let now = 0;
    const store = new KafkaProgressStore(() => now);

    store.put("1:1", event("op-a"));
    store.put("1:1", event("op-b"));
    now = PROGRESS_BOUNDS.idleMs - 1;
    store.put("1:1", event("op-b"));
    now = PROGRESS_BOUNDS.idleMs;

    expect(store.get("1:1", "op-a")).toBeUndefined();
    expect(store.get("1:1", "op-b")).toBeDefined();
    expect(store.size).toBe(1);
  });

  it("keeps at most its bound of operations, the least recently updated going first", () => {
    const store = new KafkaProgressStore(() => 0);

    for (let index = 0; index <= PROGRESS_BOUNDS.operations; index += 1) store.put("1:1", event(`op-${index}`));

    expect(store.size).toBe(PROGRESS_BOUNDS.operations);
    expect(store.get("1:1", "op-0")).toBeUndefined();
    expect(store.get("1:1", `op-${PROGRESS_BOUNDS.operations}`)).toBeDefined();
  });
});
