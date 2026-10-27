import { describe, expect, it } from "vitest";
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

const labels = (answer: { events: KafkaProgressEvent[] } | undefined) => answer?.events.map((item) => item.label);

describe("senderKey", () => {
  it("names a frame by its process and frame ids, and nothing that names no frame", () => {
    expect(senderKey({ processId: 4, frameId: 9 })).toBe("4:9");
    expect(senderKey({ processId: 4 })).toBeUndefined();
    expect(senderKey({ processId: 4, frameId: 1.5 })).toBeUndefined();
    expect(senderKey(undefined)).toBeUndefined();
  });
});

describe("KafkaProgressStore", () => {
  it("hands every event of an operation, in order, to the frame that started it, and to no other", () => {
    const store = new KafkaProgressStore();

    expect(store.put("1:1", event("op-a", "first"))).toBe(true);
    expect(store.put("1:1", event("op-a", "second"))).toBe(true);

    expect(store.get("1:1", "op-a")).toEqual({ events: [event("op-a", "first"), event("op-a", "second")], cursor: 2 });
    expect(store.get("1:2", "op-a")).toBeUndefined();
    expect(store.get(undefined, "op-a")).toBeUndefined();
    expect(store.get("1:1", 42)).toBeUndefined();
  });

  it("hands only the events after the cursor of the previous answer", () => {
    const store = new KafkaProgressStore();

    store.put("1:1", event("op-a", "1"));
    const first = store.get("1:1", "op-a");
    store.put("1:1", event("op-a", "2"));
    store.put("1:1", event("op-a", "3"));

    expect(labels(store.get("1:1", "op-a", first?.cursor))).toEqual(["2", "3"]);
    expect(store.get("1:1", "op-a", 3)).toEqual({ events: [], cursor: 3 });
    expect(labels(store.get("1:1", "op-a", "not a cursor"))).toEqual(["1", "2", "3"]);
  });

  it("keeps the latest events of a long operation, the oldest going first", () => {
    const store = new KafkaProgressStore();
    const total = PROGRESS_BOUNDS.eventsPerOperation + 5;

    for (let index = 1; index <= total; index += 1) store.put("1:1", event("op-a", String(index)));
    const answer = store.get("1:1", "op-a", 0);

    expect(answer?.cursor).toBe(total);
    expect(answer?.events).toHaveLength(PROGRESS_BOUNDS.eventsPerOperation);
    expect(answer?.events[0].label).toBe("6");
  });

  it("never lets another frame take over an operation", () => {
    const store = new KafkaProgressStore();

    store.put("1:1", event("op-a", "mine"));
    expect(store.put("1:2", event("op-a", "theirs"))).toBe(false);
    expect(labels(store.get("1:1", "op-a"))).toEqual(["mine"]);
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
