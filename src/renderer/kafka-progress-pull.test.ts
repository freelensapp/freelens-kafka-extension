// The progress of a frame's own operations, on a fake bus of two frames and a fake main that hands an event to the
// frame that started its operation only (SC-137 at unit level).

import { afterEach, describe, expect, it, vi } from "vitest";
import { KafkaProgressPull, OWNED_GRACE_MS, type ProgressTransport } from "./kafka-progress-pull";

import type { KafkaProgressEvent } from "../common/ipc";

function event(operationId: string, label: string): KafkaProgressEvent {
  return { operationId, operation: "topic", value: 10, phase: "metadata", label } as KafkaProgressEvent;
}

// The main process: the latest event per operation and its frame; a broadcast of the id to every frame.
function bus() {
  const latest = new Map<string, { frame: string; event: KafkaProgressEvent }>();
  const listeners = new Map<string, Set<(signal: unknown) => void>>();
  const pulls: Array<{ frame: string; operationId: string }> = [];
  const gates: Array<() => void> = [];
  let gated = false;

  const transport = (frame: string): ProgressTransport => ({
    listen(listener) {
      const set = listeners.get(frame) ?? new Set();
      set.add(listener);
      listeners.set(frame, set);
      return () => set.delete(listener);
    },
    async pull(operationId) {
      pulls.push({ frame, operationId });
      if (gated) await new Promise<void>((resolve) => gates.push(resolve));
      const entry = latest.get(operationId);
      return entry?.frame === frame ? entry.event : null;
    },
  });
  const emit = (frame: string, progress: KafkaProgressEvent) => {
    latest.set(progress.operationId, { frame, event: progress });
    for (const set of listeners.values()) for (const listener of set) listener({ operationId: progress.operationId });
  };
  return {
    transport,
    emit,
    pulls,
    listening: (frame: string) => listeners.get(frame)?.size ?? 0,
    gate: () => {
      gated = true;
    },
    open: () => {
      gated = false;
      for (const resolve of gates.splice(0)) resolve();
    },
  };
}

afterEach(() => {
  vi.useRealTimers();
});

describe("KafkaProgressPull", () => {
  it("gives each frame the events of its own operations, and fetches nothing for the other's", async () => {
    const main = bus();
    const a = new KafkaProgressPull(main.transport("a"));
    const b = new KafkaProgressPull(main.transport("b"));
    const seenA: string[] = [];
    const seenB: string[] = [];

    a.subscribe((progress) => seenA.push(progress.label));
    b.subscribe((progress) => seenB.push(progress.label));
    a.own("op-a");
    b.own("op-b");

    main.emit("a", event("op-a", "topic secret-orders"));
    main.emit("b", event("op-b", "group payments"));
    await vi.waitFor(() => expect(seenA).toHaveLength(1));
    await vi.waitFor(() => expect(seenB).toHaveLength(1));

    expect(seenA).toEqual(["topic secret-orders"]);
    expect(seenB).toEqual(["group payments"]);
    expect(main.pulls).toEqual([
      { frame: "a", operationId: "op-a" },
      { frame: "b", operationId: "op-b" },
    ]);
  });

  it("folds the signals that come during a fetch into one more fetch of the latest event", async () => {
    const main = bus();
    const pull = new KafkaProgressPull(main.transport("a"));
    const seen: string[] = [];

    pull.subscribe((progress) => seen.push(progress.label));
    pull.own("op-a");
    main.gate();
    main.emit("a", event("op-a", "1"));
    main.emit("a", event("op-a", "2"));
    main.emit("a", event("op-a", "3"));
    main.emit("a", event("op-a", "4"));
    await vi.waitFor(() => expect(main.pulls).toHaveLength(1));
    main.open();
    await vi.waitFor(() => expect(seen).toEqual(["4", "4"]));

    expect(main.pulls).toHaveLength(2);
  });

  it("keeps an id for a grace period after its request, then lets it go", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const main = bus();
    const pull = new KafkaProgressPull(main.transport("a"));
    const seen: string[] = [];

    pull.subscribe((progress) => seen.push(progress.label));
    const release = pull.own("op-a");
    release();
    main.emit("a", event("op-a", "late"));
    await vi.advanceTimersByTimeAsync(0);
    expect(seen).toEqual(["late"]);

    await vi.advanceTimersByTimeAsync(OWNED_GRACE_MS);
    main.emit("a", event("op-a", "after"));
    await vi.advanceTimersByTimeAsync(0);
    expect(seen).toEqual(["late"]);
  });

  it("ignores what is not a signal and an event of another operation", async () => {
    const pulls: string[] = [];
    let listener: (signal: unknown) => void = () => undefined;
    const pull = new KafkaProgressPull({
      listen: (next) => {
        listener = next;
        return () => undefined;
      },
      pull: async (operationId) => {
        pulls.push(operationId);
        return event("op-other", "not mine");
      },
    });
    const seen: string[] = [];

    pull.subscribe((progress) => seen.push(progress.label));
    pull.own("op-a");
    for (const signal of [undefined, null, "op-a", { operationId: 3 }, { operationId: "" }]) listener(signal);
    listener({ operationId: "op-a" });
    await vi.waitFor(() => expect(pulls).toEqual(["op-a"]));

    expect(seen).toEqual([]);
  });

  it("listens while someone subscribes, and stops on dispose", () => {
    const main = bus();
    const pull = new KafkaProgressPull(main.transport("a"));

    const first = pull.subscribe(() => undefined);
    const second = pull.subscribe(() => undefined);
    expect(main.listening("a")).toBe(1);
    first();
    expect(main.listening("a")).toBe(1);
    second();
    expect(main.listening("a")).toBe(0);

    pull.subscribe(() => undefined);
    pull.dispose();
    expect(main.listening("a")).toBe(0);
    pull.subscribe(() => undefined);
    expect(main.listening("a")).toBe(0);
  });
});
