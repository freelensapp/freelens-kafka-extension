import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@freelensapp/extensions", () => ({
  Common: {
    Store: {
      ExtensionStore: class {
        constructor(_options: unknown) {}
        // The host loads the file through fromStore.
        loadExtension(this: { fromStore(model: unknown): void }, _extension: unknown) {
          this.fromStore({ values: { "freelens-kafka.aggregate-health.v1:c:t": "from-disk", other: "from-disk" } });
        }
      },
    },
  },
}));

import { KafkaPersistentStateStore } from "./kafka-persistent-state-store";

describe("KafkaPersistentStateStore", () => {
  let store: KafkaPersistentStateStore;

  beforeEach(() => {
    store = new KafkaPersistentStateStore();
  });

  it("round-trips string values through the host-managed model", () => {
    store.setItem("catalog", '{"target":"kafka-a"}');

    const restored = new KafkaPersistentStateStore();
    restored.fromStore(store.toJSON());

    expect(restored.getItem("catalog")).toBe('{"target":"kafka-a"}');
    restored.removeItem("catalog");
    expect(restored.getItem("catalog")).toBeNull();
  });

  it("migrates missing legacy values without overwriting durable state", () => {
    store.setItem("selection", "durable");
    const legacy = new Map([
      ["catalog", "legacy-catalog"],
      ["selection", "legacy-selection"],
    ]);

    expect(store.migrateLegacy({ getItem: (key) => legacy.get(key) ?? null }, ["catalog", "selection"])).toEqual([
      "catalog",
    ]);
    expect(store.getItem("catalog")).toBe("legacy-catalog");
    expect(store.getItem("selection")).toBe("durable");
  });

  it("drops malformed non-string values loaded from disk", () => {
    store.fromStore({ values: { catalog: "valid", invalid: 42 as unknown as string } });

    expect(store.toJSON()).toEqual({ values: { catalog: "valid" } });
  });

  it("keeps its own keys when a model arrives from the other process, and takes every other key", () => {
    const own = (key: string) => key.startsWith("freelens-kafka.aggregate-health.v1:");

    store.keepOwnKeys(own);
    store.loadExtension({} as never);
    expect(store.getItem("freelens-kafka.aggregate-health.v1:c:t")).toBe("from-disk");

    store.setItem("freelens-kafka.aggregate-health.v1:c:t", "measured-by-main");
    // A frame saved something else from a copy that did not have the snapshot yet.
    store.fromStore({ values: { other: "changed-by-a-frame" } });
    expect(store.getItem("freelens-kafka.aggregate-health.v1:c:t")).toBe("measured-by-main");
    expect(store.getItem("other")).toBe("changed-by-a-frame");

    store.fromStore({ values: { "freelens-kafka.aggregate-health.v1:c:t": "stale", other: "again" } });
    expect(store.getItem("freelens-kafka.aggregate-health.v1:c:t")).toBe("measured-by-main");
    expect(store.getItem("other")).toBe("again");
  });

  it("takes every key when it owns none, as in a frame", () => {
    store.setItem("freelens-kafka.aggregate-health.v1:c:t", "old");
    store.fromStore({ values: { other: "x" } });
    expect(store.getItem("freelens-kafka.aggregate-health.v1:c:t")).toBeNull();
  });
});
