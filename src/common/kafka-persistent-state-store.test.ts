import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("@freelensapp/extensions", () => ({
  Common: {
    Store: {
      ExtensionStore: class {
        constructor(_options: unknown) {}
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
});
