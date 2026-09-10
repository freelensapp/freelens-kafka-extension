import { describe, expect, it } from "vitest";
import { KAFKA_WRITE_MODE_KEY, KafkaWriteSettingsStore } from "./kafka-write-settings";

describe("KafkaWriteSettingsStore", () => {
  it("is disabled by default for every target", () => {
    const store = new KafkaWriteSettingsStore();

    expect(store.get("cluster-a")).toBe(false);
    expect(store.get("cluster-b")).toBe(false);
  });

  it("stores write mode per target id without leaking to other targets", () => {
    const store = new KafkaWriteSettingsStore();

    store.set("cluster-a", true);
    expect(store.get("cluster-a")).toBe(true);
    expect(store.get("cluster-b")).toBe(false);

    store.set("cluster-b", true);
    expect(store.get("cluster-a")).toBe(true);
    expect(store.get("cluster-b")).toBe(true);
  });

  it("supports disabling a target or clearing all write mode state", () => {
    const store = new KafkaWriteSettingsStore();
    store.set("cluster-a", true);
    store.set("cluster-b", true);

    store.set("cluster-a", false);
    expect(store.get("cluster-a")).toBe(false);
    expect(store.get("cluster-b")).toBe(true);

    store.clear();
    expect(store.get("cluster-a")).toBe(false);
    expect(store.get("cluster-b")).toBe(false);
  });

  it("persists target ids and restores them in a new store", () => {
    const values = new Map<string, string>();
    const storage = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => values.set(key, value),
    };
    const first = new KafkaWriteSettingsStore(storage);
    first.set("kafka-target-a", true);

    expect(values.get(KAFKA_WRITE_MODE_KEY)).toBe('["kafka-target-a"]');
    expect(new KafkaWriteSettingsStore(storage).get("kafka-target-a")).toBe(true);
  });

  it("synchronizes store instances that share renderer storage", () => {
    const values = new Map<string, string>();
    const storage = {
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => values.set(key, value),
    };
    const first = new KafkaWriteSettingsStore(storage);
    const second = new KafkaWriteSettingsStore(storage);

    first.set("kafka-target-a", true);

    expect(second.get("kafka-target-a")).toBe(true);
  });

  it("notifies subscribers only when write mode changes", () => {
    const store = new KafkaWriteSettingsStore();
    let notifications = 0;
    const unsubscribe = store.subscribe(() => notifications++);

    store.set("cluster-a", true);
    store.set("cluster-a", true);
    store.set("cluster-a", false);
    unsubscribe();
    store.set("cluster-b", true);

    expect(notifications).toBe(2);
  });
});

describe("KafkaWriteSettingsStore.enabledTargets", () => {
  it("lists the enabled targets sorted and follows changes", () => {
    const storage = new Map<string, string>();
    const store = new KafkaWriteSettingsStore({
      getItem: (key) => storage.get(key) ?? null,
      setItem: (key, value) => void storage.set(key, value),
    });
    expect(store.enabledTargets()).toEqual([]);
    store.set("kafka-b", true);
    store.set("kafka-a", true);
    expect(store.enabledTargets()).toEqual(["kafka-a", "kafka-b"]);
    store.set("kafka-b", false);
    expect(store.enabledTargets()).toEqual(["kafka-a"]);
  });
});
