import { describe, expect, it } from "vitest";
import { KafkaOverviewSettingsStore } from "./kafka-overview-settings";

describe("KafkaOverviewSettingsStore", () => {
  it("defaults to disabled auto-refresh and the 30s interval", () => {
    const store = new KafkaOverviewSettingsStore();

    expect(store.get("target-a")).toEqual({
      enabled: false,
      intervalMs: 30_000,
    });
  });

  it("keeps cluster-scoped settings isolated per target id", () => {
    const store = new KafkaOverviewSettingsStore();
    store.set("target-a", { enabled: true, intervalMs: 10_000 });

    expect(store.get("target-a")).toEqual({ enabled: true, intervalMs: 10_000 });
    expect(store.get("target-b")).toEqual({ enabled: false, intervalMs: 30_000 });
  });

  it("clamps refresh intervals to the supported minimum", () => {
    const store = new KafkaOverviewSettingsStore();
    store.set("target-a", { enabled: true, intervalMs: 2_000 });

    expect(store.get("target-a")).toEqual({ enabled: true, intervalMs: 10_000 });
  });
});

describe("KafkaOverviewSettingsStore durable storage", () => {
  function storage() {
    const values = new Map<string, string>();
    return {
      values,
      getItem: (key: string) => values.get(key) ?? null,
      setItem: (key: string, value: string) => values.set(key, value),
    };
  }

  it("restores settings from a shared storage in a new store", () => {
    const current = storage();
    new KafkaOverviewSettingsStore(current).set("target-a", { enabled: true, intervalMs: 15_000 });

    expect(new KafkaOverviewSettingsStore(current).get("target-a")).toEqual({ enabled: true, intervalMs: 15_000 });
  });

  it("sees values the storage received after construction, as when the host store loads late", () => {
    const current = storage();
    const store = new KafkaOverviewSettingsStore(current);
    expect(store.get("target-a").enabled).toBe(false);

    current.values.set(
      "freelens-kafka.overview.v1",
      JSON.stringify({ "target-a": { enabled: true, intervalMs: 20_000 } }),
    );
    expect(store.get("target-a")).toEqual({ enabled: true, intervalMs: 20_000 });

    store.reload();
    expect(store.get("target-a")).toEqual({ enabled: true, intervalMs: 20_000 });
  });
});
