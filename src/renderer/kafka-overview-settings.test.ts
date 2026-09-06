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
