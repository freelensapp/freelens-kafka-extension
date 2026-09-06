import { describe, expect, it } from "vitest";
import { KafkaConnectionSettingsStore } from "./kafka-connection-settings";

import type { KafkaSecurityOverride } from "../common/ipc";

const override: KafkaSecurityOverride = {
  tlsMode: "enabled",
  authMode: "scram-sha-256",
  username: "alice",
  password: "session-only",
};

describe("KafkaConnectionSettingsStore", () => {
  it("isolates volatile overrides by Kubernetes cluster and Kafka target", () => {
    const store = new KafkaConnectionSettingsStore();
    store.set("kube-a", "target-a", override);

    expect(store.get("kube-a", "target-a")).toEqual(override);
    expect(store.get("kube-a", "target-b")).toBeUndefined();
    expect(store.get("kube-b", "target-a")).toBeUndefined();
  });

  it("returns copies so callers cannot mutate stored credentials", () => {
    const store = new KafkaConnectionSettingsStore();
    store.set("kube-a", "target-a", override);

    const read = store.get("kube-a", "target-a");
    if (read) read.password = "changed";

    expect(store.get("kube-a", "target-a")?.password).toBe("session-only");
  });

  it("removes one override or clears the session without persistence", () => {
    const store = new KafkaConnectionSettingsStore();
    store.set("kube-a", "target-a", override);
    store.set("kube-a", "target-b", { tlsMode: "disabled", authMode: "none" });

    store.removeTarget("kube-a", "target-a");
    expect(store.get("kube-a", "target-a")).toBeUndefined();
    expect(store.get("kube-a", "target-b")).toBeDefined();

    store.clear();
    expect(store.get("kube-a", "target-b")).toBeUndefined();
  });
});
