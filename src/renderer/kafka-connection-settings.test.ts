import { describe, expect, it } from "vitest";
import { KAFKA_SECURITY_OVERRIDES_KEY, KafkaConnectionSettingsStore } from "./kafka-connection-settings";

import type { KafkaSecurityOverride } from "../common/ipc";

const override: KafkaSecurityOverride = {
  tlsMode: "enabled",
  authMode: "scram-sha-256",
  username: "alice",
  password: "session-only",
};

function storage() {
  const values = new Map<string, string>();
  return {
    values,
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
  };
}

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

  it("persists everything but the password and restores it in a new store", () => {
    const current = storage();
    const first = new KafkaConnectionSettingsStore(current);
    first.set("kube-a", "target-a", override);
    first.set("kube-a", "target-iam", {
      tlsMode: "auto",
      authMode: "aws-msk-iam",
      awsRegion: "eu-west-1",
      awsProfile: "prod",
    });

    const raw = current.values.get(KAFKA_SECURITY_OVERRIDES_KEY) ?? "";
    expect(raw).toContain("alice");
    expect(raw).not.toContain("session-only");
    expect(raw).not.toContain("password");

    const second = new KafkaConnectionSettingsStore(current);
    expect(second.get("kube-a", "target-a")).toEqual({
      tlsMode: "enabled",
      authMode: "scram-sha-256",
      username: "alice",
    });
    expect(second.get("kube-a", "target-iam")).toEqual({
      tlsMode: "auto",
      authMode: "aws-msk-iam",
      awsRegion: "eu-west-1",
      awsProfile: "prod",
    });
  });

  it("keeps the password for the session that typed it", () => {
    const current = storage();
    const store = new KafkaConnectionSettingsStore(current);
    store.set("kube-a", "target-a", override);
    expect(store.get("kube-a", "target-a")?.password).toBe("session-only");

    store.set("kube-a", "target-a", { ...override, password: "" });
    expect(store.get("kube-a", "target-a")?.password).toBeUndefined();
    expect(store.get("kube-a", "target-a")?.username).toBe("alice");
  });

  it("removes the persisted entry with the target and on clear", () => {
    const current = storage();
    const store = new KafkaConnectionSettingsStore(current);
    store.set("kube-a", "target-a", override);
    store.set("kube-a", "target-b", { tlsMode: "disabled", authMode: "none" });

    store.removeTarget("kube-a", "target-a");
    expect(current.values.get(KAFKA_SECURITY_OVERRIDES_KEY)).not.toContain("target-a");
    expect(new KafkaConnectionSettingsStore(current).get("kube-a", "target-b")).toBeDefined();

    store.clear();
    expect(current.values.get(KAFKA_SECURITY_OVERRIDES_KEY)).toBe("{}");
  });

  it("picks up what another instance wrote to the shared storage and what the host loaded late", () => {
    const current = storage();
    const reader = new KafkaConnectionSettingsStore(current);
    expect(reader.get("kube-a", "target-a")).toBeUndefined();

    const writer = new KafkaConnectionSettingsStore(current);
    writer.set("kube-a", "target-a", override);
    expect(reader.get("kube-a", "target-a")?.username).toBe("alice");

    current.values.set(
      KAFKA_SECURITY_OVERRIDES_KEY,
      JSON.stringify({ "kube-a:target-b": { tlsMode: "enabled", authMode: "none" } }),
    );
    reader.reload();
    expect(reader.get("kube-a", "target-b")).toEqual({ tlsMode: "enabled", authMode: "none" });
    expect(reader.get("kube-a", "target-a")).toBeUndefined();
  });

  it("drops malformed persisted entries instead of trusting them", () => {
    const current = storage();
    current.values.set(
      KAFKA_SECURITY_OVERRIDES_KEY,
      JSON.stringify({
        "kube-a:bad-tls": { tlsMode: "maybe", authMode: "none" },
        "kube-a:bad-auth": { tlsMode: "auto", authMode: "kerberos" },
        "kube-a:good": { tlsMode: "enabled", authMode: "plain", username: "bob", password: "leaked?" },
        "kube-a:not-an-object": "plain",
      }),
    );

    const store = new KafkaConnectionSettingsStore(current);
    expect(store.get("kube-a", "bad-tls")).toBeUndefined();
    expect(store.get("kube-a", "bad-auth")).toBeUndefined();
    expect(store.get("kube-a", "not-an-object")).toBeUndefined();
    expect(store.get("kube-a", "good")).toEqual({ tlsMode: "enabled", authMode: "plain", username: "bob" });
  });

  it("stays usable in memory when the storage throws", () => {
    const broken = {
      getItem: () => {
        throw new Error("storage unavailable");
      },
      setItem: () => {
        throw new Error("storage unavailable");
      },
    };
    const store = new KafkaConnectionSettingsStore(broken);
    store.set("kube-a", "target-a", override);
    expect(store.get("kube-a", "target-a")).toEqual(override);
  });
});
