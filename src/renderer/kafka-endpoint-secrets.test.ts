import { describe, expect, it } from "vitest";
import { KafkaEndpointSecretsStore } from "./kafka-endpoint-secrets";

describe("KafkaEndpointSecretsStore", () => {
  it("keeps passwords per target in memory only and returns copies", () => {
    const store = new KafkaEndpointSecretsStore();
    store.set("t1", { registryPassword: "reg-secret" });
    store.set("t1", { connectPassword: "con-secret" });
    expect(store.get("t1")).toEqual({ registryPassword: "reg-secret", connectPassword: "con-secret" });
    expect(store.get("t2")).toBeUndefined();
    const copy = store.get("t1");
    if (copy) copy.registryPassword = "changed";
    expect(store.get("t1")?.registryPassword).toBe("reg-secret");
  });

  it("removes a password with an empty value and drops the target when nothing is left", () => {
    const store = new KafkaEndpointSecretsStore();
    store.set("t1", { registryPassword: "reg-secret", connectPassword: "con-secret" });
    store.set("t1", { registryPassword: "" });
    expect(store.get("t1")).toEqual({ connectPassword: "con-secret" });
    store.set("t1", { connectPassword: undefined });
    expect(store.get("t1")).toBeUndefined();
  });

  it("clears every target", () => {
    const store = new KafkaEndpointSecretsStore();
    store.set("t1", { registryPassword: "a" });
    store.set("t2", { connectPassword: "b" });
    store.clear();
    expect(store.get("t1")).toBeUndefined();
    expect(store.get("t2")).toBeUndefined();
  });
});
