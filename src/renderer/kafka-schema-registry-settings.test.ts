import { describe, expect, it } from "vitest";
import { KAFKA_SCHEMA_REGISTRY_SETTINGS_KEY, KafkaSchemaRegistrySettingsStore } from "./kafka-schema-registry-settings";

function storage() {
  const values = new Map<string, string>();
  return {
    values,
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
  };
}

describe("KafkaSchemaRegistrySettingsStore", () => {
  it("isolates settings by target and persists only non-secret fields", () => {
    const current = storage();
    const store = new KafkaSchemaRegistrySettingsStore(current);
    store.set("kafka-a", { registryUrl: "http://127.0.0.1:18081", tls: false, username: "registry" });

    expect(store.get("kafka-a")).toEqual({ registryUrl: "http://127.0.0.1:18081", tls: false, username: "registry" });
    expect(store.get("kafka-b")).toBeUndefined();
    expect(current.values.get(KAFKA_SCHEMA_REGISTRY_SETTINGS_KEY)).not.toContain("password");
  });

  it("restores settings and removes an endpoint when the URL is blank", () => {
    const current = storage();
    const first = new KafkaSchemaRegistrySettingsStore(current);
    first.set("kafka-a", { registryUrl: "http://127.0.0.1:18081", tls: true });

    const second = new KafkaSchemaRegistrySettingsStore(current);
    expect(second.get("kafka-a")?.tls).toBe(true);
    second.set("kafka-a", undefined);
    expect(second.get("kafka-a")).toBeUndefined();
  });
});
