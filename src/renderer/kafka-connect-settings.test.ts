import { describe, expect, it } from "vitest";
import { KAFKA_CONNECT_SETTINGS_KEY, KafkaConnectSettingsStore } from "./kafka-connect-settings";

function storage() {
  const values = new Map<string, string>();
  return {
    values,
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
  };
}

describe("KafkaConnectSettingsStore", () => {
  it("isolates settings by target and persists only non-secret fields", () => {
    const current = storage();
    const store = new KafkaConnectSettingsStore(current);
    store.set("kafka-a", { connectUrl: "http://127.0.0.1:18083", tls: false, username: "connect" });

    expect(store.get("kafka-a")).toEqual({ connectUrl: "http://127.0.0.1:18083", tls: false, username: "connect" });
    expect(store.get("kafka-b")).toBeUndefined();
    expect(current.values.get(KAFKA_CONNECT_SETTINGS_KEY)).not.toContain("password");
    expect(store.hasAny()).toBe(true);
  });

  it("restores settings in a new store and removes an endpoint when the URL is blank", () => {
    const current = storage();
    const first = new KafkaConnectSettingsStore(current);
    first.set("kafka-a", { connectUrl: " http://127.0.0.1:18083 ", tls: true });

    const second = new KafkaConnectSettingsStore(current);
    expect(second.get("kafka-a")).toEqual({ connectUrl: "http://127.0.0.1:18083", tls: true });

    second.set("kafka-a", { connectUrl: "  ", tls: false });
    expect(second.get("kafka-a")).toBeUndefined();
    expect(second.hasAny()).toBe(false);
  });

  it("sees values the storage received after construction and reports them through hasAny", () => {
    const current = storage();
    const store = new KafkaConnectSettingsStore(current);
    expect(store.hasAny()).toBe(false);

    current.values.set(
      KAFKA_CONNECT_SETTINGS_KEY,
      JSON.stringify({ "kafka-a": { connectUrl: "http://127.0.0.1:18083", tls: false } }),
    );
    expect(store.get("kafka-a")?.connectUrl).toBe("http://127.0.0.1:18083");

    store.reload();
    expect(store.hasAny()).toBe(true);
  });

  it("notifies subscribers and survives a storage that throws", () => {
    const broken = {
      getItem: () => {
        throw new Error("storage unavailable");
      },
      setItem: () => {
        throw new Error("storage unavailable");
      },
    };
    const store = new KafkaConnectSettingsStore(broken);
    let notified = 0;
    store.subscribe(() => {
      notified += 1;
    });
    store.set("kafka-a", { connectUrl: "http://127.0.0.1:18083", tls: false });

    expect(notified).toBe(1);
    expect(store.get("kafka-a")?.connectUrl).toBe("http://127.0.0.1:18083");
  });
});
