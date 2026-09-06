import { describe, expect, it } from "vitest";
import { KAFKA_CLUSTER_CATALOG_KEY, KafkaClusterCatalogStore } from "./kafka-cluster-catalog";

import type { DiscoveredKafkaInfo } from "../common/ipc";

function target(targetId: string): DiscoveredKafkaInfo {
  return {
    targetId,
    source: "manual",
    name: targetId,
    namespace: "",
    bootstrap: `${targetId}:9092`,
    tls: false,
    port: 9092,
    listeners: [],
    brokerPods: [],
  };
}

function storage() {
  const values = new Map<string, string>();
  return {
    values,
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => values.set(key, value),
  };
}

describe("KafkaClusterCatalogStore", () => {
  it("defaults to an empty catalog with automatic scanning disabled", () => {
    const store = new KafkaClusterCatalogStore(storage());

    expect(store.get("kube-a")).toEqual({ discovered: [], missing: [], autoScan: false });
  });

  it("isolates catalogs and persists discovered targets per Kubernetes context", () => {
    const current = storage();
    const store = new KafkaClusterCatalogStore(current);
    store.setDiscovered("kube-a", [target("kafka-a"), target("kafka-a")], 123);
    store.setAutoScan("kube-a", true);

    const restored = new KafkaClusterCatalogStore(current);
    expect(restored.get("kube-a")).toEqual({
      discovered: [target("kafka-a")],
      missing: [],
      lastScanAt: 123,
      autoScan: true,
    });
    expect(restored.get("kube-b")).toEqual({ discovered: [], missing: [], autoScan: false });
    expect(current.values.has(KAFKA_CLUSTER_CATALOG_KEY)).toBe(true);
  });

  it("does not persist secrets because it stores only discovered DTOs", () => {
    const current = storage();
    const store = new KafkaClusterCatalogStore(current);
    store.setDiscovered("kube-a", [{ ...target("kafka-a"), securityHint: { tls: true, auth: "none" } }]);

    const serialized = current.values.get(KAFKA_CLUSTER_CATALOG_KEY) ?? "";
    expect(serialized).not.toContain("password");
    expect(serialized).not.toContain("secret");
  });

  it("keeps previously discovered targets as missing until a later scan finds them", () => {
    const store = new KafkaClusterCatalogStore(storage());
    store.setDiscovered("kube-a", [target("kafka-a"), target("kafka-b")], 123);
    store.setDiscovered("kube-a", [target("kafka-a")], 456);

    expect(store.get("kube-a").discovered).toEqual([target("kafka-a")]);
    expect(store.get("kube-a").missing).toEqual([target("kafka-b")]);
    expect(store.targets("kube-a")).toEqual([target("kafka-a"), target("kafka-b")]);

    store.setDiscovered("kube-a", [target("kafka-b")], 789);
    expect(store.get("kube-a").discovered).toEqual([target("kafka-b")]);
    expect(store.get("kube-a").missing).toEqual([target("kafka-a")]);
  });
});
