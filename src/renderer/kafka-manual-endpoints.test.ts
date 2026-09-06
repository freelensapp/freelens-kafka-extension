import { describe, expect, it } from "vitest";
import { createKafkaTargetId } from "../common/kafka-target";
import {
  loadManualKafkaEndpoints,
  MANUAL_ENDPOINTS_KEY,
  mergeKafkaClusters,
  saveManualKafkaEndpoints,
} from "./kafka-manual-endpoints";

import type { DiscoveredKafkaInfo } from "../common/ipc";

class MemoryStorage {
  readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }
}

const cluster = (targetId: string, bootstrap: string): DiscoveredKafkaInfo => ({
  targetId,
  source: "manual",
  name: bootstrap,
  namespace: "",
  bootstrap,
  tls: false,
  port: 9092,
  listeners: [],
  brokerPods: [],
});

describe("manual Kafka endpoint persistence", () => {
  it("migrates legacy endpoints and deduplicates canonical identities", () => {
    const storage = new MemoryStorage();
    storage.setItem(
      MANUAL_ENDPOINTS_KEY,
      JSON.stringify([
        { ...cluster("legacy-a", "BROKER:9092"), targetId: undefined },
        { ...cluster("legacy-b", "broker"), targetId: undefined },
      ]),
    );

    const endpoints = loadManualKafkaEndpoints(storage);
    expect(endpoints).toHaveLength(1);
    expect(endpoints[0].targetId).toMatch(/^kafka-[a-f0-9]{16}$/);
  });

  it("persists endpoints and tolerates invalid storage", () => {
    const storage = new MemoryStorage();
    const endpoint = cluster(createKafkaTargetId("broker:9092"), "broker:9092");
    saveManualKafkaEndpoints(storage, [endpoint]);
    expect(loadManualKafkaEndpoints(storage)).toEqual([endpoint]);
    storage.setItem(MANUAL_ENDPOINTS_KEY, "not-json");
    expect(loadManualKafkaEndpoints(storage)).toEqual([]);
  });

  it("keeps discovered clusters authoritative over equivalent manual endpoints", () => {
    const discovered = cluster("kafka-1111111111111111", "broker:9092");
    discovered.source = "workload";
    const manual = { ...discovered, source: "manual" };
    expect(mergeKafkaClusters([discovered], [manual])).toEqual([discovered]);
  });
});
