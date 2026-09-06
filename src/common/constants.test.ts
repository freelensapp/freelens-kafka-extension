import { describe, expect, it } from "vitest";
import {
  DEFAULT_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS,
  KAFKA_ADMIN_TIMEOUT_MS,
  KAFKA_CLUSTER_HEALTH_CACHE_TTL_MS,
  KAFKA_RESOURCE_CACHE_TTL_MS,
  KAFKA_TOPIC_CONSUMERS_TIMEOUT_MS,
  MIN_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS,
} from "./constants";

describe("shared kafka constants", () => {
  it("keeps refresh defaults and admin timeouts aligned with the extension runtime", () => {
    expect(DEFAULT_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS).toBe(30_000);
    expect(MIN_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS).toBe(10_000);
    expect(KAFKA_ADMIN_TIMEOUT_MS).toBe(30_000);
    expect(KAFKA_TOPIC_CONSUMERS_TIMEOUT_MS).toBe(120_000);
    expect(KAFKA_RESOURCE_CACHE_TTL_MS).toBe(60_000);
    expect(KAFKA_CLUSTER_HEALTH_CACHE_TTL_MS).toBe(5 * 60_000);
  });
});
