/** Shared Kafka runtime constants. */

export const KAFKA_ADMIN_TIMEOUT_MS = 30_000;
export const KAFKA_CLUSTER_HEALTH_TIMEOUT_MS = 300_000;
export const KAFKA_CLUSTER_HEALTH_CACHE_TTL_MS = 5 * 60_000;
export const KAFKA_TOPIC_CONSUMERS_TIMEOUT_MS = 120_000;
export const KAFKA_RESOURCE_CACHE_TTL_MS = 60_000;

export const DEFAULT_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS = 30_000;
export const MIN_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS = 10_000;

export const KAFKA_OVERVIEW_AUTO_REFRESH_OPTIONS = [
  { value: "10000", label: "10 sec" },
  { value: "30000", label: "30 sec" },
  { value: "60000", label: "60 sec" },
  { value: "300000", label: "5 min" },
] as const;
