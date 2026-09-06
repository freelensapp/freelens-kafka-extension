import { createKafkaTargetId } from "../common/kafka-target";

import type { DiscoveredKafkaInfo } from "../common/ipc";
import type { KafkaSelectionStorage } from "./kafka-navigation";

export const MANUAL_ENDPOINTS_KEY = "freelens-kafka.manual-endpoints.v1";

export function loadManualKafkaEndpoints(storage: KafkaSelectionStorage): DiscoveredKafkaInfo[] {
  try {
    const stored = storage.getItem(MANUAL_ENDPOINTS_KEY);
    if (!stored) return [];
    const parsed = JSON.parse(stored) as unknown;
    if (!Array.isArray(parsed)) return [];

    const migrated = parsed
      .filter((entry): entry is Partial<DiscoveredKafkaInfo> & { bootstrap: string } =>
        Boolean(entry && typeof entry === "object" && typeof entry.bootstrap === "string"),
      )
      .map((entry) => ({ ...entry, targetId: createKafkaTargetId(entry.bootstrap) }) as DiscoveredKafkaInfo);

    return [...new Map(migrated.map((entry) => [entry.targetId, entry])).values()];
  } catch {
    return [];
  }
}

export function saveManualKafkaEndpoints(storage: KafkaSelectionStorage, endpoints: DiscoveredKafkaInfo[]): void {
  try {
    storage.setItem(MANUAL_ENDPOINTS_KEY, JSON.stringify(endpoints));
  } catch {
    // A locked-down renderer may disable storage; endpoints still work for this session.
  }
}

export function mergeKafkaClusters(
  discovered: DiscoveredKafkaInfo[],
  manual: DiscoveredKafkaInfo[],
): DiscoveredKafkaInfo[] {
  const merged = new Map(discovered.map((cluster) => [cluster.targetId, cluster]));
  for (const cluster of manual) {
    if (!merged.has(cluster.targetId)) merged.set(cluster.targetId, cluster);
  }
  return [...merged.values()];
}
