import type { DiscoveredKafkaInfo } from "../common/ipc";
import type { KafkaSelectionStorage } from "./kafka-navigation";

export const KAFKA_CLUSTER_CATALOG_KEY = "freelens-kafka.cluster-catalog.v1";

export interface KafkaClusterCatalog {
  discovered: DiscoveredKafkaInfo[];
  missing: DiscoveredKafkaInfo[];
  lastScanAt?: number;
  autoScan: boolean;
}

interface StoredCatalog extends KafkaClusterCatalog {}

function emptyCatalog(): KafkaClusterCatalog {
  return { discovered: [], missing: [], autoScan: false };
}

export class KafkaClusterCatalogStore {
  private readonly catalogs = new Map<string, KafkaClusterCatalog>();

  constructor(private readonly storage: KafkaSelectionStorage) {
    this.load();
  }

  get(contextId: string): KafkaClusterCatalog {
    const catalog = this.catalogs.get(contextId) ?? emptyCatalog();
    return { ...catalog, discovered: [...catalog.discovered], missing: [...catalog.missing] };
  }

  targets(contextId: string): DiscoveredKafkaInfo[] {
    const catalog = this.get(contextId);
    return [...catalog.discovered, ...catalog.missing];
  }

  setDiscovered(contextId: string, discovered: DiscoveredKafkaInfo[], scannedAt = Date.now()): void {
    const current = this.get(contextId);
    const previous = [...current.discovered, ...current.missing];
    const nextIds = new Set(discovered.map((entry) => entry.targetId));
    this.catalogs.set(contextId, {
      discovered: [...new Map(discovered.map((entry) => [entry.targetId, entry])).values()],
      missing: previous.filter((entry) => !nextIds.has(entry.targetId)),
      lastScanAt: scannedAt,
      autoScan: current.autoScan,
    });
    this.persist();
  }

  setAutoScan(contextId: string, enabled: boolean): void {
    const current = this.get(contextId);
    this.catalogs.set(contextId, { ...current, autoScan: enabled });
    this.persist();
  }

  clear(contextId?: string): void {
    if (contextId) this.catalogs.delete(contextId);
    else this.catalogs.clear();
    this.persist();
  }

  private load(): void {
    try {
      const raw = this.storage.getItem(KAFKA_CLUSTER_CATALOG_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw) as unknown;
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return;
      for (const [contextId, value] of Object.entries(parsed)) {
        if (!value || typeof value !== "object" || Array.isArray(value)) continue;
        const candidate = value as Partial<StoredCatalog>;
        const discovered = Array.isArray(candidate.discovered)
          ? candidate.discovered.filter((entry): entry is DiscoveredKafkaInfo =>
              Boolean(
                entry &&
                  typeof entry === "object" &&
                  typeof (entry as DiscoveredKafkaInfo).targetId === "string" &&
                  typeof (entry as DiscoveredKafkaInfo).bootstrap === "string",
              ),
            )
          : [];
        this.catalogs.set(contextId, {
          discovered,
          missing: Array.isArray(candidate.missing)
            ? candidate.missing.filter((entry): entry is DiscoveredKafkaInfo =>
                Boolean(
                  entry &&
                    typeof entry === "object" &&
                    typeof (entry as DiscoveredKafkaInfo).targetId === "string" &&
                    typeof (entry as DiscoveredKafkaInfo).bootstrap === "string",
                ),
              )
            : [],
          lastScanAt: typeof candidate.lastScanAt === "number" ? candidate.lastScanAt : undefined,
          autoScan: candidate.autoScan === true,
        });
      }
    } catch {
      this.catalogs.clear();
    }
  }

  private persist(): void {
    try {
      this.storage.setItem(KAFKA_CLUSTER_CATALOG_KEY, JSON.stringify(Object.fromEntries(this.catalogs)));
    } catch {
      // Catalog persistence is best effort; the current renderer keeps working in memory.
    }
  }
}
