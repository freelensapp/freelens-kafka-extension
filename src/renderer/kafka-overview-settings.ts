import {
  DEFAULT_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS,
  MIN_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS,
} from "../common/constants";

export const KAFKA_OVERVIEW_SETTINGS_KEY = "freelens-kafka.overview.v1";

export interface KafkaOverviewSettings {
  enabled: boolean;
  intervalMs: number;
}

interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

function defaultStorage(): StorageLike | undefined {
  try {
    return typeof window === "undefined" ? undefined : window.localStorage;
  } catch {
    return undefined;
  }
}

export class KafkaOverviewSettingsStore {
  private readonly settings = new Map<string, KafkaOverviewSettings>();
  private readonly storage: StorageLike | undefined;

  constructor(storage: StorageLike | undefined = defaultStorage()) {
    this.storage = storage;
    this.load();
  }

  get(targetId: string): KafkaOverviewSettings {
    const value = this.settings.get(targetId);
    if (value) return { ...value };
    return { enabled: false, intervalMs: DEFAULT_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS };
  }

  set(targetId: string, value: Partial<KafkaOverviewSettings> | undefined): void {
    const next = this.normalize(value);
    if (!next.enabled) {
      this.settings.delete(targetId);
    } else {
      this.settings.set(targetId, next);
    }
    this.persist();
  }

  clear(): void {
    this.settings.clear();
    this.persist();
  }

  private normalize(value?: Partial<KafkaOverviewSettings>): KafkaOverviewSettings {
    const enabled = Boolean(value?.enabled);
    const intervalMs = Math.max(
      MIN_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS,
      Number(value?.intervalMs ?? DEFAULT_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS) ||
        DEFAULT_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS,
    );
    return { enabled, intervalMs };
  }

  private load(): void {
    try {
      const parsed = JSON.parse(this.storage?.getItem(KAFKA_OVERVIEW_SETTINGS_KEY) ?? "{}") as unknown;
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return;
      for (const [targetId, raw] of Object.entries(parsed)) {
        if (!raw || typeof raw !== "object") continue;
        const candidate = raw as Partial<KafkaOverviewSettings>;
        const next = this.normalize(candidate);
        if (next.enabled) this.settings.set(targetId, next);
      }
    } catch {
      this.settings.clear();
    }
  }

  private persist(): void {
    try {
      const values = Object.fromEntries(this.settings.entries());
      this.storage?.setItem(KAFKA_OVERVIEW_SETTINGS_KEY, JSON.stringify(values));
    } catch {
      // Settings remain in-memory for the current session when storage is unavailable.
    }
  }
}
