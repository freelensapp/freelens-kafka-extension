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

/**
 * Auto-refresh settings of the Overview, per target. The storage is read again whenever its raw
 * value changed, so a host store that loads after construction and writes from other frames are
 * both seen (SPEC-018).
 */
export class KafkaOverviewSettingsStore {
  private readonly settings = new Map<string, KafkaOverviewSettings>();
  private readonly storage: StorageLike | undefined;
  private lastRaw: string | null | undefined;

  constructor(storage: StorageLike | undefined = defaultStorage()) {
    this.storage = storage;
    this.sync();
  }

  get(targetId: string): KafkaOverviewSettings {
    this.sync();
    const value = this.settings.get(targetId);
    if (value) return { ...value };
    return { enabled: false, intervalMs: DEFAULT_KAFKA_OVERVIEW_REFRESH_INTERVAL_MS };
  }

  set(targetId: string, value: Partial<KafkaOverviewSettings> | undefined): void {
    this.sync();
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

  /** Re-read the storage, for example once the host store has loaded from disk. */
  reload(): void {
    this.lastRaw = undefined;
    this.sync();
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

  private sync(): void {
    if (!this.storage) return;
    let raw: string | null;
    try {
      raw = this.storage.getItem(KAFKA_OVERVIEW_SETTINGS_KEY);
    } catch {
      return;
    }
    if (raw === this.lastRaw) return;
    this.lastRaw = raw;
    this.settings.clear();
    if (!raw) return;
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return;
      for (const [targetId, candidate] of Object.entries(parsed)) {
        if (!candidate || typeof candidate !== "object") continue;
        const next = this.normalize(candidate as Partial<KafkaOverviewSettings>);
        if (next.enabled) this.settings.set(targetId, next);
      }
    } catch {
      this.settings.clear();
    }
  }

  private persist(): void {
    if (!this.storage) return;
    try {
      const value = JSON.stringify(Object.fromEntries(this.settings.entries()));
      this.lastRaw = value;
      this.storage.setItem(KAFKA_OVERVIEW_SETTINGS_KEY, value);
    } catch {
      // Settings remain in-memory for the current session when storage is unavailable.
    }
  }
}
