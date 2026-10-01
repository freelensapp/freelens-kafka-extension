import { observable, runInAction } from "mobx";

export const KAFKA_CONNECT_SETTINGS_KEY = "freelens-kafka.connect.v1";

export interface KafkaConnectSettings {
  connectUrl: string;
  tls: boolean;
  username?: string;
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
 * Kafka Connect endpoint settings per target: URL, TLS flag and username. Passwords never enter
 * this store (SPEC-011). The storage is read again whenever its raw value changed, so a host
 * store that loads after construction and writes from other frames are both seen (SPEC-018).
 */
export class KafkaConnectSettingsStore {
  private readonly settings = new Map<string, KafkaConnectSettings>();
  private readonly listeners = new Set<() => void>();
  private readonly configuredState = observable.box(false);
  private lastRaw: string | null | undefined;

  constructor(private readonly storage: StorageLike | undefined = defaultStorage()) {
    this.sync();
  }

  get(targetId: string): KafkaConnectSettings | undefined {
    this.sync();
    const value = this.settings.get(targetId);
    return value ? { ...value } : undefined;
  }

  hasAny(): boolean {
    return this.configuredState.get();
  }

  set(targetId: string, value?: KafkaConnectSettings): void {
    this.sync();
    if (!value?.connectUrl.trim()) this.settings.delete(targetId);
    else this.settings.set(targetId, { ...value, connectUrl: value.connectUrl.trim() });
    this.persist();
    this.updateConfigured();
    for (const listener of this.listeners) listener();
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  /** Re-read the storage, for example once the host store has loaded from disk. */
  reload(): void {
    this.lastRaw = undefined;
    this.sync();
    this.updateConfigured();
  }

  private updateConfigured(): void {
    const next = this.settings.size > 0;
    if (this.configuredState.get() === next) return;
    runInAction(() => this.configuredState.set(next));
  }

  private sync(): void {
    if (!this.storage) return;
    let raw: string | null;
    try {
      raw = this.storage.getItem(KAFKA_CONNECT_SETTINGS_KEY);
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
      for (const [targetId, value] of Object.entries(parsed)) {
        if (!value || typeof value !== "object") continue;
        const candidate = value as Partial<KafkaConnectSettings>;
        if (typeof candidate.connectUrl === "string" && candidate.connectUrl.trim()) {
          this.settings.set(targetId, {
            connectUrl: candidate.connectUrl,
            tls: Boolean(candidate.tls),
            ...(candidate.username ? { username: candidate.username } : {}),
          });
        }
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
      this.storage.setItem(KAFKA_CONNECT_SETTINGS_KEY, value);
    } catch {
      // Keep current-session settings if storage is unavailable.
    }
  }
}
