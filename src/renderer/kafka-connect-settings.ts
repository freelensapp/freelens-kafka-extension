import { observable } from "mobx";

export const KAFKA_CONNECT_SETTINGS_KEY = "freelens-kafka.connect.v1";

export interface KafkaConnectSettings {
  connectUrl: string;
  tls: boolean;
  username?: string;
}

export class KafkaConnectSettingsStore {
  private readonly settings = new Map<string, KafkaConnectSettings>();
  private readonly configured = new Set<() => void>();
  private readonly configuredState = observable.box(false);

  constructor(private readonly storage: Storage = window.localStorage) {
    try {
      const parsed = JSON.parse(storage.getItem(KAFKA_CONNECT_SETTINGS_KEY) ?? "{}") as Record<
        string,
        KafkaConnectSettings
      >;
      for (const [targetId, value] of Object.entries(parsed)) {
        if (value?.connectUrl) this.settings.set(targetId, { ...value });
      }
      this.configuredState.set(this.settings.size > 0);
    } catch {
      this.settings.clear();
    }
  }

  get(targetId: string): KafkaConnectSettings | undefined {
    const value = this.settings.get(targetId);
    return value ? { ...value } : undefined;
  }

  hasAny(): boolean {
    return this.configuredState.get();
  }

  set(targetId: string, value?: KafkaConnectSettings): void {
    if (!value?.connectUrl.trim()) this.settings.delete(targetId);
    else this.settings.set(targetId, { ...value, connectUrl: value.connectUrl.trim() });
    this.persist();
    this.configuredState.set(this.settings.size > 0);
    for (const listener of this.configured) listener();
  }

  subscribe(listener: () => void): () => void {
    this.configured.add(listener);
    return () => this.configured.delete(listener);
  }

  private persist(): void {
    try {
      this.storage.setItem(KAFKA_CONNECT_SETTINGS_KEY, JSON.stringify(Object.fromEntries(this.settings)));
    } catch {
      // Keep current-session settings if storage is unavailable.
    }
  }
}
