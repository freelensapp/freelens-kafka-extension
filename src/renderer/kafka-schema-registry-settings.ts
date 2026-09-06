import { observable } from "mobx";

export const KAFKA_SCHEMA_REGISTRY_SETTINGS_KEY = "freelens-kafka.schema-registry.v1";

export interface SchemaRegistrySettings {
  registryUrl: string;
  tls: boolean;
  username?: string;
}

interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

export class KafkaSchemaRegistrySettingsStore {
  private readonly settings = new Map<string, SchemaRegistrySettings>();
  private readonly listeners = new Set<() => void>();
  private readonly configured = observable.box(false);

  constructor(
    private readonly storage: StorageLike | undefined = typeof window === "undefined" ? undefined : window.localStorage,
  ) {
    this.load();
  }

  get(targetId: string): SchemaRegistrySettings | undefined {
    const value = this.settings.get(targetId);
    return value ? { ...value } : undefined;
  }

  hasAny(): boolean {
    return this.configured.get();
  }

  set(targetId: string, value?: SchemaRegistrySettings): void {
    if (!value?.registryUrl.trim()) this.settings.delete(targetId);
    else this.settings.set(targetId, { ...value, registryUrl: value.registryUrl.trim() });
    this.persist();
    this.configured.set(this.settings.size > 0);
    for (const listener of this.listeners) listener();
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private load(): void {
    try {
      const parsed = JSON.parse(this.storage?.getItem(KAFKA_SCHEMA_REGISTRY_SETTINGS_KEY) ?? "{}") as unknown;
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return;
      for (const [targetId, value] of Object.entries(parsed)) {
        if (!value || typeof value !== "object") continue;
        const candidate = value as Partial<SchemaRegistrySettings>;
        if (typeof candidate.registryUrl === "string" && candidate.registryUrl.trim()) {
          this.settings.set(targetId, {
            registryUrl: candidate.registryUrl,
            tls: Boolean(candidate.tls),
            ...(candidate.username ? { username: candidate.username } : {}),
          });
        }
      }
      this.configured.set(this.settings.size > 0);
    } catch {
      this.settings.clear();
    }
  }

  private persist(): void {
    try {
      const values = Object.fromEntries(this.settings.entries());
      this.storage?.setItem(KAFKA_SCHEMA_REGISTRY_SETTINGS_KEY, JSON.stringify(values));
    } catch {
      // Settings remain available for the current session when storage is unavailable.
    }
  }
}
