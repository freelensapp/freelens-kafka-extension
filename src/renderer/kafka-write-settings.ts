import { useEffect, useState } from "react";

export const KAFKA_WRITE_MODE_KEY = "freelens-kafka.write-mode.v1";

export interface KafkaWriteSettingsStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

function defaultStorage(): KafkaWriteSettingsStorage | undefined {
  try {
    return typeof window === "undefined" ? undefined : window.localStorage;
  } catch {
    return undefined;
  }
}

export class KafkaWriteSettingsStore {
  private readonly writeMode = new Set<string>();
  private readonly listeners = new Set<() => void>();

  constructor(private readonly storage: KafkaWriteSettingsStorage | undefined = defaultStorage()) {
    this.syncFromStorage();
  }

  private syncFromStorage(): void {
    try {
      const stored = this.storage?.getItem(KAFKA_WRITE_MODE_KEY);
      if (!stored) return;
      const parsed = JSON.parse(stored) as unknown;
      if (Array.isArray(parsed)) {
        this.writeMode.clear();
        for (const targetId of parsed) {
          if (typeof targetId === "string") this.writeMode.add(targetId);
        }
      }
    } catch {
      this.writeMode.clear();
    }
  }

  get(targetId: string): boolean {
    this.syncFromStorage();
    return this.writeMode.has(targetId);
  }

  set(targetId: string, enabled: boolean): void {
    if (this.get(targetId) === enabled) return;
    if (enabled) {
      this.writeMode.add(targetId);
    } else {
      this.writeMode.delete(targetId);
    }
    this.persist();
    this.emit();
  }

  clear(): void {
    if (this.writeMode.size === 0) return;
    this.writeMode.clear();
    this.persist();
    this.emit();
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private persist(): void {
    try {
      this.storage?.setItem(KAFKA_WRITE_MODE_KEY, JSON.stringify([...this.writeMode].sort()));
    } catch {
      // Write mode remains available for this renderer session when storage is unavailable.
    }
  }

  private emit(): void {
    for (const listener of this.listeners) listener();
  }
}

export function useKafkaWriteMode(store: KafkaWriteSettingsStore, targetId?: string): boolean {
  const [enabled, setEnabled] = useState(() => Boolean(targetId && store.get(targetId)));

  useEffect(() => {
    const update = () => setEnabled(Boolean(targetId && store.get(targetId)));
    update();
    return store.subscribe(update);
  }, [store, targetId]);

  return enabled;
}
