import { Common } from "@freelensapp/extensions";
import { makeObservable, observable, toJS } from "mobx";

export interface KafkaPersistentStateModel {
  values: Record<string, string>;
}

interface LegacyStorage {
  getItem(key: string): string | null;
}

/** Durable host-managed storage for non-secret Kafka catalog and selection state. */
export class KafkaPersistentStateStore extends Common.Store.ExtensionStore<KafkaPersistentStateModel> {
  values: Record<string, string> = {};

  constructor() {
    super({
      configName: "freelens-kafka-state-store",
      defaults: { values: {} },
    });
    makeObservable(this, { values: observable });
  }

  getItem(key: string): string | null {
    return this.values[key] ?? null;
  }

  setItem(key: string, value: string): void {
    this.values = { ...this.values, [key]: value };
  }

  removeItem(key: string): void {
    if (!(key in this.values)) return;
    const { [key]: _removed, ...remaining } = this.values;
    this.values = remaining;
  }

  migrateLegacy(storage: LegacyStorage, keys: readonly string[]): string[] {
    const migrated: string[] = [];
    let values = this.values;
    for (const key of keys) {
      if (values[key] !== undefined) continue;
      try {
        const value = storage.getItem(key);
        if (value === null) continue;
        values = { ...values, [key]: value };
        migrated.push(key);
      } catch {
        // Legacy browser storage is best-effort and may be unavailable.
      }
    }
    if (migrated.length > 0) this.values = values;
    return migrated;
  }

  fromStore(model: Partial<KafkaPersistentStateModel>): void {
    const values = model.values;
    if (!values || typeof values !== "object" || Array.isArray(values)) {
      this.values = {};
      return;
    }
    this.values = Object.fromEntries(
      Object.entries(values).filter((entry): entry is [string, string] => typeof entry[1] === "string"),
    );
  }

  toJSON(): KafkaPersistentStateModel {
    return { values: toJS(this.values) };
  }
}

export function kafkaPersistentStateStore(): KafkaPersistentStateStore {
  return KafkaPersistentStateStore.getInstanceOrCreate<KafkaPersistentStateStore>();
}
