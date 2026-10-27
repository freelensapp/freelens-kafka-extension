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

  /**
   * Keys only this process writes. The host syncs the whole store between main and the frames, each side replacing
   * its copy with the other's: a frame that saves anything else would send a copy without what this process has just
   * written, and that copy would replace it, on disk too. For these keys this process keeps its own values when a
   * model arrives; the load from disk still takes them all.
   */
  private ownKeys?: (key: string) => boolean;
  private loading = false;

  keepOwnKeys(ownKeys: (key: string) => boolean): void {
    this.ownKeys = ownKeys;
  }

  override loadExtension(
    ...args: Parameters<Common.Store.ExtensionStore<KafkaPersistentStateModel>["loadExtension"]>
  ): void {
    this.loading = true;
    try {
      super.loadExtension(...args);
    } finally {
      this.loading = false;
    }
  }

  fromStore(model: Partial<KafkaPersistentStateModel>): void {
    const incoming = model.values;
    const values: Record<string, string> =
      !incoming || typeof incoming !== "object" || Array.isArray(incoming)
        ? {}
        : Object.fromEntries(
            Object.entries(incoming).filter((entry): entry is [string, string] => typeof entry[1] === "string"),
          );
    const ownKeys = this.ownKeys;

    if (ownKeys && !this.loading) {
      for (const key of Object.keys(values)) if (ownKeys(key)) delete values[key];
      for (const [key, value] of Object.entries(this.values)) if (ownKeys(key)) values[key] = value;
    }
    this.values = values;
  }

  toJSON(): KafkaPersistentStateModel {
    return { values: toJS(this.values) };
  }
}

export function kafkaPersistentStateStore(): KafkaPersistentStateStore {
  return KafkaPersistentStateStore.getInstanceOrCreate<KafkaPersistentStateStore>();
}
