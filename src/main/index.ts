import { Main } from "@freelensapp/extensions";
import { isAggregateHealthSnapshotKey } from "../common/aggregate-health-snapshot";
import { kafkaPersistentStateStore } from "../common/kafka-persistent-state-store";
import { KafkaIpcMain } from "./ipc";

export default class KafkaExtensionMain extends Main.LensExtension {
  private ipc?: KafkaIpcMain;

  async onActivate(): Promise<void> {
    const store = kafkaPersistentStateStore();
    // The aggregate health snapshots are main's: a frame's copy of the store never replaces them (SPEC-014 REQ-189).
    store.keepOwnKeys(isAggregateHealthSnapshotKey);
    store.loadExtension(this);
    this.ipc = KafkaIpcMain.createInstance(this);
  }

  protected async onDeactivate(): Promise<void> {
    await this.ipc?.shutdown();
    this.ipc = undefined;
  }
}
