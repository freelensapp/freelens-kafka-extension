import { Main } from "@freelensapp/extensions";
import { kafkaPersistentStateStore } from "../common/kafka-persistent-state-store";
import { KafkaIpcMain } from "./ipc";

export default class KafkaExtensionMain extends Main.LensExtension {
  private ipc?: KafkaIpcMain;

  async onActivate(): Promise<void> {
    kafkaPersistentStateStore().loadExtension(this);
    this.ipc = KafkaIpcMain.createInstance(this);
  }

  protected async onDeactivate(): Promise<void> {
    await this.ipc?.shutdown();
    this.ipc = undefined;
  }
}
