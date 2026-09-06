import type { KafkaSecurityOverride } from "../common/ipc";

export class KafkaConnectionSettingsStore {
  private readonly overrides = new Map<string, KafkaSecurityOverride>();

  get(kubernetesClusterId: string, targetId: string): KafkaSecurityOverride | undefined {
    const override = this.overrides.get(this.key(kubernetesClusterId, targetId));
    return override ? { ...override } : undefined;
  }

  set(kubernetesClusterId: string, targetId: string, override?: KafkaSecurityOverride): void {
    const key = this.key(kubernetesClusterId, targetId);
    if (override) {
      this.overrides.set(key, { ...override });
    } else {
      this.overrides.delete(key);
    }
  }

  removeTarget(kubernetesClusterId: string, targetId: string): void {
    this.overrides.delete(this.key(kubernetesClusterId, targetId));
  }

  clear(): void {
    this.overrides.clear();
  }

  private key(kubernetesClusterId: string, targetId: string): string {
    return `${kubernetesClusterId}:${targetId}`;
  }
}
