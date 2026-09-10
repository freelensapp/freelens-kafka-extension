/** Basic-auth passwords for the Schema Registry and Kafka Connect endpoints of a target. */
export interface KafkaEndpointSecrets {
  registryPassword?: string;
  connectPassword?: string;
}

/**
 * Session-only store for the endpoint passwords: kept in renderer memory for the current
 * Freelens session and never persisted (SPEC-010 REQ and SPEC-011: passwords MUST NOT be
 * stored). The usernames and URLs live in the persisted settings stores instead.
 */
export class KafkaEndpointSecretsStore {
  private readonly secrets = new Map<string, KafkaEndpointSecrets>();

  get(targetId: string): KafkaEndpointSecrets | undefined {
    const value = this.secrets.get(targetId);
    return value ? { ...value } : undefined;
  }

  /** Merge the given fields; an empty or undefined password removes the stored one. */
  set(targetId: string, patch: KafkaEndpointSecrets): void {
    const next: KafkaEndpointSecrets = { ...this.secrets.get(targetId) };
    for (const key of ["registryPassword", "connectPassword"] as const) {
      if (!(key in patch)) continue;
      if (patch[key]) next[key] = patch[key];
      else delete next[key];
    }
    if (next.registryPassword || next.connectPassword) this.secrets.set(targetId, next);
    else this.secrets.delete(targetId);
  }

  clear(): void {
    this.secrets.clear();
  }
}
