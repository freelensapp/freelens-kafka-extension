import type { KafkaAuthMode, KafkaSecurityOverride, KafkaTlsMode } from "../common/ipc";

export const KAFKA_SECURITY_OVERRIDES_KEY = "freelens-kafka.security-overrides.v1";

export interface KafkaConnectionSettingsStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

/** The part of a security override that may be written to disk: everything but the password. */
export type KafkaDurableSecurityOverride = Omit<KafkaSecurityOverride, "password">;

const TLS_MODES: ReadonlySet<string> = new Set<KafkaTlsMode>(["auto", "enabled", "disabled"]);
const AUTH_MODES: ReadonlySet<string> = new Set<KafkaAuthMode>([
  "auto",
  "none",
  "plain",
  "scram-sha-256",
  "scram-sha-512",
  "aws-msk-iam",
]);
const DURABLE_TEXT_FIELDS = ["username", "awsRegion", "awsProfile"] as const;

function normalizeDurable(value: unknown): KafkaDurableSecurityOverride | undefined {
  if (!value || typeof value !== "object") return undefined;
  const candidate = value as Record<string, unknown>;
  if (typeof candidate.tlsMode !== "string" || !TLS_MODES.has(candidate.tlsMode)) return undefined;
  if (typeof candidate.authMode !== "string" || !AUTH_MODES.has(candidate.authMode)) return undefined;
  const durable: KafkaDurableSecurityOverride = {
    tlsMode: candidate.tlsMode as KafkaTlsMode,
    authMode: candidate.authMode as KafkaAuthMode,
  };
  for (const field of DURABLE_TEXT_FIELDS) {
    const text = candidate[field];
    if (typeof text === "string" && text) durable[field] = text;
  }
  return durable;
}

/**
 * Security overrides of the Connection Settings drawer, isolated by Kubernetes cluster and
 * Kafka target. The non-secret part (TLS mode, authentication mode, username, AWS region and
 * profile) is kept in the given storage, which the extension backs with the host-managed
 * extension store so that it survives a Freelens restart (SPEC-018). The password is never
 * written anywhere: it stays in renderer memory for the current session and has to be typed
 * again after a restart.
 */
export class KafkaConnectionSettingsStore {
  private readonly durable = new Map<string, KafkaDurableSecurityOverride>();
  private readonly passwords = new Map<string, string>();
  private lastRaw: string | null | undefined;

  constructor(private readonly storage: KafkaConnectionSettingsStorage | undefined = undefined) {
    this.sync();
  }

  get(kubernetesClusterId: string, targetId: string): KafkaSecurityOverride | undefined {
    this.sync();
    const key = this.key(kubernetesClusterId, targetId);
    const durable = this.durable.get(key);
    if (!durable) return undefined;
    const password = this.passwords.get(key);
    return { ...durable, ...(password !== undefined ? { password } : {}) };
  }

  set(kubernetesClusterId: string, targetId: string, override?: KafkaSecurityOverride): void {
    this.sync();
    const key = this.key(kubernetesClusterId, targetId);
    if (override) {
      const { password, ...durable } = override;
      this.durable.set(key, { ...durable });
      if (password) {
        this.passwords.set(key, password);
      } else {
        this.passwords.delete(key);
      }
    } else {
      this.durable.delete(key);
      this.passwords.delete(key);
    }
    this.persist();
  }

  removeTarget(kubernetesClusterId: string, targetId: string): void {
    this.sync();
    const key = this.key(kubernetesClusterId, targetId);
    this.durable.delete(key);
    this.passwords.delete(key);
    this.persist();
  }

  clear(): void {
    this.durable.clear();
    this.passwords.clear();
    this.persist();
  }

  /** Re-read the storage, for example once the host store has loaded from disk. */
  reload(): void {
    this.lastRaw = undefined;
    this.sync();
  }

  private key(kubernetesClusterId: string, targetId: string): string {
    return `${kubernetesClusterId}:${targetId}`;
  }

  /** Mirror the storage into memory whenever its raw value changed (host load, other frames). */
  private sync(): void {
    if (!this.storage) return;
    let raw: string | null;
    try {
      raw = this.storage.getItem(KAFKA_SECURITY_OVERRIDES_KEY);
    } catch {
      return;
    }
    if (raw === this.lastRaw) return;
    this.lastRaw = raw;
    this.durable.clear();
    if (!raw) return;
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return;
      for (const [key, value] of Object.entries(parsed)) {
        const durable = normalizeDurable(value);
        if (durable) this.durable.set(key, durable);
      }
    } catch {
      this.durable.clear();
    }
  }

  private persist(): void {
    if (!this.storage) return;
    try {
      const value = JSON.stringify(Object.fromEntries(this.durable.entries()));
      this.lastRaw = value;
      this.storage.setItem(KAFKA_SECURITY_OVERRIDES_KEY, value);
    } catch {
      // The overrides stay in memory for this session when the storage is unavailable.
    }
  }
}
