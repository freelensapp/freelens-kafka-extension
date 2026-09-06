import { createHash } from "node:crypto";
import { splitBootstrap } from "../../common/reachability";
import {
  createWorkloadEnvironmentResolver,
  forEachConcurrent,
  isBootstrapKey,
  workloadContainers,
} from "./workload-environment";
import type { ConnectionOptions as TlsOptions } from "node:tls";

import type { SASLOptions } from "kafkajs";

import type {
  KafkaSaslMechanism,
  KafkaSecurityHint,
  KafkaSecurityOverride,
  KafkaSecuritySummary,
  KafkaWorkloadSourceLocator,
} from "../../common/ipc";
import type { ResolvedCredentials } from "./credentials";
import type { KubeReader } from "./kube-reader";

export interface ParsedWorkloadSecurity extends ResolvedCredentials {
  detected: boolean;
  hint: KafkaSecurityHint;
}

export interface ResolvedExternalCredentials extends ParsedWorkloadSecurity {
  matchedWorkload?: string;
}

export interface AppliedSecurity {
  sasl?: SASLOptions;
  ssl?: TlsOptions | boolean;
  summary: KafkaSecuritySummary;
}

export class KafkaCredentialProfileCache {
  private readonly profiles = new Map<string, Promise<ResolvedExternalCredentials>>();

  getOrResolve(key: string, loader: () => Promise<ResolvedExternalCredentials>): Promise<ResolvedExternalCredentials> {
    const existing = this.profiles.get(key);
    if (existing) return existing;
    const pending = Promise.resolve()
      .then(loader)
      .catch((error: unknown) => {
        this.profiles.delete(key);
        throw error;
      });
    this.profiles.set(key, pending);
    return pending;
  }

  clear(): void {
    this.profiles.clear();
  }

  delete(key: string): void {
    this.profiles.delete(key);
  }
}

const normalizeKey = (key: string): string => key.toUpperCase().replace(/[^A-Z0-9]+/g, "_");

function normalizedEnvironment(environment: Record<string, string>): Record<string, string> {
  return Object.fromEntries(Object.entries(environment).map(([key, value]) => [normalizeKey(key), value]));
}

function first(environment: Record<string, string>, keys: string[]): string | undefined {
  for (const key of keys) {
    const value = environment[key]?.trim();
    if (value) return value;
  }
  return undefined;
}

function saslMechanism(value?: string): KafkaSaslMechanism | undefined {
  const normalized = value?.trim().toLowerCase().replaceAll("_", "-");
  if (normalized === "plain") return "plain";
  if (normalized === "scram-sha-256") return "scram-sha-256";
  if (normalized === "scram-sha-512") return "scram-sha-512";
  return undefined;
}

function parseJaas(value?: string): { username?: string; password?: string } {
  if (!value) return {};
  const username = /\busername\s*=\s*["']([^"']+)["']/i.exec(value)?.[1];
  const password = /\bpassword\s*=\s*["']([^"']+)["']/i.exec(value)?.[1];
  return { username, password };
}

function pem(value?: string): string | undefined {
  return value?.includes("-----BEGIN ") ? value : undefined;
}

/** Parse a fully resolved container environment. Pure; no value is ever returned to Renderer. */
export function parseWorkloadSecurityEnvironment(environment: Record<string, string>): ParsedWorkloadSecurity {
  const env = normalizedEnvironment(environment);
  const protocol = first(env, [
    "KAFKA_SECURITY_PROTOCOL",
    "SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL",
    "SECURITY_PROTOCOL",
  ])?.toUpperCase();
  const mechanismValue = first(env, [
    "KAFKA_SASL_MECHANISM",
    "SPRING_KAFKA_PROPERTIES_SASL_MECHANISM",
    "SASL_MECHANISM",
  ]);
  const jaas = parseJaas(
    first(env, ["KAFKA_SASL_JAAS_CONFIG", "SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG", "SASL_JAAS_CONFIG"]),
  );

  const apiKey = first(env, ["KAFKA_API_KEY", "CONFLUENT_API_KEY", "KAFKA_SASL_API_KEY"]);
  const apiSecret = first(env, ["KAFKA_API_SECRET", "CONFLUENT_API_SECRET", "KAFKA_SASL_API_SECRET"]);
  const username = first(env, ["KAFKA_SASL_USERNAME", "SASL_USERNAME", "KAFKA_USERNAME"]) ?? apiKey ?? jaas.username;
  const password = first(env, ["KAFKA_SASL_PASSWORD", "SASL_PASSWORD", "KAFKA_PASSWORD"]) ?? apiSecret ?? jaas.password;
  const mechanism = saslMechanism(mechanismValue) ?? (apiKey && apiSecret ? "plain" : undefined);

  const ca = pem(first(env, ["KAFKA_SSL_CA", "KAFKA_CA_CERT", "KAFKA_SSL_CA_CERT", "SSL_CA", "CA_CERT"]));
  const cert = pem(
    first(env, ["KAFKA_SSL_CERT", "KAFKA_CLIENT_CERT", "KAFKA_SSL_CERTIFICATE", "TLS_CERT", "CLIENT_CERT"]),
  );
  const key = pem(first(env, ["KAFKA_SSL_KEY", "KAFKA_CLIENT_KEY", "TLS_KEY", "CLIENT_KEY"]));
  const tls = Boolean(protocol?.includes("SSL") || ca || (cert && key));
  const mtls = Boolean(cert && key);

  const ssl: TlsOptions | boolean | undefined = mtls
    ? { cert, key, ...(ca ? { ca: [ca] } : {}) }
    : tls
      ? ca
        ? { ca: [ca] }
        : true
      : undefined;
  const sasl =
    mechanism && username !== undefined && password !== undefined
      ? ({ mechanism, username, password } as SASLOptions)
      : undefined;
  const detected = Boolean(protocol || mechanismValue || username || password || ca || cert || key);

  return {
    detected,
    ssl,
    sasl,
    hint: {
      tls,
      auth: mtls ? "mtls" : (mechanism ?? "none"),
    },
  };
}

function bootstrapKeys(bootstrap: string): Set<string> {
  return new Set(splitBootstrap(bootstrap).map((entry) => entry.toLowerCase()));
}

function sameBootstrap(left: string, right: string): boolean {
  const expected = bootstrapKeys(right);
  return splitBootstrap(left).some((entry) => expected.has(entry.toLowerCase()));
}

function strength(parsed: ParsedWorkloadSecurity): number {
  if (parsed.hint.auth === "mtls") return 4;
  if (parsed.sasl) return 3;
  if (parsed.ssl) return 2;
  return parsed.detected ? 1 : 0;
}

function profileKey(query: {
  bootstrap: string;
  contextId?: string;
  namespace?: string;
  sourceLocator?: KafkaWorkloadSourceLocator;
  security?: KafkaSecurityOverride;
  targetId?: string;
  user?: string;
}): string {
  return `kafka-profile:${createHash("sha256")
    .update(
      JSON.stringify({
        bootstrap: query.bootstrap,
        contextId: query.contextId,
        namespace: query.namespace,
        sourceLocator: query.sourceLocator,
        security: query.security,
        targetId: query.targetId,
        user: query.user,
      }),
    )
    .digest("hex")}`;
}

/** Resolve credentials from the same workload container that references the selected bootstrap. */
export async function resolveExternalCredentials(
  reader: KubeReader,
  query: { bootstrap: string; namespace?: string; sourceLocator?: KafkaWorkloadSourceLocator },
  onProgress?: (progress: { completed: number; total: number }) => void,
): Promise<ResolvedExternalCredentials> {
  const resolver = createWorkloadEnvironmentResolver(reader);
  const workloads = await reader.listWorkloads(query.sourceLocator?.namespace ?? query.namespace);
  const targetedWorkloads = query.sourceLocator
    ? workloads.filter(
        (workload) =>
          workload.metadata?.name === query.sourceLocator?.name &&
          (workload.kind ?? "Workload") === query.sourceLocator?.kind,
      )
    : workloads;
  let best: ResolvedExternalCredentials = { detected: false, hint: { tls: false, auth: "none" } };
  let completed = 0;
  onProgress?.({ completed, total: targetedWorkloads.length });

  await forEachConcurrent(targetedWorkloads, 6, async (workload) => {
    const containers = workloadContainers(workload).filter(
      (container) => !query.sourceLocator?.container || container.name === query.sourceLocator.container,
    );
    for (const container of containers) {
      const bootstrapEnvironment = await resolver.resolve(workload, container, isBootstrapKey);
      const bootstraps = Object.values(bootstrapEnvironment);
      if (!bootstraps.some((bootstrap) => sameBootstrap(bootstrap, query.bootstrap))) continue;

      const environment = await resolver.resolve(workload, container);
      const parsed = parseWorkloadSecurityEnvironment(environment);
      const candidate: ResolvedExternalCredentials = {
        ...parsed,
        matchedWorkload: `${workload.metadata?.namespace ?? "default"}/${workload.kind ?? "Workload"}/${
          workload.metadata?.name ?? "?"
        }`,
      };
      if (strength(candidate) > strength(best)) best = candidate;
    }
    completed += 1;
    onProgress?.({ completed, total: targetedWorkloads.length });
  });

  return best;
}

export function credentialProfileKey(query: {
  bootstrap: string;
  contextId?: string;
  namespace?: string;
  sourceLocator?: KafkaWorkloadSourceLocator;
  security?: KafkaSecurityOverride;
  targetId?: string;
  user?: string;
}): string {
  return profileKey(query);
}

/** Apply explicit UI overrides without losing automatically-resolved CA/client certificates. Pure. */
export function applySecurityOverride(options: {
  automatic?: ResolvedExternalCredentials | ResolvedCredentials;
  automaticHint?: KafkaSecurityHint;
  fallbackTls: boolean;
  override?: KafkaSecurityOverride;
  source: KafkaSecuritySummary["source"];
}): AppliedSecurity {
  const automatic = options.automatic ?? {};
  const automaticHint = options.automaticHint ?? {
    tls: Boolean(automatic.ssl) || options.fallbackTls,
    auth: automatic.sasl ? (automatic.sasl.mechanism as KafkaSaslMechanism) : "none",
  };
  const override = options.override;

  let ssl =
    override?.tlsMode === "disabled"
      ? undefined
      : override?.tlsMode === "enabled"
        ? typeof automatic.ssl === "object"
          ? automatic.ssl
          : true
        : (automatic.ssl ?? (options.fallbackTls ? true : undefined));

  let sasl = automatic.sasl;
  let auth = automaticHint.auth;
  if (override?.authMode !== undefined && override.authMode !== "auto" && automaticHint.auth === "mtls") {
    if (typeof ssl === "object") {
      const { cert: _cert, key: _key, pfx: _pfx, passphrase: _passphrase, ...tlsOnly } = ssl;
      ssl = Object.keys(tlsOnly).length > 0 ? tlsOnly : true;
    }
    auth = "none";
  }
  if (override?.authMode === "none") {
    sasl = undefined;
    auth = "none";
  } else if (override && override.authMode !== "auto") {
    const username = override.username?.trim();
    const password = override.password;
    if (!username || password === undefined || password === "") {
      throw new Error(`username and password are required for ${override.authMode}`);
    }
    sasl = { mechanism: override.authMode, username, password } as SASLOptions;
    auth = override.authMode;
  }
  if (!ssl && auth === "mtls") auth = "none";

  return {
    ssl,
    sasl,
    summary: {
      tls: Boolean(ssl),
      auth,
      source: override ? "override" : options.source,
    },
  };
}
