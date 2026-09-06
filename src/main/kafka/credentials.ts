import type { ConnectionOptions as TlsOptions } from "node:tls";

import type { SASLOptions } from "kafkajs";

import type { KubeReader, KubeSecret } from "./kube-reader";

export interface ResolvedCredentials {
  sasl?: SASLOptions;
  ssl?: TlsOptions | boolean;
}

function decode(secret: KubeSecret, key: string): string | undefined {
  const value = secret.data?.[key];
  return value === undefined ? undefined : Buffer.from(value, "base64").toString("utf8");
}

/** Cluster CA for TLS listeners: Strimzi Secret `<cluster>-cluster-ca-cert`, key `ca.crt`. Pure. */
export function parseClusterCa(secret: KubeSecret): TlsOptions | undefined {
  const ca = decode(secret, "ca.crt");
  return ca ? { ca: [ca] } : undefined;
}

/** SCRAM user: `KafkaUser` Secret key `password`. Pure. */
export function parseScram(
  secret: KubeSecret,
  username: string,
  mechanism: "scram-sha-512" | "scram-sha-256" = "scram-sha-512",
): SASLOptions | undefined {
  const password = decode(secret, "password");
  return password ? ({ mechanism, username, password } as SASLOptions) : undefined;
}

/** mTLS user: `KafkaUser` Secret keys `user.crt` + `user.key` (+ optional `ca.crt`). Pure. */
export function parseMtls(secret: KubeSecret): TlsOptions | undefined {
  const cert = decode(secret, "user.crt");
  const key = decode(secret, "user.key");
  if (!cert || !key) return undefined;
  const ssl: TlsOptions = { cert, key };
  const ca = decode(secret, "ca.crt");
  if (ca) ssl.ca = [ca];
  return ssl;
}

export interface StrimziCredentialQuery {
  namespace: string;
  clusterName: string;
  /** Whether the chosen listener uses TLS (pulls the cluster CA). */
  tls?: boolean;
  /** `KafkaUser` name; its Secret carries SCRAM password or mTLS cert/key. */
  user?: string;
  mechanism?: "scram-sha-512" | "scram-sha-256";
}

/**
 * Resolve kafkajs `sasl`/`ssl` for a Strimzi cluster from Kubernetes Secrets:
 * cluster CA for TLS listeners, and SCRAM password or mTLS cert/key for a `KafkaUser`.
 */
export async function resolveStrimziCredentials(
  reader: KubeReader,
  query: StrimziCredentialQuery,
): Promise<ResolvedCredentials> {
  const creds: ResolvedCredentials = {};

  if (query.tls) {
    const caSecret = await reader.getSecret(query.namespace, `${query.clusterName}-cluster-ca-cert`);
    const ssl = caSecret ? parseClusterCa(caSecret) : undefined;
    if (ssl) creds.ssl = ssl;
  }

  if (query.user) {
    const userSecret = await reader.getSecret(query.namespace, query.user);
    if (userSecret) {
      const mtls = parseMtls(userSecret);
      if (mtls) {
        creds.ssl = {
          ...(typeof creds.ssl === "object" ? creds.ssl : {}),
          ...mtls,
        };
      } else {
        const sasl = parseScram(userSecret, query.user, query.mechanism);
        if (sasl) creds.sasl = sasl;
      }
    }
  }

  return creds;
}
