import { randomBytes } from "node:crypto";
import type { ConnectionOptions as TlsOptions } from "node:tls";

import type { SASLOptions } from "kafkajs";

export const AKHQ_VERSION = "0.28.0";
export const AKHQ_DIGEST = "sha256:c2824dc2ae442b7ec215581f7e60a9f4106ea9004057fa3a1f88a29464cd4e63";
export const AKHQ_IMAGE = `tchiotludo/akhq:${AKHQ_VERSION}@${AKHQ_DIGEST}`;
export const AKHQ_CLUSTER_ID = "slice16-read-only";
export const AKHQ_HTTP_PORT = 18_080;
export const AKHQ_MANAGEMENT_PORT = 18_081;
const AKHQ_READ_ONLY_GROUP = "slice16-read-only-group";
const AKHQ_READ_ONLY_ROLE = "slice16-read-only-role";

function requiredString(value: unknown, label: string): string {
  if (typeof value === "string" && value) return value;
  if (Buffer.isBuffer(value)) return value.toString("utf8");
  if (Array.isArray(value) && value.length > 0) {
    return value
      .map((item) => {
        if (typeof item === "string") return item;
        if (Buffer.isBuffer(item)) return item.toString("utf8");
        throw new Error(`AKHQ comparison cannot safely convert ${label} TLS material`);
      })
      .join("\n");
  }
  throw new Error(`AKHQ comparison cannot safely convert ${label} TLS material`);
}

function jaasValue(value: string): string {
  return JSON.stringify(value);
}

function saslProperties(sasl: SASLOptions): Record<string, string> {
  if (sasl.mechanism !== "plain" && sasl.mechanism !== "scram-sha-256" && sasl.mechanism !== "scram-sha-512") {
    throw new Error(`AKHQ comparison does not support SASL mechanism ${sasl.mechanism}`);
  }
  const mechanism = sasl.mechanism.toUpperCase();
  const loginModule = sasl.mechanism === "plain" ? "plain.PlainLoginModule" : "scram.ScramLoginModule";
  return {
    "sasl.mechanism": mechanism,
    "sasl.jaas.config": `org.apache.kafka.common.security.${loginModule} required username=${jaasValue(sasl.username)} password=${jaasValue(sasl.password)};`,
  };
}

function tlsProperties(ssl: TlsOptions | boolean | undefined): Record<string, string> {
  if (!ssl || ssl === true) return {};
  const properties: Record<string, string> = {};
  if (ssl.ca) {
    properties["ssl.truststore.type"] = "PEM";
    properties["ssl.truststore.certificates"] = requiredString(ssl.ca, "CA");
  }
  if (ssl.cert || ssl.key) {
    properties["ssl.keystore.type"] = "PEM";
    properties["ssl.keystore.certificate.chain"] = requiredString(ssl.cert, "client certificate");
    properties["ssl.keystore.key"] = requiredString(ssl.key, "client key");
  }
  return properties;
}

export function createAkhqReadOnlyConfiguration(options: {
  bootstrap: string;
  jwtSecret?: string;
  sasl?: SASLOptions;
  ssl?: TlsOptions | boolean;
}): Record<string, unknown> {
  const jwtSecret = options.jwtSecret ?? randomBytes(64).toString("base64url");
  if (Buffer.byteLength(jwtSecret) < 64) throw new Error("AKHQ comparison JWT secret is too short");
  const properties: Record<string, string> = {
    "bootstrap.servers": options.bootstrap,
    "security.protocol": options.sasl
      ? options.ssl
        ? "SASL_SSL"
        : "SASL_PLAINTEXT"
      : options.ssl
        ? "SSL"
        : "PLAINTEXT",
    ...tlsProperties(options.ssl),
    ...(options.sasl ? saslProperties(options.sasl) : {}),
  };
  return {
    micronaut: {
      security: {
        enabled: true,
        token: { jwt: { signatures: { secret: { generator: { secret: jwtSecret } } } } },
      },
      server: { host: "127.0.0.1", port: AKHQ_HTTP_PORT },
    },
    endpoints: { all: { port: AKHQ_MANAGEMENT_PORT } },
    akhq: {
      audit: { enabled: false },
      pagination: { "page-size": 25 },
      security: {
        "default-group": AKHQ_READ_ONLY_GROUP,
        groups: {
          [AKHQ_READ_ONLY_GROUP]: [{ clusters: [`^${AKHQ_CLUSTER_ID}$`], patterns: [".*"], role: AKHQ_READ_ONLY_ROLE }],
        },
        roles: {
          [AKHQ_READ_ONLY_ROLE]: [
            {
              actions: ["READ"],
              resources: [
                "ACL",
                "CONNECT_CLUSTER",
                "CONNECTOR",
                "CONSUMER_GROUP",
                "KSQLDB",
                "NODE",
                "SCHEMA",
                "TOPIC",
                "TOPIC_DATA",
              ],
            },
            { actions: ["READ_CONFIG"], resources: ["NODE", "TOPIC"] },
          ],
        },
      },
      connections: { [AKHQ_CLUSTER_ID]: { properties } },
    },
  };
}

export type AkhqPageKind = "consumer-groups" | "topics";
export type AkhqPageRejection =
  | "links-after"
  | "links-before"
  | "links-before-page-0"
  | "links-before-page-1"
  | "links-before-page-other"
  | "page-count"
  | "page-size"
  | "payload"
  | "records"
  | "records-group-id"
  | "records-group-members"
  | "records-group-offsets"
  | "records-group-state"
  | "records-topic-name"
  | "records-topic-partitions"
  | "requested-page"
  | "results"
  | "total";

export class AkhqPageValidationError extends Error {
  constructor(readonly reason: AkhqPageRejection) {
    super(`AKHQ useful page rejected: ${reason}`);
  }
}

function nonEmptyString(value: unknown): boolean {
  return typeof value === "string" && value.length > 0;
}

export function assertAkhqUsefulPage(value: unknown, kind: AkhqPageKind, requestedPage: number): void {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new AkhqPageValidationError("payload");
  }
  const page = value as {
    after?: unknown;
    before?: unknown;
    page?: unknown;
    pageSize?: unknown;
    results?: unknown;
    total?: unknown;
  };
  const results = Array.isArray(page.results) ? page.results : [];
  if (!Number.isInteger(requestedPage) || requestedPage < 1) {
    throw new AkhqPageValidationError("requested-page");
  }
  if (results.length === 0) throw new AkhqPageValidationError("results");
  for (const result of results) {
    if (!result || typeof result !== "object" || Array.isArray(result)) {
      throw new AkhqPageValidationError("records");
    }
    const record = result as Record<string, unknown>;
    if (kind === "topics") {
      if (!nonEmptyString(record.name)) throw new AkhqPageValidationError("records-topic-name");
      if (!Array.isArray(record.partitions)) throw new AkhqPageValidationError("records-topic-partitions");
    } else {
      if (!nonEmptyString(record.id)) throw new AkhqPageValidationError("records-group-id");
      if (!nonEmptyString(record.state)) throw new AkhqPageValidationError("records-group-state");
      if (record.members !== undefined && !Array.isArray(record.members)) {
        throw new AkhqPageValidationError("records-group-members");
      }
      if (record.offsets !== undefined && !Array.isArray(record.offsets)) {
        throw new AkhqPageValidationError("records-group-offsets");
      }
    }
  }
  if (!Number.isInteger(page.page) || Number(page.page) < requestedPage) {
    throw new AkhqPageValidationError("page-count");
  }
  if (!Number.isInteger(page.total) || Number(page.total) < results.length) {
    throw new AkhqPageValidationError("total");
  }
  if (page.pageSize !== 25) throw new AkhqPageValidationError("page-size");
  const pageCount = Number(page.page);
  const beforePage = typeof page.before === "string" ? /[?&]page=(\d+)/.exec(page.before)?.[1] : undefined;
  const beforeValid =
    requestedPage === 1
      ? page.before === null || beforePage === undefined
      : typeof page.before === "string" && page.before.includes(`page=${requestedPage - 1}`);
  const afterValid =
    requestedPage < pageCount
      ? typeof page.after === "string" && page.after.includes(`page=${requestedPage + 1}`)
      : page.after === null || page.after === "";
  if (!beforeValid) {
    throw new AkhqPageValidationError(
      beforePage === "0"
        ? "links-before-page-0"
        : beforePage === "1"
          ? "links-before-page-1"
          : beforePage
            ? "links-before-page-other"
            : "links-before",
    );
  }
  if (!afterValid) throw new AkhqPageValidationError("links-after");
}
