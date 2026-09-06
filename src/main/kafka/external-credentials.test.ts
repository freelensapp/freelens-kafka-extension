import { describe, expect, it } from "vitest";
import {
  applySecurityOverride,
  credentialProfileKey,
  KafkaCredentialProfileCache,
  parseWorkloadSecurityEnvironment,
  resolveExternalCredentials,
} from "./external-credentials";

import type { KubeObject, KubeReader, KubeSecret } from "./kube-reader";

const b64 = (value: string): string => Buffer.from(value).toString("base64");

function deployment(env: unknown[]): KubeObject {
  return {
    kind: "Deployment",
    metadata: { namespace: "apps", name: "orders" },
    spec: { template: { spec: { containers: [{ name: "app", env }] } } },
  };
}

function fakeReader(workloads: KubeObject[], secrets: Record<string, KubeSecret> = {}): KubeReader {
  return {
    listCustomResources: async () => [],
    listPods: async () => [],
    listServices: async () => [],
    listWorkloads: async () => workloads,
    getConfigMap: async () => null,
    getSecret: async (namespace, name) => secrets[`${namespace}/${name}`] ?? null,
  };
}

describe("parseWorkloadSecurityEnvironment", () => {
  it("parses TLS + SASL/PLAIN (including Confluent API key aliases)", () => {
    expect(
      parseWorkloadSecurityEnvironment({
        KAFKA_SECURITY_PROTOCOL: "SASL_SSL",
        KAFKA_API_KEY: "api-key",
        KAFKA_API_SECRET: "api-secret",
      }),
    ).toMatchObject({
      detected: true,
      ssl: true,
      sasl: { mechanism: "plain", username: "api-key", password: "api-secret" },
      hint: { tls: true, auth: "plain" },
    });
  });

  it("parses SCRAM credentials from a Spring JAAS config", () => {
    expect(
      parseWorkloadSecurityEnvironment({
        SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL: "SASL_PLAINTEXT",
        SPRING_KAFKA_PROPERTIES_SASL_MECHANISM: "SCRAM-SHA-512",
        SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG:
          'org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="secret";',
      }),
    ).toMatchObject({
      sasl: { mechanism: "scram-sha-512", username: "alice", password: "secret" },
      hint: { tls: false, auth: "scram-sha-512" },
    });
  });

  it("parses mTLS PEM material", () => {
    const parsed = parseWorkloadSecurityEnvironment({
      KAFKA_SSL_CA: "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----",
      KAFKA_SSL_CERT: "-----BEGIN CERTIFICATE-----\nCLIENT\n-----END CERTIFICATE-----",
      KAFKA_SSL_KEY: "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----",
    });
    expect(parsed.hint).toEqual({ tls: true, auth: "mtls" });
    expect(parsed.ssl).toMatchObject({ cert: expect.stringContaining("CLIENT"), key: expect.stringContaining("KEY") });
  });
});

describe("resolveExternalCredentials", () => {
  it("reads username/password from Secret refs on the workload using the selected bootstrap", async () => {
    const reader = fakeReader(
      [
        deployment([
          { name: "KAFKA_BOOTSTRAP_SERVERS", value: "broker.example.com:9092" },
          { name: "KAFKA_SASL_MECHANISM", value: "PLAIN" },
          { name: "KAFKA_SASL_USERNAME", valueFrom: { secretKeyRef: { name: "kafka-auth", key: "username" } } },
          { name: "KAFKA_SASL_PASSWORD", valueFrom: { secretKeyRef: { name: "kafka-auth", key: "password" } } },
        ]),
      ],
      {
        "apps/kafka-auth": { data: { username: b64("alice"), password: b64("secret") } },
      },
    );

    const resolved = await resolveExternalCredentials(reader, { bootstrap: "broker.example.com:9092" });
    expect(resolved.sasl).toEqual({ mechanism: "plain", username: "alice", password: "secret" });
    expect(resolved.matchedWorkload).toBe("apps/Deployment/orders");
  });

  it("does not read unrelated explicit Secret refs before the bootstrap matches", async () => {
    let secretReads = 0;
    const reader = fakeReader([
      deployment([
        { name: "KAFKA_BOOTSTRAP_SERVERS", value: "other.example.com:9092" },
        { name: "DATABASE_PASSWORD", valueFrom: { secretKeyRef: { name: "database", key: "password" } } },
      ]),
    ]);
    reader.getSecret = async () => {
      secretReads += 1;
      return null;
    };

    await resolveExternalCredentials(reader, { bootstrap: "broker.example.com:9092" });
    expect(secretReads).toBe(0);
  });

  it("reports credential-search progress by workload", async () => {
    const progress: { completed: number; total: number }[] = [];
    await resolveExternalCredentials(
      fakeReader([
        deployment([{ name: "KAFKA_BOOTSTRAP_SERVERS", value: "one.example.com:9092" }]),
        {
          ...deployment([{ name: "KAFKA_BOOTSTRAP_SERVERS", value: "two.example.com:9092" }]),
          metadata: { namespace: "apps", name: "two" },
        },
      ]),
      { bootstrap: "one.example.com:9092" },
      (event) => progress.push(event),
    );
    expect(progress[0]).toEqual({ completed: 0, total: 2 });
    expect(progress.at(-1)).toEqual({ completed: 2, total: 2 });
  });

  it("limits targeted credential resolution to the recorded namespace and workload", async () => {
    let listNamespace: string | undefined;
    let visited = 0;
    const reader = fakeReader([
      deployment([{ name: "KAFKA_BOOTSTRAP_SERVERS", value: "broker.example.com:9092" }]),
      {
        ...deployment([{ name: "KAFKA_BOOTSTRAP_SERVERS", value: "broker.example.com:9092" }]),
        metadata: { namespace: "apps", name: "other" },
      },
    ]);
    reader.listWorkloads = async (namespace) => {
      listNamespace = namespace;
      return [
        {
          ...deployment([{ name: "KAFKA_BOOTSTRAP_SERVERS", value: "broker.example.com:9092" }]),
          metadata: { namespace: "apps", name: "orders" },
        },
        {
          ...deployment([{ name: "KAFKA_BOOTSTRAP_SERVERS", value: "other.example.com:9092" }]),
          metadata: { namespace: "apps", name: "other" },
        },
      ];
    };
    const originalWorkloads = reader.listWorkloads;
    reader.listWorkloads = async (namespace) => {
      const workloads = await originalWorkloads(namespace);
      visited = workloads.length;
      return workloads;
    };

    await resolveExternalCredentials(reader, {
      bootstrap: "broker.example.com:9092",
      sourceLocator: { namespace: "apps", kind: "Deployment", name: "orders", container: "app" },
    });

    expect(listNamespace).toBe("apps");
    expect(visited).toBe(2);
  });
});

describe("KafkaCredentialProfileCache", () => {
  it("uses an opaque key isolated by context, target, user and security generation", () => {
    const base = {
      bootstrap: "broker.example.com:9092",
      contextId: "context-a",
      targetId: "target-a",
      user: "alice",
      security: { tlsMode: "enabled" as const, authMode: "plain" as const, username: "alice", password: "secret" },
    };
    const key = credentialProfileKey(base);

    expect(key).toMatch(/^kafka-profile:[a-f0-9]{64}$/);
    expect(key).not.toMatch(/broker|alice|secret|context|target/);
    expect(credentialProfileKey(base)).toBe(key);
    expect(credentialProfileKey({ ...base, contextId: "context-b" })).not.toBe(key);
    expect(credentialProfileKey({ ...base, targetId: "target-b" })).not.toBe(key);
    expect(credentialProfileKey({ ...base, user: "bob" })).not.toBe(key);
    expect(credentialProfileKey({ ...base, security: { ...base.security, password: "rotated" } })).not.toBe(key);
  });

  it("coalesces concurrent profile resolution and retries after failure", async () => {
    const cache = new KafkaCredentialProfileCache();
    let calls = 0;
    const profile = { detected: false, hint: { tls: false, auth: "none" as const } };
    const loader = async () => {
      calls += 1;
      return profile;
    };

    await expect(
      Promise.all([cache.getOrResolve("target", loader), cache.getOrResolve("target", loader)]),
    ).resolves.toEqual([profile, profile]);
    await cache.getOrResolve("target", loader);
    expect(calls).toBe(1);
    cache.clear();
    await cache.getOrResolve("target", loader);
    expect(calls).toBe(2);
  });
});

describe("applySecurityOverride", () => {
  it("supports TLS-without-auth and explicit SCRAM, while never requiring auth for plaintext", () => {
    expect(applySecurityOverride({ fallbackTls: true, source: "inferred" })).toMatchObject({
      ssl: true,
      sasl: undefined,
      summary: { tls: true, auth: "none", source: "inferred" },
    });

    expect(
      applySecurityOverride({
        fallbackTls: false,
        source: "workload",
        override: {
          tlsMode: "enabled",
          authMode: "scram-sha-256",
          username: "bob",
          password: "pw",
        },
      }),
    ).toMatchObject({
      ssl: true,
      sasl: { mechanism: "scram-sha-256", username: "bob", password: "pw" },
      summary: { tls: true, auth: "scram-sha-256", source: "override" },
    });

    expect(applySecurityOverride({ fallbackTls: false, source: "inferred" })).toMatchObject({
      ssl: undefined,
      sasl: undefined,
      summary: { tls: false, auth: "none" },
    });
  });

  it("rejects an incomplete explicit SASL override", () => {
    expect(() =>
      applySecurityOverride({
        fallbackTls: false,
        source: "inferred",
        override: { tlsMode: "auto", authMode: "plain", username: "alice" },
      }),
    ).toThrow("username and password are required");
  });

  it("removes automatic mTLS client material for a no-auth override but preserves the CA", () => {
    const applied = applySecurityOverride({
      automatic: { ssl: { ca: ["CA"], cert: "CLIENT", key: "KEY" } },
      automaticHint: { tls: true, auth: "mtls" },
      fallbackTls: true,
      source: "workload",
      override: { tlsMode: "auto", authMode: "none" },
    });
    expect(applied.ssl).toEqual({ ca: ["CA"] });
    expect(applied.summary).toEqual({ tls: true, auth: "none", source: "override" });
  });
});
