import { describe, expect, it } from "vitest";
import { AKHQ_CLUSTER_ID, assertAkhqUsefulPage, createAkhqReadOnlyConfiguration } from "./akhq-read-only-config";

describe("createAkhqReadOnlyConfiguration", () => {
  it("creates a loopback-only, audit-disabled SCRAM/TLS configuration", () => {
    const configuration = createAkhqReadOnlyConfiguration({
      bootstrap: "broker.example:9096",
      jwtSecret: "x".repeat(64),
      ssl: true,
      sasl: { mechanism: "scram-sha-512", username: "reader", password: "secret" },
    }) as {
      micronaut: {
        security: {
          enabled: boolean;
          token: { jwt: { signatures: { secret: { generator: { secret: string } } } } };
        };
        server: { host: string; port: number };
      };
      akhq: {
        audit: { enabled: boolean };
        security: {
          "default-group": string;
          groups: Record<string, Array<{ clusters: string[]; patterns: string[]; role: string }>>;
          roles: Record<string, Array<{ actions: string[]; resources: string[] }>>;
        };
        connections: Record<string, { properties: Record<string, string> }>;
      };
    };

    expect(configuration.micronaut.server).toEqual({ host: "127.0.0.1", port: 18_080 });
    expect(configuration.micronaut.security.enabled).toBe(true);
    expect(configuration.micronaut.security.token.jwt.signatures.secret.generator.secret).toBe("x".repeat(64));
    expect(configuration.akhq.audit.enabled).toBe(false);
    expect(configuration.akhq.security).toEqual({
      "default-group": "slice16-read-only-group",
      groups: {
        "slice16-read-only-group": [
          { clusters: ["^slice16-read-only$"], patterns: [".*"], role: "slice16-read-only-role" },
        ],
      },
      roles: {
        "slice16-read-only-role": [
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
    });
    expect(configuration.akhq.connections[AKHQ_CLUSTER_ID].properties).toMatchObject({
      "bootstrap.servers": "broker.example:9096",
      "security.protocol": "SASL_SSL",
      "sasl.mechanism": "SCRAM-SHA-512",
      "sasl.jaas.config": expect.stringContaining('username="reader" password="secret"'),
    });
  });

  it("maps PEM trust/client material without writing a truststore", () => {
    const configuration = createAkhqReadOnlyConfiguration({
      bootstrap: "broker.example:9093",
      ssl: { ca: ["CA-1", "CA-2"], cert: "CERT", key: "KEY" },
    }) as { akhq: { connections: Record<string, { properties: Record<string, string> }> } };
    expect(configuration.akhq.connections[AKHQ_CLUSTER_ID].properties).toMatchObject({
      "security.protocol": "SSL",
      "ssl.truststore.type": "PEM",
      "ssl.truststore.certificates": "CA-1\nCA-2",
      "ssl.keystore.type": "PEM",
      "ssl.keystore.certificate.chain": "CERT",
      "ssl.keystore.key": "KEY",
    });
  });

  it("rejects non-exportable TLS key objects", () => {
    expect(() =>
      createAkhqReadOnlyConfiguration({ bootstrap: "broker.example:9093", ssl: { cert: "CERT", key: {} as never } }),
    ).toThrow("cannot safely convert client key");
  });

  it("rejects weak JWT secrets", () => {
    expect(() => createAkhqReadOnlyConfiguration({ bootstrap: "broker.example:9093", jwtSecret: "short" })).toThrow(
      "JWT secret is too short",
    );
  });

  it("accepts only non-empty page-25 payloads as useful GET results", () => {
    const topics = {
      before: "",
      after: "/api/cluster/topic?page=2",
      page: 44,
      total: 1_096,
      pageSize: 25,
      results: [{ name: "topic", partitions: [] }],
    };
    const groups = {
      before: "",
      after: "/api/cluster/group?page=2",
      page: 46,
      total: 1_128,
      pageSize: 25,
      results: [{ id: "group", state: "STABLE" }],
    };
    expect(() => assertAkhqUsefulPage(topics, "topics", 1)).not.toThrow();
    expect(() => assertAkhqUsefulPage({ ...topics, before: null }, "topics", 1)).not.toThrow();
    expect(() => assertAkhqUsefulPage({ ...topics, before: "/" }, "topics", 1)).not.toThrow();
    expect(() => assertAkhqUsefulPage(groups, "consumer-groups", 1)).not.toThrow();
    expect(() => assertAkhqUsefulPage({ ...topics, results: [{}] }, "topics", 1)).toThrow("records-topic-name");
    expect(() => assertAkhqUsefulPage({ ...topics, before: "?page=1" }, "topics", 1)).toThrow("links-before");
    expect(() => assertAkhqUsefulPage({ ...topics, pageSize: 10 }, "topics", 1)).toThrow("page-size");
  });
});
