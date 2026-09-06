import { describe, expect, it } from "vitest";
import { createPerformanceScaleReader } from "../../../test/fixtures/performance-scale";
import {
  classifyProvider,
  discoverAllKafkas,
  discoverWorkloadKafkas,
  extractBrokerId,
  isBootstrapKey,
  parseKafkaServices,
  parseStrimziKafka,
} from "./discovery";

import type { KubeObject, KubeReader, KubeSecret } from "./kube-reader";

const kafkaCr: KubeObject = {
  apiVersion: "kafka.strimzi.io/v1beta2",
  kind: "Kafka",
  metadata: { name: "my-cluster", namespace: "kafka" },
  spec: {
    kafka: {
      listeners: [
        { name: "plain", port: 9092, type: "internal", tls: false },
        { name: "tls", port: 9093, type: "internal", tls: true },
      ],
    },
  },
  status: {
    listeners: [
      {
        name: "plain",
        bootstrapServers: "my-cluster-kafka-bootstrap.kafka.svc:9092",
      },
      {
        name: "tls",
        bootstrapServers: "my-cluster-kafka-bootstrap.kafka.svc:9093",
      },
    ],
  },
};

const brokerPods: KubeObject[] = [
  {
    metadata: {
      name: "my-cluster-kafka-0",
      namespace: "kafka",
      labels: {
        "strimzi.io/cluster": "my-cluster",
        "strimzi.io/broker-role": "true",
      },
    },
  },
  {
    metadata: {
      name: "my-cluster-kafka-2",
      namespace: "kafka",
      labels: {
        "strimzi.io/cluster": "my-cluster",
        "strimzi.io/broker-role": "true",
      },
    },
  },
  {
    metadata: {
      name: "my-cluster-kafka-1",
      namespace: "kafka",
      labels: {
        "strimzi.io/cluster": "my-cluster",
        "strimzi.io/broker-role": "true",
      },
    },
  },
  // controller-only pod: must be excluded
  {
    metadata: {
      name: "my-cluster-controller-0",
      namespace: "kafka",
      labels: {
        "strimzi.io/cluster": "my-cluster",
        "strimzi.io/broker-role": "false",
      },
    },
  },
];

describe("extractBrokerId", () => {
  it("reads the trailing numeric suffix", () => {
    expect(extractBrokerId("my-cluster-kafka-3")).toBe(3);
    expect(extractBrokerId("my-cluster-broker-0")).toBe(0);
    expect(extractBrokerId("no-number")).toBeNull();
  });
});

describe("parseStrimziKafka", () => {
  it("prefers the internal plaintext listener and sorts broker pods by id (excluding controllers)", () => {
    const discovered = parseStrimziKafka(kafkaCr, brokerPods);
    expect(discovered).toMatchObject({
      source: "strimzi",
      name: "my-cluster",
      namespace: "kafka",
      bootstrap: "my-cluster-kafka-bootstrap.kafka.svc:9092",
      tls: false,
      port: 9092,
    });
    expect(discovered?.brokerPods).toEqual([
      { brokerId: 0, pod: "my-cluster-kafka-0" },
      { brokerId: 1, pod: "my-cluster-kafka-1" },
      { brokerId: 2, pod: "my-cluster-kafka-2" },
    ]);
    expect(discovered?.listeners).toHaveLength(2);
  });

  it("returns null when no listener has a bootstrap in status", () => {
    const cr: KubeObject = {
      metadata: { name: "x", namespace: "y" },
      spec: {
        kafka: { listeners: [{ name: "plain", port: 9092, tls: false }] },
      },
      status: {},
    };
    expect(parseStrimziKafka(cr, [])).toBeNull();
  });
});

describe("parseKafkaServices", () => {
  it("matches kafka-named ports (and kafka services on 9092/9093), ignoring Alertmanager :9093", () => {
    const services: KubeObject[] = [
      {
        metadata: { name: "my-cluster-kafka-bootstrap", namespace: "kafka" },
        spec: {
          ports: [
            { name: "tcp-clients", port: 9092 },
            { name: "tcp-clientstls", port: 9093 },
          ],
        },
      },
      {
        metadata: { name: "alertmanager-operated", namespace: "kafka" },
        spec: { ports: [{ name: "http-web", port: 9093 }] },
      },
      {
        metadata: { name: "web", namespace: "kafka" },
        spec: { ports: [{ name: "http", port: 80 }] },
      },
    ];
    const candidates = parseKafkaServices(services);
    expect(candidates.map((c) => `${c.name}:${c.port}`)).toEqual([
      "my-cluster-kafka-bootstrap:9092",
      "my-cluster-kafka-bootstrap:9093",
    ]);
    expect(candidates[0]).toMatchObject({
      source: "service",
      bootstrap: "my-cluster-kafka-bootstrap.kafka.svc:9092",
      tls: false,
    });
    expect(candidates[1].tls).toBe(true);
  });
});

const b64 = (s: string): string => Buffer.from(s).toString("base64");

interface FakeData {
  crs?: KubeObject[];
  pods?: KubeObject[];
  services?: KubeObject[];
  workloads?: KubeObject[];
  configMaps?: Record<string, Record<string, string>>;
  secrets?: Record<string, KubeSecret>;
}

function fakeReader(data: FakeData): KubeReader {
  return {
    listCustomResources: async () => data.crs ?? [],
    listPods: async () => data.pods ?? [],
    listServices: async () => data.services ?? [],
    listWorkloads: async () => data.workloads ?? [],
    getConfigMap: async (ns, name) => {
      const d = data.configMaps?.[`${ns}/${name}`];
      return d ? { metadata: { namespace: ns, name }, data: d } : null;
    },
    getSecret: async (ns, name) => data.secrets?.[`${ns}/${name}`] ?? null,
  };
}

function deployment(name: string, namespace: string, env: unknown[], envFrom?: unknown[]): KubeObject {
  return {
    kind: "Deployment",
    metadata: { name, namespace },
    spec: { template: { spec: { containers: [{ name: "app", env, envFrom }] } } },
  };
}

describe("isBootstrapKey", () => {
  it("matches common bootstrap env/config keys and nothing else", () => {
    for (const k of [
      "KAFKA_BOOTSTRAP_SERVERS",
      "BOOTSTRAP_SERVERS",
      "bootstrap.servers",
      "spring.kafka.bootstrap-servers",
      "SPRING_KAFKA_BOOTSTRAP_SERVERS",
      "KAFKA_BROKERS",
      "kafka.broker",
    ]) {
      expect(isBootstrapKey(k)).toBe(true);
    }
    for (const k of ["KAFKA_TOPIC", "DATABASE_URL", "SERVERS_COUNT", "BOOTSTRAP"]) {
      expect(isBootstrapKey(k)).toBe(false);
    }
  });
});

describe("classifyProvider", () => {
  it("labels managed providers external and in-cluster DNS internal", () => {
    expect(classifyProvider("b-1.demo.abc.kafka.us-east-1.amazonaws.com:9094")).toEqual({
      provider: "MSK",
      external: true,
    });
    expect(classifyProvider("pkc-abc.eu.aws.confluent.cloud:9092")).toEqual({ provider: "Confluent", external: true });
    expect(classifyProvider("kafka-1.aivencloud.com:12345")).toEqual({ provider: "Aiven", external: true });
    expect(classifyProvider("my-cluster-kafka-bootstrap.kafka.svc:9092")).toEqual({
      provider: "In-cluster",
      external: false,
    });
    expect(classifyProvider("kafka:9092")).toEqual({ provider: "In-cluster", external: false });
    expect(classifyProvider("kafka.example.com:9092")).toEqual({ provider: "External", external: true });
  });
});

describe("discoverWorkloadKafkas", () => {
  it("finds external MSK from a literal env and aggregates the referencing workloads", async () => {
    const value = "b-1.demo.abc.kafka.us-east-1.amazonaws.com:9094,b-2.demo.abc.kafka.us-east-1.amazonaws.com:9094";
    const refs = await discoverWorkloadKafkas(
      fakeReader({
        workloads: [
          deployment("orders", "apps", [{ name: "KAFKA_BOOTSTRAP_SERVERS", value }]),
          deployment("payments", "apps", [{ name: "KAFKA_BOOTSTRAP_SERVERS", value }]),
        ],
      }),
    );
    expect(refs).toHaveLength(1);
    expect(refs[0]).toMatchObject({
      source: "workload",
      provider: "MSK",
      external: true,
      port: 9094,
      tls: true,
      sourceLocator: { namespace: "apps", kind: "Deployment", name: "orders", container: "app" },
    });
    expect([...refs[0].referencedBy].sort()).toEqual(["apps/deploy/orders", "apps/deploy/payments"]);
  });

  it("resolves bootstrap from configMapKeyRef and whole-ConfigMap envFrom", async () => {
    const refs = await discoverWorkloadKafkas(
      fakeReader({
        workloads: [
          {
            kind: "StatefulSet",
            metadata: { name: "consumer", namespace: "apps" },
            spec: {
              template: {
                spec: {
                  containers: [
                    {
                      name: "c",
                      env: [
                        { name: "BOOTSTRAP_SERVERS", valueFrom: { configMapKeyRef: { name: "kc", key: "brokers" } } },
                      ],
                      envFrom: [{ configMapRef: { name: "extra" } }],
                    },
                  ],
                },
              },
            },
          },
        ],
        configMaps: {
          "apps/kc": { brokers: "pkc-xxxx.eu-west-1.aws.confluent.cloud:9092" },
          "apps/extra": { SPRING_KAFKA_BOOTSTRAP_SERVERS: "redpanda.data.svc:9092", UNRELATED: "x" },
        },
      }),
    );
    expect(refs.map((r) => r.provider).sort()).toEqual(["Confluent", "In-cluster"]);
    expect(refs.find((r) => r.provider === "Confluent")).toMatchObject({ external: true, tls: true });
  });

  it("resolves bootstrap from a base64-decoded secretKeyRef", async () => {
    const refs = await discoverWorkloadKafkas(
      fakeReader({
        workloads: [
          deployment("d", "apps", [
            { name: "KAFKA_BROKERS", valueFrom: { secretKeyRef: { name: "ks", key: "servers" } } },
          ]),
        ],
        secrets: { "apps/ks": { data: { servers: b64("kafka.svc.cluster.local:9092") } } },
      }),
    );
    expect(refs).toHaveLength(1);
    expect(refs[0]).toMatchObject({ provider: "In-cluster", external: false });
  });

  it("attaches a non-secret SASL/TLS hint while credentials stay in Main", async () => {
    const refs = await discoverWorkloadKafkas(
      fakeReader({
        workloads: [
          deployment("secure", "apps", [
            { name: "KAFKA_BOOTSTRAP_SERVERS", value: "secure.example.com:9094" },
            { name: "KAFKA_SECURITY_PROTOCOL", value: "SASL_SSL" },
            { name: "KAFKA_SASL_MECHANISM", value: "SCRAM-SHA-512" },
            { name: "KAFKA_SASL_USERNAME", value: "alice" },
            { name: "KAFKA_SASL_PASSWORD", value: "secret" },
          ]),
        ],
      }),
    );
    expect(refs[0]).toMatchObject({
      tls: true,
      securityHint: { tls: true, auth: "scram-sha-512" },
    });
    expect(refs[0]).not.toHaveProperty("sasl");
  });

  it("reports real completed/total workload progress", async () => {
    const progress: { completed: number; total: number }[] = [];
    await discoverWorkloadKafkas(
      fakeReader({
        workloads: [
          deployment("one", "apps", [{ name: "KAFKA_BOOTSTRAP_SERVERS", value: "one:9092" }]),
          deployment("two", "apps", [{ name: "KAFKA_BOOTSTRAP_SERVERS", value: "two:9092" }]),
        ],
      }),
      undefined,
      (event) => progress.push(event),
    );
    expect(progress[0]).toEqual({ completed: 0, total: 2 });
    expect(progress.at(-1)).toEqual({ completed: 2, total: 2 });
  });
});

describe("discoverAllKafkas", () => {
  it("merges sources and drops in-cluster duplicates", async () => {
    const all = await discoverAllKafkas(
      fakeReader({
        crs: [kafkaCr],
        pods: brokerPods,
        services: [
          // Strimzi's own services — dropped by owner-name prefix
          {
            metadata: { name: "my-cluster-kafka-bootstrap", namespace: "kafka" },
            spec: { ports: [{ name: "tcp-clients", port: 9092 }] },
          },
          {
            metadata: { name: "my-cluster-kafka-brokers", namespace: "kafka" },
            spec: { ports: [{ name: "tcp-clients", port: 9092 }] },
          },
          // an unrelated in-cluster kafka service — kept
          { metadata: { name: "redpanda", namespace: "data" }, spec: { ports: [{ name: "kafka", port: 9092 }] } },
        ],
        workloads: [
          // references the strimzi bootstrap — dropped as duplicate
          deployment("app1", "apps", [
            { name: "KAFKA_BOOTSTRAP_SERVERS", value: "my-cluster-kafka-bootstrap.kafka.svc:9092" },
          ]),
          // external MSK — kept
          deployment("app2", "apps", [
            { name: "KAFKA_BOOTSTRAP_SERVERS", value: "b-1.x.kafka.eu-west-1.amazonaws.com:9094" },
          ]),
        ],
      }),
    );
    expect(all.find((k) => k.source === "strimzi")?.name).toBe("my-cluster");
    expect(all.some((k) => k.source === "service" && k.name === "redpanda")).toBe(true);
    expect(all.some((k) => k.source === "service" && k.name.startsWith("my-cluster"))).toBe(false);
    expect(all.some((k) => k.source === "workload" && k.bootstrap.includes("amazonaws.com"))).toBe(true);
    expect(all.some((k) => k.source === "workload" && k.bootstrap.includes("my-cluster-kafka-bootstrap"))).toBe(false);
  });

  it("emits workload and completion phases", async () => {
    const phases: string[] = [];
    await discoverAllKafkas(
      fakeReader({
        workloads: [deployment("app", "apps", [{ name: "KAFKA_BROKERS", value: "kafka:9092" }])],
      }),
      undefined,
      (progress) => phases.push(progress.phase),
    );
    expect(phases).toContain("workloads");
    expect(phases.at(-1)).toBe("complete");
  });
});

describe("production-scale discovery fixture", () => {
  it("collapses 1,000 workload references with one list and no unnecessary object reads", async () => {
    const { reader, calls } = createPerformanceScaleReader(1_000);
    const progress: Array<{ completed: number; total: number }> = [];

    const refs = await discoverWorkloadKafkas(reader, undefined, (event) => progress.push(event));

    expect(refs).toHaveLength(1);
    expect(refs[0].referencedBy).toHaveLength(1_000);
    expect(calls).toMatchObject({ listWorkloads: 1, getConfigMap: 0, getSecret: 0 });
    expect(progress[0]).toEqual({ completed: 0, total: 1_000 });
    expect(progress.at(-1)).toEqual({ completed: 1_000, total: 1_000 });
  });
});
