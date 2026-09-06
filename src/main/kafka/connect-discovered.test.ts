import { describe, expect, it } from "vitest";
import { matchBrokersToPods } from "./connect-discovered";

const pods = [
  { brokerId: 0, pod: "my-cluster-kafka-0" },
  { brokerId: 1, pod: "my-cluster-kafka-1" },
];

describe("matchBrokersToPods", () => {
  it("matches by advertised-host prefix (Strimzi per-broker DNS)", () => {
    const refs = matchBrokersToPods(
      [
        {
          nodeId: 0,
          host: "my-cluster-kafka-0.my-cluster-kafka-brokers.kafka.svc",
          port: 9092,
        },
        {
          nodeId: 1,
          host: "my-cluster-kafka-1.my-cluster-kafka-brokers.kafka.svc",
          port: 9092,
        },
      ],
      pods,
      "kafka",
    );
    expect(refs).toEqual([
      {
        advertisedHost: "my-cluster-kafka-0.my-cluster-kafka-brokers.kafka.svc",
        advertisedPort: 9092,
        namespace: "kafka",
        pod: "my-cluster-kafka-0",
        containerPort: 9092,
      },
      {
        advertisedHost: "my-cluster-kafka-1.my-cluster-kafka-brokers.kafka.svc",
        advertisedPort: 9092,
        namespace: "kafka",
        pod: "my-cluster-kafka-1",
        containerPort: 9092,
      },
    ]);
  });

  it("falls back to nodeId == brokerId when the host has no pod prefix", () => {
    const refs = matchBrokersToPods([{ nodeId: 1, host: "kafka-internal", port: 9092 }], pods, "kafka");
    expect(refs[0].pod).toBe("my-cluster-kafka-1");
  });

  it("throws when no pod matches", () => {
    expect(() => matchBrokersToPods([{ nodeId: 9, host: "nope", port: 9092 }], pods, "kafka")).toThrow(/no pod found/);
  });
});
