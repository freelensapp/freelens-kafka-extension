import { describe, expect, it } from "vitest";
import { canonicalKafkaBootstrap, createKafkaTargetId } from "./kafka-target";

describe("Kafka target identity", () => {
  it("canonicalizes equivalent bootstrap lists", () => {
    expect(canonicalKafkaBootstrap("SSL://BROKER-B:9093, broker-a, broker-a:9092")).toBe("broker-a:9092,broker-b:9093");
    expect(createKafkaTargetId("SSL://BROKER-B:9093, broker-a")).toBe(
      createKafkaTargetId("broker-a:9092,broker-b:9093"),
    );
  });

  it("creates stable route-safe IDs without exposing the bootstrap", () => {
    const targetId = createKafkaTargetId("b-1.orders.kafka.example.com:9094");
    expect(targetId).toMatch(/^kafka-[a-f0-9]{16}$/);
    expect(targetId).not.toContain("orders");
    expect(targetId).toBe(createKafkaTargetId("b-1.orders.kafka.example.com:9094"));
    expect(targetId).not.toBe(createKafkaTargetId("b-2.orders.kafka.example.com:9094"));
  });

  it("rejects an empty bootstrap", () => {
    expect(() => createKafkaTargetId(" , ")).toThrow("Kafka bootstrap is required");
  });
});
