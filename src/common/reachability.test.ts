import { describe, expect, it } from "vitest";
import { chooseStrategy, firstBrokerAddress, splitBootstrap } from "./reachability";

describe("splitBootstrap", () => {
  it("normalizes to host:port, defaulting a missing port and stripping a scheme", () => {
    expect(splitBootstrap("msk-1.example.com,msk-2.example.com:9094")).toEqual([
      "msk-1.example.com:9092",
      "msk-2.example.com:9094",
    ]);
    expect(splitBootstrap("PLAINTEXT://kafka.svc:9092")).toEqual(["kafka.svc:9092"]);
    expect(splitBootstrap("")).toEqual([]);
  });
});

describe("firstBrokerAddress", () => {
  it("parses host/port, strips a scheme, and takes the first of a list", () => {
    expect(
      firstBrokerAddress("b-1.demo.kafka.eu-west-1.amazonaws.com:9094,b-2.demo.kafka.eu-west-1.amazonaws.com:9094"),
    ).toEqual({ host: "b-1.demo.kafka.eu-west-1.amazonaws.com", port: 9094 });
    expect(firstBrokerAddress("PLAINTEXT://kafka.svc:9092")).toEqual({ host: "kafka.svc", port: 9092 });
    expect(firstBrokerAddress("kafka")).toEqual({ host: "kafka", port: 9092 });
  });
});

describe("chooseStrategy", () => {
  it("selects the connection strategy from source + PC reachability", () => {
    expect(chooseStrategy("strimzi", false)).toBe("portForward");
    expect(chooseStrategy("strimzi", true)).toBe("portForward");
    expect(chooseStrategy("workload", true)).toBe("direct");
    expect(chooseStrategy("service", true)).toBe("direct");
    expect(chooseStrategy("workload", false)).toBe("relay");
  });
});
