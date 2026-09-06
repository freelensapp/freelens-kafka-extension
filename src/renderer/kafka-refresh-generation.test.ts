import { describe, expect, it } from "vitest";
import { nextKafkaRefreshGeneration } from "./kafka-refresh-generation";

describe("nextKafkaRefreshGeneration", () => {
  it("keeps metadata stable for health-only auto-refresh", () => {
    expect(nextKafkaRefreshGeneration({ health: 3, metadata: 2 }, "health")).toEqual({ health: 4, metadata: 2 });
  });

  it("refreshes health and metadata for an explicit full refresh", () => {
    expect(nextKafkaRefreshGeneration({ health: 3, metadata: 2 }, "full")).toEqual({ health: 4, metadata: 3 });
  });
});
