import { describe, expect, it } from "vitest";
import { KafkaTargetSessionRegistry } from "./target-session-registry";

describe("KafkaTargetSessionRegistry", () => {
  it("returns owned sessions on invalidation and rejects late registration from an old generation", () => {
    const registry = new KafkaTargetSessionRegistry();
    const generation = registry.begin("context-a:target-a");
    expect(registry.register("context-a:target-a", generation, "session-a")).toBe(true);

    expect(registry.invalidate("context-a:target-a")).toEqual(["session-a"]);
    expect(registry.isCurrent("context-a:target-a", generation)).toBe(false);
    expect(registry.register("context-a:target-a", generation, "late-session")).toBe(false);
    expect(registry.invalidate("context-a:target-a")).toEqual([]);
  });

  it("isolates identical targets across contexts and returns unique sessions on clear", () => {
    const registry = new KafkaTargetSessionRegistry();
    const first = "context-a:target";
    const second = "context-b:target";
    registry.register(first, registry.begin(first), "shared-session");
    registry.register(second, registry.begin(second), "shared-session");
    registry.register(second, registry.begin(second), "second-session");

    expect(new Set(registry.clear())).toEqual(new Set(["shared-session", "second-session"]));
  });
});
