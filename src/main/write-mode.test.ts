import { describe, expect, it } from "vitest";
import { WriteModeRegistry } from "./write-mode";

describe("WriteModeRegistry", () => {
  it("refuses writes for unknown, disabled and missing targets", () => {
    const registry = new WriteModeRegistry();
    expect(() => registry.assertEnabled("kafka-1", "Produce message")).toThrow(/Produce message refused/);
    expect(() => registry.assertEnabled(undefined, "Delete topic")).toThrow(/write mode is not enabled/);
    registry.set("kafka-1", true);
    registry.set("kafka-1", false);
    expect(registry.isEnabled("kafka-1")).toBe(false);
    expect(() => registry.assertEnabled("kafka-1", "Reset offsets")).toThrow();
  });

  it("allows writes only for the targets enabled in this session", () => {
    const registry = new WriteModeRegistry();
    registry.set("kafka-1", true);
    expect(registry.isEnabled("kafka-1")).toBe(true);
    expect(() => registry.assertEnabled("kafka-1", "Produce message")).not.toThrow();
    expect(registry.isEnabled("kafka-2")).toBe(false);
    registry.set("", true);
    expect(registry.isEnabled("")).toBe(false);
    registry.clear();
    expect(registry.isEnabled("kafka-1")).toBe(false);
  });
});
