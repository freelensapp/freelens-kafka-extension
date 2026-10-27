import { describe, expect, it } from "vitest";
import { KAFKA_WRITE_MODE_KEY } from "../common/write-mode-key";
import { WriteModeGuard } from "./write-mode";

function store(values: Record<string, string> = {}) {
  return { values, getItem: (key: string) => values[key] ?? null };
}

describe("WriteModeGuard", () => {
  it("refuses writes for targets whose persisted write mode is off, and for none at all", () => {
    const guard = new WriteModeGuard(() => store());

    expect(() => guard.assertEnabled("kafka-1", "Produce message")).toThrow(/Produce message refused/);
    expect(() => guard.assertEnabled(undefined, "Delete topic")).toThrow(/write mode is not enabled/);
    expect(guard.isEnabled("")).toBe(false);
  });

  it("allows writes for the targets the store has on, read at every call", () => {
    const state = store({ [KAFKA_WRITE_MODE_KEY]: JSON.stringify(["kafka-1"]) });
    const guard = new WriteModeGuard(() => state);

    expect(() => guard.assertEnabled("kafka-1", "Produce message")).not.toThrow();
    expect(guard.isEnabled("kafka-2")).toBe(false);
    state.values[KAFKA_WRITE_MODE_KEY] = JSON.stringify([]);
    expect(guard.isEnabled("kafka-1")).toBe(false);
  });

  it("refuses everything for a stored value that is not a list of ids, or a store that fails", () => {
    for (const value of ["{", '{"kafka-1":true}', '"kafka-1"', "[1, null]"]) {
      expect(new WriteModeGuard(() => store({ [KAFKA_WRITE_MODE_KEY]: value })).isEnabled("kafka-1")).toBe(false);
    }
    const failing = new WriteModeGuard(() => {
      throw new Error("store not loaded");
    });
    expect(failing.isEnabled("kafka-1")).toBe(false);
  });
});
