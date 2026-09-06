import { describe, expect, it } from "vitest";
import { formatKafkaEta, type KafkaEtaEstimatorState, kafkaPhasePercent, updateKafkaEta } from "./kafka-phase-progress";

describe("kafkaPhasePercent", () => {
  it("uses only completed and total for phase-local percentage", () => {
    expect(kafkaPhasePercent({ completed: 2, total: 10 })).toBe(20);
    expect(kafkaPhasePercent({ completed: 20, total: 10 })).toBe(100);
    expect(kafkaPhasePercent({ completed: 0, total: 0 })).toBe(100);
    expect(kafkaPhasePercent({ complete: true })).toBe(100);
    expect(kafkaPhasePercent({ completed: 2 })).toBeUndefined();
  });
});

describe("updateKafkaEta", () => {
  it("estimates a stable phase after 20 percent with deterministic smoothing", () => {
    let state: KafkaEtaEstimatorState = { positiveSamples: 0 };
    ({ state } = updateKafkaEta(state, { at: 0, completed: 0, total: 100, phase: "groups" }));
    let estimate = updateKafkaEta(state, { at: 1_000, completed: 10, total: 100, phase: "groups" });
    expect(estimate.status).toBe("warming");
    state = estimate.state;
    estimate = updateKafkaEta(state, { at: 2_000, completed: 20, total: 100, phase: "groups" });

    expect(estimate).toMatchObject({ status: "ready", etaMs: 8_000 });
  });

  it("suppresses ETA before confidence and after a stall", () => {
    let state: KafkaEtaEstimatorState = { positiveSamples: 0 };
    ({ state } = updateKafkaEta(state, { at: 0, completed: 0, total: 100, phase: "groups" }));
    ({ state } = updateKafkaEta(state, { at: 1_000, completed: 10, total: 100, phase: "groups" }));
    const stalled = updateKafkaEta(state, { at: 11_000, completed: 10, total: 100, phase: "groups" });

    expect(stalled.status).toBe("stalled");
    expect(stalled.etaMs).toBeUndefined();
  });

  it("resets confidence on phase or total changes", () => {
    const first = updateKafkaEta(
      {
        phase: "groups",
        total: 100,
        firstAt: 0,
        lastAt: 2_000,
        lastCompleted: 20,
        lastProgressAt: 2_000,
        positiveSamples: 2,
        smoothedRatePerMs: 0.01,
      },
      { at: 2_100, completed: 1, total: 10, phase: "watermarks" },
    );

    expect(first.status).toBe("warming");
    expect(first.state.positiveSamples).toBe(0);
  });
});

describe("formatKafkaEta", () => {
  it("formats bounded seconds and minutes", () => {
    expect(formatKafkaEta(500)).toBe("<1s remaining");
    expect(formatKafkaEta(8_000)).toBe("8s remaining");
    expect(formatKafkaEta(61_000)).toBe("2m remaining");
  });
});
