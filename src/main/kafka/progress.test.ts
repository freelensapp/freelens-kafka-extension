import { describe, expect, it, vi } from "vitest";
import { createProgressReporter } from "./progress";

describe("createProgressReporter", () => {
  it("correlates, clamps and never decreases progress", () => {
    const emit = vi.fn();
    const report = createProgressReporter({ operationId: "op-1", operation: "discovery", emit });
    report({ value: 10, phase: "one", label: "One" });
    report({ value: 5, phase: "two", label: "Two" });
    report({ value: 150, phase: "done", label: "Done" });

    expect(emit.mock.calls.map(([event]) => event.value)).toEqual([10, 10, 100]);
    expect(emit.mock.calls[0][0]).toMatchObject({ operationId: "op-1", operation: "discovery" });
  });

  it("does not emit when the caller omitted an operation id", () => {
    const emit = vi.fn();
    createProgressReporter({ operation: "overview", emit })({ value: 50, phase: "x", label: "X" });
    expect(emit).not.toHaveBeenCalled();
  });

  it("records real cumulative and per-stage durations", () => {
    const emit = vi.fn();
    const timestamps = [1_000, 1_025, 1_090];
    const report = createProgressReporter({
      operationId: "timed-op",
      operation: "overview",
      emit,
      now: () => timestamps.shift() ?? 1_090,
    });

    report({ value: 10, phase: "connect", label: "Connecting" });
    report({ value: 70, phase: "metadata", label: "Connected" });

    expect(emit.mock.calls.map(([event]) => [event.elapsedMs, event.stageDurationMs])).toEqual([
      [25, 25],
      [90, 65],
    ]);
  });

  it("emits phase-local percentage and a confidence-gated ETA", () => {
    const emit = vi.fn();
    const timestamps = [0, 0, 1_000, 2_000, 12_000];
    const report = createProgressReporter({
      operationId: "eta-op",
      operation: "health",
      emit,
      now: () => timestamps.shift() ?? 12_000,
    });

    report({ value: 75, phase: "groups", label: "Groups", completed: 0, total: 100 });
    report({ value: 80, phase: "groups", label: "Groups", completed: 10, total: 100 });
    report({ value: 84, phase: "groups", label: "Groups", completed: 20, total: 100 });
    report({ value: 84, phase: "groups", label: "Groups", completed: 20, total: 100 });

    expect(emit.mock.calls.map(([event]) => event.phasePercent)).toEqual([0, 10, 20, 20]);
    expect(emit.mock.calls.map(([event]) => event.etaStatus)).toEqual(["warming", "warming", "ready", "stalled"]);
    expect(emit.mock.calls[2][0]).toMatchObject({ etaMs: 8_000, phaseElapsedMs: 2_000 });
    expect(emit.mock.calls[3][0].etaMs).toBeUndefined();
  });

  it("withdraws a ready ETA after a silent stall", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    try {
      const emit = vi.fn();
      const report = createProgressReporter({
        operationId: "stalled-op",
        operation: "health",
        emit,
      });

      report({ value: 75, phase: "groups", label: "Groups", completed: 0, total: 100 });
      vi.advanceTimersByTime(1_000);
      report({ value: 80, phase: "groups", label: "Groups", completed: 10, total: 100 });
      vi.advanceTimersByTime(1_000);
      report({ value: 84, phase: "groups", label: "Groups", completed: 20, total: 100 });
      expect(emit.mock.lastCall?.[0]).toMatchObject({ etaStatus: "ready", etaMs: 8_000 });

      vi.advanceTimersByTime(9_999);
      expect(emit).toHaveBeenCalledTimes(3);
      vi.advanceTimersByTime(1);
      expect(emit).toHaveBeenCalledTimes(4);
      expect(emit.mock.lastCall?.[0]).toMatchObject({ etaStatus: "stalled", phasePercent: 20 });
      expect(emit.mock.lastCall?.[0].etaMs).toBeUndefined();
    } finally {
      vi.useRealTimers();
    }
  });

  it("cancels the stall deadline when the phase completes", () => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
    try {
      const emit = vi.fn();
      const report = createProgressReporter({ operationId: "complete-op", operation: "health", emit });
      report({ value: 75, phase: "groups", label: "Groups", completed: 0, total: 10 });
      vi.advanceTimersByTime(1_000);
      report({ value: 80, phase: "groups", label: "Groups", completed: 2, total: 10 });
      vi.advanceTimersByTime(1_000);
      report({ value: 90, phase: "groups", label: "Groups", completed: 4, total: 10 });
      expect(emit.mock.lastCall?.[0].etaStatus).toBe("ready");

      report({ value: 100, phase: "complete", label: "Ready", completed: 10, total: 10 });
      vi.advanceTimersByTime(10_000);
      expect(emit).toHaveBeenCalledTimes(4);
      expect(emit.mock.lastCall?.[0].etaStatus).toBe("complete");
    } finally {
      vi.useRealTimers();
    }
  });
});
