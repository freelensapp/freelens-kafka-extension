import { describe, expect, it } from "vitest";
import {
  assertPackagedBrowserPerformanceEvidence,
  parsePackagedBrowserPerformanceEvidence,
} from "./performance-browser-evidence";

function rawBrowserEvidence() {
  const runs = [
    {
      akhqTopicPaintMs: 6_600,
      freelens: {
        aggregateStillRunning: true,
        firstUsefulPaintMs: 4_100,
        interactiveWhileUpdating: true,
        metadataPaintMs: 6_200,
        progressContinued: true,
        topologyPaintMs: 4_100,
      },
      preparationAttempts: 1,
    },
    {
      akhqTopicPaintMs: 6_800,
      freelens: {
        aggregateStillRunning: true,
        firstUsefulPaintMs: 3_800,
        interactiveWhileUpdating: true,
        metadataPaintMs: 6_600,
        progressContinued: true,
        topologyPaintMs: 3_800,
      },
      preparationAttempts: 2,
    },
    {
      akhqTopicPaintMs: 7_700,
      freelens: {
        aggregateStillRunning: true,
        firstUsefulPaintMs: 4_000,
        interactiveWhileUpdating: true,
        metadataPaintMs: 7_000,
        progressContinued: true,
        topologyPaintMs: 4_000,
      },
      preparationAttempts: 1,
    },
  ];
  return {
    schemaVersion: 1,
    capturedAt: "2026-09-01T11:00:00.000Z",
    mode: "authorized-read-only",
    packageSha256: "a".repeat(64),
    authorizationPinsVerified: true,
    freelens: {
      firstUsefulPaint: { durationMs: 4_000, p95DurationMs: 4_100, sampleCount: 3 },
      metadataPaint: { durationMs: 6_600, p95DurationMs: 7_000, sampleCount: 3 },
      topologyPaint: { durationMs: 4_000, p95DurationMs: 4_100, sampleCount: 3 },
      aggregateStillRunning: true,
      interactiveWhileUpdating: true,
      progressContinued: true,
    },
    akhq: { topicPaint: { durationMs: 6_800, p95DurationMs: 7_700, sampleCount: 3 } },
    runs,
  };
}

describe("parsePackagedBrowserPerformanceEvidence", () => {
  it("allowlists three packaged paint runs and validates pinned comparison evidence", () => {
    const evidence = parsePackagedBrowserPerformanceEvidence(rawBrowserEvidence());
    expect(() => assertPackagedBrowserPerformanceEvidence(evidence)).not.toThrow();
    expect(evidence.akhq).toMatchObject({ provider: "akhq", method: "GET" });
    expect(evidence.runs.map((run) => run.preparationAttempts)).toEqual([1, 2, 1]);
  });

  it("rejects internal timing fields and mismatched summaries", () => {
    const internal = rawBrowserEvidence();
    Object.assign(internal.runs[0].freelens, { startedAt: 1.5 });
    expect(() => parsePackagedBrowserPerformanceEvidence(internal)).toThrow("unexpected fields");
    const mismatched = rawBrowserEvidence();
    mismatched.freelens.firstUsefulPaint.durationMs = 3_999;
    expect(() => parsePackagedBrowserPerformanceEvidence(mismatched)).toThrow(
      "does not match retained browser samples",
    );
  });

  it("rejects false interaction evidence and unbounded preparation retries", () => {
    const falseProgress = rawBrowserEvidence();
    falseProgress.runs[0].freelens.progressContinued = false;
    expect(() => parsePackagedBrowserPerformanceEvidence(falseProgress)).toThrow("must be true");
    const retries = rawBrowserEvidence();
    retries.runs[0].preparationAttempts = 4;
    expect(() => parsePackagedBrowserPerformanceEvidence(retries)).toThrow("exceed the retry bound");
  });

  it("rejects paint samples that miss the SLO or AKHQ comparison", () => {
    const slow = rawBrowserEvidence();
    for (const run of slow.runs) {
      run.freelens.firstUsefulPaintMs = 7_800;
      run.freelens.metadataPaintMs = 7_800;
      run.freelens.topologyPaintMs = 7_800;
    }
    slow.freelens.firstUsefulPaint = { durationMs: 7_800, p95DurationMs: 7_800, sampleCount: 3 };
    slow.freelens.metadataPaint = { durationMs: 7_800, p95DurationMs: 7_800, sampleCount: 3 };
    slow.freelens.topologyPaint = { durationMs: 7_800, p95DurationMs: 7_800, sampleCount: 3 };
    expect(() => parsePackagedBrowserPerformanceEvidence(slow)).toThrow("exceeds the Overview paint SLO");
  });
});
