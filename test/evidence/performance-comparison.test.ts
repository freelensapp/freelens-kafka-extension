import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { AKHQ_DIGEST, AKHQ_VERSION } from "./akhq-read-only-config";
import {
  assertAkhqComparisonEvidence,
  assertSlice16PerformanceEvidence,
  attachAkhqComparison,
  attachPackagedBrowserEvidence,
} from "./performance-comparison";
import { aggregatePerformanceProbeRuns, type ReadOnlyPerformanceProbeRun } from "./performance-run-aggregation";

function run(index: number): ReadOnlyPerformanceProbeRun {
  return {
    captureId: `00000000-0000-4000-8000-${index.toString().padStart(12, "0")}`,
    capturedAt: `2026-08-26T00:00:0${index}.000Z`,
    sourceFileId: index.toString(16).padStart(64, "0"),
    mode: "read-only",
    counts: { workloads: 235, discoveredTargets: 5, brokers: 3, topics: 1_096, groups: 1_114 },
    durationsMs: {
      discovery: 20_000,
      reachability: 10,
      credentialResolution: 17_000,
      overview: { total: 6_000 },
      groups: { total: 4_500 },
      topicMetadata: { total: 1_100 },
      groupDetail: { total: 4_000 },
      topicConsumers: { total: 36_000 },
      aggregateHealth: {
        totalMs: 10_000 + index,
        warmCacheMs: 0,
        warmLoaderCalls: 0,
        warmRequestCount: 0,
        phasesMs: { topology: 3_000, groups: 6_000, watermarks: 1_000 + index },
        protocolRequests: {
          findCoordinator: 3,
          offsetFetch: 12,
          offsetFetchGroups: 1_110,
          fetchOffsetsFallback: 0,
          listOffsets: 3,
          lowOffsets: 0,
          fetchTopicOffsetsFallback: 0,
        },
        capabilities: {
          batchSupported: true,
          findCoordinator: { minVersion: 0, maxVersion: 6 },
          offsetFetch: { minVersion: 0, maxVersion: 9 },
        },
        groupBatch: {
          findCoordinatorRequests: 3,
          offsetFetchRequests: 12,
          offsetFetchGroups: 1_110,
          publicFallbacks: 0,
        },
        batch: { brokers: 3, topics: 171, partitions: 651 },
        coverage: { exact: true, unavailableGroups: 0, unavailableTopics: 0 },
      },
    },
  };
}

function browserEvidence() {
  const runs = [3_900, 4_100, 4_000].map((durationMs, index) => ({
    akhqTopicPaintMs: [6_700, 6_900, 7_000][index],
    freelens: {
      aggregateStillRunning: true,
      firstUsefulPaintMs: durationMs,
      interactiveWhileUpdating: true,
      metadataPaintMs: [6_400, 6_600, 6_800][index],
      progressContinued: true,
      topologyPaintMs: durationMs,
    },
    preparationAttempts: index + 1,
  }));
  return {
    schemaVersion: 1,
    capturedAt: "2026-09-01T11:00:00.000Z",
    mode: "authorized-read-only",
    packageSha256: "a".repeat(64),
    authorizationPinsVerified: true,
    freelens: {
      firstUsefulPaint: { durationMs: 4_000, p95DurationMs: 4_100, sampleCount: 3 },
      metadataPaint: { durationMs: 6_600, p95DurationMs: 6_800, sampleCount: 3 },
      topologyPaint: { durationMs: 4_000, p95DurationMs: 4_100, sampleCount: 3 },
      aggregateStillRunning: true,
      interactiveWhileUpdating: true,
      progressContinued: true,
    },
    akhq: { topicPaint: { durationMs: 6_900, p95DurationMs: 7_000, sampleCount: 3 } },
    runs,
  };
}

describe("attachAkhqComparison", () => {
  it("adds only allowlisted pinned GET timings and cleanup to final evidence", () => {
    const evidence = aggregatePerformanceProbeRuns([run(1), run(2), run(3)], "2026-08-26T00:01:00.000Z");
    const finalEvidence = attachAkhqComparison(evidence, {
      provider: "akhq",
      version: AKHQ_VERSION,
      digest: AKHQ_DIGEST,
      method: "GET",
      topics: { coldMs: 7_500, warmMs: [3_800, 3_700], ignoredIdentity: "removed" },
      consumerGroups: { coldMs: 14_700, warmMs: [15_900, 14_800] },
      completeGlobalLagEquivalent: false,
      cleanup: {
        createdContainerRemoved: true,
        imageRemovedOrPreexisting: true,
        kubernetesReady: true,
        temporaryConfigurationRemoved: true,
      },
      credentials: { password: "must-not-survive" },
    });

    expect(() => assertAkhqComparisonEvidence(finalEvidence)).not.toThrow();
    expect(finalEvidence).not.toHaveProperty("credentials");
    expect(finalEvidence.comparison?.topics).toMatchObject({
      coldDurationMs: 7_500,
      durationMs: 3_750,
      p95DurationMs: 3_800,
      sampleCount: 2,
    });
    const sliceEvidence = attachPackagedBrowserEvidence(finalEvidence, browserEvidence());
    expect(() => assertSlice16PerformanceEvidence(sliceEvidence)).not.toThrow();
    expect(sliceEvidence.packagedBrowser?.runs.map((run) => run.preparationAttempts)).toEqual([1, 2, 3]);
  });

  it("validates the sanitized final Slice 16 evidence artifact", () => {
    const evidence = JSON.parse(readFileSync("docs/performance/spec-014-slice-16.json", "utf8")) as unknown;
    expect(() => assertSlice16PerformanceEvidence(evidence)).not.toThrow();
  });

  it("rejects inconsistent API summaries and local final evidence", () => {
    const committed = JSON.parse(readFileSync("docs/performance/spec-014-slice-16.json", "utf8")) as {
      comparison: { topics: { durationMs: number; runs: number[] } };
      mode: string;
    };
    committed.comparison.topics.durationMs = 1;
    expect(() => assertSlice16PerformanceEvidence(committed)).toThrow("summary does not match retained samples");

    const local = JSON.parse(readFileSync("docs/performance/spec-014-slice-16.json", "utf8")) as { mode: string };
    local.mode = "local";
    expect(() => assertSlice16PerformanceEvidence(local)).toThrow("requires authorized read-only mode");
  });
});
