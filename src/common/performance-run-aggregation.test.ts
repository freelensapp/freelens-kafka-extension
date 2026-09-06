import { describe, expect, it } from "vitest";
import {
  aggregatePerformanceProbeRuns,
  parseReadOnlyPerformanceProbeRun,
  type ReadOnlyPerformanceProbeRun,
} from "./performance-run-aggregation";

function run(durationMs: number, warmCacheMs: number, captureNumber: number): ReadOnlyPerformanceProbeRun {
  return {
    captureId: `00000000-0000-4000-8000-${captureNumber.toString().padStart(12, "0")}`,
    capturedAt: `2026-08-25T12:00:0${captureNumber}.000Z`,
    sourceFileId: captureNumber.toString(16).padStart(64, "0"),
    mode: "read-only",
    counts: { workloads: 236, discoveredTargets: 5, brokers: 3, topics: 1_096, groups: 1_128 },
    durationsMs: {
      discovery: 20_000 + durationMs,
      reachability: 10,
      credentialResolution: 18_000,
      overview: { total: 5_000 },
      groups: { total: 5_000 },
      topicMetadata: { total: 1_000 },
      groupDetail: { total: 4_000 },
      topicConsumers: { total: 20_000 },
      aggregateHealth: {
        totalMs: durationMs,
        warmCacheMs,
        warmLoaderCalls: 0,
        warmRequestCount: 0,
        phasesMs: { topology: 3_000, groups: durationMs - 4_000, watermarks: 1_000 },
        protocolRequests: {
          findCoordinator: 3,
          offsetFetch: 12,
          offsetFetchGroups: 1_128,
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
          offsetFetchGroups: 1_128,
          publicFallbacks: 0,
        },
        batch: { brokers: 3, topics: 170, partitions: 661 },
        coverage: { exact: true, unavailableGroups: 0, unavailableTopics: 0 },
      },
    },
  };
}

describe("aggregatePerformanceProbeRuns", () => {
  it("creates sanitized median/P95 evidence from exactly three cold and warm runs", () => {
    const evidence = aggregatePerformanceProbeRuns(
      [run(10_108, 2, 1), run(9_612, 1, 2), run(10_066, 1, 3)],
      "2026-08-25T12:00:00.000Z",
    );

    expect(evidence.extension.aggregateHealth).toMatchObject({
      durationMs: 10_066,
      p95DurationMs: 10_108,
      sampleCount: 3,
      warmCache: { durationMs: 1, p95DurationMs: 2, sampleCount: 3, requestCount: 0, loaderCalls: 0 },
    });
    expect(evidence.extension.discovery).toMatchObject({
      durationMs: 30_066,
      p95DurationMs: 30_108,
      sampleCount: 3,
    });
    expect(evidence).not.toHaveProperty("target");
    expect(evidence).not.toHaveProperty("security");
  });

  it("strips unknown nested fields through the raw-run allowlist", () => {
    const raw = {
      ...run(10_108, 2, 1),
      target: { alternateCredentialValue: "must-not-survive" },
      durationsMs: {
        ...run(10_108, 2, 1).durationsMs,
        aggregateHealth: {
          ...run(10_108, 2, 1).durationsMs.aggregateHealth,
          alternateTlsMaterial: "must-not-survive",
        },
      },
    };
    expect(parseReadOnlyPerformanceProbeRun(raw)).not.toHaveProperty("target");
    expect(parseReadOnlyPerformanceProbeRun(raw).durationsMs.aggregateHealth).not.toHaveProperty(
      "alternateTlsMaterial",
    );
  });

  it("rejects missing, duplicate, malformed and non-finite run evidence", () => {
    const runs = [run(10_108, 2, 1), run(9_612, 1, 2), run(10_066, 1, 3)];
    const capturedAt = "2026-08-25T12:01:00.000Z";
    expect(() => aggregatePerformanceProbeRuns(runs.slice(0, 2), capturedAt)).toThrow("exactly three");
    expect(() => aggregatePerformanceProbeRuns([runs[0], runs[0], runs[2]], capturedAt)).toThrow("unique capture IDs");
    const duplicateTimestamp = { ...runs[1], capturedAt: runs[0].capturedAt };
    expect(() => aggregatePerformanceProbeRuns([runs[0], duplicateTimestamp, runs[2]], capturedAt)).toThrow(
      "unique capture timestamps",
    );
    const duplicateSource = { ...runs[1], sourceFileId: runs[0].sourceFileId };
    expect(() => aggregatePerformanceProbeRuns([runs[0], duplicateSource, runs[2]], capturedAt)).toThrow(
      "unique source files",
    );
    const missingCount = { ...runs[0], counts: { ...runs[0].counts, groups: null } };
    expect(() => parseReadOnlyPerformanceProbeRun(missingCount)).toThrow("finite non-negative integer");
    runs[1].durationsMs.aggregateHealth.capabilities.offsetFetch!.maxVersion = 8;
    expect(() => aggregatePerformanceProbeRuns(runs, capturedAt)).toThrow("inconsistent Kafka API");
    runs[1].durationsMs.aggregateHealth.capabilities.offsetFetch!.maxVersion = 9;
    runs[2].durationsMs.aggregateHealth.warmRequestCount = 1;
    expect(() => aggregatePerformanceProbeRuns(runs, capturedAt)).toThrow("request-free warm cache hit");
  });
});
