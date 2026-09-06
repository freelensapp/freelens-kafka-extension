import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  assertFinalPerformanceEvidence,
  assertPerformanceEvidence,
  assertSanitizedPerformanceEvidence,
  createPerformanceEvidence,
  summarizePerformanceDurations,
  totalDurationMs,
} from "./performance-evidence";

describe("performance evidence", () => {
  it("creates and validates a versioned, sanitized evidence record", () => {
    const evidence = createPerformanceEvidence({
      capturedAt: "2026-08-20T00:00:00.000Z",
      mode: "authorized-read-only",
      scale: { workloads: 235, topics: 1096, consumerGroups: 1151 },
      extension: {
        discovery: { durationMs: 20_042 },
        groups: { durationMs: 4_471, protocolRequests: { findCoordinator: 3, offsetFetch: 3 } },
        aggregateHealth: {
          durationMs: 37_643,
          p95DurationMs: 39_055,
          sampleCount: 3,
          protocolRequests: {
            fetchTopicOffsetsFallback: 0,
            listOffsets: 3,
            lowOffsets: 0,
          },
          batch: { brokers: 3, partitions: 661, topics: 170 },
          coverage: { exact: true, unavailableTopics: 0 },
          phases: {
            topology: { durationMs: 3_068, p95DurationMs: 3_288, sampleCount: 3 },
            groups: { durationMs: 12_556, p95DurationMs: 13_747, sampleCount: 3 },
            watermarks: { durationMs: 2_034, p95DurationMs: 2_085, sampleCount: 3 },
          },
          warmCache: { durationMs: 1, p95DurationMs: 2, sampleCount: 3, requestCount: 0, loaderCalls: 0 },
          runs: [
            {
              captureId: "00000000-0000-4000-8000-000000000001",
              capturedAt: "2026-08-25T00:00:01.000Z",
              sourceFileId: "1".padStart(64, "0"),
              durationMs: 37_643,
              operationsMs: {
                discovery: 1,
                reachability: 1,
                credentialResolution: 1,
                overview: 1,
                topics: 1,
                groups: 1,
                groupDetail: 1,
                topicConsumers: 1,
              },
              phasesMs: { topology: 3_053, groups: 32_556, watermarks: 2_034 },
              protocolRequests: { listOffsets: 3, lowOffsets: 0, fetchTopicOffsetsFallback: 0 },
              warmCache: { durationMs: 1, loaderCalls: 0, requestCount: 0 },
            },
            {
              captureId: "00000000-0000-4000-8000-000000000002",
              capturedAt: "2026-08-25T00:00:02.000Z",
              sourceFileId: "2".padStart(64, "0"),
              durationMs: 36_900,
              operationsMs: {
                discovery: 1,
                reachability: 1,
                credentialResolution: 1,
                overview: 1,
                topics: 1,
                groups: 1,
                groupDetail: 1,
                topicConsumers: 1,
              },
              phasesMs: { topology: 3_000, groups: 31_900, watermarks: 2_000 },
              protocolRequests: { listOffsets: 3, lowOffsets: 0, fetchTopicOffsetsFallback: 0 },
              warmCache: { durationMs: 1, loaderCalls: 0, requestCount: 0 },
            },
            {
              captureId: "00000000-0000-4000-8000-000000000003",
              capturedAt: "2026-08-25T00:00:03.000Z",
              sourceFileId: "3".padStart(64, "0"),
              durationMs: 39_055,
              operationsMs: {
                discovery: 1,
                reachability: 1,
                credentialResolution: 1,
                overview: 1,
                topics: 1,
                groups: 1,
                groupDetail: 1,
                topicConsumers: 1,
              },
              phasesMs: { topology: 3_288, groups: 33_682, watermarks: 2_085 },
              protocolRequests: { listOffsets: 3, lowOffsets: 0, fetchTopicOffsetsFallback: 0 },
              warmCache: { durationMs: 1, loaderCalls: 0, requestCount: 0 },
            },
          ],
        },
      },
    });

    expect(() => assertPerformanceEvidence(evidence)).not.toThrow();
    expect(() => assertSanitizedPerformanceEvidence(evidence)).not.toThrow();
    expect(evidence).not.toHaveProperty("bootstrap");
    expect(evidence).not.toHaveProperty("credentials");
  });

  it("summarizes three-run medians and nearest-rank P95 deterministically", () => {
    expect(summarizePerformanceDurations([10_108, 9_612, 10_066])).toEqual({
      durationMs: 10_066,
      p95DurationMs: 10_108,
      sampleCount: 3,
    });
    expect(() => summarizePerformanceDurations([])).toThrow("finite non-negative");
  });

  it("accepts only complete three-run final evidence with a request-free warm cache hit", () => {
    const stage = { durationMs: 1_000, p95DurationMs: 1_000, sampleCount: 3 };
    const protocolRequests = {
      fetchOffsetsFallback: 0,
      fetchTopicOffsetsFallback: 0,
      findCoordinator: 3,
      listOffsets: 3,
      lowOffsets: 0,
      offsetFetch: 12,
      offsetFetchGroups: 1_128,
    };
    const evidence = createPerformanceEvidence({
      capturedAt: "2026-08-25T00:00:00.000Z",
      mode: "authorized-read-only",
      scale: { workloads: 236, targets: 5, brokers: 3, topics: 1_096, consumerGroups: 1_128 },
      extension: {
        discovery: stage,
        reachability: stage,
        credentialResolution: stage,
        overview: stage,
        topics: stage,
        groups: stage,
        groupDetail: stage,
        topicConsumers: stage,
        aggregateHealth: {
          durationMs: 10_000,
          p95DurationMs: 11_000,
          sampleCount: 3,
          requestCount: 18,
          protocolRequests,
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
          batch: { brokers: 3, partitions: 600, topics: 170 },
          coverage: { exact: true, unavailableGroups: 0, unavailableTopics: 0 },
          phases: {
            topology: { durationMs: 3_000, p95DurationMs: 3_000, sampleCount: 3 },
            groups: { durationMs: 6_000, p95DurationMs: 7_000, sampleCount: 3 },
            watermarks: { durationMs: 1_000, p95DurationMs: 1_000, sampleCount: 3 },
          },
          warmCache: { durationMs: 1, p95DurationMs: 2, sampleCount: 3, requestCount: 0, loaderCalls: 0 },
          runs: [10_000, 9_500, 11_000].map((durationMs, index) => ({
            captureId: `00000000-0000-4000-8000-${(index + 1).toString().padStart(12, "0")}`,
            capturedAt: `2026-08-25T00:00:0${index + 1}.000Z`,
            sourceFileId: (index + 1).toString(16).padStart(64, "0"),
            durationMs,
            operationsMs: {
              discovery: 1_000,
              reachability: 1_000,
              credentialResolution: 1_000,
              overview: 1_000,
              topics: 1_000,
              groups: 1_000,
              groupDetail: 1_000,
              topicConsumers: 1_000,
            },
            phasesMs: { topology: 3_000, groups: durationMs - 4_000, watermarks: 1_000 },
            protocolRequests,
            warmCache: { durationMs: index < 2 ? 1 : 2, loaderCalls: 0, requestCount: 0 },
          })),
        },
      },
    });

    expect(() => assertFinalPerformanceEvidence(evidence)).not.toThrow();
    evidence.extension.aggregateHealth!.warmCache!.requestCount = 1;
    expect(() => assertFinalPerformanceEvidence(evidence)).toThrow("request-free warm cache hit");
    evidence.extension.aggregateHealth!.warmCache!.requestCount = 0;
    evidence.extension.aggregateHealth!.coverage.unavailableGroups = undefined;
    expect(() => assertFinalPerformanceEvidence(evidence)).toThrow("exact aggregate coverage");
    evidence.extension.aggregateHealth!.coverage.unavailableGroups = 0;
    evidence.extension.aggregateHealth!.requestCount = 1_131;
    expect(() => assertFinalPerformanceEvidence(evidence)).toThrow("protocol calls only");
    evidence.extension.aggregateHealth!.requestCount = 18;
    evidence.extension.overview!.durationMs = 999;
    expect(() => assertFinalPerformanceEvidence(evidence)).toThrow("summary does not match");
  });

  it("rejects context, endpoint and resource identities anywhere in evidence", () => {
    expect(() => assertSanitizedPerformanceEvidence({ durationsMs: {}, context: "real-context" })).toThrow(
      "sensitive field: context",
    );
    expect(() =>
      assertSanitizedPerformanceEvidence({ runs: [{ target: { host: "broker.example", groupId: "consumer" } }] }),
    ).toThrow("sensitive field: target");
    expect(() => assertSanitizedPerformanceEvidence({ Security: { SASL: { password: "secret" } } })).toThrow(
      "sensitive field: Security",
    );
  });

  it("validates the sanitized Slice 13 production evidence", () => {
    const evidence = JSON.parse(readFileSync("docs/performance/spec-014-slice-13.json", "utf8")) as unknown;

    expect(() => assertPerformanceEvidence(evidence)).not.toThrow();
    expect(() => assertSanitizedPerformanceEvidence(evidence)).not.toThrow();
  });

  it("sums only the measured stages", () => {
    expect(totalDurationMs({ durationMs: 20 }, undefined, { durationMs: 5 })).toBe(25);
  });

  it("rejects unsupported evidence versions", () => {
    expect(() =>
      assertPerformanceEvidence({
        schemaVersion: 2,
        capturedAt: "2026-08-20T00:00:00.000Z",
        mode: "local",
        scale: {},
        extension: {},
      }),
    ).toThrow("unsupported performance evidence schema");
  });
});
