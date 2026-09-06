import { AKHQ_DIGEST, AKHQ_VERSION } from "./akhq-read-only-config";
import {
  assertPackagedBrowserPerformanceEvidence,
  parsePackagedBrowserPerformanceEvidence,
} from "./performance-browser-evidence";
import {
  assertFinalPerformanceEvidence,
  assertSanitizedPerformanceEvidence,
  summarizePerformanceDurations,
} from "./performance-evidence";

import type { PerformanceEvidence } from "./performance-evidence";

function record(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as Record<string, unknown>;
}

function nonNegativeInteger(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new Error(`${label} must be a non-negative integer`);
  }
  return value;
}

function trueValue(value: unknown, label: string): true {
  if (value !== true) throw new Error(`${label} must be true`);
  return true;
}

function comparisonStage(value: unknown, label: string) {
  const source = record(value, label);
  if (!Array.isArray(source.warmMs) || source.warmMs.length !== 2) {
    throw new Error(`${label} requires exactly two warm runs`);
  }
  const warmRuns = source.warmMs.map((duration, index) => nonNegativeInteger(duration, `${label}.warmMs.${index}`));
  return {
    coldDurationMs: nonNegativeInteger(source.coldMs, `${label}.coldMs`),
    ...summarizePerformanceDurations(warmRuns),
    runs: warmRuns,
  };
}

export function attachAkhqComparison(evidence: PerformanceEvidence, rawComparison: unknown): PerformanceEvidence {
  assertFinalPerformanceEvidence(evidence);
  const source = record(rawComparison, "AKHQ comparison");
  if (source.provider !== "akhq" || source.version !== AKHQ_VERSION || source.digest !== AKHQ_DIGEST) {
    throw new Error("AKHQ comparison identity does not match the pinned release");
  }
  if (source.method !== "GET") throw new Error("AKHQ comparison must use GET only");
  if (source.completeGlobalLagEquivalent !== false) {
    throw new Error("AKHQ comparison must record the missing complete-global-lag equivalent");
  }
  const cleanup = record(source.cleanup, "AKHQ cleanup");
  const result: PerformanceEvidence = {
    ...evidence,
    comparison: {
      provider: "akhq",
      version: AKHQ_VERSION,
      digest: AKHQ_DIGEST,
      method: "GET",
      topics: comparisonStage(source.topics, "AKHQ topics"),
      consumerGroups: comparisonStage(source.consumerGroups, "AKHQ consumer groups"),
      completeGlobalLagEquivalent: false,
      cleanup: {
        createdContainerRemoved: trueValue(cleanup.createdContainerRemoved, "AKHQ container cleanup"),
        imageRemovedOrPreexisting: trueValue(cleanup.imageRemovedOrPreexisting, "AKHQ image cleanup"),
        kubernetesReady: trueValue(cleanup.kubernetesReady, "Kubernetes readiness"),
        temporaryConfigurationRemoved: trueValue(cleanup.temporaryConfigurationRemoved, "AKHQ configuration cleanup"),
      },
    },
  };
  assertAkhqComparisonEvidence(result);
  return result;
}

export function assertAkhqComparisonEvidence(value: unknown): asserts value is PerformanceEvidence {
  assertFinalPerformanceEvidence(value);
  const comparison = value.comparison;
  if (
    !comparison ||
    comparison.provider !== "akhq" ||
    comparison.version !== AKHQ_VERSION ||
    comparison.digest !== AKHQ_DIGEST ||
    comparison.method !== "GET" ||
    comparison.completeGlobalLagEquivalent !== false
  ) {
    throw new Error("Slice 16 evidence requires the pinned GET-only AKHQ comparison");
  }
  for (const [label, stage] of Object.entries({
    topics: comparison.topics,
    consumerGroups: comparison.consumerGroups,
  })) {
    if (
      !stage ||
      typeof stage.coldDurationMs !== "number" ||
      stage.coldDurationMs < 0 ||
      stage.sampleCount !== 2 ||
      stage.runs?.length !== 2 ||
      stage.durationMs === undefined ||
      stage.p95DurationMs === undefined
    ) {
      throw new Error(`Slice 16 evidence requires complete AKHQ ${label} timings`);
    }
    const runs = stage.runs.map((duration, index) => nonNegativeInteger(duration, `AKHQ ${label} run ${index}`));
    const expectedSummary = summarizePerformanceDurations(runs);
    if (
      stage.durationMs !== expectedSummary.durationMs ||
      stage.p95DurationMs !== expectedSummary.p95DurationMs ||
      stage.sampleCount !== expectedSummary.sampleCount
    ) {
      throw new Error(`Slice 16 AKHQ ${label} summary does not match retained samples`);
    }
  }
  if (!comparison.cleanup || Object.values(comparison.cleanup).some((complete) => complete !== true)) {
    throw new Error("Slice 16 evidence requires complete AKHQ cleanup");
  }
  assertSanitizedPerformanceEvidence(value);
}

export function attachPackagedBrowserEvidence(
  evidence: PerformanceEvidence,
  rawBrowserEvidence: unknown,
): PerformanceEvidence {
  assertAkhqComparisonEvidence(evidence);
  const result: PerformanceEvidence = {
    ...evidence,
    packagedBrowser: parsePackagedBrowserPerformanceEvidence(rawBrowserEvidence),
  };
  assertSlice16PerformanceEvidence(result);
  return result;
}

export function assertSlice16PerformanceEvidence(value: unknown): asserts value is PerformanceEvidence {
  assertAkhqComparisonEvidence(value);
  if (value.mode !== "authorized-read-only") {
    throw new Error("Slice 16 evidence requires authorized read-only mode");
  }
  assertPackagedBrowserPerformanceEvidence(value.packagedBrowser);
  assertSanitizedPerformanceEvidence(value);
}
