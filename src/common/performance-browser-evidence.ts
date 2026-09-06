import { AKHQ_DIGEST, AKHQ_VERSION } from "./akhq-read-only-config";
import { assertSanitizedPerformanceEvidence, summarizePerformanceDurations } from "./performance-evidence";

import type {
  PackagedBrowserPaintEvidence,
  PackagedBrowserPerformanceEvidence,
  PerformanceStageEvidence,
} from "./performance-evidence";

const BROWSER_RUN_COUNT = 3;
const OVERVIEW_SLO_MS = 5_000;

type StageSummary = Required<Pick<PerformanceStageEvidence, "durationMs" | "p95DurationMs" | "sampleCount">>;

function exactRecord(value: unknown, label: string, expectedKeys: string[]): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${label} must be an object`);
  const source = value as Record<string, unknown>;
  const actualKeys = Object.keys(source);
  if (actualKeys.length !== expectedKeys.length || expectedKeys.some((key) => !Object.hasOwn(source, key))) {
    throw new Error(`${label} has unexpected fields`);
  }
  return source;
}

function nonNegativeInteger(value: unknown, label: string, positive = false): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < (positive ? 1 : 0)) {
    throw new Error(`${label} must be a ${positive ? "positive" : "non-negative"} integer`);
  }
  return value;
}

function trueValue(value: unknown, label: string): true {
  if (value !== true) throw new Error(`${label} must be true`);
  return true;
}

function isoTimestamp(value: unknown, label: string): string {
  if (typeof value !== "string") throw new Error(`${label} must be an ISO timestamp`);
  try {
    if (new Date(value).toISOString() !== value) throw new Error();
  } catch {
    throw new Error(`${label} must be an ISO timestamp`);
  }
  return value;
}

function packageSha(value: unknown): string {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/i.test(value)) {
    throw new Error("packaged browser evidence requires a package SHA-256");
  }
  return value.toLowerCase();
}

function stageSummary(value: unknown, label: string): StageSummary {
  const source = exactRecord(value, label, ["durationMs", "p95DurationMs", "sampleCount"]);
  const summary = {
    durationMs: nonNegativeInteger(source.durationMs, `${label}.durationMs`),
    p95DurationMs: nonNegativeInteger(source.p95DurationMs, `${label}.p95DurationMs`),
    sampleCount: nonNegativeInteger(source.sampleCount, `${label}.sampleCount`, true),
  };
  if (summary.sampleCount !== BROWSER_RUN_COUNT) throw new Error(`${label} requires exactly three samples`);
  return summary;
}

function matchingSummary(summary: StageSummary, samples: number[], label: string): void {
  const expected = summarizePerformanceDurations(samples);
  if (
    summary.durationMs !== expected.durationMs ||
    summary.p95DurationMs !== expected.p95DurationMs ||
    summary.sampleCount !== expected.sampleCount
  ) {
    throw new Error(`${label} does not match retained browser samples`);
  }
}

function paintEvidence(value: unknown, label: string): PackagedBrowserPaintEvidence {
  const source = exactRecord(value, label, [
    "aggregateStillRunning",
    "firstUsefulPaintMs",
    "interactiveWhileUpdating",
    "metadataPaintMs",
    "progressContinued",
    "topologyPaintMs",
  ]);
  const result: PackagedBrowserPaintEvidence = {
    aggregateStillRunning: trueValue(source.aggregateStillRunning, `${label}.aggregateStillRunning`),
    firstUsefulPaintMs: nonNegativeInteger(source.firstUsefulPaintMs, `${label}.firstUsefulPaintMs`),
    interactiveWhileUpdating: trueValue(source.interactiveWhileUpdating, `${label}.interactiveWhileUpdating`),
    metadataPaintMs: nonNegativeInteger(source.metadataPaintMs, `${label}.metadataPaintMs`),
    progressContinued: trueValue(source.progressContinued, `${label}.progressContinued`),
    topologyPaintMs: nonNegativeInteger(source.topologyPaintMs, `${label}.topologyPaintMs`),
  };
  if (result.firstUsefulPaintMs !== Math.min(result.metadataPaintMs, result.topologyPaintMs)) {
    throw new Error(`${label}.firstUsefulPaintMs must be the first retained useful paint`);
  }
  return result;
}

export function parsePackagedBrowserPerformanceEvidence(raw: unknown): PackagedBrowserPerformanceEvidence {
  assertSanitizedPerformanceEvidence(raw);
  const source = exactRecord(raw, "packaged browser evidence", [
    "schemaVersion",
    "capturedAt",
    "mode",
    "packageSha256",
    "authorizationPinsVerified",
    "freelens",
    "akhq",
    "runs",
  ]);
  if (source.schemaVersion !== 1 || source.mode !== "authorized-read-only") {
    throw new Error("packaged browser evidence identity is invalid");
  }
  const freelensSource = exactRecord(source.freelens, "packaged browser Freelens evidence", [
    "firstUsefulPaint",
    "metadataPaint",
    "topologyPaint",
    "aggregateStillRunning",
    "interactiveWhileUpdating",
    "progressContinued",
  ]);
  const akhqSource = exactRecord(source.akhq, "packaged browser AKHQ evidence", ["topicPaint"]);
  if (!Array.isArray(source.runs) || source.runs.length !== BROWSER_RUN_COUNT) {
    throw new Error("packaged browser evidence requires exactly three runs");
  }
  const runs = source.runs.map((value, index) => {
    const run = exactRecord(value, `packaged browser run ${index}`, [
      "akhqTopicPaintMs",
      "freelens",
      "preparationAttempts",
    ]);
    const preparationAttempts = nonNegativeInteger(
      run.preparationAttempts,
      `packaged browser run ${index}.preparationAttempts`,
      true,
    );
    if (preparationAttempts > 3) throw new Error("packaged browser preparation attempts exceed the retry bound");
    return {
      akhqTopicPaintMs: nonNegativeInteger(run.akhqTopicPaintMs, `packaged browser run ${index}.akhqTopicPaintMs`),
      freelens: paintEvidence(run.freelens, `packaged browser run ${index}.freelens`),
      preparationAttempts,
    };
  });
  const result: PackagedBrowserPerformanceEvidence = {
    capturedAt: isoTimestamp(source.capturedAt, "packaged browser capturedAt"),
    mode: "authorized-read-only",
    packageSha256: packageSha(source.packageSha256),
    authorizationPinsVerified: trueValue(source.authorizationPinsVerified, "packaged browser authorization pins"),
    freelens: {
      firstUsefulPaint: stageSummary(freelensSource.firstUsefulPaint, "packaged browser first useful paint"),
      metadataPaint: stageSummary(freelensSource.metadataPaint, "packaged browser metadata paint"),
      topologyPaint: stageSummary(freelensSource.topologyPaint, "packaged browser topology paint"),
      aggregateStillRunning: trueValue(
        freelensSource.aggregateStillRunning,
        "packaged browser aggregate running evidence",
      ),
      interactiveWhileUpdating: trueValue(
        freelensSource.interactiveWhileUpdating,
        "packaged browser interaction evidence",
      ),
      progressContinued: trueValue(freelensSource.progressContinued, "packaged browser progress evidence"),
    },
    akhq: {
      provider: "akhq",
      version: AKHQ_VERSION,
      digest: AKHQ_DIGEST,
      method: "GET",
      topicPaint: stageSummary(akhqSource.topicPaint, "packaged browser AKHQ topic paint"),
    },
    runs,
  };
  matchingSummary(
    result.freelens.firstUsefulPaint,
    runs.map((run) => run.freelens.firstUsefulPaintMs),
    "packaged browser first useful paint",
  );
  matchingSummary(
    result.freelens.metadataPaint,
    runs.map((run) => run.freelens.metadataPaintMs),
    "packaged browser metadata paint",
  );
  matchingSummary(
    result.freelens.topologyPaint,
    runs.map((run) => run.freelens.topologyPaintMs),
    "packaged browser topology paint",
  );
  matchingSummary(
    result.akhq.topicPaint,
    runs.map((run) => run.akhqTopicPaintMs),
    "packaged browser AKHQ topic paint",
  );
  if (
    result.freelens.firstUsefulPaint.durationMs > OVERVIEW_SLO_MS ||
    result.freelens.topologyPaint.durationMs > OVERVIEW_SLO_MS
  ) {
    throw new Error("packaged browser evidence exceeds the Overview paint SLO");
  }
  if (result.freelens.firstUsefulPaint.durationMs > result.akhq.topicPaint.durationMs) {
    throw new Error("packaged browser evidence does not meet the AKHQ paint comparison");
  }
  return result;
}

export function assertPackagedBrowserPerformanceEvidence(
  value: unknown,
): asserts value is PackagedBrowserPerformanceEvidence {
  const source = exactRecord(value, "attached packaged browser evidence", [
    "capturedAt",
    "mode",
    "packageSha256",
    "authorizationPinsVerified",
    "freelens",
    "akhq",
    "runs",
  ]);
  const akhq = exactRecord(source.akhq, "attached packaged browser AKHQ evidence", [
    "provider",
    "version",
    "digest",
    "method",
    "topicPaint",
  ]);
  if (
    akhq.provider !== "akhq" ||
    akhq.version !== AKHQ_VERSION ||
    akhq.digest !== AKHQ_DIGEST ||
    akhq.method !== "GET"
  ) {
    throw new Error("attached packaged browser evidence requires pinned GET-only AKHQ");
  }
  parsePackagedBrowserPerformanceEvidence({
    schemaVersion: 1,
    capturedAt: source.capturedAt,
    mode: source.mode,
    packageSha256: source.packageSha256,
    authorizationPinsVerified: source.authorizationPinsVerified,
    freelens: source.freelens,
    akhq: { topicPaint: akhq.topicPaint },
    runs: source.runs,
  });
}
