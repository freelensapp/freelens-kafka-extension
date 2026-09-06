import {
  assertFinalPerformanceEvidence,
  assertSanitizedPerformanceEvidence,
  createPerformanceEvidence,
  summarizePerformanceDurations,
} from "./performance-evidence";

import type { PerformanceEvidence, PerformanceStageEvidence } from "./performance-evidence";

interface OperationTiming {
  total: number;
}

export interface ReadOnlyPerformanceProbeRun {
  captureId: string;
  capturedAt: string;
  sourceFileId: string;
  counts: {
    brokers: number;
    discoveredTargets: number;
    groups: number;
    topics: number;
    workloads: number;
  };
  durationsMs: {
    aggregateHealth: {
      batch: { brokers: number; partitions: number; topics: number };
      capabilities: {
        batchSupported: boolean;
        findCoordinator: { maxVersion: number; minVersion: number } | null;
        offsetFetch: { maxVersion: number; minVersion: number } | null;
      };
      coverage: { exact: boolean; unavailableGroups: number; unavailableTopics: number };
      groupBatch: {
        findCoordinatorRequests: number;
        offsetFetchGroups: number;
        offsetFetchRequests: number;
        publicFallbacks: number;
      };
      phasesMs: { groups: number; topology: number; watermarks: number };
      protocolRequests: {
        fetchOffsetsFallback: number;
        fetchTopicOffsetsFallback: number;
        findCoordinator: number;
        listOffsets: number;
        lowOffsets: number;
        offsetFetch: number;
        offsetFetchGroups: number;
      };
      totalMs: number;
      warmCacheMs: number;
      warmLoaderCalls: number;
      warmRequestCount: number;
    };
    credentialResolution: number;
    discovery: number;
    groupDetail?: OperationTiming;
    groups: OperationTiming;
    overview: OperationTiming;
    reachability: number;
    topicConsumers?: OperationTiming;
    topicMetadata?: OperationTiming;
  };
  mode: "read-only";
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as Record<string, unknown>;
}

function nonNegativeInteger(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || !Number.isInteger(value) || value < 0) {
    throw new Error(`${label} must be a finite non-negative integer`);
  }
  return value;
}

function booleanValue(value: unknown, label: string): boolean {
  if (typeof value !== "boolean") throw new Error(`${label} must be a boolean`);
  return value;
}

function operation(value: unknown, label: string): OperationTiming {
  const source = record(value, label);
  return { total: nonNegativeInteger(source.total, `${label}.total`) };
}

function apiRange(value: unknown, label: string): { maxVersion: number; minVersion: number } {
  const source = record(value, label);
  const minVersion = nonNegativeInteger(source.minVersion, `${label}.minVersion`);
  const maxVersion = nonNegativeInteger(source.maxVersion, `${label}.maxVersion`);
  if (maxVersion < minVersion) throw new Error(`${label} has an invalid version range`);
  return { minVersion, maxVersion };
}

function isoTimestamp(value: unknown, label: string): string {
  if (typeof value !== "string" || !value) {
    throw new Error(`${label} must be an ISO timestamp`);
  }
  try {
    if (new Date(value).toISOString() !== value) throw new Error();
  } catch {
    throw new Error(`${label} must be an ISO timestamp`);
  }
  return value;
}

function captureId(value: unknown): string {
  if (
    typeof value !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
  ) {
    throw new Error("performance run requires a UUID v4 captureId");
  }
  return value;
}

function sourceFileId(value: unknown): string {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/i.test(value)) {
    throw new Error("performance run requires an opaque sourceFileId");
  }
  return value;
}

function optionalOperation(value: unknown, label: string): OperationTiming | undefined {
  return value === undefined ? undefined : operation(value, label);
}

export function parseReadOnlyPerformanceProbeRun(value: unknown): ReadOnlyPerformanceProbeRun {
  const source = record(value, "performance run");
  if (source.mode !== "read-only") throw new Error("performance run must be read-only");
  const counts = record(source.counts, "performance run counts");
  const durations = record(source.durationsMs, "performance run durations");
  const aggregate = record(durations.aggregateHealth, "aggregate health");
  const capabilities = record(aggregate.capabilities, "aggregate capabilities");
  const batch = record(aggregate.batch, "aggregate batch");
  const coverage = record(aggregate.coverage, "aggregate coverage");
  const groupBatch = record(aggregate.groupBatch, "aggregate group batch");
  const phases = record(aggregate.phasesMs, "aggregate phases");
  const requests = record(aggregate.protocolRequests, "aggregate protocol requests");
  return {
    captureId: captureId(source.captureId),
    capturedAt: isoTimestamp(source.capturedAt, "performance run capturedAt"),
    sourceFileId: sourceFileId(source.sourceFileId),
    mode: "read-only",
    counts: {
      workloads: nonNegativeInteger(counts.workloads, "counts.workloads"),
      discoveredTargets: nonNegativeInteger(counts.discoveredTargets, "counts.discoveredTargets"),
      brokers: nonNegativeInteger(counts.brokers, "counts.brokers"),
      topics: nonNegativeInteger(counts.topics, "counts.topics"),
      groups: nonNegativeInteger(counts.groups, "counts.groups"),
    },
    durationsMs: {
      discovery: nonNegativeInteger(durations.discovery, "durations.discovery"),
      reachability: nonNegativeInteger(durations.reachability, "durations.reachability"),
      credentialResolution: nonNegativeInteger(durations.credentialResolution, "durations.credentialResolution"),
      overview: operation(durations.overview, "durations.overview"),
      groups: operation(durations.groups, "durations.groups"),
      topicMetadata: optionalOperation(durations.topicMetadata, "durations.topicMetadata"),
      groupDetail: optionalOperation(durations.groupDetail, "durations.groupDetail"),
      topicConsumers: optionalOperation(durations.topicConsumers, "durations.topicConsumers"),
      aggregateHealth: {
        totalMs: nonNegativeInteger(aggregate.totalMs, "aggregate.totalMs"),
        warmCacheMs: nonNegativeInteger(aggregate.warmCacheMs, "aggregate.warmCacheMs"),
        warmLoaderCalls: nonNegativeInteger(aggregate.warmLoaderCalls, "aggregate.warmLoaderCalls"),
        warmRequestCount: nonNegativeInteger(aggregate.warmRequestCount, "aggregate.warmRequestCount"),
        phasesMs: {
          topology: nonNegativeInteger(phases.topology, "aggregate.phases.topology"),
          groups: nonNegativeInteger(phases.groups, "aggregate.phases.groups"),
          watermarks: nonNegativeInteger(phases.watermarks, "aggregate.phases.watermarks"),
        },
        protocolRequests: {
          fetchOffsetsFallback: nonNegativeInteger(
            requests.fetchOffsetsFallback,
            "aggregate.requests.fetchOffsetsFallback",
          ),
          fetchTopicOffsetsFallback: nonNegativeInteger(
            requests.fetchTopicOffsetsFallback,
            "aggregate.requests.fetchTopicOffsetsFallback",
          ),
          findCoordinator: nonNegativeInteger(requests.findCoordinator, "aggregate.requests.findCoordinator"),
          listOffsets: nonNegativeInteger(requests.listOffsets, "aggregate.requests.listOffsets"),
          lowOffsets: nonNegativeInteger(requests.lowOffsets, "aggregate.requests.lowOffsets"),
          offsetFetch: nonNegativeInteger(requests.offsetFetch, "aggregate.requests.offsetFetch"),
          offsetFetchGroups: nonNegativeInteger(requests.offsetFetchGroups, "aggregate.requests.offsetFetchGroups"),
        },
        capabilities: {
          batchSupported: booleanValue(capabilities.batchSupported, "aggregate.capabilities.batchSupported"),
          findCoordinator: apiRange(capabilities.findCoordinator, "aggregate.capabilities.findCoordinator"),
          offsetFetch: apiRange(capabilities.offsetFetch, "aggregate.capabilities.offsetFetch"),
        },
        groupBatch: {
          findCoordinatorRequests: nonNegativeInteger(
            groupBatch.findCoordinatorRequests,
            "aggregate.groupBatch.findCoordinatorRequests",
          ),
          offsetFetchRequests: nonNegativeInteger(
            groupBatch.offsetFetchRequests,
            "aggregate.groupBatch.offsetFetchRequests",
          ),
          offsetFetchGroups: nonNegativeInteger(groupBatch.offsetFetchGroups, "aggregate.groupBatch.offsetFetchGroups"),
          publicFallbacks: nonNegativeInteger(groupBatch.publicFallbacks, "aggregate.groupBatch.publicFallbacks"),
        },
        batch: {
          brokers: nonNegativeInteger(batch.brokers, "aggregate.batch.brokers"),
          topics: nonNegativeInteger(batch.topics, "aggregate.batch.topics"),
          partitions: nonNegativeInteger(batch.partitions, "aggregate.batch.partitions"),
        },
        coverage: {
          exact: booleanValue(coverage.exact, "aggregate.coverage.exact"),
          unavailableGroups: nonNegativeInteger(coverage.unavailableGroups, "aggregate.coverage.unavailableGroups"),
          unavailableTopics: nonNegativeInteger(coverage.unavailableTopics, "aggregate.coverage.unavailableTopics"),
        },
      },
    },
  };
}

function medianInteger(values: number[]): number {
  return summarizePerformanceDurations(values).durationMs;
}

function maximum(values: number[]): number {
  return Math.max(...values);
}

function stage(values: number[], requestCounts?: number[]): PerformanceStageEvidence {
  return {
    ...summarizePerformanceDurations(values),
    ...(requestCounts ? { requestCount: maximum(requestCounts) } : {}),
  };
}

function optionalStage(
  runs: ReadOnlyPerformanceProbeRun[],
  select: (run: ReadOnlyPerformanceProbeRun) => OperationTiming | undefined,
): PerformanceStageEvidence | undefined {
  const values = runs.map(select);
  if (values.every((value) => value === undefined)) return undefined;
  if (values.some((value) => value === undefined))
    throw new Error("performance runs have inconsistent optional stages");
  return stage(values.map((value) => value!.total));
}

function assertConsistentCapabilities(runs: ReadOnlyPerformanceProbeRun[]): void {
  const expected = JSON.stringify(runs[0].durationsMs.aggregateHealth.capabilities);
  if (runs.some((run) => JSON.stringify(run.durationsMs.aggregateHealth.capabilities) !== expected)) {
    throw new Error("performance runs negotiated inconsistent Kafka API capabilities");
  }
}

export function aggregatePerformanceProbeRuns(rawRuns: unknown[], capturedAt: string): PerformanceEvidence {
  if (rawRuns.length !== 3) throw new Error("final performance aggregation requires exactly three runs");
  const runs = rawRuns.map(parseReadOnlyPerformanceProbeRun);
  if (new Set(runs.map((run) => run.captureId)).size !== runs.length) {
    throw new Error("final performance aggregation requires unique capture IDs");
  }
  if (new Set(runs.map((run) => run.capturedAt)).size !== runs.length) {
    throw new Error("final performance aggregation requires unique capture timestamps");
  }
  if (new Set(runs.map((run) => run.sourceFileId)).size !== runs.length) {
    throw new Error("final performance aggregation requires unique source files");
  }
  for (const run of runs) assertSanitizedPerformanceEvidence(run);
  assertConsistentCapabilities(runs);

  const aggregates = runs.map((run) => run.durationsMs.aggregateHealth);
  const protocolKeys = [
    "fetchOffsetsFallback",
    "fetchTopicOffsetsFallback",
    "findCoordinator",
    "listOffsets",
    "lowOffsets",
    "offsetFetch",
    "offsetFetchGroups",
  ] as const;
  const protocolRequests = Object.fromEntries(
    protocolKeys.map((key) => [key, maximum(aggregates.map((aggregate) => aggregate.protocolRequests[key]))]),
  );
  const evidence = createPerformanceEvidence({
    capturedAt,
    mode: "authorized-read-only",
    scale: {
      workloads: medianInteger(runs.map((run) => run.counts.workloads)),
      targets: medianInteger(runs.map((run) => run.counts.discoveredTargets)),
      brokers: medianInteger(runs.map((run) => run.counts.brokers)),
      topics: medianInteger(runs.map((run) => run.counts.topics)),
      consumerGroups: medianInteger(runs.map((run) => run.counts.groups)),
    },
    extension: {
      discovery: stage(runs.map((run) => run.durationsMs.discovery)),
      reachability: stage(runs.map((run) => run.durationsMs.reachability)),
      credentialResolution: stage(runs.map((run) => run.durationsMs.credentialResolution)),
      overview: stage(runs.map((run) => run.durationsMs.overview.total)),
      topics: optionalStage(runs, (run) => run.durationsMs.topicMetadata),
      groups: stage(runs.map((run) => run.durationsMs.groups.total)),
      groupDetail: optionalStage(runs, (run) => run.durationsMs.groupDetail),
      topicConsumers: optionalStage(runs, (run) => run.durationsMs.topicConsumers),
      aggregateHealth: {
        ...stage(
          aggregates.map((aggregate) => aggregate.totalMs),
          aggregates.map(
            (aggregate) =>
              aggregate.protocolRequests.findCoordinator +
              aggregate.protocolRequests.offsetFetch +
              aggregate.protocolRequests.listOffsets,
          ),
        ),
        protocolRequests,
        capabilities: aggregates[0].capabilities,
        groupBatch: {
          findCoordinatorRequests: maximum(aggregates.map((aggregate) => aggregate.groupBatch.findCoordinatorRequests)),
          offsetFetchGroups: maximum(aggregates.map((aggregate) => aggregate.groupBatch.offsetFetchGroups)),
          offsetFetchRequests: maximum(aggregates.map((aggregate) => aggregate.groupBatch.offsetFetchRequests)),
          publicFallbacks: maximum(aggregates.map((aggregate) => aggregate.groupBatch.publicFallbacks)),
        },
        batch: {
          brokers: medianInteger(aggregates.map((aggregate) => aggregate.batch.brokers)),
          partitions: medianInteger(aggregates.map((aggregate) => aggregate.batch.partitions)),
          topics: medianInteger(aggregates.map((aggregate) => aggregate.batch.topics)),
        },
        coverage: {
          exact: aggregates.every((aggregate) => aggregate.coverage.exact),
          unavailableGroups: maximum(aggregates.map((aggregate) => aggregate.coverage.unavailableGroups)),
          unavailableTopics: maximum(aggregates.map((aggregate) => aggregate.coverage.unavailableTopics)),
        },
        phases: {
          topology: stage(aggregates.map((aggregate) => aggregate.phasesMs.topology)),
          groups: stage(aggregates.map((aggregate) => aggregate.phasesMs.groups)),
          watermarks: stage(aggregates.map((aggregate) => aggregate.phasesMs.watermarks)),
        },
        warmCache: {
          ...stage(
            aggregates.map((aggregate) => aggregate.warmCacheMs),
            aggregates.map((aggregate) => aggregate.warmRequestCount),
          ),
          loaderCalls: maximum(aggregates.map((aggregate) => aggregate.warmLoaderCalls)),
        },
        runs: aggregates.map((aggregate, index) => ({
          captureId: runs[index].captureId,
          capturedAt: runs[index].capturedAt,
          sourceFileId: runs[index].sourceFileId,
          durationMs: aggregate.totalMs,
          operationsMs: {
            discovery: runs[index].durationsMs.discovery,
            reachability: runs[index].durationsMs.reachability,
            credentialResolution: runs[index].durationsMs.credentialResolution,
            overview: runs[index].durationsMs.overview.total,
            topics: runs[index].durationsMs.topicMetadata!.total,
            groups: runs[index].durationsMs.groups.total,
            groupDetail: runs[index].durationsMs.groupDetail!.total,
            topicConsumers: runs[index].durationsMs.topicConsumers!.total,
          },
          phasesMs: aggregate.phasesMs,
          protocolRequests: aggregate.protocolRequests,
          warmCache: {
            durationMs: aggregate.warmCacheMs,
            loaderCalls: aggregate.warmLoaderCalls,
            requestCount: aggregate.warmRequestCount,
          },
        })),
      },
    },
  });
  assertFinalPerformanceEvidence(evidence);
  return evidence;
}
