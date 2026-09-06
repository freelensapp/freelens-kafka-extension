import type { ClusterOverviewHealthDto } from "./ipc";

const AGGREGATE_HEALTH_SNAPSHOT_PREFIX = "freelens-kafka.aggregate-health.v1";

export interface PersistedAggregateHealthSnapshot {
  data: ClusterOverviewHealthDto;
  lastComplete?: ClusterOverviewHealthDto;
  updatedAt: number;
}

function nonNegativeInteger(value: unknown): number | undefined {
  return Number.isSafeInteger(value) && Number(value) >= 0 ? Number(value) : undefined;
}

function healthDto(value: unknown): ClusterOverviewHealthDto | undefined {
  if (!value || typeof value !== "object") return undefined;
  const candidate = value as Record<string, unknown>;
  const onlineBrokers = nonNegativeInteger(candidate.onlineBrokers);
  const unavailablePartitions = nonNegativeInteger(candidate.unavailablePartitions);
  const underReplicatedPartitions = nonNegativeInteger(candidate.underReplicatedPartitions);
  const consumerGroupLag = candidate.consumerGroupLag;
  if (
    onlineBrokers === undefined ||
    unavailablePartitions === undefined ||
    underReplicatedPartitions === undefined ||
    typeof consumerGroupLag !== "string" ||
    !/^(?:≥)?\d+$|^Unavailable$|^—$/.test(consumerGroupLag)
  ) {
    return undefined;
  }

  const unavailableTopics = nonNegativeInteger(candidate.consumerGroupLagUnavailableTopics);
  const unavailableGroups = nonNegativeInteger(candidate.consumerGroupLagUnavailableGroups);
  const topologyMeasuredAt = nonNegativeInteger(candidate.topologyMeasuredAt);
  const rawCoverage = candidate.consumerGroupLagCoverage;
  let coverage: ClusterOverviewHealthDto["consumerGroupLagCoverage"];
  if (rawCoverage && typeof rawCoverage === "object") {
    const raw = rawCoverage as Record<string, unknown>;
    const resolvedGroups = nonNegativeInteger(raw.resolvedGroups);
    const totalGroups = nonNegativeInteger(raw.totalGroups);
    const coverageUnavailableGroups = nonNegativeInteger(raw.unavailableGroups);
    const startedAt = nonNegativeInteger(raw.startedAt);
    const completedAt = nonNegativeInteger(raw.completedAt);
    if (
      typeof raw.complete === "boolean" &&
      resolvedGroups !== undefined &&
      totalGroups !== undefined &&
      coverageUnavailableGroups !== undefined &&
      resolvedGroups + coverageUnavailableGroups <= totalGroups &&
      (!raw.complete ||
        (/^\d+$/.test(consumerGroupLag) &&
          resolvedGroups === totalGroups &&
          coverageUnavailableGroups === 0 &&
          (unavailableGroups ?? 0) === 0 &&
          (unavailableTopics ?? 0) === 0))
    ) {
      coverage = {
        complete: raw.complete,
        resolvedGroups,
        totalGroups,
        unavailableGroups: coverageUnavailableGroups,
        ...(startedAt === undefined ? {} : { startedAt }),
        ...(completedAt === undefined ? {} : { completedAt }),
      };
    }
  }

  return {
    onlineBrokers,
    unavailablePartitions,
    underReplicatedPartitions,
    consumerGroupLag,
    ...(topologyMeasuredAt === undefined ? {} : { topologyMeasuredAt }),
    ...(unavailableTopics === undefined || unavailableTopics === 0
      ? {}
      : { consumerGroupLagUnavailableTopics: unavailableTopics }),
    ...(unavailableGroups === undefined || unavailableGroups === 0
      ? {}
      : { consumerGroupLagUnavailableGroups: unavailableGroups }),
    ...(coverage ? { consumerGroupLagCoverage: coverage } : {}),
  };
}

export function isExactAggregateHealth(
  data: Partial<ClusterOverviewHealthDto> | undefined,
): data is ClusterOverviewHealthDto {
  return Boolean(
    data &&
      typeof data.consumerGroupLag === "string" &&
      /^\d+$/.test(data.consumerGroupLag) &&
      !data.consumerGroupLagUnavailableGroups &&
      !data.consumerGroupLagUnavailableTopics &&
      data.consumerGroupLagCoverage?.complete === true &&
      data.consumerGroupLagCoverage.unavailableGroups === 0 &&
      data.consumerGroupLagCoverage.resolvedGroups === data.consumerGroupLagCoverage.totalGroups,
  );
}

function exactHealthDto(value: unknown): ClusterOverviewHealthDto | undefined {
  const data = healthDto(value);
  return isExactAggregateHealth(data) ? data : undefined;
}

export function aggregateHealthSnapshotKey(contextId: string, targetId: string): string {
  return `${AGGREGATE_HEALTH_SNAPSHOT_PREFIX}:${encodeURIComponent(contextId)}:${encodeURIComponent(targetId)}`;
}

export function encodeAggregateHealthSnapshot(snapshot: PersistedAggregateHealthSnapshot): string {
  const data = healthDto(snapshot.data);
  const lastComplete = exactHealthDto(snapshot.lastComplete);
  const updatedAt = nonNegativeInteger(snapshot.updatedAt);
  if (!data || updatedAt === undefined) throw new Error("Aggregate health snapshot is invalid");
  return JSON.stringify({
    schemaVersion: 1,
    updatedAt,
    data,
    ...(lastComplete ? { lastComplete } : {}),
  });
}

export function decodeAggregateHealthSnapshot(raw: string | null): PersistedAggregateHealthSnapshot | undefined {
  if (!raw || raw.length > 4_096) return undefined;
  try {
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    if (parsed.schemaVersion !== 1) return undefined;
    const data = healthDto(parsed.data);
    const lastComplete = exactHealthDto(parsed.lastComplete);
    const updatedAt = nonNegativeInteger(parsed.updatedAt);
    if (!data || updatedAt === undefined) return undefined;
    return { data, updatedAt, ...(lastComplete ? { lastComplete } : {}) };
  } catch {
    return undefined;
  }
}
