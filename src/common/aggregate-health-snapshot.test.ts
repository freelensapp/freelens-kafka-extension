import { describe, expect, it } from "vitest";
import {
  aggregateHealthSnapshotKey,
  decodeAggregateHealthSnapshot,
  encodeAggregateHealthSnapshot,
} from "./aggregate-health-snapshot";

import type { ClusterOverviewHealthDto } from "./ipc";

const health: ClusterOverviewHealthDto = {
  onlineBrokers: 3,
  unavailablePartitions: 0,
  underReplicatedPartitions: 1,
  consumerGroupLag: "≥42",
  consumerGroupLagUnavailableGroups: 1,
  consumerGroupLagCoverage: {
    complete: false,
    completedAt: 2_000,
    resolvedGroups: 9,
    startedAt: 1_000,
    totalGroups: 10,
    unavailableGroups: 1,
  },
};

describe("aggregate health snapshot", () => {
  it("round-trips only the bounded aggregate DTO", () => {
    const raw = encodeAggregateHealthSnapshot({
      data: {
        ...health,
        groupId: "must-not-persist",
        topicName: "must-not-persist",
        offset: "99",
        credentials: { password: "must-not-persist" },
      } as ClusterOverviewHealthDto,
      updatedAt: 2_000,
    });

    expect(raw.length).toBeLessThan(1_000);
    expect(raw).not.toMatch(/groupId|topicName|offset|credentials|password|must-not-persist/);
    expect(decodeAggregateHealthSnapshot(raw)).toEqual({ data: health, updatedAt: 2_000 });
  });

  it("rejects malformed, oversized and inconsistent coverage records", () => {
    expect(decodeAggregateHealthSnapshot("not-json")).toBeUndefined();
    expect(decodeAggregateHealthSnapshot("x".repeat(4_097))).toBeUndefined();
    expect(
      decodeAggregateHealthSnapshot(
        JSON.stringify({
          schemaVersion: 1,
          updatedAt: 1,
          data: {
            ...health,
            consumerGroupLagCoverage: { complete: false, resolvedGroups: 10, totalGroups: 2, unavailableGroups: 1 },
          },
        }),
      )?.data.consumerGroupLagCoverage,
    ).toBeUndefined();
    expect(
      decodeAggregateHealthSnapshot(
        JSON.stringify({
          schemaVersion: 1,
          updatedAt: 1,
          data: {
            ...health,
            consumerGroupLag: "42",
            consumerGroupLagUnavailableGroups: undefined,
            consumerGroupLagCoverage: { complete: true, resolvedGroups: 9, totalGroups: 10, unavailableGroups: 0 },
          },
        }),
      )?.data.consumerGroupLagCoverage,
    ).toBeUndefined();
  });

  it("uses encoded non-secret target identities as the storage key", () => {
    expect(aggregateHealthSnapshotKey("context/a", "kafka-123")).toBe(
      "freelens-kafka.aggregate-health.v1:context%2Fa:kafka-123",
    );
  });

  it("drops a lastComplete value unless coverage and lag are exact", () => {
    const raw = encodeAggregateHealthSnapshot({
      data: health,
      lastComplete: health,
      updatedAt: 2_000,
    });
    expect(decodeAggregateHealthSnapshot(raw)?.lastComplete).toBeUndefined();

    const exact: ClusterOverviewHealthDto = {
      ...health,
      consumerGroupLag: "42",
      consumerGroupLagUnavailableGroups: undefined,
      consumerGroupLagCoverage: {
        ...health.consumerGroupLagCoverage!,
        complete: true,
        resolvedGroups: 10,
        unavailableGroups: 0,
      },
    };
    expect(
      decodeAggregateHealthSnapshot(
        encodeAggregateHealthSnapshot({ data: exact, lastComplete: exact, updatedAt: 2_000 }),
      )?.lastComplete,
    ).toEqual(exact);
  });
});
