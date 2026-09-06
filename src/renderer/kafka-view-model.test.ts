import { describe, expect, it } from "vitest";
import {
  clusterHealthMetricValue,
  filterTopicNames,
  kafkaDecimalSortKey,
  kafkaHealthCompletionMetadata,
  kafkaHealthCoverageLabel,
  kafkaHealthCoverageState,
  kafkaHealthNeedsCoverageNotice,
  kafkaHealthProgressIsCompact,
  kafkaProgressCount,
  kafkaProgressIsDeterminate,
  kafkaProgressPhasePercent,
  kafkaUsageContext,
  kafkaUsageSummary,
  matchesKafkaMessageFilters,
  topicPartitionHealth,
} from "./kafka-view-model";

import type { DiscoveredKafkaInfo, KafkaRecordDto } from "../common/ipc";

const kafka = (overrides: Partial<DiscoveredKafkaInfo>): DiscoveredKafkaInfo => ({
  targetId: "kafka-orders",
  source: "workload",
  name: "orders",
  namespace: "first-seen",
  bootstrap: "broker:9092",
  tls: false,
  port: 9092,
  listeners: [],
  brokerPods: [],
  ...overrides,
});

describe("kafkaUsageSummary", () => {
  it("summarizes workload usage instead of exposing the first encountered namespace", () => {
    const target = kafka({
      referencedBy: ["orders/deploy/api", "payments/deploy/worker", "orders/sts/consumer"],
    });
    expect(kafkaUsageSummary(target)).toMatchObject({
      primary: "3 workloads",
      secondary: "2 namespaces",
      workloadCount: 3,
      namespaceCount: 2,
    });
    expect(kafkaUsageContext(target)).toBe("Used by 3 workloads across 2 namespaces");
    expect(kafkaUsageSummary(target).title).toContain("orders, payments");
    expect(kafkaUsageSummary(target).primary).not.toContain("first-seen");
  });

  it("labels Strimzi and Service values as resource namespaces", () => {
    expect(kafkaUsageSummary(kafka({ source: "strimzi", namespace: "streaming" }))).toMatchObject({
      primary: "streaming",
      secondary: "Strimzi resource namespace",
    });
    expect(kafkaUsageSummary(kafka({ source: "service", namespace: "data" }))).toMatchObject({
      primary: "data",
      secondary: "Service namespace",
    });
  });

  it("labels manual endpoints as not Kubernetes-owned", () => {
    expect(kafkaUsageSummary(kafka({ source: "manual", namespace: "" }))).toMatchObject({
      primary: "Manual endpoint",
      secondary: "Not Kubernetes-owned",
    });
  });
});

describe("clusterHealthMetricValue", () => {
  it("distinguishes pending, unavailable, unmeasured and measured zero values", () => {
    expect(clusterHealthMetricValue({ loading: true }, 0)).toBe("Updating");
    expect(clusterHealthMetricValue({ loading: false, error: "timed out" }, 0)).toBe("Unavailable");
    expect(clusterHealthMetricValue({ loading: false }, undefined)).toBe("—");
    expect(clusterHealthMetricValue({ loading: false }, 0)).toBe(0);
  });

  it("keeps a progressively measured value visible while the remaining health work continues", () => {
    expect(clusterHealthMetricValue({ loading: true }, 3, true)).toBe(3);
    expect(clusterHealthMetricValue({ loading: false, error: "lag timed out" }, 3, true)).toBe(3);
  });
});

describe("kafkaProgressIsDeterminate", () => {
  it("uses phase-local completed/total instead of the weighted operation value", () => {
    expect(kafkaProgressIsDeterminate({ value: 40 })).toBe(false);
    expect(kafkaProgressIsDeterminate({ value: 80, completed: 2, total: 10 })).toBe(true);
    expect(kafkaProgressPhasePercent({ value: 80, completed: 2, total: 10 })).toBe(20);
    expect(kafkaProgressPhasePercent({ value: 40, completed: 0, total: 0 })).toBe(100);
    expect(kafkaProgressIsDeterminate({ value: 100 })).toBe(true);
    expect(kafkaProgressPhasePercent({ value: 100 })).toBe(100);
  });

  it("renders known empty phases as an explicit 0/0 count", () => {
    expect(kafkaProgressCount({ completed: 0, total: 0 })).toBe("0/0");
    expect(kafkaProgressCount({ completed: 2, total: 10 })).toBe("2/10");
    expect(kafkaProgressCount({ completed: 2 })).toBeUndefined();
  });
});

describe("kafkaHealthCompletionMetadata", () => {
  it("preserves authoritative Main progress source and timestamp through promise completion", () => {
    expect(kafkaHealthCompletionMetadata({ source: "cache", updatedAt: 1_234 }, 9_999)).toEqual({
      source: "cache",
      updatedAt: 1_234,
    });
    expect(kafkaHealthCompletionMetadata({ source: "persisted", updatedAt: 1_000 })).toEqual({
      source: "persisted",
      updatedAt: 1_000,
    });
  });

  it("falls back to network provenance and the cache timestamp without completion progress metadata", () => {
    expect(kafkaHealthCompletionMetadata({}, 2_345)).toEqual({ source: "network", updatedAt: 2_345 });
  });
});

describe("kafkaHealthProgressIsCompact", () => {
  it("collapses only background Health phases after topology is available", () => {
    expect(kafkaHealthProgressIsCompact({ operation: "health", phase: "connect" })).toBe(false);
    expect(kafkaHealthProgressIsCompact({ operation: "health", phase: "connect" }, undefined, true)).toBe(true);
    expect(kafkaHealthProgressIsCompact({ operation: "health", phase: "topology" })).toBe(true);
    expect(kafkaHealthProgressIsCompact({ operation: "health", phase: "groups" })).toBe(true);
    expect(kafkaHealthProgressIsCompact({ operation: "health", phase: "watermarks" })).toBe(true);
    expect(kafkaHealthProgressIsCompact({ operation: "health", phase: "groups" }, "failed")).toBe(false);
    expect(kafkaHealthProgressIsCompact({ operation: "health", phase: "groups" }, "failed", true)).toBe(false);
    expect(kafkaHealthProgressIsCompact({ operation: "groups", phase: "groups" })).toBe(false);
  });
});

describe("kafkaHealthCoverageLabel", () => {
  it("distinguishes exact coverage from a truthful lower bound", () => {
    const exact = {
      onlineBrokers: 3,
      unavailablePartitions: 0,
      underReplicatedPartitions: 0,
      consumerGroupLag: "42",
      consumerGroupLagCoverage: {
        complete: true,
        resolvedGroups: 10,
        totalGroups: 10,
        unavailableGroups: 0,
      },
    };
    const lowerBound = {
      ...exact,
      consumerGroupLag: "≥40",
      consumerGroupLagUnavailableGroups: 1,
      consumerGroupLagUnavailableTopics: 2,
      consumerGroupLagCoverage: {
        complete: false,
        resolvedGroups: 9,
        totalGroups: 10,
        unavailableGroups: 1,
      },
    };
    expect(kafkaHealthCoverageLabel(exact)).toBe("Exact · 10/10 groups");
    expect(kafkaHealthCoverageState(exact)).toBe("exact");
    expect(kafkaHealthCoverageLabel(lowerBound)).toBe(
      "Lower bound · 9/10 groups · 1 group unavailable, 2 topics unavailable",
    );
    expect(kafkaHealthCoverageState(lowerBound)).toBe("lower-bound");
    expect(kafkaHealthNeedsCoverageNotice(exact)).toBe(false);
    expect(kafkaHealthNeedsCoverageNotice(lowerBound)).toBe(true);
    expect(kafkaHealthNeedsCoverageNotice({ onlineBrokers: 3, topologyMeasuredAt: 1_000 })).toBe(false);
    expect(kafkaHealthCoverageState({ onlineBrokers: 3, topologyMeasuredAt: 1_000 })).toBe("unavailable");
    const inconsistentComplete = {
      ...exact,
      consumerGroupLagCoverage: { ...exact.consumerGroupLagCoverage, resolvedGroups: 9 },
    };
    expect(kafkaHealthCoverageLabel(inconsistentComplete)).toBe("Lower bound · 9/10 groups");
    expect(kafkaHealthCoverageState(inconsistentComplete)).toBe("lower-bound");
    expect(kafkaHealthNeedsCoverageNotice(inconsistentComplete)).toBe(true);
    expect(
      kafkaHealthNeedsCoverageNotice({
        ...lowerBound,
        consumerGroupLagUnavailableGroups: 0,
        consumerGroupLagUnavailableTopics: 0,
      }),
    ).toBe(true);
  });
});

describe("topic view model", () => {
  it("filters message key, value and headers without decoding binary payloads", () => {
    const message: KafkaRecordDto = {
      topic: "orders",
      partition: 0,
      offset: "1",
      timestamp: "1000",
      key: { format: "text", byteLength: 5, truncated: false, text: "order-1" },
      value: { format: "json", byteLength: 18, truncated: false, text: '{"state":"error"}' },
      headers: [
        {
          name: "x-trace-id",
          value: { format: "text", byteLength: 3, truncated: false, text: "abc" },
        },
      ],
    };

    expect(
      matchesKafkaMessageFilters(message, {
        key: "order",
        value: "/ERROR/i",
        headerKey: "x-trace-id",
        headerValue: "abc",
      }),
    ).toBe(true);
    expect(matchesKafkaMessageFilters(message, { key: "payment", value: "", headerKey: "", headerValue: "" })).toBe(
      false,
    );
    expect(matchesKafkaMessageFilters(message, { key: "", value: "", headerKey: "missing", headerValue: "" })).toBe(
      false,
    );
    expect(matchesKafkaMessageFilters(message, { key: "", value: "/[", headerKey: "", headerValue: "" })).toBe(false);
  });

  it("sorts arbitrary-precision decimal strings without converting them to numbers", () => {
    const values = ["10", "2", "90071992547409930", "—", "100"];
    expect(
      values.sort((left, right) => {
        const [leftLength, leftValue] = kafkaDecimalSortKey(left);
        const [rightLength, rightValue] = kafkaDecimalSortKey(right);
        return leftLength - rightLength || leftValue.localeCompare(rightValue);
      }),
    ).toEqual(["—", "2", "10", "100", "90071992547409930"]);
  });

  it("filters topic names case-insensitively in deterministic order", () => {
    expect(filterTopicNames(["payments", "__consumer_offsets", "Orders"], "ER")).toEqual([
      "__consumer_offsets",
      "Orders",
    ]);
    expect(filterTopicNames(["payments", "Orders"], "")).toEqual(["Orders", "payments"]);
  });

  it("prioritizes unavailable and under-replicated partition states", () => {
    const partition = {
      partitionId: 0,
      leader: 1,
      replicas: [1, 2],
      isr: [1, 2],
      offlineReplicas: [],
      errorCode: 0,
      underReplicated: false,
      unavailable: false,
    };
    expect(topicPartitionHealth(partition)).toBe("Healthy");
    expect(topicPartitionHealth({ ...partition, underReplicated: true })).toBe("Under-replicated");
    expect(topicPartitionHealth({ ...partition, underReplicated: true, unavailable: true })).toBe("Unavailable");
  });
});
