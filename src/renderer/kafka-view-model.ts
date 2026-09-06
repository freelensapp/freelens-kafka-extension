import { isExactAggregateHealth } from "../common/aggregate-health-snapshot";
import { kafkaPhasePercent } from "../common/kafka-phase-progress";

import type {
  ClusterOverviewHealthDto,
  DiscoveredKafkaInfo,
  KafkaMessageBytesDto,
  KafkaProgressEvent,
  KafkaProgressOperation,
  KafkaRecordDto,
  TopicPartitionDto,
} from "../common/ipc";

export interface KafkaUsageSummary {
  primary: string;
  secondary: string;
  title: string;
  sortValue: string;
  workloadCount: number;
  namespaceCount: number;
}

function plural(count: number, singular: string): string {
  return `${count} ${singular}${count === 1 ? "" : "s"}`;
}

export function kafkaUsageSummary(kafka: DiscoveredKafkaInfo): KafkaUsageSummary {
  if (kafka.source === "manual") {
    return {
      primary: "Manual endpoint",
      secondary: "Not Kubernetes-owned",
      title: "Added manually; no Kubernetes workload usage is known.",
      sortValue: "manual",
      workloadCount: 0,
      namespaceCount: 0,
    };
  }

  if (kafka.source === "workload" && kafka.referencedBy?.length) {
    const namespaces = [
      ...new Set(kafka.referencedBy.map((reference) => reference.split("/")[0]).filter(Boolean)),
    ].sort();
    const workloadCount = kafka.referencedBy.length;
    return {
      primary: plural(workloadCount, "workload"),
      secondary: plural(namespaces.length, "namespace"),
      title: `Used across: ${namespaces.join(", ")}`,
      sortValue: `${String(namespaces.length).padStart(5, "0")}/${String(workloadCount).padStart(5, "0")}`,
      workloadCount,
      namespaceCount: namespaces.length,
    };
  }

  const namespace = kafka.namespace || "default";
  const source = kafka.source === "strimzi" ? "Strimzi resource" : kafka.source === "service" ? "Service" : "Resource";
  return {
    primary: namespace,
    secondary: `${source} namespace`,
    title: `${source} is defined in Kubernetes namespace ${namespace}.`,
    sortValue: namespace.toLowerCase(),
    workloadCount: 0,
    namespaceCount: 1,
  };
}

export function kafkaUsageContext(kafka: DiscoveredKafkaInfo): string {
  const usage = kafkaUsageSummary(kafka);
  if (kafka.source === "workload") return `Used by ${usage.primary} across ${usage.secondary}`;
  if (kafka.source === "manual") return "Manual endpoint · not referenced by Kubernetes";
  return `${usage.secondary}: ${usage.primary}`;
}

export function createOperationId(operation: KafkaProgressOperation): string {
  return `${operation}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

export function clusterHealthMetricValue<T extends number | string>(
  state: { loading: boolean; error?: string },
  measured: T | undefined,
  progressive = false,
): T | "Updating" | "Unavailable" | "—" {
  if (progressive && measured !== undefined) return measured;
  if (state.loading) return "Updating";
  if (state.error) return "Unavailable";
  return measured ?? "—";
}

export function kafkaProgressPhasePercent(
  progress: Pick<KafkaProgressEvent, "completed" | "phasePercent" | "total" | "value">,
): number | undefined {
  return (
    progress.phasePercent ??
    kafkaPhasePercent({
      completed: progress.completed,
      complete: progress.value >= 100,
      total: progress.total,
    })
  );
}

export function kafkaProgressIsDeterminate(
  progress: Pick<KafkaProgressEvent, "completed" | "phasePercent" | "total" | "value">,
): boolean {
  return kafkaProgressPhasePercent(progress) !== undefined;
}

export function kafkaProgressCount(progress: Pick<KafkaProgressEvent, "completed" | "total">): string | undefined {
  return progress.total === undefined ? undefined : `${progress.completed ?? 0}/${progress.total}`;
}

export function kafkaHealthCompletionMetadata(
  current: { source?: "cache" | "network" | "persisted"; updatedAt?: number },
  fallbackUpdatedAt?: number,
): { source: "cache" | "network" | "persisted"; updatedAt?: number } {
  return {
    source: current.source ?? "network",
    updatedAt: current.updatedAt ?? fallbackUpdatedAt,
  };
}

export function kafkaHealthProgressIsCompact(
  progress: Pick<KafkaProgressEvent, "operation" | "phase">,
  error?: string,
  hasSnapshot = false,
): boolean {
  return (
    !error &&
    progress.operation === "health" &&
    (hasSnapshot || ["topology", "groups", "watermarks"].includes(progress.phase))
  );
}

export function kafkaHealthCoverageLabel(health: ClusterOverviewHealthDto | undefined): string {
  if (!health) return "Waiting for first result";
  const coverage = health.consumerGroupLagCoverage;
  if (!coverage) return "Coverage unavailable";
  const exact = isExactAggregateHealth(health);
  const unavailableTopics = health.consumerGroupLagUnavailableTopics ?? 0;
  const unavailableGroups = health.consumerGroupLagUnavailableGroups ?? coverage.unavailableGroups;
  const unavailable = [
    unavailableGroups > 0 ? `${unavailableGroups} group${unavailableGroups === 1 ? "" : "s"} unavailable` : undefined,
    unavailableTopics > 0 ? `${unavailableTopics} topic${unavailableTopics === 1 ? "" : "s"} unavailable` : undefined,
  ].filter((value): value is string => Boolean(value));
  return `${exact ? "Exact" : "Lower bound"} · ${coverage.resolvedGroups}/${coverage.totalGroups} groups${
    unavailable.length > 0 ? ` · ${unavailable.join(", ")}` : ""
  }`;
}

export function kafkaHealthCoverageState(
  health: Partial<ClusterOverviewHealthDto> | undefined,
): "exact" | "lower-bound" | "unavailable" {
  if (!health?.consumerGroupLagCoverage) return "unavailable";
  return isExactAggregateHealth(health) ? "exact" : "lower-bound";
}

export function kafkaHealthNeedsCoverageNotice(health: Partial<ClusterOverviewHealthDto> | undefined): boolean {
  if (!health?.consumerGroupLag || health.consumerGroupLag === "—") return false;
  return !isExactAggregateHealth(health);
}

export function filterTopicNames(topics: string[], query: string): string[] {
  const normalizedQuery = query.trim().toLowerCase();
  return topics
    .filter((topic) => !normalizedQuery || topic.toLowerCase().includes(normalizedQuery))
    .sort((left, right) => left.localeCompare(right));
}

export function kafkaDecimalSortKey(value: string): [number, string] {
  return value === "—" ? [-1, ""] : [value.length, value];
}

function messageBytesSearchText(bytes: KafkaMessageBytesDto): string {
  return [bytes.text, bytes.base64].filter((value): value is string => value !== undefined).join(" ");
}

function matchesFilter(value: string, filter: string): boolean {
  const trimmed = filter.trim();
  if (!trimmed) return true;
  if (!trimmed.startsWith("/") || trimmed.lastIndexOf("/") <= 0) {
    return value.toLowerCase().includes(trimmed.toLowerCase());
  }

  const lastSlash = trimmed.lastIndexOf("/");
  try {
    return new RegExp(trimmed.slice(1, lastSlash), trimmed.slice(lastSlash + 1)).test(value);
  } catch {
    return false;
  }
}

export interface KafkaMessageFilters {
  key: string;
  value: string;
  headerKey: string;
  headerValue: string;
}

export function matchesKafkaMessageFilters(message: KafkaRecordDto, filters: KafkaMessageFilters): boolean {
  if (!matchesFilter(messageBytesSearchText(message.key), filters.key)) return false;
  if (!matchesFilter(messageBytesSearchText(message.value), filters.value)) return false;

  const headerKey = filters.headerKey.trim().toLowerCase();
  if (!headerKey) return true;
  return message.headers.some(
    (header) =>
      header.name.toLowerCase() === headerKey &&
      matchesFilter(messageBytesSearchText(header.value), filters.headerValue),
  );
}

export function topicPartitionHealth(partition: TopicPartitionDto): "Healthy" | "Under-replicated" | "Unavailable" {
  if (partition.unavailable) return "Unavailable";
  if (partition.underReplicated) return "Under-replicated";
  return "Healthy";
}
