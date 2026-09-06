export interface PerformanceStageEvidence {
  durationMs: number;
  p95DurationMs?: number;
  sampleCount?: number;
  requestCount?: number;
  protocolRequests?: {
    fetchTopicOffsetsFallback?: number;
    fetchOffsetsFallback?: number;
    findCoordinator?: number;
    offsetFetch?: number;
    listOffsets?: number;
    lowOffsets?: number;
    offsetFetchGroups?: number;
  };
}

export interface AggregateHealthPerformanceEvidence extends PerformanceStageEvidence {
  capabilities?: {
    batchSupported: boolean;
    findCoordinator: { maxVersion: number; minVersion: number } | null;
    offsetFetch: { maxVersion: number; minVersion: number } | null;
  };
  batch: {
    brokers: number;
    partitions: number;
    topics: number;
  };
  groupBatch?: {
    findCoordinatorRequests: number;
    offsetFetchGroups: number;
    offsetFetchRequests: number;
    publicFallbacks: number;
  };
  coverage: {
    exact: boolean;
    unavailableGroups?: number;
    unavailableTopics: number;
  };
  phases: {
    groups: PerformanceStageEvidence;
    topology: PerformanceStageEvidence;
    watermarks: PerformanceStageEvidence;
  };
  warmCache?: PerformanceStageEvidence & {
    loaderCalls: number;
  };
  runs: Array<{
    captureId: string;
    capturedAt: string;
    sourceFileId: string;
    durationMs: number;
    operationsMs: {
      credentialResolution: number;
      discovery: number;
      groupDetail: number;
      groups: number;
      overview: number;
      reachability: number;
      topicConsumers: number;
      topics: number;
    };
    phasesMs: {
      groups: number;
      topology: number;
      watermarks: number;
    };
    protocolRequests: NonNullable<PerformanceStageEvidence["protocolRequests"]>;
    warmCache: {
      durationMs: number;
      loaderCalls: number;
      requestCount: number;
    };
  }>;
}

export interface PackagedBrowserPaintEvidence {
  aggregateStillRunning: true;
  firstUsefulPaintMs: number;
  interactiveWhileUpdating: true;
  metadataPaintMs: number;
  progressContinued: true;
  topologyPaintMs: number;
}

export interface PackagedBrowserPerformanceEvidence {
  capturedAt: string;
  mode: "authorized-read-only";
  packageSha256: string;
  authorizationPinsVerified: true;
  freelens: {
    firstUsefulPaint: Required<Pick<PerformanceStageEvidence, "durationMs" | "p95DurationMs" | "sampleCount">>;
    metadataPaint: Required<Pick<PerformanceStageEvidence, "durationMs" | "p95DurationMs" | "sampleCount">>;
    topologyPaint: Required<Pick<PerformanceStageEvidence, "durationMs" | "p95DurationMs" | "sampleCount">>;
    aggregateStillRunning: true;
    interactiveWhileUpdating: true;
    progressContinued: true;
  };
  akhq: {
    provider: "akhq";
    version: string;
    digest: string;
    method: "GET";
    topicPaint: Required<Pick<PerformanceStageEvidence, "durationMs" | "p95DurationMs" | "sampleCount">>;
  };
  runs: Array<{
    akhqTopicPaintMs: number;
    freelens: PackagedBrowserPaintEvidence;
    preparationAttempts: number;
  }>;
}

export interface PerformanceEvidence {
  schemaVersion: 1;
  capturedAt: string;
  mode: "local" | "authorized-read-only";
  scale: {
    workloads?: number;
    targets?: number;
    brokers?: number;
    topics?: number;
    consumerGroups?: number;
  };
  extension: {
    discovery?: PerformanceStageEvidence;
    reachability?: PerformanceStageEvidence;
    credentialResolution?: PerformanceStageEvidence;
    overview?: PerformanceStageEvidence;
    topics?: PerformanceStageEvidence;
    groups?: PerformanceStageEvidence;
    topicConsumers?: PerformanceStageEvidence;
    groupDetail?: PerformanceStageEvidence;
    aggregateHealth?: AggregateHealthPerformanceEvidence;
  };
  comparison?: {
    provider: "akhq";
    version?: string;
    digest?: string;
    method?: "GET";
    topics?: PerformanceStageEvidence & { coldDurationMs?: number; runs?: number[] };
    consumerGroups?: PerformanceStageEvidence & { coldDurationMs?: number; runs?: number[] };
    completeGlobalLagEquivalent?: boolean;
    cleanup?: {
      createdContainerRemoved: boolean;
      imageRemovedOrPreexisting: boolean;
      kubernetesReady: boolean;
      temporaryConfigurationRemoved: boolean;
    };
  };
  packagedBrowser?: PackagedBrowserPerformanceEvidence;
}

export function createPerformanceEvidence(evidence: Omit<PerformanceEvidence, "schemaVersion">): PerformanceEvidence {
  return { schemaVersion: 1, ...evidence };
}

export function totalDurationMs(...stages: Array<PerformanceStageEvidence | undefined>): number {
  return stages.reduce((total, stage) => total + (stage?.durationMs ?? 0), 0);
}

export function summarizePerformanceDurations(
  samples: number[],
): Required<Pick<PerformanceStageEvidence, "durationMs" | "p95DurationMs" | "sampleCount">> {
  if (
    samples.length === 0 ||
    samples.some((sample) => !Number.isFinite(sample) || !Number.isInteger(sample) || sample < 0)
  ) {
    throw new Error("performance duration samples must be finite non-negative integers");
  }
  const sorted = [...samples].sort((left, right) => left - right);
  const middle = Math.floor(sorted.length / 2);
  const median = sorted.length % 2 === 0 ? Math.round((sorted[middle - 1] + sorted[middle]) / 2) : sorted[middle];
  const p95 = sorted[Math.max(0, Math.ceil(sorted.length * 0.95) - 1)];
  return { durationMs: median, p95DurationMs: p95, sampleCount: sorted.length };
}

const SENSITIVE_EVIDENCE_KEYS = new Set([
  "authorizedcontext",
  "bootstrap",
  "context",
  "credential",
  "credentials",
  "endpoint",
  "endpoints",
  "groupid",
  "host",
  "hostname",
  "key",
  "password",
  "privatekey",
  "sasl",
  "secret",
  "secrets",
  "security",
  "ssl",
  "target",
  "targethost",
  "tls",
  "topicname",
  "username",
]);

export function assertSanitizedPerformanceEvidence(value: unknown): void {
  const visit = (candidate: unknown): void => {
    if (Array.isArray(candidate)) {
      for (const item of candidate) visit(item);
      return;
    }
    if (!candidate || typeof candidate !== "object") return;
    for (const [key, child] of Object.entries(candidate)) {
      const normalizedKey = key.toLowerCase().replaceAll(/[^a-z0-9]/g, "");
      if (SENSITIVE_EVIDENCE_KEYS.has(normalizedKey)) {
        throw new Error(`performance evidence contains sensitive field: ${key}`);
      }
      visit(child);
    }
  };
  visit(value);
}

export function assertPerformanceEvidence(value: unknown): asserts value is PerformanceEvidence {
  if (!value || typeof value !== "object") throw new Error("performance evidence must be an object");
  const evidence = value as Partial<PerformanceEvidence>;
  if (evidence.schemaVersion !== 1) throw new Error("unsupported performance evidence schema");
  if (typeof evidence.capturedAt !== "string" || !evidence.capturedAt) {
    throw new Error("performance evidence requires capturedAt");
  }
  if (evidence.mode !== "local" && evidence.mode !== "authorized-read-only") {
    throw new Error("performance evidence requires a valid mode");
  }
  if (!evidence.scale || typeof evidence.scale !== "object") {
    throw new Error("performance evidence requires scale");
  }
  if (!evidence.extension || typeof evidence.extension !== "object") {
    throw new Error("performance evidence requires extension stages");
  }
}

function requireNonNegativeInteger(value: unknown, label: string, positive = false): number {
  if (typeof value !== "number" || !Number.isFinite(value) || !Number.isInteger(value) || value < (positive ? 1 : 0)) {
    throw new Error(`${label} must be a finite ${positive ? "positive" : "non-negative"} integer`);
  }
  return value;
}

function requireStageSummary(
  value: PerformanceStageEvidence | undefined,
  label: string,
  expectedRunCount: number,
): PerformanceStageEvidence {
  if (!value) throw new Error(`final performance evidence requires ${label}`);
  requireNonNegativeInteger(value.durationMs, `${label}.durationMs`);
  requireNonNegativeInteger(value.p95DurationMs, `${label}.p95DurationMs`);
  if (requireNonNegativeInteger(value.sampleCount, `${label}.sampleCount`, true) !== expectedRunCount) {
    throw new Error(`${label} requires exactly ${expectedRunCount} samples`);
  }
  return value;
}

const PROTOCOL_REQUEST_KEYS = [
  "fetchOffsetsFallback",
  "fetchTopicOffsetsFallback",
  "findCoordinator",
  "listOffsets",
  "lowOffsets",
  "offsetFetch",
  "offsetFetchGroups",
] as const;

function requireProtocolRequests(
  value: PerformanceStageEvidence["protocolRequests"],
  label: string,
): Required<NonNullable<PerformanceStageEvidence["protocolRequests"]>> {
  if (!value) throw new Error(`${label} requires protocol request counts`);
  for (const key of PROTOCOL_REQUEST_KEYS) requireNonNegativeInteger(value[key], `${label}.${key}`);
  return value as Required<NonNullable<PerformanceStageEvidence["protocolRequests"]>>;
}

function requireApiRange(value: unknown, label: string): void {
  if (!value || typeof value !== "object") throw new Error(`${label} requires a negotiated API range`);
  const range = value as { maxVersion?: unknown; minVersion?: unknown };
  const minVersion = requireNonNegativeInteger(range.minVersion, `${label}.minVersion`);
  const maxVersion = requireNonNegativeInteger(range.maxVersion, `${label}.maxVersion`);
  if (maxVersion < minVersion) throw new Error(`${label} has an invalid negotiated API range`);
}

function requireIsoTimestamp(value: unknown, label: string): string {
  if (typeof value !== "string") throw new Error(`${label} must be an ISO timestamp`);
  try {
    if (new Date(value).toISOString() !== value) throw new Error();
  } catch {
    throw new Error(`${label} must be an ISO timestamp`);
  }
  return value;
}

function requireCaptureId(value: unknown, label: string): string {
  if (
    typeof value !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
  ) {
    throw new Error(`${label} must be a UUID v4`);
  }
  return value;
}

function requireMatchingSummary(summary: PerformanceStageEvidence | undefined, samples: number[], label: string): void {
  if (!summary) throw new Error(`${label} summary is unavailable`);
  const expected = summarizePerformanceDurations(samples);
  if (
    summary.durationMs !== expected.durationMs ||
    summary.p95DurationMs !== expected.p95DurationMs ||
    summary.sampleCount !== expected.sampleCount
  ) {
    throw new Error(`${label} summary does not match its retained samples`);
  }
}

export function assertFinalPerformanceEvidence(
  value: unknown,
  expectedRunCount = 3,
): asserts value is PerformanceEvidence {
  assertPerformanceEvidence(value);
  assertSanitizedPerformanceEvidence(value);
  requireIsoTimestamp(value.capturedAt, "final performance evidence capturedAt");
  for (const key of ["workloads", "targets", "brokers", "topics", "consumerGroups"] as const) {
    requireNonNegativeInteger(value.scale[key], `scale.${key}`, true);
  }
  const requiredStages = {
    discovery: value.extension.discovery,
    reachability: value.extension.reachability,
    credentialResolution: value.extension.credentialResolution,
    overview: value.extension.overview,
    topics: value.extension.topics,
    groups: value.extension.groups,
    groupDetail: value.extension.groupDetail,
    topicConsumers: value.extension.topicConsumers,
  };
  for (const [label, stage] of Object.entries(requiredStages)) {
    requireStageSummary(stage, label, expectedRunCount);
  }
  const aggregate = value.extension.aggregateHealth;
  if (!aggregate) throw new Error("final performance evidence requires aggregate health");
  if (!Array.isArray(aggregate.runs) || aggregate.runs.length !== expectedRunCount) {
    throw new Error(`final performance evidence requires exactly ${expectedRunCount} aggregate runs`);
  }
  requireStageSummary(aggregate, "aggregateHealth", expectedRunCount);
  requireNonNegativeInteger(aggregate.requestCount, "aggregateHealth.requestCount", true);
  if (aggregate.capabilities?.batchSupported !== true) {
    throw new Error("final performance evidence requires supported batch APIs");
  }
  requireApiRange(aggregate.capabilities.findCoordinator, "aggregateHealth.findCoordinator");
  requireApiRange(aggregate.capabilities.offsetFetch, "aggregateHealth.offsetFetch");
  for (const [key, count] of Object.entries(aggregate.batch)) {
    requireNonNegativeInteger(count, `aggregateHealth.batch.${key}`, true);
  }
  if (
    aggregate.coverage.exact !== true ||
    aggregate.coverage.unavailableGroups !== 0 ||
    aggregate.coverage.unavailableTopics !== 0
  ) {
    throw new Error("final performance evidence requires exact aggregate coverage");
  }
  const groupBatch = aggregate.groupBatch;
  if (!groupBatch) throw new Error("final performance evidence requires group batch counts");
  requireNonNegativeInteger(
    groupBatch.findCoordinatorRequests,
    "aggregateHealth.groupBatch.findCoordinatorRequests",
    true,
  );
  requireNonNegativeInteger(groupBatch.offsetFetchGroups, "aggregateHealth.groupBatch.offsetFetchGroups", true);
  requireNonNegativeInteger(groupBatch.offsetFetchRequests, "aggregateHealth.groupBatch.offsetFetchRequests", true);
  if (requireNonNegativeInteger(groupBatch.publicFallbacks, "aggregateHealth.groupBatch.publicFallbacks") !== 0) {
    throw new Error("final performance evidence requires zero public group fallbacks");
  }
  const aggregateRequests = requireProtocolRequests(aggregate.protocolRequests, "aggregateHealth.requests");
  requireNonNegativeInteger(aggregateRequests.findCoordinator, "aggregateHealth.requests.findCoordinator", true);
  requireNonNegativeInteger(aggregateRequests.listOffsets, "aggregateHealth.requests.listOffsets", true);
  requireNonNegativeInteger(aggregateRequests.offsetFetch, "aggregateHealth.requests.offsetFetch", true);
  requireNonNegativeInteger(aggregateRequests.offsetFetchGroups, "aggregateHealth.requests.offsetFetchGroups", true);
  if (
    aggregateRequests.fetchOffsetsFallback !== 0 ||
    aggregateRequests.fetchTopicOffsetsFallback !== 0 ||
    aggregateRequests.lowOffsets !== 0
  ) {
    throw new Error("final performance evidence requires the lossless supported batch path");
  }
  const expectedRequestCount =
    aggregateRequests.findCoordinator + aggregateRequests.offsetFetch + aggregateRequests.listOffsets;
  if (aggregate.requestCount !== expectedRequestCount) {
    throw new Error("aggregateHealth.requestCount must count protocol calls only");
  }
  if (aggregate.durationMs > 30_000 || aggregate.phases.watermarks.durationMs > 10_000) {
    throw new Error("final performance evidence exceeds aggregate health SLOs");
  }
  for (const [label, phase] of Object.entries(aggregate.phases)) {
    requireStageSummary(phase, `aggregateHealth.phases.${label}`, expectedRunCount);
  }
  const warmCache = requireStageSummary(aggregate.warmCache, "aggregateHealth.warmCache", expectedRunCount);
  if (warmCache.durationMs > 500 || aggregate.warmCache?.loaderCalls !== 0 || aggregate.warmCache.requestCount !== 0) {
    throw new Error("final performance evidence requires a request-free warm cache hit within 500 ms");
  }
  const captureIds = new Set<string>();
  const captureTimestamps = new Set<string>();
  const sourceFileIds = new Set<string>();
  for (const [index, run] of aggregate.runs.entries()) {
    captureIds.add(requireCaptureId(run.captureId, `aggregateHealth.runs.${index}.captureId`));
    captureTimestamps.add(requireIsoTimestamp(run.capturedAt, `aggregateHealth.runs.${index}.capturedAt`));
    if (!/^[0-9a-f]{64}$/i.test(run.sourceFileId)) {
      throw new Error(`aggregateHealth.runs.${index}.sourceFileId must be an opaque SHA-256`);
    }
    sourceFileIds.add(run.sourceFileId);
    requireNonNegativeInteger(run.durationMs, `aggregateHealth.runs.${index}.durationMs`);
    for (const [label, duration] of Object.entries(run.phasesMs)) {
      requireNonNegativeInteger(duration, `aggregateHealth.runs.${index}.phases.${label}`);
    }
    for (const [label, duration] of Object.entries(run.operationsMs)) {
      requireNonNegativeInteger(duration, `aggregateHealth.runs.${index}.operations.${label}`);
    }
    requireNonNegativeInteger(run.warmCache.durationMs, `aggregateHealth.runs.${index}.warmCache.durationMs`);
    if (
      requireNonNegativeInteger(run.warmCache.loaderCalls, `aggregateHealth.runs.${index}.warmCache.loaderCalls`) !==
        0 ||
      requireNonNegativeInteger(run.warmCache.requestCount, `aggregateHealth.runs.${index}.warmCache.requestCount`) !==
        0
    ) {
      throw new Error("final performance evidence requires request-free warm run samples");
    }
    const requests = requireProtocolRequests(run.protocolRequests, `aggregateHealth.runs.${index}.requests`);
    if (requests.fetchOffsetsFallback !== 0 || requests.fetchTopicOffsetsFallback !== 0 || requests.lowOffsets !== 0) {
      throw new Error("final performance evidence requires the lossless supported batch path");
    }
  }
  if (
    captureIds.size !== expectedRunCount ||
    captureTimestamps.size !== expectedRunCount ||
    sourceFileIds.size !== expectedRunCount
  ) {
    throw new Error("final performance evidence requires unique run captures");
  }
  const operationStages = {
    discovery: value.extension.discovery,
    reachability: value.extension.reachability,
    credentialResolution: value.extension.credentialResolution,
    overview: value.extension.overview,
    topics: value.extension.topics,
    groups: value.extension.groups,
    groupDetail: value.extension.groupDetail,
    topicConsumers: value.extension.topicConsumers,
  };
  for (const [label, summary] of Object.entries(operationStages)) {
    requireMatchingSummary(
      summary,
      aggregate.runs.map((run) => run.operationsMs[label as keyof typeof run.operationsMs]),
      label,
    );
  }
  requireMatchingSummary(
    aggregate,
    aggregate.runs.map((run) => run.durationMs),
    "aggregateHealth",
  );
  for (const [label, summary] of Object.entries(aggregate.phases)) {
    requireMatchingSummary(
      summary,
      aggregate.runs.map((run) => run.phasesMs[label as keyof typeof run.phasesMs]),
      `aggregateHealth.phases.${label}`,
    );
  }
  requireMatchingSummary(
    aggregate.warmCache,
    aggregate.runs.map((run) => run.warmCache.durationMs),
    "aggregateHealth.warmCache",
  );
}
