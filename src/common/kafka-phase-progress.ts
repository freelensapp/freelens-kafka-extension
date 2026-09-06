export type KafkaEtaStatus = "complete" | "ready" | "stalled" | "unavailable" | "warming";

export interface KafkaEtaEstimatorState {
  firstAt?: number;
  lastAt?: number;
  lastCompleted?: number;
  lastProgressAt?: number;
  phase?: string;
  positiveSamples: number;
  smoothedRatePerMs?: number;
  total?: number;
}

export interface KafkaEtaSample {
  at: number;
  completed?: number;
  phase: string;
  total?: number;
}

export interface KafkaEtaEstimate {
  etaMs?: number;
  state: KafkaEtaEstimatorState;
  status: KafkaEtaStatus;
}

export interface KafkaEtaOptions {
  alpha?: number;
  minElapsedMs?: number;
  minFraction?: number;
  minPositiveSamples?: number;
  stallMs?: number;
}

export function kafkaPhasePercent(options: {
  completed?: number;
  complete?: boolean;
  total?: number;
}): number | undefined {
  if (options.complete) return 100;
  if (options.total === undefined || options.completed === undefined) return undefined;
  if (options.total <= 0) return 100;
  const ratio = options.completed / options.total;
  return Math.max(0, Math.min(100, Math.round(ratio * 100)));
}

export function updateKafkaEta(
  previous: KafkaEtaEstimatorState,
  sample: KafkaEtaSample,
  {
    alpha = 0.35,
    minElapsedMs = 1_000,
    minFraction = 0.2,
    minPositiveSamples = 2,
    stallMs = 10_000,
  }: KafkaEtaOptions = {},
): KafkaEtaEstimate {
  const completed = sample.completed;
  const total = sample.total;
  if (completed === undefined || total === undefined || total <= 0) {
    return {
      state: { phase: sample.phase, positiveSamples: 0, total },
      status: total === 0 ? "complete" : "unavailable",
      ...(total === 0 ? { etaMs: 0 } : {}),
    };
  }

  const reset =
    previous.phase !== sample.phase ||
    previous.total !== total ||
    previous.lastAt === undefined ||
    previous.lastCompleted === undefined ||
    sample.at < previous.lastAt ||
    completed < previous.lastCompleted;
  if (reset) {
    return {
      state: {
        firstAt: sample.at,
        lastAt: sample.at,
        lastCompleted: completed,
        lastProgressAt: sample.at,
        phase: sample.phase,
        positiveSamples: 0,
        total,
      },
      status: completed >= total ? "complete" : "warming",
      ...(completed >= total ? { etaMs: 0 } : {}),
    };
  }

  const previousLastAt = previous.lastAt as number;
  const previousLastCompleted = previous.lastCompleted as number;
  const deltaMs = sample.at - previousLastAt;
  const deltaCompleted = completed - previousLastCompleted;
  let smoothedRatePerMs = previous.smoothedRatePerMs;
  let positiveSamples = previous.positiveSamples;
  let lastProgressAt = previous.lastProgressAt ?? previousLastAt;
  if (deltaMs > 0 && deltaCompleted > 0) {
    const instantRate = deltaCompleted / deltaMs;
    smoothedRatePerMs =
      smoothedRatePerMs === undefined
        ? instantRate
        : Math.max(0, Math.min(1, alpha)) * instantRate + (1 - Math.max(0, Math.min(1, alpha))) * smoothedRatePerMs;
    positiveSamples++;
    lastProgressAt = sample.at;
  }

  const state: KafkaEtaEstimatorState = {
    ...previous,
    lastAt: sample.at,
    lastCompleted: completed,
    lastProgressAt,
    positiveSamples,
    smoothedRatePerMs,
  };
  if (completed >= total) return { etaMs: 0, state, status: "complete" };
  if (sample.at - lastProgressAt >= stallMs) return { state, status: "stalled" };

  const elapsedMs = sample.at - (previous.firstAt ?? sample.at);
  const fraction = completed / total;
  if (
    elapsedMs < minElapsedMs ||
    fraction < minFraction ||
    positiveSamples < minPositiveSamples ||
    smoothedRatePerMs === undefined ||
    smoothedRatePerMs <= 0
  ) {
    return { state, status: "warming" };
  }

  return {
    etaMs: Math.max(0, Math.round((total - completed) / smoothedRatePerMs)),
    state,
    status: "ready",
  };
}

export function formatKafkaEta(etaMs: number): string {
  if (etaMs < 1_000) return "<1s remaining";
  const seconds = Math.ceil(etaMs / 1_000);
  if (seconds < 60) return `${seconds}s remaining`;
  const minutes = Math.ceil(seconds / 60);
  return `${minutes}m remaining`;
}
