import { type KafkaEtaEstimatorState, kafkaPhasePercent, updateKafkaEta } from "../../common/kafka-phase-progress";

import type { KafkaProgressEvent, KafkaProgressOperation } from "../../common/ipc";

export type KafkaProgressUpdate = Omit<KafkaProgressEvent, "operationId" | "operation">;
export type KafkaProgressReporter = (update: KafkaProgressUpdate) => void;

/** Clamp and enforce monotonic progress for one correlated operation. */
export function createProgressReporter(options: {
  operationId?: string;
  operation: KafkaProgressOperation;
  emit: (event: KafkaProgressEvent) => void;
  now?: () => number;
}): KafkaProgressReporter {
  const now = options.now ?? Date.now;
  const startedAt = now();
  let previousUpdateAt = startedAt;
  let phaseStartedAt = startedAt;
  let previousPhase: string | undefined;
  let etaState: KafkaEtaEstimatorState = { positiveSamples: 0 };
  let lastValue = 0;
  let stallTimer: ReturnType<typeof setTimeout> | undefined;
  const clearStallTimer = () => {
    if (stallTimer !== undefined) clearTimeout(stallTimer);
    stallTimer = undefined;
  };
  return (update) => {
    if (!options.operationId) return;
    clearStallTimer();
    const updatedAt = now();
    if (update.phase !== previousPhase) {
      previousPhase = update.phase;
      phaseStartedAt = updatedAt;
      etaState = { positiveSamples: 0 };
    }
    const value = Math.max(lastValue, Math.min(100, Math.max(0, Math.round(update.value))));
    const complete = value >= 100 || update.phase === "complete";
    const phasePercent = kafkaPhasePercent({ completed: update.completed, total: update.total, complete });
    const estimate = updateKafkaEta(etaState, {
      at: updatedAt,
      completed: update.completed,
      phase: update.phase,
      total: update.total,
    });
    etaState = estimate.state;
    lastValue = value;
    const event: KafkaProgressEvent = {
      ...update,
      value,
      phasePercent,
      phaseElapsedMs: Math.max(0, updatedAt - phaseStartedAt),
      etaStatus: complete ? "complete" : estimate.status,
      ...(complete ? { etaMs: 0 } : estimate.etaMs === undefined ? {} : { etaMs: estimate.etaMs }),
      elapsedMs: Math.max(0, updatedAt - startedAt),
      stageDurationMs: Math.max(0, updatedAt - previousUpdateAt),
      operationId: options.operationId,
      operation: options.operation,
    };
    options.emit(event);
    previousUpdateAt = updatedAt;
    if (event.etaStatus === "ready") {
      stallTimer = setTimeout(() => {
        stallTimer = undefined;
        const stalledAt = now();
        const { etaMs: _etaMs, ...previousEvent } = event;
        options.emit({
          ...previousEvent,
          etaStatus: "stalled",
          elapsedMs: Math.max(0, stalledAt - startedAt),
          phaseElapsedMs: Math.max(0, stalledAt - phaseStartedAt),
          stageDurationMs: Math.max(0, stalledAt - previousUpdateAt),
        });
        previousUpdateAt = stalledAt;
      }, 10_000);
      stallTimer.unref?.();
    }
  };
}
