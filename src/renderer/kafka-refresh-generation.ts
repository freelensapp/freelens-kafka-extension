export interface KafkaRefreshGeneration {
  health: number;
  metadata: number;
}

export function nextKafkaRefreshGeneration(
  current: KafkaRefreshGeneration,
  mode: "health" | "full",
): KafkaRefreshGeneration {
  return {
    health: current.health + 1,
    metadata: current.metadata + (mode === "full" ? 1 : 0),
  };
}
