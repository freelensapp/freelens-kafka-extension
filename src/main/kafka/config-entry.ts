import type { ConfigEntries, ConfigSource } from "kafkajs";

import type { KafkaConfigEntryDto } from "../../common/ipc";

function configSourceLabel(source: ConfigSource): string {
  switch (source) {
    case 1:
      return "DYNAMIC_TOPIC_CONFIG";
    case 2:
      return "DYNAMIC_BROKER_CONFIG";
    case 3:
      return "DYNAMIC_DEFAULT_BROKER_CONFIG";
    case 4:
      return "STATIC_BROKER_CONFIG";
    case 5:
      return "DEFAULT";
    case 6:
      return "DYNAMIC_BROKER_LOGGER_CONFIG";
    default:
      return "UNKNOWN";
  }
}

export function toConfigEntries(entries: ConfigEntries[]): KafkaConfigEntryDto[] {
  return entries
    .map((entry) => ({
      name: entry.configName,
      value: entry.isSensitive ? "****" : entry.configValue,
      source: configSourceLabel(entry.configSource),
      readOnly: entry.readOnly,
      sensitive: entry.isSensitive,
    }))
    .sort((left, right) => left.name.localeCompare(right.name));
}
