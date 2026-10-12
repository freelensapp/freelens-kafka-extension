import type { TopicSizesDto } from "../common/ipc";

const UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"] as const;

/** Human-readable binary size ("1.50 KiB", "10.0 MiB", "4.66 GiB"); "n/a" for invalid input. */
export function formatBytes(value: string | number | bigint): string {
  let bytes = Number(value);
  if (typeof value === "string" && value.trim() === "") bytes = Number.NaN;
  if (!Number.isFinite(bytes) || bytes < 0) return "n/a";
  let unit = 0;
  while (bytes >= 1024 && unit < UNITS.length - 1) {
    bytes /= 1024;
    unit += 1;
  }
  const digits = unit === 0 ? 0 : bytes >= 100 ? 0 : bytes >= 10 ? 1 : 2;
  return `${bytes.toFixed(digits)} ${UNITS[unit]}`;
}

/**
 * Hover text of a topic size: the leader and replica footprint, the warning of a lower bound and
 * what the size reader had to work around (a retry, a broker left out with its reason).
 */
export function topicSizeTitle(
  size: { leaderBytes: string; replicaBytes: string; exact: boolean },
  notes: readonly string[] = [],
): string {
  const footprint = `Leader replicas ${formatBytes(size.leaderBytes)}, all replicas ${formatBytes(size.replicaBytes)}`;
  const bound = size.exact ? "" : " (lower bound: a broker did not report)";
  return `${footprint}${bound}${notes.length > 0 ? `. Size reader: ${notes.join("; ")}` : ""}`;
}

/** What a page knows of the topic sizes of a cluster: the last read, or why it failed. */
export interface TopicSizeRead {
  data?: TopicSizesDto;
  error?: string;
}

/**
 * The size of one topic as a page shows it: the text, its hover text and whether it is a
 * placeholder. `n/a` when the read failed (with the reason) or the brokers do not offer the API,
 * `—` when the read has no size for the topic, `…` while the read is running.
 */
export function topicSizeText(
  read: TopicSizeRead | undefined,
  topic: string,
): { text: string; title?: string; placeholder: boolean } {
  const data = read?.data;
  if (data) {
    if (!data.supported) return { text: "n/a", title: "The brokers do not offer DescribeLogDirs", placeholder: true };
    const size = data.topics[topic];
    if (!size) return { text: "—", placeholder: true };
    return {
      text: `${size.exact ? "" : "≥ "}${formatBytes(size.leaderBytes)}`,
      title: topicSizeTitle(size, data.notes),
      placeholder: false,
    };
  }
  if (read?.error) return { text: "n/a", title: read.error, placeholder: true };
  return { text: "…", placeholder: true };
}

/** Sum of decimal byte strings (int64 as strings), as a string; invalid entries count as zero. */
export function sumBytes(values: Iterable<string | undefined>): string {
  let total = 0n;
  for (const value of values) {
    if (!value) continue;
    try {
      const parsed = BigInt(value);
      if (parsed > 0n) total += parsed;
    } catch {
      // ignore malformed entries
    }
  }
  return total.toString();
}
