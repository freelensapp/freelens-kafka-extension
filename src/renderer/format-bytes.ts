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
