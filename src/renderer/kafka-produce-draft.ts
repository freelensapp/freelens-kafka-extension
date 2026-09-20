/** Header lines of the Produce compose panel (SPEC-009 REQ-207): `key=value`, one per line. */
export interface ParsedProduceHeaders {
  headers: Record<string, string>;
  /** 1-based numbers of the non-empty lines that are not `key=value`. */
  invalidLines: number[];
}

/**
 * Parse the multi-line headers field. Only the first `=` separates the key from the value, so
 * values may contain `=` (base64 padding, query strings). Blank lines are skipped, keys are
 * trimmed, an empty value is a valid header and a repeated key keeps its last value. Pure.
 */
export function parseProduceHeaders(text: string): ParsedProduceHeaders {
  const headers: Record<string, string> = {};
  const invalidLines: number[] = [];

  text.split("\n").forEach((rawLine, index) => {
    const line = rawLine.replace(/\r$/, "");
    if (line.trim() === "") return;

    const separator = line.indexOf("=");
    const key = separator < 0 ? "" : line.slice(0, separator).trim();
    if (!key) {
      invalidLines.push(index + 1);
      return;
    }
    headers[key] = line.slice(separator + 1);
  });

  return { headers, invalidLines };
}

export interface ParsedProducePartition {
  partition?: number;
  error?: string;
}

/**
 * Parse the optional partition field: empty means the default partitioner, anything else must be
 * a partition index of the topic. Pure.
 */
export function parseProducePartition(text: string, partitionCount?: number): ParsedProducePartition {
  const value = text.trim();
  if (value === "") return {};
  if (!/^\d+$/.test(value)) return { error: "Partition must be a whole number." };

  const partition = Number(value);
  if (!Number.isSafeInteger(partition)) return { error: "Partition must be a whole number." };
  if (partitionCount !== undefined && partitionCount > 0 && partition >= partitionCount) {
    return { error: `This topic has partitions 0 to ${partitionCount - 1}.` };
  }

  return { partition };
}

/** Wording for the header lines that cannot be sent. Pure. */
export function describeInvalidHeaderLines(invalidLines: number[]): string | undefined {
  if (invalidLines.length === 0) return undefined;
  const lines = invalidLines.join(", ");

  return invalidLines.length === 1
    ? `Line ${lines} is not in the key=value form.`
    : `Lines ${lines} are not in the key=value form.`;
}
