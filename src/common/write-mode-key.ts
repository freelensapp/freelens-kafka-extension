/**
 * The key, in the extension store, of the targets whose write mode is on: a JSON array of target ids. The renderer
 * writes it; main reads its own copy of the store, which the host keeps in sync (SPEC-009, SPEC-019 REQ-233).
 */
export const KAFKA_WRITE_MODE_KEY = "freelens-kafka.write-mode.v1";

/** The targets of a stored value; none for a value that is not an array of ids. */
export function parseWriteModeTargets(stored: string | null | undefined): Set<string> {
  if (!stored) return new Set();
  try {
    const parsed = JSON.parse(stored) as unknown;

    return new Set(
      Array.isArray(parsed) ? parsed.filter((id): id is string => typeof id === "string" && id !== "") : [],
    );
  } catch {
    return new Set();
  }
}
