import { KAFKA_WRITE_MODE_KEY, parseWriteModeTargets } from "../common/write-mode-key";

/** What the guard reads of the extension store: main's own copy, which the host keeps in sync with the renderer. */
export interface WriteModeStorage {
  getItem(key: string): string | null;
}

/**
 * The write guard of the main process (SPEC-009 REQ-197): a write IPC call is refused for a target whose write mode is
 * not on in the extension store. It reads the persisted state at every call, so it holds before any cluster frame has
 * started and whatever the frames did, and a renderer bug cannot reach a broker with a write (SPEC-019, REQ-233).
 */
export class WriteModeGuard {
  constructor(private readonly storage: () => WriteModeStorage) {}

  isEnabled(targetId: string | undefined): boolean {
    if (!targetId) return false;
    try {
      return parseWriteModeTargets(this.storage().getItem(KAFKA_WRITE_MODE_KEY)).has(targetId);
    } catch {
      return false;
    }
  }

  /** Throws when the operation targets a Kafka target whose write mode is not on. */
  assertEnabled(targetId: string | undefined, operation: string): void {
    if (this.isEnabled(targetId)) return;
    throw new Error(
      `${operation} refused: write mode is not enabled for this Kafka target. Enable it in the connection settings first.`,
    );
  }
}
