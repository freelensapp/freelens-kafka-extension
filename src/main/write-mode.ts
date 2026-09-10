/**
 * Main-process mirror of the renderer's per-target write mode (SPEC-009 REQ-197).
 *
 * The renderer persists the switch and hides every write control while it is off; this
 * registry makes the main process refuse write IPC calls for targets whose write mode was
 * never enabled in this session, so a renderer bug cannot reach a broker with a write.
 * The renderer syncs the state on activation and on every change.
 */
export class WriteModeRegistry {
  private readonly enabled = new Set<string>();

  set(targetId: string, enabled: boolean): void {
    if (!targetId) return;
    if (enabled) this.enabled.add(targetId);
    else this.enabled.delete(targetId);
  }

  isEnabled(targetId: string | undefined): boolean {
    return Boolean(targetId && this.enabled.has(targetId));
  }

  /** Throws when the operation targets a Kafka target whose write mode is not enabled. */
  assertEnabled(targetId: string | undefined, operation: string): void {
    if (this.isEnabled(targetId)) return;
    throw new Error(
      `${operation} refused: write mode is not enabled for this Kafka target. Enable it in the connection settings first.`,
    );
  }

  clear(): void {
    this.enabled.clear();
  }
}
