// The progress of this frame's operations: every frame is told the id of an operation with a new event; this frame
// fetches the event only for the ids it sent, one fetch at a time per operation, with the signals that come during a
// fetch folded into one more (SPEC-019, REQ-227). The main process hands an event to the frame that started its
// operation only; the ids this frame owns spare the fetches it would be refused.

import type { KafkaProgressEvent } from "../common/ipc";

export interface ProgressTransport {
  /** Listens for the signals of the main process; what it returns stops listening. */
  listen(listener: (signal: unknown) => void): () => void;
  /** The latest event of the operation, or nothing. */
  pull(operationId: string): Promise<KafkaProgressEvent | null | undefined>;
}

/** How long an id stays this frame's after its request settled, for the signals that arrive after the answer. */
export const OWNED_GRACE_MS = 5_000;

function operationIdOf(signal: unknown): string | undefined {
  const operationId = (signal as { operationId?: unknown } | null | undefined)?.operationId;
  return typeof operationId === "string" && operationId.length > 0 ? operationId : undefined;
}

export class KafkaProgressPull {
  private readonly owned = new Map<string, number>();
  private readonly releases = new Map<string, ReturnType<typeof setTimeout>>();
  private readonly fetching = new Map<string, { again: boolean }>();
  private readonly listeners = new Set<(event: KafkaProgressEvent) => void>();
  private stopListening?: () => void;
  private disposed = false;

  constructor(private readonly transport: ProgressTransport) {}

  /** Makes `operationId` this frame's while its request runs; what it returns ends that, after a grace period. */
  own(operationId: unknown): () => void {
    if (this.disposed || typeof operationId !== "string" || !operationId) return () => undefined;
    const pending = this.releases.get(operationId);

    if (pending !== undefined) {
      clearTimeout(pending);
      this.releases.delete(operationId);
    }
    this.owned.set(operationId, (this.owned.get(operationId) ?? 0) + 1);
    let released = false;

    return () => {
      if (released) return;
      released = true;
      const count = (this.owned.get(operationId) ?? 1) - 1;

      if (count > 0) {
        this.owned.set(operationId, count);
        return;
      }
      const timer = setTimeout(() => {
        this.releases.delete(operationId);
        this.owned.delete(operationId);
      }, OWNED_GRACE_MS);

      this.releases.set(operationId, timer);
    };
  }

  /** Told the progress events of this frame's operations; what it returns unsubscribes. */
  subscribe(listener: (event: KafkaProgressEvent) => void): () => void {
    if (this.disposed) return () => undefined;
    this.listeners.add(listener);
    this.stopListening ??= this.transport.listen((signal) => this.signalled(signal));
    return () => {
      this.listeners.delete(listener);
      if (this.listeners.size === 0) this.stop();
    };
  }

  /** Nothing more is listened to or fetched: the extension is deactivated. */
  dispose(): void {
    this.disposed = true;
    this.listeners.clear();
    this.stop();
    for (const timer of this.releases.values()) clearTimeout(timer);
    this.releases.clear();
    this.owned.clear();
  }

  private stop(): void {
    this.stopListening?.();
    this.stopListening = undefined;
  }

  private signalled(signal: unknown): void {
    const operationId = operationIdOf(signal);

    if (this.disposed || operationId === undefined || !this.owned.has(operationId)) return;
    const running = this.fetching.get(operationId);

    if (running) {
      running.again = true;
      return;
    }
    void this.fetch(operationId);
  }

  private async fetch(operationId: string): Promise<void> {
    const state = { again: false };

    this.fetching.set(operationId, state);
    try {
      do {
        state.again = false;
        const event = await this.transport.pull(operationId).catch(() => undefined);

        if (this.disposed) return;
        if (event && event.operationId === operationId) {
          for (const listener of [...this.listeners]) listener(event);
        }
      } while (state.again && this.owned.has(operationId));
    } finally {
      this.fetching.delete(operationId);
    }
  }
}
