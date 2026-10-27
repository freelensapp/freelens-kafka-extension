// The progress of this frame's operations: every frame is told the id of an operation with a new event; this frame
// fetches the events only for the ids it sent, every event since its last fetch of the operation (a cursor), one
// fetch at a time per operation, with the signals that come during a fetch folded into one more (SPEC-019, REQ-227).
// The main process hands the events to the frame that started their operation only; the ids this frame owns spare
// the fetches it would be refused.

import type { KafkaProgressEvent, KafkaProgressEvents } from "../common/ipc";

export interface ProgressTransport {
  /** Listens for the signals of the main process; what it returns stops listening. */
  listen(listener: (signal: unknown) => void): () => void;
  /** The events of the operation after the cursor `after`, with the cursor to ask from next time, or nothing. */
  pull(operationId: string, after: number | undefined): Promise<KafkaProgressEvents | null | undefined>;
}

/** How long an id stays this frame's after its request settled, for the signals that arrive after the answer. */
export const OWNED_GRACE_MS = 5_000;

function operationIdOf(signal: unknown): string | undefined {
  const operationId = (signal as { operationId?: unknown } | null | undefined)?.operationId;
  return typeof operationId === "string" && operationId.length > 0 ? operationId : undefined;
}

const nextTask = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

export class KafkaProgressPull {
  private readonly owned = new Map<string, number>();
  private readonly releases = new Map<string, ReturnType<typeof setTimeout>>();
  private readonly fetching = new Map<string, { again: boolean; done: Promise<void> }>();
  private readonly cursors = new Map<string, number>();
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
        this.cursors.delete(operationId);
      }, OWNED_GRACE_MS);

      this.releases.set(operationId, timer);
    };
  }

  /**
   * Delivers the events of the operation not delivered yet, and resolves once they are: called when its request has
   * answered, so that its progress reaches the page before its result, as when every event was a message on the
   * channel of the answer.
   */
  async flush(operationId: unknown): Promise<void> {
    if (this.disposed || typeof operationId !== "string" || !this.owned.has(operationId) || this.listeners.size === 0) {
      return;
    }
    const running = this.fetching.get(operationId);

    if (running) {
      running.again = true;
      await running.done;
      return;
    }
    await this.fetch(operationId);
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
    this.cursors.clear();
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

  private fetch(operationId: string): Promise<void> {
    const state = { again: false, done: Promise.resolve() };

    state.done = this.pullAll(operationId, state);
    this.fetching.set(operationId, state);
    return state.done;
  }

  private async pullAll(operationId: string, state: { again: boolean }): Promise<void> {
    // The fetch is registered by the caller first, in the same task.
    await Promise.resolve();
    try {
      do {
        state.again = false;
        const answer = await this.transport.pull(operationId, this.cursors.get(operationId)).catch(() => undefined);

        if (this.disposed) return;
        if (answer && Array.isArray(answer.events) && Number.isInteger(answer.cursor)) {
          this.cursors.set(operationId, answer.cursor);
          let first = true;
          for (const event of answer.events) {
            if (event?.operationId !== operationId) continue;
            // One task per event, as when every event was a message of its own: the page renders each step of a
            // burst instead of batching them into the last one.
            if (!first) await nextTask();
            first = false;
            if (this.disposed) return;
            for (const listener of [...this.listeners]) listener(event);
          }
        }
      } while (state.again && this.owned.has(operationId));
    } finally {
      this.fetching.delete(operationId);
    }
  }
}
