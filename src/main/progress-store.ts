// The progress of the operations of the frames, kept in the main process: the recent events of each operation, in
// order, bound to the frame that started it. Every frame is told only that an operation has a new event; the events
// themselves are handed to the frame that started the operation, when it asks, from the cursor of its last answer
// (SPEC-019, REQ-227). Every event reaches that frame, so a burst (a fast scan reporting each workload) is not folded
// into its last event.

import type { IpcMainInvokeEvent } from "electron";

import type { KafkaProgressEvent, KafkaProgressEvents } from "../common/ipc";

/**
 * What the store reads of the event of a handler: the identifiers of the sending frame's process and of the frame,
 * which tell the frames of one window apart. Only the type comes from Electron.
 */
export type IpcSenderEvent = Partial<Pick<IpcMainInvokeEvent, "processId" | "frameId">>;

/** The frame of an event, or nothing for what names no frame. */
export function senderKey(event: IpcSenderEvent | undefined): string | undefined {
  if (!event || !Number.isInteger(event.processId) || !Number.isInteger(event.frameId)) return undefined;
  return `${event.processId}:${event.frameId}`;
}

export const PROGRESS_BOUNDS = {
  /** An operation without a new event for this long is forgotten. */
  idleMs: 60_000,
  /** At most this many operations are kept; the least recently updated go first. */
  operations: 500,
  /** At most this many events are kept for an operation; the oldest go first. */
  eventsPerOperation: 64,
} as const;

interface Entry {
  owner: string;
  /** The events kept, in order; the cursor of the first one is `next - events.length + 1`. */
  events: KafkaProgressEvent[];
  /** The cursor of the last event put. */
  next: number;
  at: number;
}

export class KafkaProgressStore {
  private readonly entries = new Map<string, Entry>();

  constructor(private readonly now: () => number = Date.now) {}

  get size(): number {
    return this.entries.size;
  }

  /**
   * Keeps `event` as the next event of its operation for `owner`. False, and nothing kept, when the operation is
   * another frame's: an id is the frame's own, and one frame may not take over the progress of another.
   */
  put(owner: string, event: KafkaProgressEvent): boolean {
    const at = this.now();

    this.prune(at);
    const known = this.entries.get(event.operationId);

    if (known && known.owner !== owner) return false;
    const entry = known ?? { owner, events: [], next: 0, at };

    entry.events.push(event);
    if (entry.events.length > PROGRESS_BOUNDS.eventsPerOperation) entry.events.shift();
    entry.next += 1;
    entry.at = at;
    this.entries.delete(event.operationId);
    this.entries.set(event.operationId, entry);
    while (this.entries.size > PROGRESS_BOUNDS.operations) {
      const oldest = this.entries.keys().next().value;

      if (oldest === undefined) break;
      this.entries.delete(oldest);
    }
    return true;
  }

  /**
   * The events of the operation after `after` (all those kept without it), for the frame that started it; nothing
   * for any other frame.
   */
  get(owner: string | undefined, operationId: unknown, after?: unknown): KafkaProgressEvents | undefined {
    if (owner === undefined || typeof operationId !== "string") return undefined;
    this.prune(this.now());
    const entry = this.entries.get(operationId);

    if (entry?.owner !== owner) return undefined;
    const first = entry.next - entry.events.length + 1;
    const from = typeof after === "number" && Number.isInteger(after) ? Math.max(after + 1, first) : first;

    return { events: entry.events.slice(from - first), cursor: entry.next };
  }

  clear(): void {
    this.entries.clear();
  }

  private prune(at: number): void {
    // Entries are in the order of their last event: the idle ones are at the front.
    for (const [operationId, entry] of this.entries) {
      if (at - entry.at < PROGRESS_BOUNDS.idleMs) break;
      this.entries.delete(operationId);
    }
  }
}
