// The progress of the operations of the frames, kept in the main process: the latest event of each operation, bound
// to the frame that started it. Every frame is told only that an operation has a new event; the event itself is
// handed to the frame that started the operation, when it asks (SPEC-019, REQ-227).

import type { IpcMainInvokeEvent } from "electron";

import type { KafkaProgressEvent } from "../common/ipc";

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
} as const;

interface Entry {
  owner: string;
  event: KafkaProgressEvent;
  at: number;
}

export class KafkaProgressStore {
  private readonly entries = new Map<string, Entry>();

  constructor(private readonly now: () => number = Date.now) {}

  get size(): number {
    return this.entries.size;
  }

  /**
   * Keeps `event` as the latest of its operation for `owner`. False, and nothing kept, when the operation is another
   * frame's: an id is the frame's own, and one frame may not take over the progress of another.
   */
  put(owner: string, event: KafkaProgressEvent): boolean {
    const at = this.now();

    this.prune(at);
    const known = this.entries.get(event.operationId);

    if (known && known.owner !== owner) return false;
    this.entries.delete(event.operationId);
    this.entries.set(event.operationId, { owner, event, at });
    while (this.entries.size > PROGRESS_BOUNDS.operations) {
      const oldest = this.entries.keys().next().value;

      if (oldest === undefined) break;
      this.entries.delete(oldest);
    }
    return true;
  }

  /** The latest event of the operation, for the frame that started it; nothing for any other frame. */
  get(owner: string | undefined, operationId: unknown): KafkaProgressEvent | undefined {
    if (owner === undefined || typeof operationId !== "string") return undefined;
    this.prune(this.now());
    const entry = this.entries.get(operationId);

    return entry?.owner === owner ? entry.event : undefined;
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
