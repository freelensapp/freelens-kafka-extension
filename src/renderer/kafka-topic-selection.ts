/** Selection helpers for the batch topic deletion in the Topics list (SPEC-009 REQ-203–REQ-205). */

export type TopicSelectionState = "none" | "some" | "all";

export interface DeleteTopicsOutcome {
  deleted: string[];
  failed: { topic: string }[];
}

/** Kafka internal topics (`__consumer_offsets`, `__transaction_state`, ...) are never selected as a page. */
export function isInternalTopic(name: string): boolean {
  return name.startsWith("__");
}

/** Returns a new set with `name` added or removed; the input set is never mutated. */
export function toggleTopicSelection(selected: ReadonlySet<string>, name: string, checked: boolean): Set<string> {
  const next = new Set(selected);
  if (checked) next.add(name);
  else next.delete(name);
  return next;
}

/** The topics of a list page that "select this page" acts on: the application topics only. */
export function selectableTopics(visible: readonly string[]): string[] {
  return visible.filter((name) => !isInternalTopic(name));
}

/** Adds (or removes) every selectable topic of the visible page, keeping the selections made elsewhere. */
export function selectVisibleTopics(
  selected: ReadonlySet<string>,
  visible: readonly string[],
  checked: boolean,
): Set<string> {
  const next = new Set(selected);
  for (const name of selectableTopics(visible)) {
    if (checked) next.add(name);
    else next.delete(name);
  }
  return next;
}

/** "all" when every selectable visible topic is selected, "some" for a part of them, "none" otherwise. */
export function visibleSelectionState(selected: ReadonlySet<string>, visible: readonly string[]): TopicSelectionState {
  const candidates = selectableTopics(visible);
  if (candidates.length === 0) return "none";
  const count = candidates.filter((name) => selected.has(name)).length;
  if (count === 0) return "none";
  return count === candidates.length ? "all" : "some";
}

/** Drops the names that no longer exist; returns the same set instance when nothing changed. */
export function pruneTopicSelection(selected: ReadonlySet<string>, existing: readonly string[]): ReadonlySet<string> {
  if (selected.size === 0) return selected;
  const known = new Set(existing);
  const next = new Set([...selected].filter((name) => known.has(name)));
  return next.size === selected.size ? selected : next;
}

/** "1 topic", "3 topics". */
export function formatTopicCount(count: number): string {
  return `${count} ${count === 1 ? "topic" : "topics"}`;
}

/** One-line outcome of a batch deletion for the status banner (REQ-205). */
export function describeDeleteTopicsOutcome(outcome: DeleteTopicsOutcome): string {
  const total = outcome.deleted.length + outcome.failed.length;
  if (outcome.failed.length === 0) return `Deleted ${formatTopicCount(outcome.deleted.length)}`;
  if (outcome.deleted.length === 0) return `None of the ${formatTopicCount(total)} was deleted`;
  return `Deleted ${outcome.deleted.length} of ${formatTopicCount(total)}, ${outcome.failed.length} failed`;
}
