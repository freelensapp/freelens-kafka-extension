import { describe, expect, it } from "vitest";
import {
  describeDeleteTopicsOutcome,
  formatTopicCount,
  isInternalTopic,
  pruneTopicSelection,
  selectVisibleTopics,
  toggleTopicSelection,
  visibleSelectionState,
} from "./kafka-topic-selection";

describe("kafka-topic-selection", () => {
  it("toggles single names without mutating the previous set", () => {
    const initial: ReadonlySet<string> = new Set(["orders"]);
    const added = toggleTopicSelection(initial, "payments", true);
    expect([...added]).toEqual(["orders", "payments"]);
    expect([...initial]).toEqual(["orders"]);
    expect([...toggleTopicSelection(added, "orders", false)]).toEqual(["payments"]);
  });

  it("selects a page without the internal topics and keeps the selections made elsewhere", () => {
    const page = ["__consumer_offsets", "orders", "payments"];
    const selected = selectVisibleTopics(new Set(["hidden-on-another-page"]), page, true);
    expect([...selected].sort()).toEqual(["hidden-on-another-page", "orders", "payments"]);
    expect(isInternalTopic("__consumer_offsets")).toBe(true);
    expect([...selectVisibleTopics(selected, page, false)]).toEqual(["hidden-on-another-page"]);
  });

  it("reports the page selection state from the selectable topics only", () => {
    const page = ["__consumer_offsets", "orders", "payments"];
    expect(visibleSelectionState(new Set(), page)).toBe("none");
    expect(visibleSelectionState(new Set(["orders"]), page)).toBe("some");
    expect(visibleSelectionState(new Set(["orders", "payments"]), page)).toBe("all");
    expect(visibleSelectionState(new Set(["orders"]), ["__consumer_offsets"])).toBe("none");
  });

  it("prunes the names that disappeared and keeps the instance when nothing changed", () => {
    const selected: ReadonlySet<string> = new Set(["orders", "payments"]);
    expect(pruneTopicSelection(selected, ["orders", "payments", "other"])).toBe(selected);
    expect([...pruneTopicSelection(selected, ["orders"])]).toEqual(["orders"]);
    const empty: ReadonlySet<string> = new Set();
    expect(pruneTopicSelection(empty, [])).toBe(empty);
  });

  it("describes the outcome with the right plural and the partial counts", () => {
    expect(formatTopicCount(1)).toBe("1 topic");
    expect(formatTopicCount(3)).toBe("3 topics");
    expect(describeDeleteTopicsOutcome({ deleted: ["a", "b"], failed: [] })).toBe("Deleted 2 topics");
    expect(describeDeleteTopicsOutcome({ deleted: ["a"], failed: [] })).toBe("Deleted 1 topic");
    expect(describeDeleteTopicsOutcome({ deleted: ["a", "b"], failed: [{ topic: "c" }] })).toBe(
      "Deleted 2 of 3 topics, 1 failed",
    );
    expect(describeDeleteTopicsOutcome({ deleted: [], failed: [{ topic: "c" }, { topic: "d" }] })).toBe(
      "None of the 2 topics was deleted",
    );
  });
});
