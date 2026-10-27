import { describe, expect, it } from "vitest";
import { parseSources, walk } from "../../test/source-ast";

import type { SourceNode } from "../../test/source-ast";

/** `TableCell` elements that pass a `title`, as `file:line`. */
function titledTableCells(): string[] {
  return parseSources(import.meta.dirname, /\.tsx$/).flatMap((file) =>
    [...walk(file.program)]
      .filter((node) => node.type === "JSXOpeningElement")
      .filter((node) => {
        const name = node.name as SourceNode;
        const tag = file.text.slice(name.start, name.end);
        const hasTitle = (node.attributes as SourceNode[]).some(
          (attribute) => attribute.type === "JSXAttribute" && (attribute.name as { name?: string }).name === "title",
        );
        return /(^|\.)TableCell$/.test(tag) && hasTitle;
      })
      .map((node) => `${file.path}:${file.lineOf(node.start)}`),
  );
}

describe("table cells", () => {
  // The host's TableCell renders `title || children`: a title on the cell replaces what the cell holds, so a formatted
  // timestamp shows as raw milliseconds and an icon or a badge disappears. The tooltip goes on an element inside.
  it("never take a title, which would replace their content", () => {
    expect(titledTableCells()).toEqual([]);
  });
});
