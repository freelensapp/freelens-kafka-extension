import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import ts from "typescript";
import { describe, expect, it } from "vitest";

const RENDERER = import.meta.dirname;

function sourceFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) return sourceFiles(path);
    return /\.tsx$/.test(entry.name) && !/\.test\.tsx$/.test(entry.name) ? [path] : [];
  });
}

/** `TableCell` elements that pass a `title`, as `file:line`. */
function titledTableCells(path: string): string[] {
  const file = ts.createSourceFile(path, readFileSync(path, "utf8"), ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const found: string[] = [];
  const visit = (node: ts.Node) => {
    if (ts.isJsxOpeningElement(node) || ts.isJsxSelfClosingElement(node)) {
      const tag = node.tagName.getText(file);
      const hasTitle = node.attributes.properties.some(
        (attribute) => ts.isJsxAttribute(attribute) && attribute.name.getText(file) === "title",
      );
      if (/(^|\.)TableCell$/.test(tag) && hasTitle) {
        const { line } = file.getLineAndCharacterOfPosition(node.getStart(file));
        found.push(`${relative(RENDERER, path)}:${line + 1}`);
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(file);
  return found;
}

describe("table cells", () => {
  // The host's TableCell renders `title || children`: a title on the cell replaces what the cell holds, so a formatted
  // timestamp shows as raw milliseconds and an icon or a badge disappears. The tooltip goes on an element inside.
  it("never take a title, which would replace their content", () => {
    const titled = sourceFiles(RENDERER).flatMap(titledTableCells);

    expect(titled).toEqual([]);
  });
});
