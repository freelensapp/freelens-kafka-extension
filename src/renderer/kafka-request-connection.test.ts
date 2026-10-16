import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import ts from "typescript";
import { describe, expect, it } from "vitest";

const RENDERER = import.meta.dirname;

function sourceFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) return sourceFiles(path);
    return /\.tsx?$/.test(entry.name) && !/\.test\.tsx?$/.test(entry.name) ? [path] : [];
  });
}

function propertyNames(literal: ts.ObjectLiteralExpression): Set<string> {
  const names = new Set<string>();
  for (const property of literal.properties) {
    if (property.name && (ts.isIdentifier(property.name) || ts.isStringLiteral(property.name))) {
      names.add(property.name.text);
    }
  }
  return names;
}

/** Object literals that select a Kafka target for the main process: they name the cluster and its bootstrap. */
function targetRequests(path: string): { at: string; names: Set<string> }[] {
  const file = ts.createSourceFile(path, readFileSync(path, "utf8"), ts.ScriptTarget.Latest, true);
  const found: { at: string; names: Set<string> }[] = [];
  const visit = (node: ts.Node) => {
    if (ts.isObjectLiteralExpression(node)) {
      const names = propertyNames(node);
      if (names.has("clusterName") && names.has("bootstrap")) {
        const { line } = file.getLineAndCharacterOfPosition(node.getStart(file));
        found.push({ at: `${relative(RENDERER, path)}:${line + 1}`, names });
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(file);
  return found;
}

describe("requests to the main process", () => {
  // The main process picks the session and its credentials from these two fields. A request without
  // them gets a session of its own without the saved Security override, so a broker that requires
  // authentication (AWS IAM on MSK, SASL) closes its connection while the other pages work (#82).
  it("carry the saved Security override and the workload source of the target", () => {
    const requests = sourceFiles(RENDERER).flatMap(targetRequests);
    const missing = requests
      .filter(({ names }) => !names.has("security") || !names.has("sourceLocator"))
      .map(({ at, names }) =>
        [at, names.has("security") ? "" : "security", names.has("sourceLocator") ? "" : "sourceLocator"]
          .filter(Boolean)
          .join(" "),
      );

    // The scan itself must keep finding the requests, or the check below would pass on nothing.
    expect(requests.length).toBeGreaterThan(15);
    expect(missing).toEqual([]);
  });
});
