import { describe, expect, it } from "vitest";
import { parseSources, walk } from "../../test/source-ast";

import type { SourceNode } from "../../test/source-ast";

function propertyNames(literal: SourceNode): Set<string> {
  const names = new Set<string>();
  for (const property of literal.properties as SourceNode[]) {
    if (property.type !== "Property" || property.computed) continue;
    const key = property.key as { type: string; name?: string; value?: unknown };
    if (key.type === "Identifier" && key.name) names.add(key.name);
    if (key.type === "Literal" && typeof key.value === "string") names.add(key.value);
  }
  return names;
}

/** Object literals that select a Kafka target for the main process: they name the cluster and its bootstrap. */
function targetRequests(): { at: string; names: Set<string> }[] {
  return parseSources(import.meta.dirname, /\.tsx?$/).flatMap((file) =>
    [...walk(file.program)]
      .filter((node) => node.type === "ObjectExpression")
      .map((node) => ({ node, names: propertyNames(node) }))
      .filter(({ names }) => names.has("clusterName") && names.has("bootstrap"))
      .map(({ node, names }) => ({ at: `${file.path}:${file.lineOf(node.start)}`, names })),
  );
}

describe("requests to the main process", () => {
  // The main process picks the session and its credentials from these two fields. A request without
  // them gets a session of its own without the saved Security override, so a broker that requires
  // authentication (AWS IAM on MSK, SASL) closes its connection while the other pages work (#82).
  it("carry the saved Security override and the workload source of the target", () => {
    const requests = targetRequests();
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
