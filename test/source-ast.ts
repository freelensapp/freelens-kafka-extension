// Parsing of the extension's own sources for the tests that check their structure.
//
// Vite's `parseAst` is Rolldown's Oxc parser: it reads TypeScript and TSX and returns an ESTree with the offsets of
// every node. TypeScript itself is no use here: the `typescript` package of TypeScript 7 exports no compiler API.
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import { parseAst } from "vite";

export interface SourceNode {
  type: string;
  start: number;
  end: number;
  [key: string]: unknown;
}

export interface SourceFile {
  /** The path relative to the directory the files were listed from. */
  path: string;
  text: string;
  program: SourceNode;
  /** The 1-based line of an offset. */
  lineOf(offset: number): number;
}

/** The files under `directory` whose name matches `pattern`, test files left out, parsed. */
export function parseSources(directory: string, pattern: RegExp): SourceFile[] {
  const list = (dir: string): string[] =>
    readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) return list(path);
      return pattern.test(entry.name) && !/\.test\.tsx?$/.test(entry.name) ? [path] : [];
    });

  return list(directory).map((path) => {
    const text = readFileSync(path, "utf8");
    const program = parseAst(text, { lang: path.endsWith(".tsx") ? "tsx" : "ts" }) as unknown as SourceNode;
    const lineStarts = [0];
    for (let index = text.indexOf("\n"); index !== -1; index = text.indexOf("\n", index + 1)) {
      lineStarts.push(index + 1);
    }
    const lineOf = (offset: number) => {
      let line = 0;
      while (line + 1 < lineStarts.length && lineStarts[line + 1] <= offset) line++;
      return line + 1;
    };
    return { path: relative(directory, path), text, program, lineOf };
  });
}

/** Every node of the tree, depth first. */
export function* walk(node: unknown): Generator<SourceNode> {
  if (Array.isArray(node)) {
    for (const child of node) yield* walk(child);
    return;
  }
  if (!node || typeof node !== "object" || typeof (node as SourceNode).type !== "string") return;
  yield node as SourceNode;
  for (const [key, value] of Object.entries(node)) {
    if (key !== "parent" && value && typeof value === "object") yield* walk(value);
  }
}
