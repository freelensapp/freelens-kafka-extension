import { createHash } from "node:crypto";
import { readFileSync, realpathSync } from "node:fs";
import { aggregatePerformanceProbeRuns } from "../../src/common/performance-run-aggregation";

function main(): void {
  const runFiles = process.argv.slice(2);
  if (runFiles.length !== 3) {
    throw new Error("Usage: pnpm evidence:aggregate <run-1.json> <run-2.json> <run-3.json>");
  }
  const canonicalRunFiles = runFiles.map((runFile) => realpathSync(runFile));
  if (new Set(canonicalRunFiles).size !== canonicalRunFiles.length) {
    throw new Error("Performance aggregation requires three distinct canonical run files");
  }
  const runs = canonicalRunFiles.map((runFile) => {
    const raw = JSON.parse(readFileSync(runFile, "utf8")) as unknown;
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
      throw new Error("Performance aggregation requires object run files");
    }
    return {
      ...(raw as Record<string, unknown>),
      sourceFileId: createHash("sha256").update(runFile).digest("hex"),
    };
  });
  const evidence = aggregatePerformanceProbeRuns(runs, new Date().toISOString());
  process.stdout.write(`${JSON.stringify(evidence, null, 2)}\n`);
}

try {
  main();
} catch (error: unknown) {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
}
