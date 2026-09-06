import { readFileSync } from "node:fs";
import { attachAkhqComparison, attachPackagedBrowserEvidence } from "../../src/common/performance-comparison";

function main(): void {
  const [performanceFile, comparisonFile, browserFile] = process.argv.slice(2);
  if (!performanceFile || !comparisonFile || !browserFile) {
    throw new Error("Usage: finalize-performance-evidence <performance.json> <comparison.json> <browser.json>");
  }
  const performance = JSON.parse(readFileSync(performanceFile, "utf8")) as unknown;
  const comparison = JSON.parse(readFileSync(comparisonFile, "utf8")) as unknown;
  const browser = JSON.parse(readFileSync(browserFile, "utf8")) as unknown;
  const withComparison = attachAkhqComparison(performance as never, comparison);
  process.stdout.write(`${JSON.stringify(attachPackagedBrowserEvidence(withComparison, browser), null, 2)}\n`);
}

try {
  main();
} catch (error: unknown) {
  process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
  process.exitCode = 1;
}
