/**
 * READ-ONLY validation of discovery + reachability + Direct connect against a REAL cluster's Kafka.
 *
 * Per ../../TESTING-SAFETY.md this performs ONLY reads:
 *   - discovery  = list Deployments/StatefulSets/DaemonSets/Services (+ get referenced ConfigMaps/Secrets)
 *   - reachability = a short TCP connect to each bootstrap (no bytes sent)
 *   - metadata   = describeCluster + listTopics (read-only)
 * It NEVER produces, creates, alters, deletes or commits anything. Secret VALUES are never printed —
 * only the discovered bootstrap endpoint (a hostname:port) and topic names/counts.
 *
 * Usage: ALLOW_REAL_READ_ONLY=1 KUBE_CONTEXT=my-context AUTHORIZED_KUBE_CONTEXT=my-context pnpm tsx test/e2e/probe-real.ts
 */
import { discoverAllKafkas } from "../../src/main/kafka/discovery";
import { connectDirect } from "../../src/main/kafka/kafka-connection";
import { createKubeReader } from "../../src/main/kafka/kube-reader";
import { probeBootstrapReachable } from "../../src/main/kafka/reachability";

function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) => setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)),
  ]);
}

async function readMetadata(bootstrap: string, ssl: boolean): Promise<string> {
  const conn = await connectDirect({ bootstrap, ssl: ssl ? true : undefined });
  try {
    const ov = await withTimeout(conn.overview(), 15_000, "overview");
    return `brokers=${ov.brokers.length} controller=${ov.controller} topics=${ov.topics.length}`;
  } finally {
    await conn.disconnect();
  }
}

async function main(): Promise<void> {
  const context = process.env.KUBE_CONTEXT;
  const authorized = process.env.AUTHORIZED_KUBE_CONTEXT;
  if (process.env.ALLOW_REAL_READ_ONLY !== "1" || !context || !authorized || context !== authorized) {
    throw new Error(
      "refusing real-cluster probe: require ALLOW_REAL_READ_ONLY=1 and matching KUBE_CONTEXT/AUTHORIZED_KUBE_CONTEXT",
    );
  }
  console.log(`# READ-ONLY Kafka probe via kube context: ${context}\n`);

  const reader = createKubeReader({ context });
  console.log("discovering (read-only)…");
  const startedAt = Date.now();
  let lastValue = -1;
  const kafkas = await withTimeout(
    discoverAllKafkas(reader, undefined, (progress) => {
      if (progress.value === lastValue) return;
      lastValue = progress.value;
      const count = progress.total ? ` (${progress.completed ?? 0}/${progress.total})` : "";
      console.log(`  [${String(progress.value).padStart(3)}%] ${progress.label}${count}`);
    }),
    180_000,
    "discovery",
  );
  console.log(`\ndiscovered ${kafkas.length} Kafka target(s) in ${Date.now() - startedAt} ms:\n`);

  for (const k of kafkas) {
    const provider = "provider" in k ? k.provider : k.source === "strimzi" ? "Strimzi" : "In-cluster";
    console.log(`● [${k.source}] ${k.name}  (ns: ${k.namespace})`);
    console.log(`    provider:  ${provider}${"external" in k && k.external ? " · external" : ""}`);
    console.log(`    bootstrap: ${k.bootstrap}   tls-guess: ${k.tls}`);
    if ("referencedBy" in k && k.referencedBy?.length) console.log(`    used by:   ${k.referencedBy.join(", ")}`);

    const reachable = await probeBootstrapReachable(k.bootstrap, 5_000);
    console.log(`    from PC:   ${reachable ? "YES ✓" : "no ✗"}`);

    if (reachable && k.source !== "strimzi") {
      let done = false;
      for (const ssl of [false, true]) {
        try {
          const info = await withTimeout(readMetadata(k.bootstrap, ssl), 20_000, "connect");
          console.log(`    connected (${ssl ? "TLS" : "PLAINTEXT"}): ${info}`);
          done = true;
          break;
        } catch (err) {
          console.log(`    connect ${ssl ? "TLS" : "PLAINTEXT"}: ${(err as Error).message.split("\n")[0]}`);
        }
      }
      if (!done) console.log("    (could not read metadata with PLAINTEXT or TLS-no-auth)");
    }
    console.log("");
  }
  console.log("# done — no writes performed.");
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err instanceof Error ? err.message : err);
    process.exit(1);
  });
