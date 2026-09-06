/*
 * REAL-API validation of P2 discovery + credentials against the kind cluster.
 *
 * Validates the actual `@kubernetes/client-node` reads in `createKubeReader`
 * (CustomObjects, Pods, Secrets, Services) — the part unit tests (fakes) cannot
 * cover. Uses a minimal FAKE Strimzi CRD + fixtures (not the real operator);
 * the fixture pods need not run, discovery only reads their objects.
 *
 * Run:
 *   pnpm kind:disc:up        # apply CRD + fixtures
 *   pnpm itest:disc:kind
 *   pnpm kind:disc:down
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import { resolveStrimziCredentials } from "../../src/main/kafka/credentials";
import { discoverKafkaServices, discoverStrimziKafkas } from "../../src/main/kafka/discovery";
import { createKubeReader } from "../../src/main/kafka/kube-reader";

const log = (m = ""): void => void fs.writeSync(1, `${m}\n`);
setTimeout(() => {
  log("[timeout] aborting discovery integration test");
  process.exit(2);
}, 30_000);

const CONTEXT = process.env.KUBE_CONTEXT ?? "kind-kind";
const NAMESPACE = "default";

async function main(): Promise<void> {
  const reader = createKubeReader({ context: CONTEXT });

  const kafkas = await discoverStrimziKafkas(reader, NAMESPACE);
  log(`discovered strimzi kafkas: ${kafkas.length}`);
  const mc = kafkas.find((k) => k.name === "my-cluster");
  assert.ok(mc, "expected to discover my-cluster");
  log(`  ${mc.name}: bootstrap=${mc.bootstrap} tls=${mc.tls} port=${mc.port}`);
  log(`  brokers=${mc.brokerPods.map((b) => `${b.brokerId}:${b.pod}`).join(", ")}`);
  assert.equal(mc.bootstrap, "my-cluster-kafka-bootstrap.default.svc:9092");
  assert.equal(mc.tls, false);
  assert.deepEqual(mc.brokerPods, [
    { brokerId: 0, pod: "my-cluster-kafka-0" },
    { brokerId: 1, pod: "my-cluster-kafka-1" },
    { brokerId: 2, pod: "my-cluster-kafka-2" },
  ]);

  const creds = await resolveStrimziCredentials(reader, {
    namespace: NAMESPACE,
    clusterName: "my-cluster",
    tls: true,
    user: "my-user",
  });
  const username = creds.sasl && "username" in creds.sasl ? creds.sasl.username : undefined;
  log(`  creds: sasl=${creds.sasl ? `${creds.sasl.mechanism}/${username}` : "none"} ssl=${creds.ssl ? "yes" : "no"}`);
  assert.equal(username, "my-user");
  assert.ok(creds.ssl, "expected cluster CA ssl");

  const services = await discoverKafkaServices(reader, NAMESPACE);
  log(`  service candidates in ${NAMESPACE}: ${services.map((s) => `${s.name}:${s.port}`).join(", ") || "(none)"}`);
  // The kube-prometheus stack in this namespace exposes :9093 (Alertmanager); it must NOT be matched.
  assert.ok(
    services.every((s) => s.name === "my-cluster-kafka-bootstrap"),
    "service discovery should only match the kafka bootstrap service, not Alertmanager:9093",
  );
  assert.ok(
    services.some((s) => s.port === 9092) && services.some((s) => s.port === 9093),
    "expected both bootstrap ports (9092 + 9093)",
  );

  log("\n[OK] P2 discovery + credentials validated against real kind API reads.");
  process.exit(0);
}

main().catch((err) => {
  log(`\n[FAIL] ${(err as Error).message}`);
  process.exit(1);
});
