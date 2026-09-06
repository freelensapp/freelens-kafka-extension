/**
 * Opt-in READ-ONLY evidence for the renderer resource cache on the explicitly authorized context.
 * Performs Kubernetes discovery, TCP reachability and Kafka describeCluster/listTopics only.
 * It never writes resources, produces messages, changes configs or commits offsets.
 *
 * Usage:
 *   ALLOW_REAL_READ_ONLY=1 KUBE_CONTEXT=my-context AUTHORIZED_KUBE_CONTEXT=my-context pnpm tsx test/e2e/cache-timing.real.ts
 */
import { createKafkaTargetId } from "../../src/common/kafka-target";
import { type AnyDiscoveredKafka, discoverAllKafkas } from "../../src/main/kafka/discovery";
import { connectDirect } from "../../src/main/kafka/kafka-connection";
import { createKubeReader } from "../../src/main/kafka/kube-reader";
import { probeBootstrapReachable } from "../../src/main/kafka/reachability";
import { KafkaResourceCache } from "../../src/renderer/kafka-resource-cache";

import type { ClusterOverviewDto, DiscoveredKafkaInfo } from "../../src/common/ipc";

function authorizedContext(): string {
  const context = process.env.KUBE_CONTEXT;
  const authorized = process.env.AUTHORIZED_KUBE_CONTEXT;
  if (process.env.ALLOW_REAL_READ_ONLY !== "1" || !context || !authorized || context !== authorized) {
    throw new Error(
      "Refusing real-cluster timing: require ALLOW_REAL_READ_ONLY=1 and matching KUBE_CONTEXT/AUTHORIZED_KUBE_CONTEXT",
    );
  }
  return context;
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) => {
      const timeout = setTimeout(() => reject(new Error(`${label} timed out after ${timeoutMs}ms`)), timeoutMs);
      timeout.unref();
    }),
  ]);
}

async function timed<T>(action: () => Promise<T>): Promise<{ durationMs: number; value: T }> {
  const startedAt = performance.now();
  const value = await action();
  return { durationMs: Math.round(performance.now() - startedAt), value };
}

function toDto(discovered: AnyDiscoveredKafka): DiscoveredKafkaInfo {
  const base = {
    targetId: createKafkaTargetId(discovered.bootstrap),
    source: discovered.source,
    name: discovered.name,
    namespace: discovered.namespace,
    bootstrap: discovered.bootstrap,
    tls: discovered.tls,
    port: discovered.port,
  };

  if (discovered.source === "strimzi") {
    return {
      ...base,
      listeners: discovered.listeners,
      brokerPods: discovered.brokerPods,
      provider: "Strimzi",
      external: false,
      securityHint: { tls: discovered.tls, auth: "none" },
    };
  }
  if (discovered.source === "service") {
    return {
      ...base,
      listeners: [],
      brokerPods: [],
      provider: "In-cluster",
      external: false,
      securityHint: { tls: discovered.tls, auth: "none" },
    };
  }
  return {
    ...base,
    listeners: [],
    brokerPods: [],
    provider: discovered.provider,
    external: discovered.external,
    referencedBy: discovered.referencedBy,
    securityHint: discovered.securityHint ?? { tls: discovered.tls, auth: "none" },
  };
}

async function readMetadata(discovered: AnyDiscoveredKafka): Promise<ClusterOverviewDto> {
  let lastError: unknown;
  for (const ssl of [...new Set([discovered.tls, !discovered.tls])]) {
    const connection = await connectDirect({ bootstrap: discovered.bootstrap, ssl: ssl ? true : undefined });
    try {
      const overview = await withTimeout(connection.overview(), 20_000, "Kafka metadata read");
      return {
        ...overview,
        security: { tls: ssl, auth: "none", source: "inferred" },
      };
    } catch (error) {
      lastError = error;
    } finally {
      await connection.disconnect();
    }
  }
  throw lastError instanceof Error ? lastError : new Error("Kafka metadata read failed");
}

async function main(): Promise<void> {
  const context = authorizedContext();
  const reader = createKubeReader({ context });
  const cache = new KafkaResourceCache();
  let rawDiscovered: AnyDiscoveredKafka[] = [];

  const coldDiscovery = await timed(() =>
    cache.loadDiscovery(context, "real-discovery-cold", async () => {
      rawDiscovered = await withTimeout(discoverAllKafkas(reader), 180_000, "Kubernetes discovery");
      return rawDiscovered.map(toDto);
    }),
  );
  const warmDiscovery = await timed(() =>
    cache.loadDiscovery(context, "real-discovery-warm", async () => {
      throw new Error("warm discovery invoked its loader");
    }),
  );

  const bootstraps = coldDiscovery.value.map((target) => target.bootstrap);
  const coldReachability = await timed(() =>
    cache.loadReachability(context, bootstraps, async (missingBootstraps) => {
      const entries = await Promise.all(
        missingBootstraps.map(
          async (bootstrap) => [bootstrap, await probeBootstrapReachable(bootstrap, 5_000)] as const,
        ),
      );
      return Object.fromEntries(entries) as Record<string, boolean>;
    }),
  );
  const warmReachability = await timed(() =>
    cache.loadReachability(context, bootstraps, async () => {
      throw new Error("warm reachability invoked its loader");
    }),
  );

  const directCandidates = rawDiscovered.filter(
    (candidate) => candidate.source !== "strimzi" && coldReachability.value[candidate.bootstrap] === true,
  );
  let selected: AnyDiscoveredKafka | undefined;
  let coldMetadata: { durationMs: number; value: ClusterOverviewDto } | undefined;

  for (const candidate of directCandidates) {
    try {
      const targetId = createKafkaTargetId(candidate.bootstrap);
      coldMetadata = await timed(() =>
        cache.loadOverview(context, targetId, "real-overview-cold", () => readMetadata(candidate)),
      );
      selected = candidate;
      break;
    } catch {
      // Try another already-discovered, PC-reachable target without exposing its identity.
    }
  }

  if (!selected || !coldMetadata) {
    throw new Error("No PC-reachable discovered Kafka target accepted read-only metadata with inferred TLS");
  }

  const targetId = createKafkaTargetId(selected.bootstrap);
  const beforeWarm = cache.stats(context, targetId);
  const warmMetadata = await timed(() =>
    cache.loadOverview(context, targetId, "real-overview-warm", async () => {
      throw new Error("warm metadata invoked its loader");
    }),
  );
  const afterWarm = cache.stats(context, targetId);
  const snapshot = cache.readOverview(context, targetId);

  const evidence = {
    context,
    mode: "read-only",
    targetCount: coldDiscovery.value.length,
    reachableCount: Object.values(coldReachability.value).filter(Boolean).length,
    brokerCount: warmMetadata.value.brokers.length,
    topicCount: warmMetadata.value.topics.length,
    durationsMs: {
      discoveryCold: coldDiscovery.durationMs,
      discoveryWarm: warmDiscovery.durationMs,
      reachabilityCold: coldReachability.durationMs,
      reachabilityWarm: warmReachability.durationMs,
      metadataCold: coldMetadata.durationMs,
      metadataWarm: warmMetadata.durationMs,
    },
    cacheAgeMs: snapshot.updatedAt ? Date.now() - snapshot.updatedAt : undefined,
    requestCounts: afterWarm,
    warmRequestDelta: {
      discovery: afterWarm.discoveryRequests - beforeWarm.discoveryRequests,
      overview: afterWarm.overviewRequests - beforeWarm.overviewRequests,
      reachability: afterWarm.reachabilityRequests - beforeWarm.reachabilityRequests,
    },
  };

  process.stdout.write(`${JSON.stringify(evidence)}\n`);
}

const globalTimeout = setTimeout(() => {
  process.stderr.write("Read-only cache timing exceeded 300000ms\n");
  process.exit(2);
}, 300_000);
globalTimeout.unref();

main()
  .then(() => process.exit(0))
  .catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(1);
  });
