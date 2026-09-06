/**
 * Opt-in GET-only timing comparison against a pinned localhost-only AKHQ container.
 * Credentials are resolved in-process, written to one mode-0600 temporary file and never printed.
 * Requires matching context/host aliases plus caller-supplied `AUTHORIZED_KUBE_CONTEXT_SHA256`
 * and `AUTHORIZED_KAFKA_TARGET_HOST_SHA256` pins; no target identity is stored in this repository.
 */
import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  AKHQ_CLUSTER_ID,
  AKHQ_DIGEST,
  AKHQ_HTTP_PORT,
  AKHQ_IMAGE,
  AKHQ_MANAGEMENT_PORT,
  AKHQ_VERSION,
  type AkhqPageKind,
  AkhqPageValidationError,
  assertAkhqUsefulPage,
  createAkhqReadOnlyConfiguration,
} from "../../src/common/akhq-read-only-config";
import {
  assertSanitizedPerformanceEvidence,
  summarizePerformanceDurations,
} from "../../src/common/performance-evidence";
import { firstBrokerAddress, splitBootstrap } from "../../src/common/reachability";
import { authorizeReadOnlyTarget, selectSingleAuthorizedTarget } from "../../src/common/read-only-target-authorization";
import { type AnyDiscoveredKafka, discoverAllKafkas } from "../../src/main/kafka/discovery";
import { resolveExternalCredentials } from "../../src/main/kafka/external-credentials";
import { createKubeReader } from "../../src/main/kafka/kube-reader";

const CONTAINER_NAME = "freelens-kafka-akhq-read-only";
const OWNER_LABEL = "io.freelens.test=slice16-akhq-read-only";
const OWNER_LABEL_VALUE = "slice16-akhq-read-only";
const STARTUP_TIMEOUT_MS = 180_000;
let failurePhase = "authorization";

function matchesHost(candidate: AnyDiscoveredKafka, targetHost: string): boolean {
  return splitBootstrap(candidate.bootstrap).some(
    (entry) => firstBrokerAddress(entry).host.toLowerCase() === targetHost,
  );
}

function docker(args: string[], label: string): string {
  try {
    return execFileSync("docker", args, { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
  } catch {
    throw new Error(`${label} failed`);
  }
}

function dockerObjectExists(kind: "container" | "image", name: string): boolean {
  return spawnSync("docker", [kind, "inspect", name], { stdio: "ignore" }).status === 0;
}

function ownedContainerId(reference: string): string {
  const identity = docker(
    ["container", "inspect", "--format", '{{.Id}}\t{{index .Config.Labels "io.freelens.test"}}', reference],
    "AKHQ ownership inspection",
  );
  const separator = identity.indexOf("\t");
  if (separator < 1) throw new Error("AKHQ comparison container identity is invalid");
  const containerId = identity.slice(0, separator);
  const owner = identity.slice(separator + 1);
  if (owner !== OWNER_LABEL_VALUE) throw new Error("AKHQ comparison container ownership mismatch");
  return containerId;
}

function removeOwnedStaleContainer(): void {
  if (!dockerObjectExists("container", CONTAINER_NAME)) return;
  const containerId = ownedContainerId(CONTAINER_NAME);
  docker(["rm", "-f", containerId], "stale AKHQ cleanup");
}

function removeCreatedContainer(containerId: string): void {
  if (!dockerObjectExists("container", containerId)) return;
  ownedContainerId(containerId);
  docker(["rm", "-f", containerId], "AKHQ container cleanup");
}

async function waitForReady(): Promise<void> {
  const deadline = Date.now() + STARTUP_TIMEOUT_MS;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(`http://127.0.0.1:${AKHQ_MANAGEMENT_PORT}/health`, {
        signal: AbortSignal.timeout(2_000),
      });
      if (response.ok) return;
    } catch {
      // The pinned container is still starting.
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error("AKHQ comparison startup timed out");
}

async function timedGet(pathname: string, kind: AkhqPageKind, requestedPage: number): Promise<number> {
  const startedAt = performance.now();
  const response = await fetch(`http://127.0.0.1:${AKHQ_HTTP_PORT}${pathname}`, {
    headers: { accept: "application/json" },
    method: "GET",
    signal: AbortSignal.timeout(180_000),
  });
  if (response.status !== 200) throw new Error("AKHQ GET returned a non-200 status");
  try {
    assertAkhqUsefulPage(await response.json(), kind, requestedPage);
  } catch (error) {
    if (error instanceof AkhqPageValidationError) failurePhase = `${kind}-${error.reason}`;
    throw error;
  }
  return Math.round(performance.now() - startedAt);
}

async function threeGets(
  pathname: string,
  kind: AkhqPageKind,
  requestedPage: number,
): Promise<{ coldMs: number; warmMs: number[] }> {
  return {
    coldMs: await timedGet(pathname, kind, requestedPage),
    warmMs: [await timedGet(pathname, kind, requestedPage), await timedGet(pathname, kind, requestedPage)],
  };
}

async function main(): Promise<void> {
  const { context, targetHost } = authorizeReadOnlyTarget(process.env);
  failurePhase = "discovery";
  const reader = createKubeReader({ context });
  const targets = await discoverAllKafkas(reader);
  const target = selectSingleAuthorizedTarget(targets, targetHost, matchesHost);
  failurePhase = "credentials";
  const credentials =
    target.source === "workload"
      ? await resolveExternalCredentials(reader, { bootstrap: target.bootstrap })
      : { detected: false, hint: { tls: target.tls, auth: "none" as const } };
  failurePhase = "configuration";
  const configuration = createAkhqReadOnlyConfiguration({
    bootstrap: target.bootstrap,
    ssl: credentials.ssl ?? (target.tls ? true : undefined),
    sasl: credentials.sasl,
  });

  const temporaryDirectory = mkdtempSync(path.join(tmpdir(), "freelens-akhq-read-only-"));
  const configurationPath = path.join(temporaryDirectory, "application.json");
  let imagePreexisting = true;
  let imageOwnershipKnown = false;
  let createdContainerId: string | undefined;
  let evidence: Record<string, unknown> | undefined;
  const cleanupFailures: string[] = [];
  try {
    writeFileSync(configurationPath, `${JSON.stringify(configuration)}\n`, { encoding: "utf8", mode: 0o600 });
    if ((statSync(configurationPath).mode & 0o777) !== 0o600) {
      throw new Error("AKHQ temporary configuration permissions are not private");
    }
    imagePreexisting = dockerObjectExists("image", AKHQ_IMAGE);
    imageOwnershipKnown = true;
    failurePhase = "stale-container-cleanup";
    removeOwnedStaleContainer();
    failurePhase = "image";
    if (!imagePreexisting) docker(["pull", "--quiet", AKHQ_IMAGE], "pinned AKHQ image pull");
    const repoDigests = JSON.parse(
      docker(["image", "inspect", "--format", "{{json .RepoDigests}}", AKHQ_IMAGE], "AKHQ digest inspection"),
    ) as string[];
    if (!repoDigests.some((digest) => digest.endsWith(`@${AKHQ_DIGEST}`))) {
      throw new Error("Pinned AKHQ image digest was not verified");
    }
    failurePhase = "container-start";
    createdContainerId = docker(
      [
        "run",
        "--detach",
        "--name",
        CONTAINER_NAME,
        "--label",
        OWNER_LABEL,
        "--network",
        "host",
        "--user",
        `${process.getuid?.() ?? 1000}:${process.getgid?.() ?? 1000}`,
        "--volume",
        `${configurationPath}:/app/application.json:ro`,
        "--env",
        "MICRONAUT_CONFIG_FILES=/app/application.json",
        AKHQ_IMAGE,
      ],
      "AKHQ container start",
    );
    if (ownedContainerId(createdContainerId) !== createdContainerId) {
      throw new Error("AKHQ created container identity changed unexpectedly");
    }
    failurePhase = "readiness";
    await waitForReady();

    failurePhase = "topics-get";
    const topics = await threeGets(
      `/api/${encodeURIComponent(AKHQ_CLUSTER_ID)}/topic?search=&show=HIDE_INTERNAL&page=1&uiPageSize=25`,
      "topics",
      1,
    );
    failurePhase = "consumer-groups-get";
    const consumerGroups = await threeGets(
      `/api/${encodeURIComponent(AKHQ_CLUSTER_ID)}/group?search=&page=1`,
      "consumer-groups",
      1,
    );
    evidence = {
      provider: "akhq",
      version: AKHQ_VERSION,
      digest: AKHQ_DIGEST,
      method: "GET",
      topics: { ...topics, warmSummary: summarizePerformanceDurations(topics.warmMs) },
      consumerGroups: {
        ...consumerGroups,
        warmSummary: summarizePerformanceDurations(consumerGroups.warmMs),
      },
      completeGlobalLagEquivalent: false,
    };
  } finally {
    const operationPhase = failurePhase;
    failurePhase = "cleanup";
    if (createdContainerId) {
      try {
        removeCreatedContainer(createdContainerId);
      } catch {
        cleanupFailures.push("container");
      }
    }
    try {
      rmSync(temporaryDirectory, { force: true, recursive: true });
    } catch {
      cleanupFailures.push("configuration");
    }
    if (imageOwnershipKnown && !imagePreexisting && dockerObjectExists("image", AKHQ_IMAGE)) {
      try {
        docker(["image", "rm", AKHQ_IMAGE], "AKHQ image cleanup");
      } catch {
        cleanupFailures.push("image");
      }
    }
    if (cleanupFailures.length === 0) failurePhase = operationPhase;
  }

  failurePhase = "kubernetes-readiness";
  const cleanup = {
    createdContainerRemoved: !createdContainerId || !dockerObjectExists("container", createdContainerId),
    temporaryConfigurationRemoved: !existsSync(configurationPath),
    imageRemovedOrPreexisting: !imageOwnershipKnown || imagePreexisting || !dockerObjectExists("image", AKHQ_IMAGE),
    kubernetesReady: false,
  };
  try {
    cleanup.kubernetesReady =
      execFileSync("kubectl", ["--context", context, "get", "--raw=/readyz"], {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "ignore"],
      }).trim() === "ok";
  } catch {
    cleanup.kubernetesReady = false;
  }
  if (cleanupFailures.length > 0 || !Object.values(cleanup).every(Boolean) || !evidence) {
    failurePhase = "cleanup";
    throw new Error("AKHQ comparison cleanup failed");
  }
  failurePhase = "evidence-validation";
  const result = { ...evidence, cleanup };
  assertSanitizedPerformanceEvidence(result);
  process.stdout.write(`${JSON.stringify(result)}\n`);
}

main().catch(() => {
  process.stderr.write(`Read-only AKHQ comparison failed: ${failurePhase}\n`);
  process.exit(1);
});
