import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import {
  AKHQ_DIGEST,
  AKHQ_HTTP_PORT,
  AKHQ_IMAGE,
  AKHQ_MANAGEMENT_PORT,
  createAkhqReadOnlyConfiguration,
} from "../../src/common/akhq-read-only-config";
import { firstBrokerAddress, splitBootstrap } from "../../src/common/reachability";
import { authorizeReadOnlyTarget, selectSingleAuthorizedTarget } from "../../src/common/read-only-target-authorization";
import { type AnyDiscoveredKafka, discoverAllKafkas } from "../../src/main/kafka/discovery";
import { resolveExternalCredentials } from "../../src/main/kafka/external-credentials";
import { createKubeReader } from "../../src/main/kafka/kube-reader";

const OWNER_LABEL_NAME = "io.freelens.test";
const STARTUP_TIMEOUT_MS = 180_000;
const EXPECTED_LOCK_PATH = path.join(tmpdir(), "freelens-kafka-akhq-browser.lock");
let failurePhase = "authorization";
let stopRequested = false;
let resolveStop: () => void = () => undefined;
const stopSignal = new Promise<void>((resolve) => {
  resolveStop = resolve;
});

const requestStop = () => {
  stopRequested = true;
  resolveStop();
};
process.once("SIGINT", requestStop);
process.once("SIGTERM", requestStop);

function throwIfStopRequested(): void {
  if (stopRequested) throw new Error("AKHQ browser session was cancelled during startup");
}

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

function dockerObjectExists(kind: "container" | "image", reference: string): boolean {
  const result = spawnSync("docker", [kind, "inspect", reference], {
    encoding: "utf8",
    stdio: ["ignore", "ignore", "pipe"],
  });
  if (!result.error && result.status === 0) return true;
  const stderr = typeof result.stderr === "string" ? result.stderr : "";
  if (!result.error && result.status === 1 && /no such (?:container|image|object)/i.test(stderr)) return false;
  throw new Error(`AKHQ browser Docker ${kind} inspection failed`);
}

function ownedContainerId(reference: string, expectedOwner: string, requireRunning = false): string {
  const identity = docker(
    [
      "container",
      "inspect",
      "--format",
      `{{.Id}}\t{{index .Config.Labels "${OWNER_LABEL_NAME}"}}\t{{.State.Running}}\t{{.Config.Image}}`,
      reference,
    ],
    "AKHQ browser ownership inspection",
  );
  const [containerId, actualOwner, running, image] = identity.split("\t");
  if (!containerId || actualOwner !== expectedOwner || image !== AKHQ_IMAGE || (requireRunning && running !== "true")) {
    throw new Error("AKHQ browser container ownership mismatch");
  }
  return containerId;
}

function removeOwnedContainer(reference: string, expectedOwner: string): void {
  if (!dockerObjectExists("container", reference)) return;
  const containerId = ownedContainerId(reference, expectedOwner);
  docker(["rm", "-f", containerId], "AKHQ browser container cleanup");
  if (dockerObjectExists("container", containerId)) throw new Error("AKHQ browser container cleanup was incomplete");
}

function portIsOpen(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = connect({ host: "127.0.0.1", port });
    let settled = false;
    const finish = (open: boolean) => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(open);
    };
    socket.once("connect", () => finish(true));
    socket.once("error", () => finish(false));
    socket.setTimeout(500, () => finish(false));
  });
}

function assertLockOwner(lockPath: string, sessionId: string): void {
  if (!existsSync(lockPath) || readFileSync(lockPath, "utf8") !== `${sessionId}\n`) {
    throw new Error("AKHQ browser lifecycle lock ownership mismatch");
  }
}

async function assertPortsFree(): Promise<void> {
  if ((await portIsOpen(AKHQ_HTTP_PORT)) || (await portIsOpen(AKHQ_MANAGEMENT_PORT))) {
    throw new Error("AKHQ browser ports are already in use");
  }
}

async function waitForReady(containerId: string, owner: string): Promise<void> {
  const deadline = Date.now() + STARTUP_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (stopRequested) throw new Error("AKHQ browser session was cancelled during startup");
    ownedContainerId(containerId, owner, true);
    try {
      const response = await fetch(`http://127.0.0.1:${AKHQ_MANAGEMENT_PORT}/health`, {
        signal: AbortSignal.timeout(2_000),
      });
      if (response.ok) {
        ownedContainerId(containerId, owner, true);
        return;
      }
    } catch {
      // The pinned container is still starting.
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error("AKHQ browser startup timed out");
}

async function monitorSession(containerId: string, owner: string, lockPath: string): Promise<void> {
  while (!stopRequested) {
    assertLockOwner(lockPath, owner);
    ownedContainerId(containerId, owner, true);
    await Promise.race([stopSignal, new Promise<void>((resolve) => setTimeout(resolve, 500))]);
  }
  assertLockOwner(lockPath, owner);
  ownedContainerId(containerId, owner, true);
  process.stdout.write("AKHQ_BROWSER_STOPPING_VERIFIED\n");
}

async function main(): Promise<void> {
  const { context, targetHost } = authorizeReadOnlyTarget(process.env);
  const sessionId = process.env.AKHQ_BROWSER_SESSION_ID;
  const suppliedRuntime = process.env.AKHQ_BROWSER_RUNTIME_DIR;
  const lockPath = process.env.AKHQ_BROWSER_LOCK_PATH;
  if (!sessionId || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(sessionId)) {
    throw new Error("AKHQ browser session identity is unavailable");
  }
  const containerName = `freelens-kafka-akhq-browser-${sessionId}`;
  if (process.env.AKHQ_BROWSER_CONTAINER_NAME !== containerName || lockPath !== EXPECTED_LOCK_PATH) {
    throw new Error("AKHQ browser session ownership is invalid");
  }
  assertLockOwner(lockPath, sessionId);
  failurePhase = "discovery";
  const reader = createKubeReader({ context });
  const target = selectSingleAuthorizedTarget(await discoverAllKafkas(reader), targetHost, matchesHost);
  throwIfStopRequested();
  failurePhase = "credentials";
  const credentials =
    target.source === "workload"
      ? await resolveExternalCredentials(reader, { bootstrap: target.bootstrap })
      : { detected: false, hint: { tls: target.tls, auth: "none" as const } };
  throwIfStopRequested();
  failurePhase = "configuration";
  const configuration = createAkhqReadOnlyConfiguration({
    bootstrap: target.bootstrap,
    ssl: credentials.ssl ?? (target.tls ? true : undefined),
    sasl: credentials.sasl,
  });

  const temporaryDirectory = suppliedRuntime ?? mkdtempSync(path.join(tmpdir(), "freelens-akhq-browser-read-only-"));
  if (suppliedRuntime) {
    if (!existsSync(suppliedRuntime)) throw new Error("AKHQ browser runtime directory is unavailable");
    const parentOwnerPath = path.join(suppliedRuntime, ".parent-owner");
    if (!existsSync(parentOwnerPath) || readFileSync(parentOwnerPath, "utf8") !== `${sessionId}\n`) {
      throw new Error("AKHQ browser runtime ownership mismatch");
    }
    writeFileSync(path.join(suppliedRuntime, ".owner"), `${sessionId}\n`, { encoding: "utf8", mode: 0o600 });
  }
  const configurationPath = path.join(temporaryDirectory, "application.json");
  let createdContainerId: string | undefined;
  const cleanupFailures: string[] = [];
  try {
    throwIfStopRequested();
    writeFileSync(configurationPath, `${JSON.stringify(configuration)}\n`, { encoding: "utf8", mode: 0o600 });
    if ((statSync(configurationPath).mode & 0o777) !== 0o600) {
      throw new Error("AKHQ browser configuration permissions are not private");
    }
    failurePhase = "stale-container-cleanup";
    removeOwnedContainer(containerName, sessionId);
    failurePhase = "image";
    if (!dockerObjectExists("image", AKHQ_IMAGE)) {
      docker(["pull", "--quiet", AKHQ_IMAGE], "pinned AKHQ browser image pull");
    }
    throwIfStopRequested();
    const repoDigests = JSON.parse(
      docker(["image", "inspect", "--format", "{{json .RepoDigests}}", AKHQ_IMAGE], "AKHQ digest inspection"),
    ) as string[];
    if (!repoDigests.some((digest) => digest.endsWith(`@${AKHQ_DIGEST}`))) {
      throw new Error("Pinned AKHQ browser image digest was not verified");
    }
    throwIfStopRequested();
    assertLockOwner(lockPath, sessionId);
    await assertPortsFree();
    failurePhase = "container-start";
    createdContainerId = docker(
      [
        "run",
        "--detach",
        "--name",
        containerName,
        "--label",
        `${OWNER_LABEL_NAME}=${sessionId}`,
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
      "AKHQ browser container start",
    );
    if (ownedContainerId(createdContainerId, sessionId, true) !== createdContainerId) {
      throw new Error("AKHQ browser container identity changed unexpectedly");
    }
    failurePhase = "readiness";
    await waitForReady(createdContainerId, sessionId);
    assertLockOwner(lockPath, sessionId);
    if (stopRequested) throw new Error("AKHQ browser session was cancelled during startup");
    process.stdout.write("AKHQ_BROWSER_READY\n");
    failurePhase = "session";
    await monitorSession(createdContainerId, sessionId, lockPath);
  } finally {
    failurePhase = "cleanup";
    try {
      removeOwnedContainer(containerName, sessionId);
    } catch {
      cleanupFailures.push("container");
    }
    try {
      rmSync(temporaryDirectory, { force: true, recursive: true });
    } catch {
      cleanupFailures.push("configuration");
    }
  }
  if (
    cleanupFailures.length > 0 ||
    dockerObjectExists("container", containerName) ||
    existsSync(temporaryDirectory) ||
    existsSync(configurationPath)
  ) {
    throw new Error("AKHQ browser cleanup failed");
  }
  process.stdout.write("AKHQ_BROWSER_CLEAN\n");
}

main().catch(() => {
  process.stderr.write(`Read-only AKHQ browser session failed: ${failurePhase}\n`);
  process.exit(1);
});
