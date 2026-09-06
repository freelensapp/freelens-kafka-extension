import { execFileSync, spawn, spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import {
  chmodSync,
  existsSync,
  linkSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  rmSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { setImmediate } from "node:timers";
import { describe, expect, it } from "@jest/globals";
import { _electron as electron } from "playwright";
import type { ChildProcessWithoutNullStreams } from "node:child_process";

import type { ElectronApplication, Frame, Page } from "playwright";

const RUN_COUNT = 3;
const OVERVIEW_SLO_MS = 5_000;
const AKHQ_BROWSER_LOCK_PATH = path.join(tmpdir(), "freelens-kafka-akhq-browser.lock");
const AKHQ_BROWSER_ORIGIN = "http://127.0.0.1:18080";
const EXPECTED_EXTENSION_NAME = "@freelensapp/kafka-extension";
const describeReal = process.env.ALLOW_REAL_READ_ONLY === "1" ? describe : describe.skip;

type PaintEvidence = {
  aggregateStillRunning: boolean;
  firstUsefulPaintMs: number;
  interactiveWhileUpdating: boolean;
  metadataPaintMs: number;
  progressContinued: boolean;
  topologyPaintMs: number;
};

type RunEvidence = {
  akhqTopicPaintMs: number;
  freelens: PaintEvidence;
  preparationAttempts: number;
};

type AkhqBrowserSession = {
  assertRunning(): void;
  guard<T>(operation: Promise<T>): Promise<T>;
  stop(): Promise<void>;
};

class PackagedPreparationError extends Error {}

function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function requiredSha256(value: string | undefined): string | undefined {
  const normalized = value?.trim().toLowerCase();
  return normalized && /^[0-9a-f]{64}$/.test(normalized) ? normalized : undefined;
}

function authorization(): {
  context: string;
  contextSha256: string;
  targetHost: string;
  targetHostSha256: string;
} {
  const context = process.env.KUBE_CONTEXT;
  const authorizedContext = process.env.AUTHORIZED_KUBE_CONTEXT;
  const contextSha256 = requiredSha256(process.env.AUTHORIZED_KUBE_CONTEXT_SHA256);
  const targetHost = process.env.KAFKA_TARGET_HOST?.trim().toLowerCase();
  const authorizedTargetHost = process.env.AUTHORIZED_KAFKA_TARGET_HOST?.trim().toLowerCase();
  const targetHostSha256 = requiredSha256(process.env.AUTHORIZED_KAFKA_TARGET_HOST_SHA256);
  if (
    process.env.ALLOW_REAL_READ_ONLY !== "1" ||
    !context ||
    context !== authorizedContext ||
    !contextSha256 ||
    sha256(context) !== contextSha256 ||
    !targetHost ||
    targetHost !== authorizedTargetHost ||
    !targetHostSha256 ||
    sha256(targetHost) !== targetHostSha256
  ) {
    throw new Error("Packaged read-only performance authorization refused");
  }
  return { context, contextSha256, targetHost, targetHostSha256 };
}

function summarize(samples: number[]): { durationMs: number; p95DurationMs: number; sampleCount: number } {
  const sorted = [...samples].sort((left, right) => left - right);
  return {
    durationMs: sorted[Math.floor(sorted.length / 2)],
    p95DurationMs: sorted[Math.max(0, Math.ceil(sorted.length * 0.95) - 1)],
    sampleCount: sorted.length,
  };
}

async function bounded<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  const expired = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(() => reject(new Error(`${label} timed out`)), timeoutMs);
  });
  return Promise.race([promise, expired]).finally(() => {
    if (timeout) clearTimeout(timeout);
  });
}

function dockerExists(kind: "container" | "image", reference: string): boolean {
  const result = spawnSync("docker", [kind, "inspect", reference], {
    encoding: "utf8",
    stdio: ["ignore", "ignore", "pipe"],
  });
  if (!result.error && result.status === 0) return true;
  const stderr = typeof result.stderr === "string" ? result.stderr : "";
  if (!result.error && result.status === 1 && /no such (?:container|image|object)/i.test(stderr)) return false;
  throw new Error(`AKHQ browser Docker ${kind} inspection failed`);
}

function removeOwnedAkhqBrowserContainer(containerName: string, expectedOwner: string): void {
  if (!dockerExists("container", containerName)) return;
  let identity: string;
  try {
    identity = execFileSync(
      "docker",
      ["container", "inspect", "--format", '{{.Id}}\t{{index .Config.Labels "io.freelens.test"}}', containerName],
      { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] },
    ).trim();
  } catch {
    throw new Error("AKHQ browser container ownership inspection failed");
  }
  const [containerId, actualOwner] = identity.split("\t");
  if (!containerId || actualOwner !== expectedOwner) {
    throw new Error("AKHQ browser container ownership mismatch");
  }
  try {
    execFileSync("docker", ["rm", "-f", containerId], { stdio: "ignore" });
  } catch {
    throw new Error("AKHQ browser container cleanup failed");
  }
  if (dockerExists("container", containerId)) throw new Error("AKHQ browser container cleanup was incomplete");
}

function removeOwnedDirectory(directory: string, markerName: string, expectedOwner: string, allowAbsent = false): void {
  if (!existsSync(directory)) {
    if (allowAbsent) return;
    throw new Error("AKHQ browser owned directory is missing");
  }
  const marker = path.join(directory, markerName);
  if (!existsSync(marker) || readFileSync(marker, "utf8") !== `${expectedOwner}\n`) {
    throw new Error("AKHQ browser directory ownership mismatch");
  }
  rmSync(directory, { force: true, recursive: true });
  if (existsSync(directory)) throw new Error("AKHQ browser directory cleanup failed");
}

function removeOwnedLock(lockPath: string, expectedOwner: string): void {
  if (!existsSync(lockPath) || readFileSync(lockPath, "utf8") !== `${expectedOwner}\n`) {
    throw new Error("AKHQ browser lifecycle lock ownership mismatch");
  }
  rmSync(lockPath, { force: true });
  if (existsSync(lockPath)) throw new Error("AKHQ browser lifecycle lock cleanup failed");
}

const AKHQ_HELPER_FAILURE_PHASES = new Set([
  "authorization",
  "cleanup",
  "configuration",
  "container-start",
  "credentials",
  "discovery",
  "image",
  "readiness",
  "session",
  "stale-container-cleanup",
]);

function helperFailurePhase(stderr: string): string | undefined {
  const prefix = "Read-only AKHQ browser session failed: ";
  const phase = stderr
    .split(/\r?\n/)
    .find((line) => line.startsWith(prefix))
    ?.slice(prefix.length);
  return phase && AKHQ_HELPER_FAILURE_PHASES.has(phase) ? phase : undefined;
}

function publishEvidence(outputPath: string, evidence: unknown): void {
  if (existsSync(outputPath)) throw new Error("Packaged performance output already exists");
  const temporaryPath = `${outputPath}.${randomUUID()}.tmp`;
  try {
    writeFileSync(temporaryPath, `${JSON.stringify(evidence, null, 2)}\n`, {
      encoding: "utf8",
      flag: "wx",
      mode: 0o600,
    });
    if ((statSync(temporaryPath).mode & 0o777) !== 0o600) {
      throw new Error("Packaged performance output permissions are not private");
    }
    linkSync(temporaryPath, outputPath);
  } catch {
    throw new Error("Packaged performance output publication failed");
  } finally {
    rmSync(temporaryPath, { force: true });
  }
}

async function closeElectronApplication(app: ElectronApplication): Promise<void> {
  const process = app.process();
  const exited =
    process.exitCode !== null
      ? Promise.resolve()
      : new Promise<void>((resolve) => {
          process.once("exit", () => resolve());
        });
  try {
    await bounded(app.close(), 30_000, "Electron close");
    await bounded(exited, 30_000, "Electron process exit");
  } catch {
    if (process.exitCode === null) process.kill("SIGKILL");
    await bounded(exited, 30_000, "forced Electron process exit");
  }
}

async function waitForMainWindow(app: ElectronApplication): Promise<Page> {
  const deadline = Date.now() + 50_000;
  while (Date.now() < deadline) {
    const window = app.windows().find((candidate) => candidate.url().startsWith("https://renderer.freelens.app"));
    if (window) return window;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error("Freelens main window did not open");
}

async function installExtension(app: ElectronApplication, window: Page, extensionPath: string): Promise<void> {
  await window.click("[data-testid=welcome-menu-container] li a");
  await app.evaluate(async ({ app: electronApp }) => {
    await electronApp.applicationMenu
      ?.getMenuItemById(process.platform === "darwin" ? "mac" : "file")
      ?.submenu?.getMenuItemById("navigate-to-extensions")
      ?.click();
  });
  const textbox = window.getByPlaceholder("Name or file path or URL");
  const installButton = window.locator('button[class*="Button install-module__button--"]');
  const readyInstallButton = window.locator('button[class*="Button install-module__button--"][data-waiting="false"]');
  const extensionNameSelector = 'div[class*="installed-extensions-module__extensionName--"]';
  const extensionListed = () =>
    window.evaluate(
      ({ expectedName, selector }) =>
        [...document.querySelectorAll<HTMLElement>(selector)].filter(
          (element) => element.textContent?.trim() === expectedName,
        ).length === 1,
      { expectedName: EXPECTED_EXTENSION_NAME, selector: extensionNameSelector },
    );
  for (let attempt = 0; attempt < 2 && !(await extensionListed()); attempt++) {
    await textbox.fill(extensionPath);
    if ((await textbox.inputValue()) !== extensionPath) throw new Error("Packaged extension path was not accepted");
    await installButton.waitFor({ state: "visible", timeout: 30_000 });
    await window.evaluate(
      ({ expectedName, selector }) => {
        const installWindow = window as typeof window & {
          slice16InstallObserver?: MutationObserver;
          slice16InstallOutcome?: "idle" | "listed";
          slice16InstallSawBusy?: boolean;
        };
        installWindow.slice16InstallObserver?.disconnect();
        installWindow.slice16InstallOutcome = undefined;
        installWindow.slice16InstallSawBusy = false;
        const button = document.querySelector<HTMLElement>('button[class*="Button install-module__button--"]');
        if (!button) throw new Error("Packaged extension install control is unavailable");
        const check = (records: MutationRecord[] = []) => {
          if (installWindow.slice16InstallOutcome !== undefined) return;
          const listed = [...document.querySelectorAll<HTMLElement>(selector)].filter(
            (element) => element.textContent?.trim() === expectedName,
          ).length;
          if (
            records.some(
              (record) =>
                record.target === button && record.attributeName === "data-waiting" && record.oldValue === "false",
            )
          ) {
            installWindow.slice16InstallSawBusy = true;
          }
          if (listed === 1) installWindow.slice16InstallOutcome = "listed";
          if (button?.dataset.waiting === "true") installWindow.slice16InstallSawBusy = true;
          if (installWindow.slice16InstallSawBusy && button?.dataset.waiting === "false" && listed === 0) {
            installWindow.slice16InstallOutcome = "idle";
          }
        };
        const observer = new MutationObserver(check);
        observer.observe(document.documentElement, {
          attributeOldValue: true,
          attributes: true,
          childList: true,
          subtree: true,
        });
        installWindow.slice16InstallObserver = observer;
        check();
      },
      { expectedName: EXPECTED_EXTENSION_NAME, selector: extensionNameSelector },
    );
    let outcome: "idle" | "listed" | undefined;
    try {
      await readyInstallButton.click({ timeout: 30_000 });
      const outcomeHandle = await window.waitForFunction(
        () => (window as typeof window & { slice16InstallOutcome?: "idle" | "listed" }).slice16InstallOutcome ?? false,
        undefined,
        { timeout: 120_000 },
      );
      outcome = (await outcomeHandle.jsonValue()) as "idle" | "listed";
      await outcomeHandle.dispose();
    } finally {
      await window.evaluate(() => {
        const installWindow = window as typeof window & { slice16InstallObserver?: MutationObserver };
        installWindow.slice16InstallObserver?.disconnect();
        installWindow.slice16InstallObserver = undefined;
      });
    }
    if (!(await extensionListed())) {
      if (outcome !== "idle") throw new Error("Packaged extension listing was not durable");
      const closeNotifications = window.locator('i[data-testid*="close-notification-for-notification_"]:visible');
      while ((await closeNotifications.count()) > 0) await closeNotifications.first().click();
    }
  }
  if (!(await extensionListed())) throw new Error("Packaged extension was not listed after installation");
  const enabledState = await window.waitForFunction(
    ({ expectedName, selector }) => {
      const names = [...document.querySelectorAll<HTMLElement>(selector)].filter(
        (element) => element.textContent?.trim() === expectedName,
      );
      if (names.length !== 1) return false;
      const state = names[0]
        .closest<HTMLElement>(".tr")
        ?.querySelector<HTMLElement>('div[class*="installed-extensions-module__enabled--"]');
      const rectangle = state?.getBoundingClientRect();
      return state?.textContent?.trim() === "Enabled" && rectangle && rectangle.width > 0 && rectangle.height > 0;
    },
    { expectedName: EXPECTED_EXTENSION_NAME, selector: extensionNameSelector },
    { timeout: 120_000 },
  );
  await enabledState.dispose();
  const closeNotifications = window.locator('i[data-testid*="close-notification-for-notification_"]:visible');
  while ((await closeNotifications.count()) > 0) await closeNotifications.first().click();
  await window.click('div[class*="close-button-module__closeButton--"][aria-label="Close"]');
}

type AuthorizedTargetMarkerWindow = typeof window & {
  slice16AuthorizedClick?: string;
  slice16AuthorizedClickAt?: number;
  slice16CancelAuthorizedTarget?: (marker: string) => void;
  slice16MarkAuthorizedTarget?: (
    rootSelector: string,
    expectedHash: string,
    marker: string,
    kind: "bootstrap" | "text",
  ) => Promise<HTMLElement | false>;
};

function installAuthorizedTargetMarker(): void {
  const targetWindow = window as AuthorizedTargetMarkerWindow;
  const cleanups = new Map<string, () => void>();
  targetWindow.slice16CancelAuthorizedTarget = (marker) => cleanups.get(marker)?.();
  targetWindow.slice16MarkAuthorizedTarget = async (rootSelector, expectedHash, marker, kind) => {
    cleanups.get(marker)?.();
    const root = document.querySelector(rootSelector);
    if (!root) return false;
    const candidateSelector =
      kind === "bootstrap" ? ".TableRow[data-bootstrap]" : ".TableRow > .TableCell:first-of-type > span";
    const valuesFor = (node: HTMLElement) =>
      kind === "bootstrap"
        ? (node.dataset.bootstrap ?? "")
            .split(",")
            .map((entry) => entry.trim().replace(/:\d+$/, "").toLowerCase())
            .filter(Boolean)
        : [node.textContent?.trim() ?? ""];
    const rowFor = (node: HTMLElement) => (kind === "bootstrap" ? node : node.closest<HTMLElement>(".TableRow"));
    const digestRecords: MutationRecord[] = [];
    const digestObserver = new MutationObserver((records) => {
      digestRecords.push(...records);
    });
    digestObserver.observe(root, { attributes: true, childList: true, characterData: true, subtree: true });
    const snapshots = [...root.querySelectorAll<HTMLElement>(candidateSelector)].map((node) => ({
      node,
      row: rowFor(node),
      values: valuesFor(node),
    }));
    const snapshotIsStable = () => {
      const liveNodes = [...root.querySelectorAll<HTMLElement>(candidateSelector)];
      return (
        liveNodes.length === snapshots.length &&
        liveNodes.every(
          (node, index) =>
            node === snapshots[index].node &&
            rowFor(node) === snapshots[index].row &&
            JSON.stringify(valuesFor(node)) === JSON.stringify(snapshots[index].values) &&
            (kind !== "bootstrap" || snapshots[index].row?.getAttribute("role") === "button"),
        )
      );
    };
    const mutationTouchesSnapshot = (records: MutationRecord[]) =>
      records.some((record) => {
        if (record.type === "attributes") {
          return snapshots.some(
            ({ node, row }) =>
              record.target === node || node.contains(record.target) || (kind === "bootstrap" && record.target === row),
          );
        }
        if (record.type === "characterData") {
          return snapshots.some(({ node }) => node.contains(record.target));
        }
        const changedNodes = [...record.addedNodes, ...record.removedNodes];
        return (
          snapshots.some(({ node }) => node === record.target || node.contains(record.target)) ||
          changedNodes.some(
            (changedNode) =>
              snapshots.some(
                ({ node, row }) =>
                  changedNode === node ||
                  changedNode === row ||
                  (changedNode instanceof Element &&
                    (changedNode.contains(node) || (row !== null && changedNode.contains(row)))),
              ) ||
              (changedNode instanceof Element &&
                (changedNode.matches(candidateSelector) || changedNode.querySelector(candidateSelector) !== null)),
          )
        );
      });
    const digests = await Promise.all(
      snapshots.map(({ values }) =>
        Promise.all(
          values.map(async (value) => {
            const bytes = new TextEncoder().encode(value);
            return [...new Uint8Array(await crypto.subtle.digest("SHA-256", bytes))]
              .map((byte) => byte.toString(16).padStart(2, "0"))
              .join("");
          }),
        ),
      ),
    );
    digestRecords.push(...digestObserver.takeRecords());
    digestObserver.disconnect();
    if (mutationTouchesSnapshot(digestRecords) || !snapshotIsStable()) {
      return false;
    }
    const matches = snapshots.filter((_snapshot, index) => digests[index].includes(expectedHash));
    if (matches.length !== 1) return false;
    const row = matches[0].row;
    if (!row || !root.contains(row) || (kind === "bootstrap" && row.getAttribute("role") !== "button")) return false;
    const clickTarget =
      kind === "bootstrap" ? row.querySelector<HTMLElement>(":scope > .TableCell.nameCell") : matches[0].node;
    if (!clickTarget || !row.contains(clickTarget)) return false;
    clickTarget.dataset.slice16AuthorizedTarget = marker;
    const removeMarkers = () => {
      clickTarget.removeAttribute("data-slice16-authorized-target");
      for (const candidate of root.querySelectorAll<HTMLElement>("[data-slice16-authorized-target]")) {
        if (candidate.dataset.slice16AuthorizedTarget === marker) {
          candidate.removeAttribute("data-slice16-authorized-target");
        }
      }
    };
    let invalidationObserver: MutationObserver;
    const clickTargetIsStable = () =>
      clickTarget.isConnected &&
      row.contains(clickTarget) &&
      (kind !== "bootstrap" || row.querySelector(":scope > .TableCell.nameCell") === clickTarget);
    const mutationTouchesClickTarget = (records: MutationRecord[]) =>
      records.some((record) => {
        if (record.target === clickTarget || clickTarget.contains(record.target)) return true;
        if (record.type !== "childList") return false;
        return [...record.addedNodes, ...record.removedNodes].some(
          (node) =>
            node === clickTarget ||
            (node instanceof Element && (node.contains(clickTarget) || clickTarget.contains(node))),
        );
      });
    const invalidate = () => {
      removeMarkers();
      invalidationObserver.disconnect();
    };
    const clickGuard = (event: MouseEvent) => {
      const pendingRecords = invalidationObserver.takeRecords();
      const hasPendingMutation = mutationTouchesSnapshot(pendingRecords) || mutationTouchesClickTarget(pendingRecords);
      const authorized =
        !hasPendingMutation &&
        row.isConnected &&
        root.isConnected &&
        root.contains(row) &&
        clickTarget.dataset.slice16AuthorizedTarget === marker &&
        event.composedPath().includes(clickTarget) &&
        event.composedPath().includes(row) &&
        clickTargetIsStable() &&
        snapshotIsStable();
      if (authorized) {
        targetWindow.slice16AuthorizedClick = marker;
        targetWindow.slice16AuthorizedClickAt = performance.now();
      }
      cleanups.get(marker)?.();
      if (!authorized) {
        event.preventDefault();
        event.stopImmediatePropagation();
      }
    };
    const cleanup = () => {
      removeMarkers();
      invalidationObserver.disconnect();
      document.removeEventListener("click", clickGuard, true);
      cleanups.delete(marker);
    };
    invalidationObserver = new MutationObserver((records) => {
      if (
        mutationTouchesSnapshot(records) ||
        mutationTouchesClickTarget(records) ||
        !clickTargetIsStable() ||
        !snapshotIsStable()
      ) {
        invalidate();
      }
    });
    cleanups.set(marker, cleanup);
    document.addEventListener("click", clickGuard, true);
    invalidationObserver.observe(root, { attributes: true, childList: true, characterData: true, subtree: true });
    return clickTarget;
  };
}

async function assertAuthorizedTargetMarkerRaceGuard(page: Page): Promise<void> {
  await page.evaluate(async () => {
    const targetWindow = window as AuthorizedTargetMarkerWindow;
    const mark = targetWindow.slice16MarkAuthorizedTarget;
    const cancel = targetWindow.slice16CancelAuthorizedTarget;
    if (!mark || !cancel) throw new Error("Authorized target marker was not installed");
    const root = document.createElement("div");
    root.id = `slice16-marker-race-${crypto.randomUUID()}`;
    const fixtureHtml =
      '<div class="TableRow"><div class="TableCell"><div class="Avatar">avatar</div><span>authorized</span><i class="pinIcon"></i></div></div>';
    root.innerHTML = fixtureHtml;
    document.body.appendChild(root);
    try {
      const digest = [...new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode("authorized")))]
        .map((byte) => byte.toString(16).padStart(2, "0"))
        .join("");
      const cell = root.querySelector<HTMLElement>(".TableCell > span");
      if (!cell) throw new Error("Authorized target race fixture was unavailable");
      queueMicrotask(() => {
        cell.textContent = "changed-during-digest";
      });
      if (await mark(`#${root.id}`, digest, "digest-race", "text")) {
        throw new Error("Authorized target digest race was not rejected");
      }
      cell.textContent = "authorized";
      if (!(await mark(`#${root.id}`, digest, "post-marker-race", "text"))) {
        throw new Error("Authorized target stable fixture was rejected");
      }
      cell.textContent = "changed-after-marker";
      await Promise.resolve();
      if (root.querySelector('[data-slice16-authorized-target="post-marker-race"]')) {
        throw new Error("Authorized target marker was not invalidated");
      }
      cancel("post-marker-race");
      cell.textContent = "authorized";
      const unrelatedStatus = document.createElement("div");
      unrelatedStatus.className = "TableCell status";
      root.querySelector(".TableRow")?.appendChild(unrelatedStatus);
      const unrelatedMutationTarget = await mark(`#${root.id}`, digest, "unrelated-status-mutation", "text");
      if (!unrelatedMutationTarget) throw new Error("Authorized target unrelated-status fixture was rejected");
      unrelatedStatus.textContent = "updated";
      if (!unrelatedMutationTarget.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true }))) {
        throw new Error("Authorized target unrelated status mutation was not ignored");
      }
      cell.textContent = "authorized";
      const replacementRaceTarget = await mark(`#${root.id}`, digest, "replacement-race", "text");
      if (!replacementRaceTarget) throw new Error("Authorized target replacement fixture was rejected");
      const replacement = replacementRaceTarget.cloneNode(true) as HTMLElement;
      replacementRaceTarget.replaceWith(replacement);
      await Promise.resolve();
      if (
        replacementRaceTarget.isConnected ||
        replacementRaceTarget.hasAttribute("data-slice16-authorized-target") ||
        replacement.hasAttribute("data-slice16-authorized-target")
      ) {
        throw new Error("Authorized target replacement race was not rejected");
      }
      cancel("replacement-race");
      root.innerHTML = fixtureHtml;
      const mutationCell = root.querySelector<HTMLElement>(".TableCell > span");
      if (!mutationCell) throw new Error("Authorized target mutation fixture was unavailable");
      const mutationRaceTarget = await mark(`#${root.id}`, digest, "same-node-mutation-race", "text");
      if (!mutationRaceTarget) throw new Error("Authorized target mutation fixture was rejected");
      mutationCell.textContent = "changed-before-click";
      if (
        mutationRaceTarget.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, composed: true }))
      ) {
        throw new Error("Authorized target same-node mutation race was not rejected");
      }
      root.innerHTML = fixtureHtml;
      const attributeRaceTarget = await mark(`#${root.id}`, digest, "same-task-attribute-race", "text");
      if (!attributeRaceTarget) throw new Error("Authorized target attribute fixture was rejected");
      attributeRaceTarget.setAttribute("data-race", "changed");
      if (
        attributeRaceTarget.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, composed: true }))
      ) {
        throw new Error("Authorized target same-task attribute race was not rejected");
      }
      root.innerHTML = fixtureHtml;
      const transientMoveRaceTarget = await mark(`#${root.id}`, digest, "same-root-move-race", "text");
      if (!transientMoveRaceTarget) throw new Error("Authorized target transient move fixture was rejected");
      const targetParent = transientMoveRaceTarget.parentElement;
      if (!targetParent) throw new Error("Authorized target transient move parent was unavailable");
      targetParent.removeChild(transientMoveRaceTarget);
      targetParent.appendChild(transientMoveRaceTarget);
      if (
        transientMoveRaceTarget.dispatchEvent(
          new MouseEvent("click", { bubbles: true, cancelable: true, composed: true }),
        )
      ) {
        throw new Error("Authorized target same-root move race was not rejected");
      }
      root.innerHTML = fixtureHtml;
      const moveRaceTarget = await mark(`#${root.id}`, digest, "same-node-move-race", "text");
      if (!moveRaceTarget) throw new Error("Authorized target move fixture was rejected");
      const parking = document.createElement("div");
      document.body.appendChild(parking);
      try {
        parking.appendChild(moveRaceTarget);
        if (
          moveRaceTarget.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, composed: true }))
        ) {
          throw new Error("Authorized target same-node move race was not rejected");
        }
      } finally {
        parking.remove();
      }
    } finally {
      root.remove();
    }
  });
}

async function clickUniqueHashedTarget(
  surface: Frame | Page,
  rootSelector: string,
  expectedHash: string,
  kind: "bootstrap" | "text",
  timeoutMs: number,
  beforeAttempt?: (marker: string) => Promise<void>,
): Promise<void> {
  try {
    await surface.evaluate(installAuthorizedTargetMarker);
    await assertAuthorizedTargetMarkerRaceGuard(surface);
    await surface.evaluate(() => {
      const targetWindow = window as AuthorizedTargetMarkerWindow;
      targetWindow.slice16AuthorizedClick = undefined;
      targetWindow.slice16AuthorizedClickAt = undefined;
    });
  } catch {
    throw new Error("Authorized target selection race guard failed");
  }
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const marker = randomUUID();
    await beforeAttempt?.(marker);
    let markedRow: ReturnType<Awaited<ReturnType<typeof surface.waitForFunction>>["asElement"]>;
    try {
      const remainingMs = Math.max(1, deadline - Date.now());
      markedRow = (
        await surface.waitForFunction(
          async ({ hash, kind, marker, selector }) => {
            const mark = (window as AuthorizedTargetMarkerWindow).slice16MarkAuthorizedTarget;
            return (await mark?.(selector, hash, marker, kind)) ?? false;
          },
          { hash: expectedHash, kind, marker, selector: rootSelector },
          { timeout: remainingMs },
        )
      ).asElement();
      if (!markedRow) continue;
      const stillAuthorized = await markedRow.evaluate(
        (row, nonce) => row instanceof HTMLElement && row.isConnected && row.dataset.slice16AuthorizedTarget === nonce,
        marker,
      );
      if (!stillAuthorized) continue;
      await markedRow.click({ timeout: Math.min(30_000, remainingMs) });
      const clickAuthorized = await surface.evaluate(
        (nonce) => (window as AuthorizedTargetMarkerWindow).slice16AuthorizedClick === nonce,
        marker,
      );
      if (clickAuthorized) return;
    } catch {
      if (Date.now() >= deadline) break;
    } finally {
      await surface
        .evaluate((nonce) => (window as AuthorizedTargetMarkerWindow).slice16CancelAuthorizedTarget?.(nonce), marker)
        .catch(() => undefined);
      await markedRow?.dispose();
    }
  }
  throw new Error("Authorized target selection did not stabilize");
}

async function launchAuthorizedCluster(app: ElectronApplication, window: Page, contextSha256: string): Promise<Frame> {
  try {
    await app.evaluate(async ({ app: electronApp }) => {
      await electronApp.applicationMenu
        ?.getMenuItemById("view")
        ?.submenu?.getMenuItemById("navigate-to-catalog")
        ?.click();
    });
  } catch {
    throw new Error("Authorized cluster launch failed during catalog navigation");
  }
  const catalog = window.locator('[data-testid^="catalog-list-for-"]');
  try {
    await catalog.waitFor({ state: "visible", timeout: 120_000 });
  } catch {
    throw new Error("Authorized cluster launch failed while waiting for the catalog");
  }
  try {
    await clickUniqueHashedTarget(window, '[data-testid^="catalog-list-for-"]', contextSha256, "text", 120_000);
  } catch (error) {
    if (error instanceof Error && error.message.startsWith("Authorized target selection")) throw error;
    throw new Error("Authorized cluster launch failed during target selection");
  }
  let entityId: string;
  try {
    const routeEntityId = await window.waitForFunction(
      () => /^\/cluster\/([0-9a-f]{32})$/.exec(window.location.pathname)?.[1] ?? false,
      undefined,
      { timeout: 120_000 },
    );
    entityId = (await routeEntityId.jsonValue()) as string;
  } catch {
    throw new Error("Authorized cluster launch failed while waiting for the route");
  }
  let frameElement;
  try {
    frameElement = await window.waitForSelector(`iframe[id="cluster-frame-${entityId}"]`, { timeout: 120_000 });
  } catch {
    throw new Error("Authorized cluster launch failed while waiting for the frame");
  }
  const frame = await frameElement.contentFrame();
  if (!frame) throw new Error("Authorized Kubernetes cluster frame was unavailable");
  try {
    await frame.waitForSelector("[data-testid=cluster-sidebar]", { timeout: 120_000 });
  } catch {
    throw new Error("Authorized cluster launch failed while waiting for the sidebar");
  }
  return frame;
}

async function openKafkaClusters(frame: Frame): Promise<void> {
  const clusters = frame.locator('[data-testid="link-for-sidebar-item-freelensapp--kafka-extension-kafka-clusters"]');
  if (!(await clusters.isVisible().catch(() => false))) {
    await frame.click('[data-testid="link-for-sidebar-item-freelensapp--kafka-extension-kafka"]');
  }
  await clusters.waitFor({ state: "visible", timeout: 60_000 });
  await clusters.click();
  await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"), undefined, {
    timeout: 60_000,
  });
}

async function startAkhqBrowserSession(extensionRoot: string): Promise<AkhqBrowserSession> {
  const sessionId = randomUUID();
  const containerName = `freelens-kafka-akhq-browser-${sessionId}`;
  try {
    writeFileSync(AKHQ_BROWSER_LOCK_PATH, `${sessionId}\n`, { encoding: "utf8", flag: "wx", mode: 0o600 });
  } catch {
    throw new Error("AKHQ browser lifecycle lock is already held");
  }
  if (
    readFileSync(AKHQ_BROWSER_LOCK_PATH, "utf8") !== `${sessionId}\n` ||
    (statSync(AKHQ_BROWSER_LOCK_PATH).mode & 0o777) !== 0o600
  ) {
    throw new Error("AKHQ browser lock ownership setup failed");
  }
  let runtimeRoot: string | undefined;
  let runtimeMarkerWritten = false;
  try {
    if (readFileSync(AKHQ_BROWSER_LOCK_PATH, "utf8") !== `${sessionId}\n`) {
      throw new Error("AKHQ browser lock ownership mismatch");
    }
    runtimeRoot = mkdtempSync(path.join(tmpdir(), "freelens-akhq-browser-owner-"));
    chmodSync(runtimeRoot, 0o700);
    writeFileSync(path.join(runtimeRoot, ".parent-owner"), `${sessionId}\n`, {
      encoding: "utf8",
      flag: "wx",
      mode: 0o600,
    });
    runtimeMarkerWritten = true;
  } catch {
    const failures: string[] = [];
    if (runtimeRoot) {
      try {
        if (runtimeMarkerWritten) {
          removeOwnedDirectory(runtimeRoot, ".parent-owner", sessionId);
        } else {
          rmSync(runtimeRoot, { force: true, recursive: true });
          if (existsSync(runtimeRoot)) throw new Error("AKHQ browser runtime cleanup failed");
        }
      } catch {
        failures.push("runtime");
      }
    }
    if (failures.length === 0) {
      try {
        removeOwnedLock(AKHQ_BROWSER_LOCK_PATH, sessionId);
      } catch {
        failures.push("lock");
      }
    }
    throw new Error(
      failures.length > 0 ? "AKHQ browser ownership cleanup failed" : "AKHQ browser ownership setup failed",
    );
  }
  if (!runtimeRoot) throw new Error("AKHQ browser runtime setup failed");
  const ownedRuntimeRoot = runtimeRoot;
  const cleanupOwnedResources = (releaseLock = true) => {
    const failures: string[] = [];
    try {
      removeOwnedAkhqBrowserContainer(containerName, sessionId);
    } catch {
      failures.push("container");
    }
    try {
      removeOwnedDirectory(ownedRuntimeRoot, ".parent-owner", sessionId, true);
    } catch {
      failures.push("runtime");
    }
    if (releaseLock) {
      if (failures.length === 0) {
        try {
          removeOwnedLock(AKHQ_BROWSER_LOCK_PATH, sessionId);
        } catch {
          failures.push("lock");
        }
      }
    }
    if (failures.length > 0) throw new Error("AKHQ browser parent cleanup failed");
  };
  let child: ChildProcessWithoutNullStreams;
  try {
    child = spawn(process.execPath, ["--import", "tsx", "test/e2e/akhq-browser-session.real.ts"], {
      cwd: extensionRoot,
      env: {
        ...process.env,
        AKHQ_BROWSER_CONTAINER_NAME: containerName,
        AKHQ_BROWSER_LOCK_PATH,
        AKHQ_BROWSER_RUNTIME_DIR: ownedRuntimeRoot,
        AKHQ_BROWSER_SESSION_ID: sessionId,
      },
      stdio: ["pipe", "pipe", "pipe"],
    });
  } catch {
    try {
      cleanupOwnedResources();
    } catch {
      throw new Error("AKHQ browser spawn cleanup failed");
    }
    throw new Error("AKHQ browser helper spawn failed");
  }
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk: Buffer) => {
    stdout = `${stdout}${chunk.toString("utf8")}`.slice(-4_096);
  });
  child.stderr.on("data", (chunk: Buffer) => {
    stderr = `${stderr}${chunk.toString("utf8")}`.slice(-4_096);
  });
  let closeCode: number | null | undefined;
  const closed = new Promise<number | null>((resolve) =>
    child.once("close", (code) => {
      closeCode = code;
      resolve(code);
    }),
  );
  const helperExitError = () => {
    const phase = helperFailurePhase(stderr);
    return new Error(phase ? `AKHQ browser helper stopped during ${phase}` : "AKHQ browser helper stopped");
  };
  const assertRunning = () => {
    if (closeCode !== undefined || child.exitCode !== null || child.signalCode !== null) throw helperExitError();
  };
  const stopChild = async (): Promise<void> => {
    if (closeCode === undefined) {
      child.kill("SIGTERM");
      await closed;
    }
  };
  const stopAndCleanup = async (requireCleanExit: boolean): Promise<void> => {
    const failures: string[] = [];
    let processStopped = true;
    try {
      await stopChild();
    } catch {
      processStopped = false;
      failures.push("process");
    }
    try {
      cleanupOwnedResources(processStopped);
    } catch {
      failures.push("resources");
    }
    if (
      requireCleanExit &&
      (closeCode !== 0 ||
        !stdout.includes("AKHQ_BROWSER_STOPPING_VERIFIED\n") ||
        !stdout.includes("AKHQ_BROWSER_CLEAN\n"))
    ) {
      failures.push("protocol");
    }
    if (failures.length > 0) throw new Error(`AKHQ browser helper cleanup failed: ${failures.join("+")}`);
  };
  const ready = new Promise<void>((resolve, reject) => {
    const inspect = () => {
      if (stdout.includes("AKHQ_BROWSER_READY\n")) {
        resolve();
      }
    };
    child.stdout.on("data", inspect);
    child.once("error", () => {
      reject(new Error("AKHQ browser helper could not start"));
    });
    child.once("close", () => {
      if (stdout.includes("AKHQ_BROWSER_READY\n")) return;
      reject(helperExitError());
    });
    inspect();
  });
  try {
    await bounded(ready, 240_000, "AKHQ browser helper startup");
  } catch {
    try {
      await stopAndCleanup(false);
    } catch {
      throw new Error("AKHQ browser startup cleanup failed");
    }
    throw new Error("AKHQ browser helper did not become ready");
  }

  return {
    assertRunning,
    guard: async <T>(operation: Promise<T>): Promise<T> => {
      assertRunning();
      const result = await Promise.race([
        operation,
        closed.then(() => {
          throw helperExitError();
        }),
      ]);
      assertRunning();
      return result;
    },
    stop: () => stopAndCleanup(true),
  };
}

async function measureAkhqTopicPaint(page: Page, extensionRoot: string): Promise<number> {
  const session = await startAkhqBrowserSession(extensionRoot);
  const browserContext = page.context();
  let cleanupFailure: Error | undefined;
  const isAkhqServiceWorker = (url: string) => new URL(url).origin === AKHQ_BROWSER_ORIGIN;
  let serviceWorkerObserved = browserContext.serviceWorkers().some((worker) => isAkhqServiceWorker(worker.url()));
  const recordServiceWorker = (worker: { url(): string }) => {
    if (isAkhqServiceWorker(worker.url())) serviceWorkerObserved = true;
  };
  let forbiddenRequestObserved = false;
  const observeRequest = (request: { method(): string; redirectedFrom(): unknown; url(): string }) => {
    if (
      request.method() !== "GET" ||
      request.redirectedFrom() !== null ||
      new URL(request.url()).origin !== AKHQ_BROWSER_ORIGIN
    ) {
      forbiddenRequestObserved = true;
    }
  };
  browserContext.on("serviceworker", recordServiceWorker);
  page.on("request", observeRequest);
  try {
    await page.addInitScript(() => {
      if (!("serviceWorker" in navigator)) return;
      Object.defineProperty(navigator.serviceWorker, "register", {
        configurable: false,
        value: () => Promise.reject(new DOMException("Service Worker registration is blocked", "SecurityError")),
        writable: false,
      });
    });
    session.assertRunning();
    await page.route("**/*", async (route) => {
      const request = route.request();
      if (request.method() !== "GET" || new URL(request.url()).origin !== AKHQ_BROWSER_ORIGIN) {
        forbiddenRequestObserved = true;
        await route.abort("blockedbyclient");
        return;
      }
      await route.continue();
    });
    await page.routeWebSocket("**/*", (webSocket) => {
      forbiddenRequestObserved = true;
      webSocket.close();
    });
    const durationMs = await session.guard(
      (async () => {
        const startedAt = performance.now();
        await page.goto(`${AKHQ_BROWSER_ORIGIN}/ui/slice16-read-only/topic`, {
          timeout: 180_000,
          waitUntil: "domcontentloaded",
        });
        const currentUrl = new URL(page.url());
        if (
          currentUrl.origin !== AKHQ_BROWSER_ORIGIN ||
          currentUrl.pathname !== "/ui/slice16-read-only/topic" ||
          (await page.title()) !== "AKHQ"
        ) {
          throw new Error("AKHQ browser page identity was not verified");
        }
        const topicsHeading = page.getByRole("heading", { exact: true, level: 1, name: "Topics" });
        await topicsHeading.waitFor({ state: "visible", timeout: 180_000 });
        await page.waitForFunction(
          () =>
            [...document.querySelectorAll<HTMLElement>("table tbody tr")].some((row) => {
              const rectangle = row.getBoundingClientRect();
              return rectangle.width > 0 && rectangle.height > 0 && row.querySelectorAll("td").length > 1;
            }),
          undefined,
          { timeout: 180_000 },
        );
        await page.evaluate(
          () => new Promise<void>((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve()))),
        );
        const stillVisible = await page.evaluate(() =>
          [...document.querySelectorAll<HTMLElement>("table tbody tr")].some((row) => {
            const rectangle = row.getBoundingClientRect();
            return rectangle.width > 0 && rectangle.height > 0 && row.querySelectorAll("td").length > 1;
          }),
        );
        if (!stillVisible || !(await topicsHeading.isVisible())) {
          throw new Error("AKHQ topic rows were not visible after rendering");
        }
        if (forbiddenRequestObserved || serviceWorkerObserved) {
          throw new Error("AKHQ browser attempted a forbidden request");
        }
        return Math.round(performance.now() - startedAt);
      })(),
    );
    session.assertRunning();
    return durationMs;
  } finally {
    page.removeListener("request", observeRequest);
    if (serviceWorkerObserved || browserContext.serviceWorkers().some((worker) => isAkhqServiceWorker(worker.url()))) {
      cleanupFailure ??= new Error("AKHQ browser Service Worker boundary failed");
    }
    browserContext.off("serviceworker", recordServiceWorker);
    try {
      await session.stop();
    } catch {
      cleanupFailure ??= new Error("AKHQ browser helper cleanup failed");
    }
    if (cleanupFailure) throw cleanupFailure;
  }
}

async function measureOverviewPaint(frame: Frame, targetHostSha256: string): Promise<PaintEvidence> {
  const scan = frame.getByRole("button", { name: "Scan Kubernetes" }).last();
  await scan.waitFor({ state: "visible", timeout: 60_000 });
  await scan.click();
  await frame.waitForFunction(
    async (expectedHash) => {
      const digest = async (value: string) => {
        const bytes = new TextEncoder().encode(value.trim().toLowerCase());
        return [...new Uint8Array(await crypto.subtle.digest("SHA-256", bytes))]
          .map((byte) => byte.toString(16).padStart(2, "0"))
          .join("");
      };
      const rows = [...document.querySelectorAll<HTMLElement>(".KafkaClusterTable .TableRow[data-bootstrap]")];
      const matches = [];
      for (const row of rows) {
        const hosts = (row.dataset.bootstrap ?? "").split(",").map((entry) => entry.trim().replace(/:\d+$/, ""));
        if ((await Promise.all(hosts.map(digest))).includes(expectedHash)) matches.push(row);
      }
      return matches.length === 1 && matches[0].getAttribute("role") === "button";
    },
    targetHostSha256,
    { timeout: 180_000 },
  );
  await frame.evaluate(() => {
    const performanceWindow = window as typeof window & {
      kafkaPackagedPaintExpectedMarker?: string;
      kafkaPackagedPaintEvidence?: Partial<PaintEvidence> & { startedAt?: number };
      kafkaPackagedPaintObserver?: MutationObserver;
      slice16AuthorizedClick?: string;
      slice16AuthorizedClickAt?: number;
    };
    const evidence: Partial<PaintEvidence> & { startedAt?: number } = {};
    let framePending = false;
    const mark = () => {
      if (
        performanceWindow.kafkaPackagedPaintExpectedMarker === undefined ||
        performanceWindow.slice16AuthorizedClick !== performanceWindow.kafkaPackagedPaintExpectedMarker ||
        performanceWindow.slice16AuthorizedClickAt === undefined
      ) {
        return;
      }
      if (framePending) return;
      evidence.startedAt ??= performanceWindow.slice16AuthorizedClickAt;
      framePending = true;
      requestAnimationFrame(() =>
        requestAnimationFrame(() => {
          framePending = false;
          const page = document.querySelector<HTMLElement>('[data-testid="kafka-overview-page"]');
          if (!page || page.getBoundingClientRect().height <= 0) return;
          const elapsed = Math.round(performance.now() - evidence.startedAt);
          const summary = page.querySelector<HTMLElement>('[aria-label="Kafka connection summary"]');
          if (evidence.metadataPaintMs === undefined && summary && summary.getBoundingClientRect().height > 0) {
            evidence.metadataPaintMs = elapsed;
          }
          const topology = page.querySelector<HTMLElement>('[data-testid="kafka-topology-freshness"]');
          const metricValue = (label: string) =>
            [...page.querySelectorAll<HTMLElement>(".KafkaMetric")]
              .find((metric) => metric.textContent?.includes(label))
              ?.querySelector<HTMLElement>(".KafkaMetricValue");
          const onlineBroker = metricValue("Online brokers");
          const unavailablePartitions = metricValue("Unavailable partitions");
          const underReplicated = metricValue("Under-replicated");
          const visibleNumber = (element: HTMLElement | null | undefined) =>
            Boolean(
              element && element.getBoundingClientRect().height > 0 && /^\d+$/.test(element.textContent?.trim() ?? ""),
            );
          if (
            evidence.topologyPaintMs === undefined &&
            topology &&
            topology.getBoundingClientRect().height > 0 &&
            Number(topology.dataset.updatedAt) > 0 &&
            visibleNumber(onlineBroker) &&
            visibleNumber(unavailablePartitions) &&
            visibleNumber(underReplicated)
          ) {
            evidence.topologyPaintMs = elapsed;
            evidence.aggregateStillRunning = page.dataset.healthState === "updating";
          }
          const useful = [evidence.metadataPaintMs, evidence.topologyPaintMs].filter(
            (value): value is number => value !== undefined,
          );
          if (useful.length > 0) evidence.firstUsefulPaintMs = Math.min(...useful);
        }),
      );
    };
    const observer = new MutationObserver(mark);
    observer.observe(document.documentElement, {
      attributes: true,
      childList: true,
      characterData: true,
      subtree: true,
    });
    performanceWindow.kafkaPackagedPaintEvidence = evidence;
    performanceWindow.kafkaPackagedPaintObserver = observer;
    mark();
  });
  await clickUniqueHashedTarget(frame, ".KafkaClusterTable", targetHostSha256, "bootstrap", 180_000, async (marker) => {
    await frame.evaluate((expectedMarker) => {
      const performanceWindow = window as typeof window & { kafkaPackagedPaintExpectedMarker?: string };
      performanceWindow.kafkaPackagedPaintExpectedMarker = expectedMarker;
    }, marker);
  });
  await frame.waitForFunction(
    () => {
      const evidence = (window as typeof window & { kafkaPackagedPaintEvidence?: Partial<PaintEvidence> })
        .kafkaPackagedPaintEvidence;
      return evidence?.metadataPaintMs !== undefined && evidence.topologyPaintMs !== undefined;
    },
    undefined,
    { timeout: 180_000 },
  );
  const summary = frame.locator('details[data-testid="kafka-health-progress"] > summary');
  await summary.waitFor({ state: "visible", timeout: 30_000 });
  await summary.click();
  const interactiveWhileUpdating = await frame
    .locator('details[data-testid="kafka-health-progress"]')
    .evaluate((element) => (element as HTMLDetailsElement).open);
  const postInteraction = await frame.evaluate(() => {
    const page = document.querySelector<HTMLElement>('[data-testid="kafka-overview-page"]');
    const progressElement = document.querySelector<HTMLElement>('.KafkaOperationProgress[data-operation="health"]');
    return {
      completed: progressElement?.dataset.completed ?? null,
      healthState: page?.dataset.healthState ?? null,
      phase: progressElement?.dataset.phase ?? null,
    };
  });
  if (postInteraction.healthState !== "updating" || postInteraction.phase === null) {
    throw new Error("Aggregate progress was not active after the interaction");
  }
  await summary.click();
  await frame.waitForFunction(
    (initial) => {
      const page = document.querySelector<HTMLElement>('[data-testid="kafka-overview-page"]');
      const progressElement = document.querySelector<HTMLElement>('.KafkaOperationProgress[data-operation="health"]');
      if (initial.healthState === "updating" && page?.dataset.healthState === "ready") return true;
      if (!progressElement) return false;
      const phase = progressElement.dataset.phase ?? null;
      const completed = progressElement.dataset.completed ?? null;
      return (phase !== null && phase !== initial.phase) || (completed !== null && completed !== initial.completed);
    },
    postInteraction,
    { timeout: 180_000 },
  );
  return frame.evaluate((interactive) => {
    const performanceWindow = window as typeof window & {
      kafkaPackagedPaintEvidence?: Partial<PaintEvidence>;
      kafkaPackagedPaintObserver?: MutationObserver;
    };
    performanceWindow.kafkaPackagedPaintObserver?.disconnect();
    const evidence = performanceWindow.kafkaPackagedPaintEvidence;
    if (
      evidence?.aggregateStillRunning === undefined ||
      evidence.firstUsefulPaintMs === undefined ||
      evidence.metadataPaintMs === undefined ||
      evidence.topologyPaintMs === undefined
    ) {
      throw new Error("Packaged Overview paint evidence was incomplete");
    }
    return {
      aggregateStillRunning: evidence.aggregateStillRunning,
      firstUsefulPaintMs: evidence.firstUsefulPaintMs,
      interactiveWhileUpdating: interactive,
      metadataPaintMs: evidence.metadataPaintMs,
      progressContinued: true,
      topologyPaintMs: evidence.topologyPaintMs,
    } satisfies PaintEvidence;
  }, interactiveWhileUpdating);
}

async function runPaintMeasurement(
  index: number,
  context: string,
  contextSha256: string,
  targetHostSha256: string,
): Promise<Omit<RunEvidence, "preparationAttempts">> {
  const extensionPath = process.env.EXTENSION_PATH;
  if (!extensionPath?.endsWith(".tgz")) throw new Error("A packaged extension path is required");
  const expectedPackageHash = process.env.EXPECTED_EXTENSION_SHA256;
  if (!expectedPackageHash || !/^[0-9a-f]{64}$/i.test(expectedPackageHash)) {
    throw new Error("EXPECTED_EXTENSION_SHA256 is required");
  }
  const packageHash = createHash("sha256").update(readFileSync(extensionPath)).digest("hex");
  if (packageHash !== expectedPackageHash) throw new Error("Packaged extension identity did not match");
  const extensionRoot = path.dirname(extensionPath);
  const executablePath = path.resolve(process.cwd(), "dist/linux-unpacked/freelens");
  const runtimeRoot = mkdtempSync(path.join(tmpdir(), `freelens-kafka-paint-${index}-`));
  const homeDirectory = path.join(runtimeRoot, "home");
  const kubeconfigPath = path.join(homeDirectory, ".kube", "config");
  const appDataDirectory = path.join(runtimeRoot, "app-data");
  const storeDirectory = path.join(appDataDirectory, "Freelens");
  let app: ElectronApplication | undefined;
  let cleanupFailure: Error | undefined;
  let operationFailure: unknown;
  let operationPhase = "runtime-setup";
  try {
    mkdirSync(path.dirname(kubeconfigPath), { recursive: true });
    mkdirSync(storeDirectory, { recursive: true });
    const userHome = process.env.HOME;
    if (!userHome) throw new Error("Packaged performance credential home is unavailable");
    const awsDirectory = path.join(userHome, ".aws");
    const awsDirectoryPath = realpathSync(awsDirectory);
    if (!statSync(awsDirectoryPath).isDirectory() || awsDirectoryPath.startsWith(`${runtimeRoot}${path.sep}`)) {
      throw new Error("Packaged performance AWS credential directory is invalid");
    }
    const awsLinkPath = path.join(homeDirectory, ".aws");
    symlinkSync(awsDirectoryPath, awsLinkPath, "dir");
    if (!lstatSync(awsLinkPath).isSymbolicLink() || realpathSync(awsLinkPath) !== awsDirectoryPath) {
      throw new Error("Packaged performance AWS credential bridge is invalid");
    }
    operationPhase = "kubeconfig-export";
    let kubeconfig: string;
    try {
      kubeconfig = execFileSync("kubectl", ["config", "view", "--raw", "--minify", "--context", context], {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "ignore"],
      });
    } catch {
      throw new Error("Packaged performance kubeconfig creation failed");
    }
    operationPhase = "kubeconfig-isolation";
    writeFileSync(kubeconfigPath, kubeconfig, { encoding: "utf8", mode: 0o600 });
    chmodSync(kubeconfigPath, 0o600);
    let visibleContexts: string[];
    try {
      visibleContexts = execFileSync(
        "kubectl",
        ["--kubeconfig", kubeconfigPath, "config", "get-contexts", "-o", "name"],
        { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] },
      )
        .trim()
        .split(/\s+/);
    } catch {
      throw new Error("Packaged performance kubeconfig validation failed");
    }
    if (visibleContexts.length !== 1 || sha256(visibleContexts[0]) !== contextSha256) {
      throw new Error("Packaged performance kubeconfig was not isolated");
    }
    writeFileSync(
      path.join(storeDirectory, "lens-user-store.json"),
      `${JSON.stringify({
        __internal__: { migrations: { version: "0.1.0" } },
        preferences: { syncKubeconfigEntries: [{ filePath: kubeconfigPath }] },
      })}\n`,
      { encoding: "utf8", mode: 0o600 },
    );

    const packageJson = JSON.parse(readFileSync(path.resolve(process.cwd(), "package.json"), "utf8")) as {
      devDependencies: { playwright: string };
    };
    operationPhase = "electron-launch";
    global.setImmediate = setImmediate;
    app = await electron.launch({
      args: ["--integration-testing"],
      executablePath,
      bypassCSP: true,
      env: {
        ...process.env,
        HOME: homeDirectory,
        KUBECONFIG: kubeconfigPath,
        FREELENS_INTEGRATION_TESTING_DIR: appDataDirectory,
        LOG_LEVEL: "warn",
        PW_VERSION_OVERRIDE: packageJson.devDependencies.playwright.replace(/[^0-9.]/g, ""),
      },
      timeout: 100_000,
    });
    operationPhase = "main-window";
    const window = await waitForMainWindow(app);
    operationPhase = "extension-install";
    try {
      await installExtension(app, window, extensionPath);
    } catch {
      throw new PackagedPreparationError("Packaged performance preparation failed: extension-install");
    }
    operationPhase = "cluster-launch";
    const frame = await launchAuthorizedCluster(app, window, contextSha256);
    operationPhase = "kafka-navigation";
    await openKafkaClusters(frame);
    operationPhase = "overview-paint";
    const freelens = await measureOverviewPaint(frame, targetHostSha256);
    operationPhase = "akhq-paint";
    const akhqTopicPaintMs = await measureAkhqTopicPaint(window, extensionRoot);
    operationPhase = "complete";
    return { akhqTopicPaintMs, freelens };
  } catch (error) {
    operationFailure = error;
    throw error;
  } finally {
    if (app) {
      try {
        await closeElectronApplication(app);
      } catch {
        cleanupFailure = new Error("Packaged performance Electron cleanup failed");
      }
    }
    try {
      rmSync(runtimeRoot, { force: true, recursive: true });
    } catch {
      cleanupFailure ??= new Error("Packaged performance runtime cleanup failed");
    }
    if (existsSync(runtimeRoot)) cleanupFailure ??= new Error("Packaged performance runtime cleanup failed");
    if (cleanupFailure) {
      if (operationFailure) {
        throw new Error(`Packaged performance run failed during ${operationPhase}; cleanup also failed`);
      }
      throw cleanupFailure;
    }
  }
}

async function runPaintMeasurementWithPreparationRetry(
  index: number,
  context: string,
  contextSha256: string,
  targetHostSha256: string,
): Promise<RunEvidence> {
  for (let preparationAttempts = 1; preparationAttempts <= 3; preparationAttempts++) {
    try {
      return {
        ...(await runPaintMeasurement(index, context, contextSha256, targetHostSha256)),
        preparationAttempts,
      };
    } catch (error) {
      if (!(error instanceof PackagedPreparationError) || preparationAttempts === 3) throw error;
    }
  }
  throw new Error("Packaged performance preparation retry invariant failed");
}

describeReal("Kafka authorized packaged performance", () => {
  it(
    "paints first-use Overview and topology within the production SLO",
    async () => {
      const { context, contextSha256, targetHost, targetHostSha256 } = authorization();
      const outputPath = process.env.PACKAGED_PERFORMANCE_OUTPUT;
      if (!outputPath) throw new Error("PACKAGED_PERFORMANCE_OUTPUT is required");
      if (existsSync(outputPath)) throw new Error("PACKAGED_PERFORMANCE_OUTPUT must not already exist");
      const packageSha256 = process.env.EXPECTED_EXTENSION_SHA256;
      if (!packageSha256 || !/^[0-9a-f]{64}$/i.test(packageSha256)) {
        throw new Error("EXPECTED_EXTENSION_SHA256 is required");
      }
      const runs: RunEvidence[] = [];
      for (let index = 1; index <= RUN_COUNT; index++) {
        runs.push(await runPaintMeasurementWithPreparationRetry(index, context, contextSha256, targetHostSha256));
      }
      const evidence = {
        schemaVersion: 1,
        capturedAt: new Date().toISOString(),
        mode: "authorized-read-only",
        packageSha256,
        authorizationPinsVerified: sha256(context) === contextSha256 && sha256(targetHost) === targetHostSha256,
        freelens: {
          firstUsefulPaint: summarize(runs.map((run) => run.freelens.firstUsefulPaintMs)),
          metadataPaint: summarize(runs.map((run) => run.freelens.metadataPaintMs)),
          topologyPaint: summarize(runs.map((run) => run.freelens.topologyPaintMs)),
          aggregateStillRunning: runs.every((run) => run.freelens.aggregateStillRunning),
          interactiveWhileUpdating: runs.every((run) => run.freelens.interactiveWhileUpdating),
          progressContinued: runs.every((run) => run.freelens.progressContinued),
        },
        akhq: { topicPaint: summarize(runs.map((run) => run.akhqTopicPaintMs)) },
        runs,
      };
      expect(evidence.freelens.firstUsefulPaint.durationMs).toBeLessThanOrEqual(OVERVIEW_SLO_MS);
      expect(evidence.freelens.topologyPaint.durationMs).toBeLessThanOrEqual(OVERVIEW_SLO_MS);
      expect(evidence.freelens.firstUsefulPaint.durationMs).toBeLessThanOrEqual(evidence.akhq.topicPaint.durationMs);
      expect(evidence.freelens.aggregateStillRunning).toBe(true);
      expect(evidence.freelens.interactiveWhileUpdating).toBe(true);
      expect(evidence.freelens.progressContinued).toBe(true);
      publishEvidence(outputPath, evidence);
    },
    20 * 60 * 1000,
  );
});
