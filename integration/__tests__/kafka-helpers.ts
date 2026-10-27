/**
 * Copyright (c) Freelens Authors. All rights reserved.
 * Licensed under MIT License. See LICENSE in root directory for more information.
 */

// What the integration sessions share, after the template's `extension-helpers.ts`: they attach over the Chrome
// DevTools Protocol to Freelens started with `pnpm dev` from a freelensapp/freelens checkout, and drive its renderer
// with Playwright. The cluster is the repository's lab (`pnpm lab:up`): the kind cluster `freelens-kafka-lab`, whose
// kubeconfig is `.lab/kubeconfig`; Freelens must sync that file (`pnpm lab:freelens` starts it so). Nothing reads or
// changes the developer's kubeconfig (SPEC-019, REQ-232). Its name does not match `*.tests.ts`, so Vitest does not
// collect it as a test file.

import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import { chromium } from "playwright";

import type { Browser, ConsoleMessage, Frame, Locator, Page } from "playwright";

export const extensionName = "@freelensapp/kafka-extension";

/** The root of this checkout, installed in place, where the fixture scripts run. */
export const repoRoot = path.resolve(import.meta.dirname, "..", "..");

const cdpUrl = process.env.FREELENS_CDP_URL || "http://127.0.0.1:9223";

// The log of the main process, which CDP does not carry: the output of `pnpm dev` of Freelens.
const mainLogFile = process.env.FREELENS_MAIN_LOG;

/** The lab's kubeconfig and context: nothing else is used. */
export const labKubeconfig = path.join(repoRoot, ".lab", "kubeconfig");
export const labContext = "kind-freelens-kafka-lab";

const sanitizedExtensionId = extensionName.replace("@", "").replace("/", "--");

export const viewTimeout = { timeout: 60 * 1000 };

const ansiEscapePattern = /\u001b\[[0-9;]*m/g;

/**
 * A class of the extension in a selector: a local class of its CSS module, which the build hashes with its name
 * inside (`_KafkaClusterTable_x1y2z_42`), or one of its unstyled hook classes, which stay as they are.
 */
export const k = (name: string) => `:is(.${name}, [class*="_${name}_"])`;

export interface AppSession {
  browser: Browser;
  /** The root frame's page, where the catalog and the extensions render. */
  window: Page;
  /** The errors logged so far. */
  errors(): string[];
  /** Stops collecting errors and closes the connection; returns every error logged. */
  close(): Promise<string[]>;
}

function checkedLabKubeconfig(): string | undefined {
  if (!fs.existsSync(labKubeconfig)) return `no lab kubeconfig at ${labKubeconfig}: run 'pnpm lab:up'`;
  const text = fs.readFileSync(labKubeconfig, "utf8");
  const servers = [...text.matchAll(/^\s*server:\s*(\S+)\s*$/gm)].map((match) => match[1]);

  if (
    servers.length === 0 ||
    servers.some((server) => !/^https:\/\/(127\.0\.0\.1|localhost|\[::1\]):\d+\/?$/.test(server))
  ) {
    return `${labKubeconfig} names a server that is not local`;
  }
  return undefined;
}

/**
 * Whether Freelens dev answers on its CDP endpoint and the lab cluster is reachable. Outside CI a session skips with a
 * warning otherwise; in CI the run fails.
 */
export async function appReady({ cluster }: { cluster: boolean }): Promise<boolean> {
  let reason: string | undefined;

  try {
    const response = await fetch(new URL("/json/version", cdpUrl), { signal: AbortSignal.timeout(5000) });

    if (!response.ok) reason = `${cdpUrl} answered ${response.status}`;
  } catch (error) {
    reason = `Freelens dev does not answer on ${cdpUrl}: ${error instanceof Error ? error.message : error}`;
  }
  if (!reason && cluster) {
    reason = checkedLabKubeconfig();
    if (
      !reason &&
      spawnSync("kubectl", ["--kubeconfig", labKubeconfig, "--context", labContext, "get", "--raw", "/readyz"])
        .status !== 0
    ) {
      reason = `the lab cluster ${labContext} is not reachable`;
    }
  }
  if (!reason) return true;
  if (process.env.CI) throw new Error(`Cannot run the integration tests: ${reason}`);
  console.warn(`Skipping the integration tests: ${reason}`);
  return false;
}

function collectErrors(window: Page) {
  const errorLogs: string[] = [];
  let mainLogOffset = mainLogFile && fs.existsSync(mainLogFile) ? fs.statSync(mainLogFile).size : 0;

  const onConsole = (msg: ConsoleMessage) => {
    if (msg.type() === "error") errorLogs.push(`[renderer] ${msg.text().replaceAll(ansiEscapePattern, "")}`);
  };
  const onPageError = (error: Error) => {
    errorLogs.push(`[renderer] ${error.stack ?? error.message}`);
  };
  const readMainLog = () => {
    if (!mainLogFile || !fs.existsSync(mainLogFile)) return;
    const size = fs.statSync(mainLogFile).size;

    if (size < mainLogOffset) mainLogOffset = 0;
    if (size === mainLogOffset) return;
    const fd = fs.openSync(mainLogFile, "r");

    try {
      const buffer = Buffer.alloc(size - mainLogOffset);

      fs.readSync(fd, buffer, 0, buffer.length, mainLogOffset);
      mainLogOffset = size;
      for (const line of buffer.toString("utf8").split("\n")) {
        const text = line.replaceAll(ansiEscapePattern, "");

        if (/^(?:[\w@/.-]+:[\w:-]+: )?\s*error:/.test(text)) errorLogs.push(`[main] ${text.trim()}`);
      }
    } finally {
      fs.closeSync(fd);
    }
  };

  window.on("console", onConsole);
  window.on("pageerror", onPageError);
  return {
    errors: () => {
      readMainLog();
      return [...errorLogs];
    },
    stop: () => {
      window.off("console", onConsole);
      window.off("pageerror", onPageError);
    },
  };
}

async function rootPage(browser: Browser): Promise<Page> {
  const deadline = Date.now() + viewTimeout.timeout;

  while (Date.now() < deadline) {
    const page = browser
      .contexts()
      .flatMap((context) => context.pages())
      .find(
        (candidate) => URL.canParse(candidate.url()) && new URL(candidate.url()).hostname === "renderer.freelens.app",
      );

    if (page) {
      await page.waitForFunction(() => globalThis.FreelensExtensionApi?.Renderer !== undefined, null, viewTimeout);
      return page;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error(`No Freelens window on ${cdpUrl}`);
}

/** Connects to Freelens dev; closing the session closes only the connection, the app keeps running. */
export async function connectApp(): Promise<AppSession> {
  const browser = await chromium.connectOverCDP(cdpUrl);

  try {
    const window = await rootPage(browser);
    const collector = collectErrors(window);

    return {
      browser,
      window,
      errors: collector.errors,
      close: async () => {
        const errors = collector.errors();

        collector.stop();
        await browser.close();
        return errors;
      },
    };
  } catch (error) {
    await browser.close();
    throw error;
  }
}

/** Navigates the root frame with the extension API: the application menu belongs to the main process. */
export async function navigate(window: Page, url: string) {
  await window.evaluate((target) => {
    const renderer = globalThis.FreelensExtensionApi.Renderer;

    if (!renderer) throw new Error("The page has no Renderer API");
    renderer.Navigation.navigate(target);
  }, url);
}

/** Dismisses the notifications, so that one in its enter animation does not take a click. */
export async function dismissNotifications(window: Page) {
  const selector =
    'i[data-testid*="close-notification-for-notification_"], div[class*="close-button-module__closeButton--"][aria-label="Close"]';

  for (let attempt = 0; attempt < 10; attempt++) {
    const buttons = await window.$$(selector);

    if (buttons.length === 0) break;
    for (const button of buttons) await button.click({ force: true }).catch(() => {});
    await window.waitForTimeout(200);
  }
}

async function uninstallExtension(window: Page, row: Locator) {
  await row.locator('[id^="menu-actions-for-installed-extensions-for-"]').click();
  await window.locator(".MenuActions .MenuItem").getByText("Uninstall", { exact: true }).click();
  await window.getByTestId("confirmation-dialog").getByTestId("confirm").click();
  await row.waitFor({ state: "detached", ...viewTimeout });
}

/**
 * Installs this checkout in place from the Extensions page and waits until it is enabled; an enabled installation of
 * the checkout is kept, any other installation of the extension is uninstalled first.
 */
export async function installExtension({ window }: AppSession) {
  await navigate(window, "/extensions");
  const table = window.getByTestId("extensions-table");
  const row = table.locator("tbody tr", { hasText: extensionName });
  const enabled = row.locator("td").nth(2).getByText("Enabled", { exact: true });

  await table.or(window.getByText("There are no extensions installed.", { exact: true })).waitFor(viewTimeout);
  if (await row.count()) {
    const inPlace = await row.locator(`[title="Loaded in place from ${repoRoot}"]`).count();

    if (inPlace && (await enabled.count())) return;
    await uninstallExtension(window, row);
  }
  await window.getByPlaceholder("Name, URL, or path to a package or directory").fill(repoRoot);
  await window.getByRole("button", { name: "Install", exact: true }).click();
  await window.getByTestId("confirmation-dialog").getByTestId("confirm").click(viewTimeout);
  await enabled.waitFor(viewTimeout);
  await dismissNotifications(window);
}

/** The catalog id of the lab cluster, looked up by its context name. */
export async function labClusterId(window: Page): Promise<string> {
  const id = await window.waitForFunction(
    (name) =>
      globalThis.FreelensExtensionApi.Renderer?.Catalog.catalogEntities
        .getItemsForApiKind("entity.k8slens.dev/v1alpha1", "KubernetesCluster")
        .find((entity) => entity.getName() === name)
        ?.getId(),
    labContext,
    viewTimeout,
  );

  return id.jsonValue() as Promise<string>;
}

/** Opens the lab cluster and returns its frame, once the extension's sidebar entry is there. */
export async function openLabCluster(window: Page): Promise<Frame> {
  const id = await labClusterId(window);

  await navigate(window, `/cluster/${id}`);
  const element = await window.waitForSelector(`#cluster-frame-${id}`, { timeout: 120_000 });
  const frame = await element.contentFrame();

  if (!frame) throw new Error(`No iframe for ${labContext}`);
  await frame.waitForSelector("[data-testid=cluster-sidebar]", { timeout: 120_000 });
  await frame.waitForSelector(`[data-testid="link-for-sidebar-item-${sanitizedExtensionId}-kafka"]`, {
    timeout: 120_000,
  });
  return frame;
}

/** Runs a command, failing with its stderr. */
function run(command: string, args: string[], env: Record<string, string> = {}): string {
  const { error, status, signal, stdout, stderr } = spawnSync(command, args, {
    cwd: repoRoot,
    encoding: "utf8",
    env: { ...process.env, ...env },
    timeout: 5 * 60 * 1000,
  });

  if (error) throw new Error(`${command} ${args.join(" ")} failed: ${error.message}`);
  if (status !== 0) throw new Error(`${command} ${args.join(" ")} exited with ${status ?? signal}: ${stderr.trim()}`);
  return stdout;
}

/** `scripts/lab.sh` of the repository: `lab("apply", "direct")`. */
export function lab(...args: string[]): string {
  return run("bash", ["scripts/lab.sh", ...args]);
}

/** kubectl on the lab only, by the lab's kubeconfig file and context. */
export function labKubectl(...args: string[]): string {
  return run("kubectl", ["--kubeconfig", labKubeconfig, "--context", labContext, ...args]);
}

/** A command in the repository, as the fixture scripts run: `fixture("pnpm", ["tsx", "test/e2e/…"], env)`. */
export function fixture(command: string, args: string[], env: Record<string, string> = {}): string {
  return run(command, args, env);
}

/**
 * The extension store file of the profile Freelens dev runs with: `FREELENS_INTEGRATION_TESTING_DIR`, under the
 * application folder `FreelensDev` (`pnpm dev`) or `Freelens` (a packaged app).
 */
export function kafkaStorePath(): string {
  const profile = process.env.FREELENS_INTEGRATION_TESTING_DIR;

  if (!profile) throw new Error("FREELENS_INTEGRATION_TESTING_DIR is not set: the tests read the extension store");
  const relative = ["extension-store", "@freelensapp", "kafka-extension", "freelens-kafka-state-store.json"];
  const candidates = ["FreelensDev", "Freelens"].map((app) => path.join(profile, app, ...relative));

  return candidates.find((candidate) => fs.existsSync(candidate)) ?? candidates[0];
}

/** The values of the extension store, as main and the renderer keep them in sync on disk. */
export function kafkaStoreValues(): Record<string, string> {
  try {
    const model = JSON.parse(fs.readFileSync(kafkaStorePath(), "utf8")) as { values?: Record<string, string> };

    return model.values ?? {};
  } catch {
    return {};
  }
}
