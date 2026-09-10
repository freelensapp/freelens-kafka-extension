/**
 * Copyright (c) OpenLens Authors. All rights reserved.
 * Licensed under MIT License. See LICENSE in root directory for more information.
 */

import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "@jest/globals";
import {
  type ConsoleMessage,
  type ElectronApplication,
  _electron as electron,
  type Frame,
  type Locator,
  type Page,
} from "playwright";
import { type KafkaConnectFixture, startKafkaConnectFixture } from "../fixtures/kafka-connect-fixture";
import { type SchemaRegistryFixture, startSchemaRegistryFixture } from "../fixtures/schema-registry-fixture";
import { kindReady } from "../helpers/kind";
import * as utils from "../helpers/utils";

const TEST_KIND_CLUSTER_NAME = process.env.TEST_KIND_CLUSTER_NAME || "kind";
const TEST_NAMESPACE = process.env.TEST_NAMESPACE || "integration-tests";

describe("extensions page tests", () => {
  let window: Page;
  let cleanup: undefined | (() => Promise<void>);
  const errorLogs: string[] = [];
  const processErrorLogs: string[] = [];
  const outputErrorPattern = /\[out\]\s*error:/i;
  const ansiEscapePattern = /\u001b\[[0-9;]*m/g;
  let processOutputBuffer = "";
  let restoreProcessOutputHooks: undefined | (() => void);

  const collectOutputErrors = (chunk: string | Uint8Array) => {
    const text = typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
    processOutputBuffer += text;

    // Keep buffer bounded while preserving enough tail to match split patterns across chunks.
    if (processOutputBuffer.length > 200_000) {
      processOutputBuffer = processOutputBuffer.slice(-20_000);
    }

    const normalizedOutput = processOutputBuffer.replaceAll(ansiEscapePattern, "");

    if (outputErrorPattern.test(normalizedOutput)) {
      processErrorLogs.push(normalizedOutput.trim());
      processOutputBuffer = "";
    }
  };

  const logger = (msg: ConsoleMessage) => {
    const text = msg.text();
    const normalizedText = text.replaceAll(ansiEscapePattern, "");

    console.log(text);

    // Some app logs are emitted as "log" messages, so inspect both console type and message content.
    if (msg.type() === "error" || outputErrorPattern.test(normalizedText)) {
      errorLogs.push(`[${msg.type()}] ${normalizedText}`);
    }
  };

  beforeAll(async () => {
    let app: ElectronApplication;

    const originalStdoutWrite = process.stdout.write.bind(process.stdout);
    const originalStderrWrite = process.stderr.write.bind(process.stderr);

    process.stdout.write = ((chunk, encoding, cb) => {
      collectOutputErrors(chunk);

      return originalStdoutWrite(chunk, encoding as never, cb as never);
    }) as typeof process.stdout.write;

    process.stderr.write = ((chunk, encoding, cb) => {
      collectOutputErrors(chunk);

      return originalStderrWrite(chunk, encoding as never, cb as never);
    }) as typeof process.stderr.write;

    restoreProcessOutputHooks = () => {
      process.stdout.write = originalStdoutWrite;
      process.stderr.write = originalStderrWrite;
    };

    ({ window, cleanup, app } = await utils.start());
    window.on("console", logger);
    console.log("await utils.clickWelcomeButton");
    await utils.clickWelcomeButton(window);

    // Navigate to extensions page
    console.log("await app.evaluate");
    await app.evaluate(async ({ app }) => {
      await app.applicationMenu
        ?.getMenuItemById(process.platform === "darwin" ? "mac" : "file")
        ?.submenu?.getMenuItemById("navigate-to-extensions")
        ?.click();
    });

    // Trigger extension install
    const textbox = window.getByPlaceholder("Name or file path or URL");
    console.log("await textbox.fill");
    await textbox.fill(process.env.EXTENSION_PATH || "@freelensapp/kafka-extension");
    const install_button_selector = 'button[class*="Button install-module__button--"]';
    console.log("await window.click [data-waiting=false]");
    await window.click(install_button_selector.concat("[data-waiting=false]"));

    // Expect extension to be listed in installed list and enabled
    console.log('await window.waitForSelector div[class*="installed-extensions-module__extensionName--"]');
    const installedExtensionName = await (
      await window.waitForSelector('div[class*="installed-extensions-module__extensionName--"]', { timeout: 120_000 })
    ).textContent();
    expect(installedExtensionName).toBe("@freelensapp/kafka-extension");
    const installedExtensionState = await (
      await window.waitForSelector('div[class*="installed-extensions-module__enabled--"]', { timeout: 120_000 })
    ).textContent();
    expect(installedExtensionState).toBe("Enabled");
    console.log('await window.click i[data-testid*="close-notification-for-notification_"]');
    await window.click('i[data-testid*="close-notification-for-notification_"]');
    console.log('await window.click div[class*=[close-button-module__closeButton--"][aria-label="Close"]');
    await window.click('div[class*="close-button-module__closeButton--"][aria-label="Close"]');
  }, 180 * 1000);

  afterAll(
    async () => {
      // Keep listeners active through cleanup to catch late shutdown errors in CI logs.
      await cleanup?.();
      window.off("console", logger);
      restoreProcessOutputHooks?.();
    },
    10 * 60 * 1000,
  );

  it(
    "installs and enables the extension",
    async () => {
      // The beforeAll already waits for the "Enabled" state; assert no extension-scoped errors.
      const kafkaErrors = [...errorLogs, ...processErrorLogs].filter((line) => /kafka/i.test(line));
      expect(kafkaErrors).toEqual([]);
    },
    100 * 60 * 1000,
  );
});

// Opening the extension's cluster page in a real cluster frame is the only way to catch
// runtime-only failures (e.g. creating an IPC `Singleton` via `new` instead of
// `createInstance()`) that unit tests and the static gates cannot see. This block is
// skipped unless a KinD cluster named `${TEST_KIND_CLUSTER_NAME}` is available.
const clusterDescribe = kindReady(TEST_KIND_CLUSTER_NAME, TEST_NAMESPACE) ? describe : describe.skip;

async function launchKindClusterFromCatalogStable(kindClusterName: string, window: Page): Promise<Frame> {
  const catalogList = window.locator('[data-testid^="catalog-list-for-"]');
  await catalogList.waitFor({ state: "visible", timeout: 120_000 });

  const search = catalogList.getByPlaceholder("Search...");
  await search.fill(`kind-${kindClusterName}`);

  const rowSelector = `div.TableCell >> text='kind-${kindClusterName}'`;
  await window.waitForSelector(rowSelector, { timeout: 120_000 });

  return utils.launchKindClusterFromCatalog(kindClusterName, window);
}

async function waitForFreelensMainWindow(app: ElectronApplication): Promise<Page> {
  const deadline = Date.now() + 50_000;
  while (Date.now() < deadline) {
    const mainWindow = app.windows().find((page) => page.url().startsWith("https://renderer.freelens.app"));
    if (mainWindow) return mainWindow;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error("Freelens did not open the main renderer window after restart");
}

clusterDescribe("Kafka cluster page", () => {
  let window: Page;
  let cleanup: undefined | (() => Promise<void>);
  let frame: Frame;
  let electronApp: ElectronApplication;
  let schemaRegistryFixture: SchemaRegistryFixture;
  let kafkaConnectFixture: KafkaConnectFixture;
  const DIRECT_KAFKA_BROKER = "127.0.0.1:19092";
  const ACL_KAFKA_BROKER = "127.0.0.1:19095";
  const DIRECT_TOPIC = "freelens-orders";

  const extensionRootForFixtureCommands =
    process.env.EXTENSION_PATH && process.env.EXTENSION_PATH.endsWith(".tgz")
      ? path.dirname(process.env.EXTENSION_PATH)
      : process.cwd();

  const directTopicExists = (topic: string): boolean => {
    const topics = execFileSync(
      "docker",
      [
        "exec",
        "freelens-kafka-direct",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        DIRECT_KAFKA_BROKER,
        "--list",
      ],
      { cwd: extensionRootForFixtureCommands, encoding: "utf8", env: process.env },
    );
    return topics.split(/\r?\n/).includes(topic);
  };

  const waitForDirectTopicState = async (topic: string, expected: boolean): Promise<void> => {
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      if (directTopicExists(topic) === expected) return;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
    throw new Error(`Timed out waiting for local topic ${topic} to become ${expected ? "available" : "absent"}`);
  };

  const waitForDurableCatalogTarget = async (bootstrap: string): Promise<void> => {
    const integrationDirectory = process.env.FREELENS_INTEGRATION_TESTING_DIR;
    if (!integrationDirectory) throw new Error("FREELENS_INTEGRATION_TESTING_DIR is unavailable");
    const storePath = path.join(
      integrationDirectory,
      "Freelens",
      "extension-store",
      "@freelensapp",
      "kafka-extension",
      "freelens-kafka-state-store.json",
    );
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      try {
        const model = JSON.parse(readFileSync(storePath, "utf8")) as { values?: Record<string, string> };
        const stored = model.values?.["freelens-kafka.cluster-catalog.v1"];
        if (stored) {
          const catalogs = JSON.parse(stored) as Record<
            string,
            { discovered?: Array<{ bootstrap?: string }>; missing?: Array<{ bootstrap?: string }> }
          >;
          const found = Object.values(catalogs).some((catalog) =>
            [...(catalog.discovered ?? []), ...(catalog.missing ?? [])].some(
              (target) => target.bootstrap === bootstrap,
            ),
          );
          if (found) return;
        }
      } catch {
        // The host writes the extension store asynchronously after the observable update.
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    throw new Error("Durable Kafka catalog did not contain the scanned target");
  };

  const waitForDurableHealthSnapshot = async (targetId: string): Promise<void> => {
    const integrationDirectory = process.env.FREELENS_INTEGRATION_TESTING_DIR;
    if (!integrationDirectory) throw new Error("FREELENS_INTEGRATION_TESTING_DIR is unavailable");
    const storePath = path.join(
      integrationDirectory,
      "Freelens",
      "extension-store",
      "@freelensapp",
      "kafka-extension",
      "freelens-kafka-state-store.json",
    );
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      try {
        const model = JSON.parse(readFileSync(storePath, "utf8")) as { values?: Record<string, string> };
        const entry = Object.entries(model.values ?? {}).find(
          ([key]) => key.startsWith("freelens-kafka.aggregate-health.v1:") && key.endsWith(`:${targetId}`),
        );
        if (entry) {
          const raw = entry[1];
          if (/bootstrap|credentials|groupId|offset|password|sourceLocator|topicName/i.test(raw)) {
            throw new Error("Durable aggregate health contains a forbidden identity or credential field");
          }
          const snapshot = JSON.parse(raw) as {
            schemaVersion?: number;
            data?: { consumerGroupLag?: string; onlineBrokers?: number };
          };
          if (
            snapshot.schemaVersion === 1 &&
            snapshot.data?.onlineBrokers === 1 &&
            /^(?:≥)?\d+$/.test(snapshot.data.consumerGroupLag ?? "")
          ) {
            return;
          }
        }
      } catch (error) {
        if (error instanceof Error && error.message.includes("forbidden identity")) throw error;
      }
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    throw new Error("Durable aggregate health snapshot was not written");
  };

  const writePartialHealthSnapshot = (targetId: string): void => {
    const integrationDirectory = process.env.FREELENS_INTEGRATION_TESTING_DIR;
    if (!integrationDirectory) throw new Error("FREELENS_INTEGRATION_TESTING_DIR is unavailable");
    const storePath = path.join(
      integrationDirectory,
      "Freelens",
      "extension-store",
      "@freelensapp",
      "kafka-extension",
      "freelens-kafka-state-store.json",
    );
    const model = JSON.parse(readFileSync(storePath, "utf8")) as { values?: Record<string, string> };
    const key = Object.keys(model.values ?? {}).find(
      (candidate) => candidate.startsWith("freelens-kafka.aggregate-health.v1:") && candidate.endsWith(`:${targetId}`),
    );
    if (!key || !model.values) throw new Error("Durable aggregate health key is unavailable");
    const now = Date.now();
    model.values[key] = JSON.stringify({
      schemaVersion: 1,
      updatedAt: now - 60_000,
      data: {
        onlineBrokers: 1,
        unavailablePartitions: 0,
        underReplicatedPartitions: 0,
        topologyMeasuredAt: now - 60_000,
        consumerGroupLag: "≥2",
        consumerGroupLagUnavailableGroups: 1,
        consumerGroupLagUnavailableTopics: 1,
        consumerGroupLagCoverage: {
          complete: false,
          startedAt: now - 62_000,
          completedAt: now - 60_000,
          resolvedGroups: 0,
          totalGroups: 1,
          unavailableGroups: 1,
        },
      },
    });
    writeFileSync(storePath, `${JSON.stringify(model, null, 2)}\n`);
  };

  const produceDirectTopicPartition0Messages = (lines: string[]) => {
    if (lines.length === 0) {
      return;
    }

    execFileSync("pnpm", ["tsx", "test/e2e/produce-direct-tail-messages.ts"], {
      cwd: extensionRootForFixtureCommands,
      env: {
        ...process.env,
        KAFKA_LOCAL: DIRECT_KAFKA_BROKER,
        KAFKA_TAIL_MESSAGES: JSON.stringify(lines),
        KAFKA_TAIL_TOPIC: DIRECT_TOPIC,
      },
      stdio: "inherit",
    });
  };

  const openKafkaMenuItem = async (menuId: string): Promise<void> => {
    const item = frame.locator(`[data-testid="link-for-sidebar-item-freelensapp--kafka-extension-${menuId}"]`);

    if (!(await item.isVisible())) {
      await frame.click('[data-testid="link-for-sidebar-item-freelensapp--kafka-extension-kafka"]');
    }

    await item.waitFor({ state: "visible", timeout: 60_000 });
    await item.click();
  };

  const ensureKafkaClusterRow = async (bootstrap: string): Promise<Locator> => {
    await openKafkaMenuItem("kafka-clusters");
    await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
    const row = frame.locator(`.KafkaClusterTable .TableRow[data-bootstrap="${bootstrap}"]`);
    if (!(await row.isVisible().catch(() => false))) {
      const scan = frame.getByRole("button", { name: "Scan Kubernetes" }).last();
      await scan.waitFor({ state: "visible", timeout: 30_000 });
      await scan.click();
    }
    await row.waitFor({ state: "visible", timeout: 60_000 });
    return row;
  };

  beforeAll(
    async () => {
      const extensionPath = process.env.EXTENSION_PATH;

      if (extensionPath && extensionPath.endsWith(".tgz")) {
        const extensionRoot = path.dirname(extensionPath);
        execFileSync(
          "kubectl",
          ["apply", "-f", path.join(extensionRoot, "test/e2e/fixtures/workload-direct-kind.yaml")],
          {
            cwd: extensionRoot,
            env: process.env,
            stdio: "inherit",
          },
        );

        execFileSync(
          "docker",
          [
            "exec",
            "freelens-kafka-direct",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "127.0.0.1:19092",
            "--delete",
            "--topic",
            "freelens-orders",
          ],
          {
            cwd: extensionRoot,
            env: process.env,
            stdio: "inherit",
          },
        );
        await waitForDirectTopicState(DIRECT_TOPIC, false);

        execFileSync(
          "docker",
          [
            "exec",
            "freelens-kafka-direct",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "127.0.0.1:19092",
            "--create",
            "--if-not-exists",
            "--topic",
            "freelens-orders",
            "--partitions",
            "3",
            "--replication-factor",
            "1",
          ],
          {
            cwd: extensionRoot,
            env: process.env,
            stdio: "inherit",
          },
        );
        await waitForDirectTopicState(DIRECT_TOPIC, true);

        execFileSync(
          "docker",
          [
            "exec",
            "freelens-kafka-direct",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "127.0.0.1:19092",
            "--create",
            "--if-not-exists",
            "--topic",
            "freelens-orders-archive-with-a-very-long-topic-name-for-layout-validation",
            "--partitions",
            "1",
            "--replication-factor",
            "1",
          ],
          {
            cwd: extensionRoot,
            env: process.env,
            stdio: "inherit",
          },
        );

        execFileSync("pnpm", ["tsx", "test/e2e/setup-direct-messages.ts"], {
          cwd: extensionRoot,
          env: process.env,
          stdio: "inherit",
        });

        execFileSync("pnpm", ["tsx", "test/e2e/setup-direct-group.ts"], {
          cwd: extensionRoot,
          env: {
            ...process.env,
            ALLOW_LOCAL_MUTATING_KAFKA_FIXTURE: "1",
            KAFKA_LOCAL: DIRECT_KAFKA_BROKER,
          },
          stdio: "inherit",
        });
        execFileSync("pnpm", ["tsx", "test/e2e/setup-schema-message.ts"], {
          cwd: extensionRoot,
          env: { ...process.env, KAFKA_LOCAL: DIRECT_KAFKA_BROKER },
          stdio: "inherit",
        });
        execFileSync("pnpm", ["tsx", "test/e2e/setup-schema-protobuf-message.ts"], {
          cwd: extensionRoot,
          env: { ...process.env, KAFKA_LOCAL: DIRECT_KAFKA_BROKER },
          stdio: "inherit",
        });
      }

      ({ window, cleanup, app: electronApp } = await utils.start());
      await utils.clickWelcomeButton(window);

      // Install + enable the packed extension from the Extensions page.
      await electronApp.evaluate(async ({ app }) => {
        await app.applicationMenu
          ?.getMenuItemById(process.platform === "darwin" ? "mac" : "file")
          ?.submenu?.getMenuItemById("navigate-to-extensions")
          ?.click();
      });
      const textbox = window.getByPlaceholder("Name or file path or URL");
      await textbox.fill(process.env.EXTENSION_PATH || "@freelensapp/kafka-extension");
      const installButton = 'button[class*="Button install-module__button--"]';
      await window.click(installButton.concat("[data-waiting=false]"));
      await window.waitForSelector('div[class*="installed-extensions-module__enabled--"]', { timeout: 120_000 });
      await window.click('i[data-testid*="close-notification-for-notification_"]');
      await window.click('div[class*="close-button-module__closeButton--"][aria-label="Close"]');

      if (extensionPath && extensionPath.endsWith(".tgz")) {
        schemaRegistryFixture = await startSchemaRegistryFixture();
        kafkaConnectFixture = await startKafkaConnectFixture();
      }

      // Back to the catalog and connect the KinD cluster.
      await electronApp.evaluate(async ({ app }) => {
        await app.applicationMenu?.getMenuItemById("view")?.submenu?.getMenuItemById("navigate-to-catalog")?.click();
      });
      frame = await launchKindClusterFromCatalogStable(TEST_KIND_CLUSTER_NAME, window);
    },
    10 * 60 * 1000,
  );

  afterAll(
    async () => {
      await cleanup?.();
      await schemaRegistryFixture?.close();
      await kafkaConnectFixture?.close();
      if (process.env.EXTENSION_PATH?.endsWith(".tgz")) {
        const extensionRoot = path.dirname(process.env.EXTENSION_PATH);
        execFileSync(
          "kubectl",
          [
            "delete",
            "-f",
            path.join(extensionRoot, "test/e2e/fixtures/workload-direct-kind.yaml"),
            "--ignore-not-found",
          ],
          {
            cwd: extensionRoot,
            env: process.env,
            stdio: "inherit",
          },
        );
      }
    },
    10 * 60 * 1000,
  );

  it(
    "opens the Kafka page without crashing",
    async () => {
      const coldDiscoveryStartedAt = Date.now();
      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));

      // The page's own root element only exists if the renderer did not crash into the
      // extension error boundary (which is what the `new Singleton` bug produced).
      await frame.waitForSelector(".KafkaOverviewPage", { timeout: 60_000 });
      await frame.waitForSelector('.KafkaOverviewPage h1 >> text="Kafka clusters"', { timeout: 60_000 });

      const discoveryProgress = frame.locator('.KafkaOperationProgress[data-operation="discovery"]');
      expect(await discoveryProgress.count()).toBe(0);
      await frame.getByTestId("kafka-clusters-onboarding").waitFor({ state: "visible", timeout: 30_000 });
      await frame.evaluate(() => {
        type DiscoveryProgressEvidence = {
          completed: string | null;
          etaState: string | null;
          mode: string | null;
          phase: string | null;
          progress: string | null;
          total: string | null;
        };
        const extensionWindow = window as typeof window & {
          kafkaDiscoveryProgressEvents?: DiscoveryProgressEvidence[];
          kafkaDiscoveryProgressObserver?: MutationObserver;
        };
        extensionWindow.kafkaDiscoveryProgressEvents = [];
        const record = () => {
          const element = document.querySelector('.KafkaOperationProgress[data-operation="discovery"]');
          if (!element) return;
          const snapshot: DiscoveryProgressEvidence = {
            completed: element.getAttribute("data-completed"),
            etaState: element.getAttribute("data-eta-state"),
            mode: element.getAttribute("data-progress-mode"),
            phase: element.getAttribute("data-phase"),
            progress: element.getAttribute("data-progress"),
            total: element.getAttribute("data-total"),
          };
          const events = extensionWindow.kafkaDiscoveryProgressEvents ?? [];
          if (JSON.stringify(events.at(-1)) !== JSON.stringify(snapshot)) events.push(snapshot);
        };
        const observer = new MutationObserver(record);
        observer.observe(document.documentElement, {
          attributeFilter: [
            "data-completed",
            "data-eta-state",
            "data-phase",
            "data-progress",
            "data-progress-mode",
            "data-total",
          ],
          attributes: true,
          childList: true,
          subtree: true,
        });
        extensionWindow.kafkaDiscoveryProgressObserver = observer;
        record();
      });
      await frame.getByRole("button", { name: "Scan Kubernetes" }).last().click();
      const progressEvidence = (await (
        await frame.waitForFunction(
          () => {
            const element = document.querySelector('.KafkaOperationProgress[data-operation="discovery"]');
            const line = element?.querySelector(".KafkaProgressLine");
            if (!element || !line) return false;
            return {
              completed: element.getAttribute("data-completed"),
              etaState: element.getAttribute("data-eta-state"),
              height: getComputedStyle(line).height,
              mode: element.getAttribute("data-progress-mode"),
              operationState: element.getAttribute("data-operation-state"),
              phase: element.getAttribute("data-phase"),
              progress: element.getAttribute("data-progress"),
              total: element.getAttribute("data-total"),
              valueLabel: element.querySelector(".KafkaProgressMeasure > strong")?.textContent?.trim(),
            };
          },
          undefined,
          { timeout: 60_000 },
        )
      ).jsonValue()) as {
        completed: string | null;
        etaState: string | null;
        height: string;
        mode: string | null;
        operationState: string | null;
        phase: string | null;
        progress: string | null;
        total: string | null;
        valueLabel?: string;
      };
      expect(Number(progressEvidence.progress)).toBeGreaterThanOrEqual(0);
      expect(Number(progressEvidence.progress)).toBeLessThanOrEqual(100);
      expect(progressEvidence.mode).toBe(progressEvidence.total === null ? "indeterminate" : "determinate");
      if (progressEvidence.total !== null) {
        const total = Number(progressEvidence.total);
        const completed = Number(progressEvidence.completed ?? 0);
        const expectedPercent = total <= 0 ? 100 : Math.round((completed / total) * 100);
        expect(Number(progressEvidence.progress)).toBe(expectedPercent);
      }
      expect(progressEvidence.valueLabel).toBe(
        progressEvidence.mode === "indeterminate" ? "Updating" : `${progressEvidence.progress}% of phase`,
      );
      expect(progressEvidence.operationState).toBeTruthy();
      expect(progressEvidence.phase).toBeTruthy();
      expect(["complete", "ready", "stalled", "unavailable", "warming"]).toContain(progressEvidence.etaState);
      expect(progressEvidence.height).toBe("6px");

      // Discovery must finish and resolve to a list or the empty/error state without a stale progress panel.
      await frame.waitForFunction(
        () =>
          !document.querySelector('.KafkaOperationProgress[data-operation="discovery"]') &&
          Boolean(document.querySelector(".KafkaClusterTable, .KafkaPageState.empty, .KafkaDiscoveryError")),
        undefined,
        { timeout: 60_000 },
      );
      const discoveryProgressEvents = (await frame.evaluate(() => {
        const extensionWindow = window as typeof window & {
          kafkaDiscoveryProgressEvents?: Array<{
            completed: string | null;
            etaState: string | null;
            mode: string | null;
            phase: string | null;
            progress: string | null;
            total: string | null;
          }>;
          kafkaDiscoveryProgressObserver?: MutationObserver;
        };
        extensionWindow.kafkaDiscoveryProgressObserver?.disconnect();
        delete extensionWindow.kafkaDiscoveryProgressObserver;
        return extensionWindow.kafkaDiscoveryProgressEvents ?? [];
      })) as Array<{
        completed: string | null;
        etaState: string | null;
        mode: string | null;
        phase: string | null;
        progress: string | null;
        total: string | null;
      }>;
      expect(discoveryProgressEvents.length).toBeGreaterThan(0);
      const countableDiscoveryEvents = discoveryProgressEvents.filter(
        (event) =>
          event.mode === "determinate" && event.completed !== null && event.total !== null && Number(event.total) > 0,
      );
      expect(countableDiscoveryEvents.length).toBeGreaterThan(0);
      for (const event of countableDiscoveryEvents) {
        const total = Number(event.total);
        const completed = Number(event.completed);
        expect(Number(event.progress)).toBe(Math.round((completed / total) * 100));
      }
      expect(
        countableDiscoveryEvents.some((event) =>
          ["complete", "ready", "stalled", "warming"].includes(event.etaState ?? ""),
        ),
      ).toBe(true);
      const pageText = await frame.locator(".KafkaOverviewPage").innerText();
      expect(pageText).not.toContain("Discovery failed");
      const coldDiscoveryDurationMs = Date.now() - coldDiscoveryStartedAt;

      // P3.3: native Freelens UI + explicitly injected styles (external extension loader only
      // requires renderer.js; it does not load a separately-emitted CSS asset).
      expect(await frame.locator("#freelens-kafka-extension-styles").count()).toBe(1);
      expect(await frame.locator(".KafkaPageHeader").isVisible()).toBe(true);

      // SPEC-003: the disposable direct Kafka fixture exposes a deterministic three-partition topic.
      const directRow = await ensureKafkaClusterRow("127.0.0.1:19092");
      await waitForDurableCatalogTarget("127.0.0.1:19092");
      await frame.waitForFunction(
        () => {
          const row = document.querySelector('.KafkaClusterTable .TableRow[data-bootstrap="127.0.0.1:19092"]');
          return row?.getAttribute("role") === "button" && row.getAttribute("aria-disabled") !== "true";
        },
        undefined,
        { timeout: 30_000 },
      );
      await directRow.locator('button[aria-label^="Connection settings"]').click();
      const settingsDrawer = frame.getByTestId("kafka-connection-settings");
      await settingsDrawer.waitFor({ state: "visible", timeout: 30_000 });
      expect(await frame.locator(".KafkaClusterDetail").count()).toBe(0);
      expect(await settingsDrawer.innerText()).toContain("Security override");
      expect(await settingsDrawer.innerText()).toContain("Password overrides stay in memory only");
      expect(await settingsDrawer.locator(".KafkaMetricStrip, .KafkaMetadataTabs, .KafkaTopicTable").count()).toBe(0);
      expect(await settingsDrawer.locator(".KafkaPartitionTable, .KafkaBrokerTable").count()).toBe(0);
      expect(await settingsDrawer.locator(".KafkaConnectionVerified").count()).toBe(0);
      const settingsForm = settingsDrawer.locator("form.KafkaSecurityForm");
      expect(await settingsForm.count()).toBe(1);
      const applySettings = settingsForm.getByRole("button", { name: "Apply and reconnect" });
      expect(await applySettings.getAttribute("type")).toBe("submit");

      await applySettings.focus();
      await applySettings.press("Enter");
      await settingsDrawer.locator(".KafkaConnectionVerified").waitFor({ state: "visible", timeout: 120_000 });
      expect(await settingsDrawer.locator(".KafkaConnectionVerified").innerText()).toContain("Connection verified");
      expect(new URL(frame.url()).pathname).toMatch(/\/kafka-clusters$/);
      expect(frame.url()).not.toMatch(/password|username|authMode|tlsMode/i);

      await window.setViewportSize({ width: 760, height: 700 });
      expect(
        await settingsDrawer
          .locator(".KafkaConnectionSettingsBody")
          .evaluate((element) => element.scrollWidth <= element.clientWidth + 1),
      ).toBe(true);
      await window.setViewportSize({ width: 1365, height: 839 });
      await frame
        .locator(
          ".Drawer.KafkaConnectionSettingsDrawer .drawer-title [data-testid], .Drawer.KafkaConnectionSettingsDrawer .drawer-title .Icon",
        )
        .last()
        .click();

      // P3.2 (j): a user can add a reachable-or-not manual bootstrap even when Kubernetes does
      // not reference it. Use a closed local port: no external network/cluster is touched.
      await frame.locator('button:has-text("Add endpoint")').click();
      await frame.getByPlaceholder("broker-1.example.com:9092,broker-2.example.com:9092").fill("127.0.0.1:1");
      await frame.locator('.KafkaManualEndpoint button:has-text("Add")').click();

      const manualRow = frame.locator('.KafkaClusterTable .TableRow[data-bootstrap="127.0.0.1:1"]');
      await manualRow.waitFor({ state: "visible", timeout: 30_000 });
      expect(await manualRow.innerText()).toContain("Manual");
      expect(await manualRow.innerText()).toContain("Unknown");
      expect(await frame.locator(".KafkaToolbar").isVisible()).toBe(true);
      expect(await frame.locator(".KafkaClusterTable .TableHead").isVisible()).toBe(true);
      expect(await frame.locator(".KafkaClusterTable .TableHead").innerText()).toContain("Kubernetes usage");

      await manualRow.locator('button[aria-label^="Connection settings"]').click();
      await settingsDrawer.waitFor({ state: "visible", timeout: 30_000 });
      expect(await settingsDrawer.getByRole("button", { name: "Remove endpoint" }).isVisible()).toBe(true);
      expect(await settingsDrawer.locator(".KafkaConnectionVerified").count()).toBe(0);
      await settingsDrawer.getByRole("button", { name: "Remove endpoint" }).click();
      await manualRow.waitFor({ state: "detached", timeout: 30_000 });
      expect(await frame.locator(".KafkaConnectionSettingsDrawer").count()).toBe(0);

      const filter = frame.getByPlaceholder("Filter by name, namespace, bootstrap or workload");
      await filter.fill("does-not-exist");
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("query") === "does-not-exist");
      expect(await frame.locator(".KafkaNoMatches").innerText()).toContain("No clusters match");
      await filter.fill("");
      await frame.waitForFunction(() => (new URL(window.location.href).searchParams.get("query") ?? "") === "");
      expect(
        await frame.locator(".KafkaOverviewPage").evaluate((element) => element.scrollWidth <= element.clientWidth + 1),
      ).toBe(true);

      // SPEC-004: the row opens Overview while its trailing action owns only Connection Settings.
      await window.setViewportSize({ width: 760, height: 700 });
      await frame.evaluate(() => {
        const extensionWindow = window as typeof window & {
          kafkaCompactHealthEvidence?: {
            completed: number;
            detailsTop: number;
            detailedProgress: number;
            etaState: string | null;
            etaText: string;
            expandedNoOverflow: boolean;
            expanded: boolean;
            height: number;
            initiallyOpen: boolean;
            liveDisplay: string;
            liveHeight: string;
            liveStatus: string;
            liveVisibility: string;
            liveWidth: string;
            metricColumns: number;
            metricsNoOverflow: boolean;
            noOverflow: boolean;
            slotHeight: number;
            total: number;
          };
          kafkaHealthObserver?: MutationObserver;
          kafkaHealthProgressObserved?: boolean;
        };
        const observeHealthProgress = () => {
          if (document.querySelector('[data-testid="kafka-health-progress"]')) {
            extensionWindow.kafkaHealthProgressObserved = true;
          }
          if (extensionWindow.kafkaCompactHealthEvidence) return;
          const details = document.querySelector<HTMLDetailsElement>(
            'details[data-testid="kafka-health-progress"][data-compact="true"]',
          );
          const summary = details?.querySelector("summary");
          const progress = details?.querySelector<HTMLElement>(".KafkaOperationProgress");
          const metrics = document.querySelector<HTMLElement>(".KafkaMetrics");
          const liveStatus = details?.querySelector<HTMLElement>(".KafkaProgressLiveStatus");
          const resourceDetails = document.querySelector<HTMLElement>(
            '.KafkaResourceDetails[aria-label="Kafka connection summary"]',
          );
          const slot = details?.closest<HTMLElement>(".KafkaHealthProgressSlot");
          if (!details || !summary || !progress || !metrics || !liveStatus || !resourceDetails || !slot) return;
          const liveStyle = getComputedStyle(liveStatus);
          const initiallyOpen = details.open;
          const detailsTop = resourceDetails.getBoundingClientRect().top;
          const slotHeight = slot.getBoundingClientRect().height;
          details.open = true;
          extensionWindow.kafkaCompactHealthEvidence = {
            completed: Number(progress.dataset.completed),
            detailsTop,
            detailedProgress: Number(progress.dataset.progress),
            etaState: progress.dataset.etaState ?? null,
            etaText: progress.querySelector('[data-testid="kafka-progress-eta"]')?.textContent?.trim() ?? "",
            expandedNoOverflow: details.scrollWidth <= details.clientWidth + 1,
            expanded: details.open,
            height: summary.getBoundingClientRect().height,
            initiallyOpen,
            liveDisplay: liveStyle.display,
            liveHeight: liveStyle.height,
            liveStatus: liveStatus.textContent?.trim() ?? "",
            liveVisibility: liveStyle.visibility,
            liveWidth: liveStyle.width,
            metricColumns: getComputedStyle(metrics).gridTemplateColumns.split(" ").length,
            metricsNoOverflow: metrics.scrollWidth <= metrics.clientWidth + 1,
            noOverflow: summary.scrollWidth <= summary.clientWidth + 1,
            slotHeight,
            total: Number(progress.dataset.total),
          };
          details.open = initiallyOpen;
        };
        const observer = new MutationObserver(observeHealthProgress);
        observer.observe(document.documentElement, { childList: true, subtree: true });
        extensionWindow.kafkaHealthObserver = observer;
        observeHealthProgress();
      });
      await directRow.focus();
      await directRow.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));
      const targetId = new URL(frame.url()).searchParams.get("target");
      expect(targetId).toMatch(/^kafka-[a-f0-9]{16}$/);
      const overviewPage = frame.locator('[data-testid="kafka-overview-page"]');
      await overviewPage.waitFor({ state: "visible", timeout: 120_000 });
      const initialHealthState = await overviewPage.getAttribute("data-health-state");
      expect(["updating", "ready"]).toContain(initialHealthState);
      const healthMetricValue = (label: string) =>
        overviewPage.locator(".KafkaMetric").filter({ hasText: label }).locator(".KafkaMetricValue");
      if (initialHealthState === "updating") {
        expect(["Updating", "1"]).toContain(await healthMetricValue("Online brokers").innerText());
        expect(["Updating", "0"]).toContain(await healthMetricValue("Unavailable partitions").innerText());
        expect(["Updating", "0"]).toContain(await healthMetricValue("Under-replicated").innerText());
        expect(["Updating", "0"]).toContain(await healthMetricValue("Consumer lag").innerText());
      }
      await frame.waitForFunction(
        () =>
          document.querySelector('[data-testid="kafka-overview-page"]')?.getAttribute("data-health-state") === "ready",
        undefined,
        { timeout: 120_000 },
      );
      expect(await healthMetricValue("Online brokers").innerText()).toBe("1");
      expect(await healthMetricValue("Unavailable partitions").innerText()).toBe("0");
      expect(await healthMetricValue("Under-replicated").innerText()).toBe("0");
      expect(await healthMetricValue("Consumer lag").innerText()).toMatch(/^\d+$/);
      await waitForDurableHealthSnapshot(targetId!);
      const healthProgressEvidence = await frame.evaluate(() => {
        const extensionWindow = window as typeof window & {
          kafkaCompactHealthEvidence?: {
            completed: number;
            detailsTop: number;
            detailedProgress: number;
            etaState: string | null;
            etaText: string;
            expandedNoOverflow: boolean;
            expanded: boolean;
            height: number;
            initiallyOpen: boolean;
            liveDisplay: string;
            liveHeight: string;
            liveStatus: string;
            liveVisibility: string;
            liveWidth: string;
            metricColumns: number;
            metricsNoOverflow: boolean;
            noOverflow: boolean;
            slotHeight: number;
            total: number;
          };
          kafkaHealthObserver?: MutationObserver;
          kafkaHealthProgressObserved?: boolean;
        };
        extensionWindow.kafkaHealthObserver?.disconnect();
        delete extensionWindow.kafkaHealthObserver;
        return {
          compact: extensionWindow.kafkaCompactHealthEvidence,
          observed: extensionWindow.kafkaHealthProgressObserved,
        };
      });
      expect(healthProgressEvidence.observed).toBe(true);
      const compactEvidence = healthProgressEvidence.compact;
      expect(compactEvidence).toBeDefined();
      if (!compactEvidence) throw new Error("Compact Health progress was not observed after topology");
      expect(compactEvidence.height).toBeGreaterThanOrEqual(40);
      expect(compactEvidence.noOverflow).toBe(true);
      expect(compactEvidence.expandedNoOverflow).toBe(true);
      expect(compactEvidence.metricColumns).toBe(2);
      expect(compactEvidence.metricsNoOverflow).toBe(true);
      expect(compactEvidence.initiallyOpen).toBe(false);
      expect(compactEvidence.expanded).toBe(true);
      expect(compactEvidence.detailedProgress).toBe(
        compactEvidence.total <= 0 ? 100 : Math.round((compactEvidence.completed / compactEvidence.total) * 100),
      );
      expect(["complete", "ready", "stalled", "unavailable", "warming"]).toContain(compactEvidence.etaState);
      expect(compactEvidence.liveStatus).toContain("%");
      expect(compactEvidence.liveStatus).toMatch(/\d+\/\d+/);
      expect(compactEvidence.liveDisplay).not.toBe("none");
      expect(compactEvidence.liveVisibility).toBe("visible");
      expect(compactEvidence.liveWidth).toBe("1px");
      expect(compactEvidence.liveHeight).toBe("1px");
      if (compactEvidence.etaState === "ready") {
        expect(compactEvidence.etaText).toMatch(/remaining$/);
      }
      const settledHealthGeometry = await overviewPage.evaluate((element) => {
        const slot = element.querySelector<HTMLElement>(".KafkaHealthProgressSlot");
        const details = element.querySelector<HTMLElement>(
          '.KafkaResourceDetails[aria-label="Kafka connection summary"]',
        );
        if (!slot || !details) throw new Error("Settled Health geometry is unavailable");
        return {
          detailsTop: details.getBoundingClientRect().top,
          slotHeight: slot.getBoundingClientRect().height,
        };
      });
      expect(Math.abs(settledHealthGeometry.slotHeight - compactEvidence.slotHeight)).toBeLessThanOrEqual(1);
      expect(Math.abs(settledHealthGeometry.detailsTop - compactEvidence.detailsTop)).toBeLessThanOrEqual(1);
      expect(await overviewPage.evaluate((element) => element.scrollWidth <= element.clientWidth + 1)).toBe(true);
      await window.setViewportSize({ width: 1365, height: 839 });
      expect(await frame.locator('[data-testid="kafka-health-notice"]').count()).toBe(0);
      expect(await frame.locator('[data-testid="tab-layout"]').count()).toBe(0);
      expect(await overviewPage.locator(".KafkaResourceMetricStrip").innerText()).toMatch(/brokers/i);
      expect(
        await overviewPage.locator(".KafkaMetrics").evaluate((element) => {
          const metrics = [...element.querySelectorAll(":scope > .KafkaMetric")];
          const widths = metrics.map((metric) => metric.getBoundingClientRect().width);
          return {
            columns: getComputedStyle(element).gridTemplateColumns.split(" ").length,
            display: getComputedStyle(element).display,
            metricCount: metrics.length,
            widthsAligned: Math.max(...widths) - Math.min(...widths) <= 1,
            valuesSeparated: metrics.every((metric) => {
              const value = metric.querySelector(".KafkaMetricValue")?.getBoundingClientRect();
              const label = metric.querySelector(".KafkaMetricLabel")?.getBoundingClientRect();
              return Boolean(value && label && value.bottom <= label.top);
            }),
          };
        }),
      ).toEqual({ columns: 3, display: "grid", metricCount: 6, widthsAligned: true, valuesSeparated: true });
      expect(await overviewPage.locator(".KafkaMetrics").innerText()).not.toMatch(/Available\s*Metadata/i);
      expect(await overviewPage.locator(".KafkaResourceDetails").innerText()).toContain("Direct");
      expect(await overviewPage.getByTestId("kafka-metadata-freshness").innerText()).toBeTruthy();
      expect(await overviewPage.getByTestId("kafka-topology-freshness").innerText()).toBeTruthy();
      expect(await overviewPage.getByTestId("kafka-aggregate-freshness").innerText()).toBeTruthy();
      const metadataUpdatedAt = Number(
        await overviewPage.getByTestId("kafka-metadata-freshness").getAttribute("data-updated-at"),
      );
      const topologyUpdatedAt = Number(
        await overviewPage.getByTestId("kafka-topology-freshness").getAttribute("data-updated-at"),
      );
      const aggregateUpdatedAt = Number(
        await overviewPage.getByTestId("kafka-aggregate-freshness").getAttribute("data-updated-at"),
      );
      expect(metadataUpdatedAt).toBeGreaterThan(0);
      expect(topologyUpdatedAt).toBeGreaterThan(0);
      expect(aggregateUpdatedAt).toBeGreaterThanOrEqual(topologyUpdatedAt);
      expect(await overviewPage.getByTestId("kafka-health-coverage").getAttribute("data-coverage")).toBe("exact");
      expect(await overviewPage.getByTestId("kafka-health-coverage").innerText()).toMatch(/^Exact · \d+\/\d+ groups$/);

      const cacheRequestCounts = async () =>
        frame.getByTestId("kafka-cache-status").evaluate((element) => ({
          cacheSource: element.getAttribute("data-cache-source"),
          cacheState: element.getAttribute("data-cache-state"),
          discovery: Number(element.getAttribute("data-discovery-requests")),
          health: Number(element.getAttribute("data-health-requests")),
          overview: Number(element.getAttribute("data-overview-requests")),
          reachability: Number(element.getAttribute("data-reachability-requests")),
          text: element.querySelector(".KafkaCacheStatusLabel")?.textContent?.trim() ?? "",
          updatedAt: Number(element.getAttribute("data-updated-at")),
        }));
      const coldRequestCounts = await cacheRequestCounts();
      expect(coldRequestCounts.cacheSource).toBe("cache");
      expect(coldRequestCounts.cacheState).toBe("cached");
      expect(coldRequestCounts.text).toMatch(/^Cached /);
      expect(coldRequestCounts.updatedAt).toBeGreaterThan(0);
      expect(coldRequestCounts.discovery).toBe(1);

      // Slice 6C: resource navigation reuses the same non-secret discovery, reachability and
      // metadata snapshots. No workload scan, TCP probe batch or Kafka connection is repeated.
      const warmTopicsStartedAt = Date.now();
      await openKafkaMenuItem("kafka-topics");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-topics"));
      await frame.locator('[data-testid="kafka-topics-page"]').waitFor({ state: "visible", timeout: 120_000 });
      const warmTopicsDurationMs = Date.now() - warmTopicsStartedAt;
      expect(await cacheRequestCounts()).toEqual(coldRequestCounts);

      // Returning to Overview reuses the expensive health snapshot instead of rebuilding group lag.
      await openKafkaMenuItem("kafka-overview");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));
      await overviewPage.waitFor({ state: "visible", timeout: 30_000 });
      expect(await overviewPage.getAttribute("data-health-state")).toBe("ready");
      expect(await frame.locator('[data-testid="kafka-health-progress"]').count()).toBe(0);
      expect(await cacheRequestCounts()).toEqual(coldRequestCounts);

      // Brokers is a real resource page and reports metadata identity, never inferred per-broker health.
      const warmBrokersStartedAt = Date.now();
      await frame.click('[data-testid="link-for-sidebar-item-freelensapp--kafka-extension-kafka-brokers"]');
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-brokers"));
      const brokersPage = frame.locator('[data-testid="kafka-brokers-page"]');
      await brokersPage.waitFor({ state: "visible", timeout: 120_000 });
      expect(await frame.locator('[data-testid="tab-layout"]').count()).toBe(0);
      expect(await brokersPage.locator(".KafkaBrokerPageTable .TableRow").count()).toBe(1);
      expect(await brokersPage.locator(".KafkaBrokerPageTable").innerText()).toContain("127.0.0.1");
      expect(await brokersPage.innerText()).not.toContain("Healthy");
      expect(await brokersPage.evaluate((element) => element.scrollWidth <= element.clientWidth + 1)).toBe(true);
      expect(await cacheRequestCounts()).toEqual(coldRequestCounts);
      const warmBrokersDurationMs = Date.now() - warmBrokersStartedAt;

      const brokerRow = brokersPage.locator(".KafkaBrokerPageTable .TableRow").first();
      const brokerId = await brokerRow.getAttribute("data-broker");
      expect(brokerId).toMatch(/^\d+$/);
      await brokerRow.focus();
      await brokerRow.press("Enter");
      await frame.waitForFunction((id) => new URL(window.location.href).searchParams.get("broker") === id, brokerId, {
        timeout: 30_000,
      });

      const brokerWorkspace = frame.locator('[data-testid="kafka-broker-workspace"]');
      try {
        await brokerWorkspace.waitFor({ state: "visible", timeout: 60_000 });
      } catch {
        const diagnostic = await frame.evaluate(() => {
          const url = new URL(window.location.href);
          return {
            broker: url.searchParams.get("broker"),
            brokerRows: [...document.querySelectorAll(".KafkaBrokerPageTable .TableRow")].map((row) =>
              row.getAttribute("data-broker"),
            ),
            error: document.querySelector(".KafkaPageState.error")?.textContent?.trim() ?? "none",
            errorBoundary: document.querySelector(".ErrorBoundary")?.textContent?.trim() ?? "none",
            extensionError:
              document.querySelector('[data-testid="extension-page-error"]')?.textContent?.trim() ?? "none",
            header: document.querySelector(".KafkaPageHeader h1")?.textContent?.trim() ?? "none",
            pathname: window.location.pathname,
            target: url.searchParams.get("target"),
          };
        });
        throw new Error(`Broker Workspace did not mount: ${JSON.stringify(diagnostic)}`);
      }
      const brokerConfigTable = brokerWorkspace.locator(".KafkaBrokerConfigTable");
      await brokerConfigTable.waitFor({ state: "visible", timeout: 30_000 });
      expect(await brokerConfigTable.locator(".TableRow").count()).toBeGreaterThan(0);
      expect(await brokerConfigTable.locator(".TableRow .configSourceCell").first().innerText()).toMatch(
        /CONFIG|DEFAULT/,
      );

      await frame.locator('.KafkaPageHeader button:has-text("Brokers")').click();
      await brokersPage.waitFor({ state: "visible", timeout: 30_000 });
      expect(new URL(frame.url()).searchParams.get("broker")).toBeFalsy();

      if (process.env.KAFKA_CACHE_TIMING_EVIDENCE === "1") {
        console.info(
          `LOCAL_CACHE_EVIDENCE=${JSON.stringify({
            durationsMs: {
              discoveryCold: coldDiscoveryDurationMs,
              topicsWarm: warmTopicsDurationMs,
              brokersWarm: warmBrokersDurationMs,
            },
            cacheAgeMs: Math.max(0, Date.now() - coldRequestCounts.updatedAt),
            requestCounts: {
              discovery: coldRequestCounts.discovery,
              health: coldRequestCounts.health,
              overview: coldRequestCounts.overview,
              reachability: coldRequestCounts.reachability,
            },
            warmRequestDelta: { discovery: 0, health: 0, overview: 0, reachability: 0 },
          })}`,
        );
      }

      await frame.evaluate(() => {
        const status = document.querySelector('[data-testid="kafka-cache-status"]');
        const observeRefreshing = () => {
          const status = document.querySelector('[data-testid="kafka-cache-status"]');
          if (
            status?.getAttribute("data-cache-state") === "refreshing" &&
            Boolean(document.querySelector('[data-testid="kafka-brokers-page"]'))
          ) {
            document.documentElement.dataset.kafkaRefreshingPageObserved = "true";
          }
        };
        const observer = new MutationObserver(observeRefreshing);
        if (status) observer.observe(status, { attributes: true, attributeFilter: ["data-cache-state"] });
        (window as typeof window & { kafkaRefreshObserver?: MutationObserver }).kafkaRefreshObserver = observer;
        observeRefreshing();
      });
      await frame.locator(".KafkaRefreshButton").click();
      await frame.waitForFunction(
        (before) => {
          const status = document.querySelector('[data-testid="kafka-cache-status"]');
          const state = status?.getAttribute("data-cache-state");
          return Boolean(
            status &&
              state !== "loading" &&
              state !== "refreshing" &&
              Number(status.getAttribute("data-discovery-requests")) === before.discovery &&
              Number(status.getAttribute("data-overview-requests")) === before.overview + 1 &&
              Number(status.getAttribute("data-reachability-requests")) === before.reachability + 1,
          );
        },
        coldRequestCounts,
        { timeout: 120_000 },
      );
      const refreshingPageObserved = await frame.evaluate(() => {
        const extensionWindow = window as typeof window & { kafkaRefreshObserver?: MutationObserver };
        extensionWindow.kafkaRefreshObserver?.disconnect();
        delete extensionWindow.kafkaRefreshObserver;
        return document.documentElement.dataset.kafkaRefreshingPageObserved;
      });
      expect(refreshingPageObserved).toBe("true");
      const refreshedRequestCounts = await cacheRequestCounts();
      expect(refreshedRequestCounts).toMatchObject({
        cacheState: "updated",
        discovery: coldRequestCounts.discovery,
        overview: coldRequestCounts.overview + 1,
        reachability: coldRequestCounts.reachability + 1,
      });
      expect(refreshedRequestCounts.text).toMatch(/^Updated /);
      expect(refreshedRequestCounts.updatedAt).toBeGreaterThanOrEqual(coldRequestCounts.updatedAt);

      await window.setViewportSize({ width: 760, height: 700 });
      expect(
        await frame.locator(".KafkaPageShell").evaluate((element) => element.scrollWidth <= element.clientWidth + 1),
      ).toBe(true);
      expect(
        await brokersPage
          .locator(".KafkaBrokerPageTable")
          .evaluate((element) => element.scrollWidth <= element.clientWidth + 1),
      ).toBe(true);

      // SPEC-004/SPEC-003: Topics is the primary list and Topic Workspace remains on the same route,
      // keeping Topics active while preserving URL-backed filter and entity state.
      await window.setViewportSize({ width: 1365, height: 839 });
      await frame.click('[data-testid="link-for-sidebar-item-freelensapp--kafka-extension-kafka-topics"]');
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-topics"));
      const topicsPage = frame.locator('[data-testid="kafka-topics-page"]');
      await topicsPage.waitFor({ state: "visible", timeout: 120_000 });
      expect(await topicsPage.locator(".KafkaTopicPageTable .TableRow").count()).toBeLessThanOrEqual(100);
      expect(await frame.locator('[data-testid="tab-layout"]').count()).toBe(0);
      expect(new URL(frame.url()).searchParams.get("target")).toBe(targetId);
      const warmRequestCounts = await cacheRequestCounts();
      expect(warmRequestCounts).toMatchObject({
        cacheState: "cached",
        discovery: refreshedRequestCounts.discovery,
        overview: refreshedRequestCounts.overview,
        reachability: refreshedRequestCounts.reachability,
        updatedAt: refreshedRequestCounts.updatedAt,
      });
      expect(warmRequestCounts.text).toMatch(/^Cached /);

      const longTopicRow = topicsPage.locator(
        '.KafkaTopicPageTable .TableRow[data-topic="freelens-orders-archive-with-a-very-long-topic-name-for-layout-validation"]',
      );
      await longTopicRow.waitFor({ state: "visible", timeout: 30_000 });
      const topicGeometry = await topicsPage.locator(".KafkaTopicPageTable").evaluate((table) => {
        const head = table.querySelector(".TableHead");
        const row = table.querySelector(
          '.TableRow[data-topic="freelens-orders-archive-with-a-very-long-topic-name-for-layout-validation"]',
        );
        const classes = ["topicNameCell", "topicTypeCell", "topicActionCell"];
        const boxes = (container: Element | null) =>
          classes.map((className) => {
            const element = container?.querySelector(`.${className}`);
            const rect = element?.getBoundingClientRect();
            return rect ? { left: rect.left, width: rect.width } : null;
          });
        const headerBoxes = boxes(head);
        const rowBoxes = boxes(row);
        const aligned = headerBoxes.every((header, index) => {
          const data = rowBoxes[index];
          return Boolean(
            header && data && Math.abs(header.left - data.left) <= 1 && Math.abs(header.width - data.width) <= 1,
          );
        });

        const nameHeader = head?.querySelector(".topicNameCell");
        const headerContent = nameHeader?.querySelector(".content");
        const previousScrollTop = (table as HTMLElement).scrollTop;
        const canScroll = table.scrollHeight > table.clientHeight;

        if (canScroll) {
          (table as HTMLElement).scrollTop = previousScrollTop + 120;
        }

        return {
          aligned,
          headerBoxes,
          rowBoxes,
          headerReadable: Boolean(
            nameHeader &&
              headerContent &&
              headerContent.scrollWidth <= headerContent.clientWidth + 1 &&
              nameHeader.getBoundingClientRect().width > 100,
          ),
          overflowY: getComputedStyle(table).overflowY,
          scrollMoved: !canScroll || (table as HTMLElement).scrollTop > previousScrollTop,
        };
      });
      expect(topicGeometry.aligned).toBe(true);
      expect(topicGeometry.headerReadable).toBe(true);
      expect(topicGeometry.overflowY).toMatch(/auto|scroll/);
      expect(topicGeometry.scrollMoved).toBe(true);

      await window.setViewportSize({ width: 760, height: 700 });
      expect(
        await topicsPage.locator(".KafkaMetrics").evaluate((element) => {
          const metrics = [...element.querySelectorAll(":scope > .KafkaMetric")];
          const container = element.getBoundingClientRect();
          const last = metrics.at(-1)?.getBoundingClientRect();
          return {
            columns: getComputedStyle(element).gridTemplateColumns.split(" ").length,
            lastSpansWidth: Boolean(last && Math.abs(last.width - container.width) <= 1),
          };
        }),
      ).toEqual({ columns: 2, lastSpansWidth: true });
      expect(
        await topicsPage.locator(".KafkaTopicPageTable").evaluate((table) => {
          const headCells = [...table.querySelectorAll(".TableHead > .TableCell")];
          const rowCells = [
            ...table.querySelectorAll(
              '.TableRow[data-topic="freelens-orders-archive-with-a-very-long-topic-name-for-layout-validation"] > .TableCell',
            ),
          ];
          return headCells.every((header, index) => {
            const row = rowCells[index];
            const headerRect = header.getBoundingClientRect();
            const rowRect = row?.getBoundingClientRect();
            return Boolean(
              rowRect &&
                Math.abs(headerRect.left - rowRect.left) <= 1 &&
                Math.abs(headerRect.width - rowRect.width) <= 1,
            );
          });
        }),
      ).toBe(true);
      await window.setViewportSize({ width: 1365, height: 839 });

      const pageTopicFilter = topicsPage.getByPlaceholder("Filter topics");
      await pageTopicFilter.fill("does-not-exist");
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("query") === "does-not-exist");
      expect(await topicsPage.locator(".KafkaTopicPrompt").innerText()).toContain("No topics match");
      await pageTopicFilter.fill("orders");

      const pageTopicRow = topicsPage.locator('.KafkaTopicPageTable .TableRow[data-topic="freelens-orders"]');
      await pageTopicRow.waitFor({ state: "visible", timeout: 30_000 });
      await pageTopicRow.focus();
      await pageTopicRow.press("Enter");
      await frame.waitForFunction(
        () =>
          window.location.pathname.endsWith("/kafka-topics") &&
          new URL(window.location.href).searchParams.get("topic") === "freelens-orders",
      );

      const topicWorkspace = frame.locator('[data-testid="kafka-topic-workspace"]');
      await topicWorkspace.waitFor({ state: "visible", timeout: 120_000 });
      expect(new URL(frame.url()).searchParams.get("query")).toBe("orders");
      expect(await topicWorkspace.locator(".KafkaTopicMetricStrip > div").first().innerText()).toMatch(
        /3\s*partitions/i,
      );
      expect(await topicWorkspace.locator('[role="tab"]').allTextContents()).toEqual([
        "Overview",
        "Messages",
        "Partitions",
        "Consumers",
        "Configuration",
      ]);

      await topicWorkspace.getByRole("tab", { name: "Consumers" }).click();
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("view") === "consumers");
      const consumersTable = topicWorkspace.locator(".KafkaTopicConsumersTable");
      await consumersTable.waitFor({ state: "visible", timeout: 30_000 });
      const topicConsumerRow = consumersTable.locator('.TableRow[data-group="freelens-orders-consumer"]');
      await topicConsumerRow.waitFor({ state: "visible", timeout: 30_000 });
      expect(await topicConsumerRow.locator(".consumerStateCell").innerText()).toMatch(/Empty|Stable/);
      expect(await topicConsumerRow.locator(".consumerLagCell").innerText()).toMatch(/^\d+$/);

      await topicWorkspace.getByRole("tab", { name: "Configuration" }).click();
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("view") === "configuration");
      const configurationTable = topicWorkspace.locator(".KafkaTopicConfigTable");
      await configurationTable.waitFor({ state: "visible", timeout: 30_000 });
      expect(await configurationTable.locator(".TableRow").count()).toBeGreaterThan(0);
      expect(await configurationTable.locator(".TableRow .configNameCell").allTextContents()).toEqual(
        expect.arrayContaining(["cleanup.policy", "retention.ms"]),
      );

      await topicWorkspace.getByRole("tab", { name: "Messages" }).click();
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("view") === "messages");
      const messagesBrowser = topicWorkspace.getByTestId("kafka-messages-browser");
      await messagesBrowser.waitFor({ state: "visible", timeout: 30_000 });
      expect(await messagesBrowser.getAttribute("data-browse-state")).toBe("idle");
      expect(await messagesBrowser.getAttribute("data-browse-requests")).toBe("0");
      expect(await messagesBrowser.locator(".KafkaMessageTable").count()).toBe(0);
      expect(await messagesBrowser.innerText()).toContain("Browse is idle");
      expect(await frame.locator(".Select__menu").count()).toBe(0);

      const controlsGeometry = () =>
        messagesBrowser.locator(".KafkaMessageControls").evaluate((controls) => {
          const partitionField = controls.querySelector<HTMLElement>(".KafkaMessagePartitionField");
          const partitionSelect = controls.querySelector<HTMLElement>(".KafkaMessagePartitionSelect");
          const startField = controls.querySelector<HTMLElement>(".KafkaMessageStartField");
          const fieldBox = partitionField?.getBoundingClientRect();
          const selectBox = partitionSelect?.getBoundingClientRect();
          const startBox = startField?.getBoundingClientRect();
          const separated = Boolean(
            fieldBox &&
              startBox &&
              (fieldBox.right <= startBox.left + 1 ||
                startBox.right <= fieldBox.left + 1 ||
                fieldBox.bottom <= startBox.top + 1 ||
                startBox.bottom <= fieldBox.top + 1),
          );
          return {
            selectContained: Boolean(
              fieldBox && selectBox && selectBox.left >= fieldBox.left - 1 && selectBox.right <= fieldBox.right + 1,
            ),
            separated,
          };
        });
      expect(await controlsGeometry()).toEqual({ selectContained: true, separated: true });

      const partitionSelect = messagesBrowser.locator(".KafkaMessagePartitionSelect .Select__control");
      await partitionSelect.click();
      const partitionMenu = frame.locator(".Select__menu");
      await partitionMenu.waitFor({ state: "visible", timeout: 10_000 });
      await partitionMenu.locator(".Select__option").filter({ hasText: "Partition 1" }).click();
      expect(await messagesBrowser.locator(".KafkaMessagePartitionSelect .Select__single-value").innerText()).toBe(
        "Partition 1",
      );
      await partitionSelect.click();
      await frame.locator(".Select__menu .Select__option").filter({ hasText: "Partition 0" }).click();
      expect(await messagesBrowser.locator(".KafkaMessagePartitionSelect .Select__single-value").innerText()).toBe(
        "Partition 0",
      );
      expect(await frame.locator(".Select__menu").count()).toBe(0);

      const startModeGeometry = () =>
        messagesBrowser.locator(".KafkaMessageStartField").evaluate((field) => {
          const modes = field.querySelector<HTMLElement>(".KafkaMessageStartModes");
          const buttons = [...field.querySelectorAll<HTMLButtonElement>(".KafkaMessageStartModes .Button")];
          const boxes = buttons.map((button) => button.getBoundingClientRect());
          return {
            containerNoScroll: Boolean(
              modes && modes.scrollWidth <= modes.clientWidth + 1 && modes.scrollHeight <= modes.clientHeight + 1,
            ),
            display: modes ? getComputedStyle(modes).display : "missing",
            labels: buttons.map((button) => button.textContent?.trim()),
            legend: field.querySelector("legend")?.textContent?.trim(),
            noOverlap: boxes.every((box, index) => index === 0 || box.left >= boxes[index - 1].right - 1),
            pseudoElementsHidden: buttons.every(
              (button) =>
                getComputedStyle(button, "::before").display === "none" &&
                getComputedStyle(button, "::after").display === "none",
            ),
            readable: buttons.every((button) => button.scrollWidth <= button.clientWidth + 1),
          };
        });
      expect(await startModeGeometry()).toEqual({
        containerNoScroll: true,
        display: "grid",
        labels: ["Latest window", "Earliest", "Offset", "Timestamp"],
        legend: "Start position",
        noOverlap: true,
        pseudoElementsHidden: true,
        readable: true,
      });

      await messagesBrowser.getByRole("button", { name: "Earliest" }).click();
      await messagesBrowser.getByLabel("Record limit").fill("2");
      await messagesBrowser.getByRole("button", { name: "Browse messages" }).click();
      try {
        await frame.waitForFunction(
          () => {
            const state = document
              .querySelector('[data-testid="kafka-messages-browser"]')
              ?.getAttribute("data-browse-state");
            return state === "complete" || state === "error";
          },
          undefined,
          { timeout: 45_000 },
        );
      } catch {
        const diagnostic = await frame.evaluate(() => ({
          browseRequests: document
            .querySelector('[data-testid="kafka-messages-browser"]')
            ?.getAttribute("data-browse-requests"),
          browseState: document
            .querySelector('[data-testid="kafka-messages-browser"]')
            ?.getAttribute("data-browse-state"),
          pageError: document.querySelector('[data-testid="extension-page-error"]')?.textContent?.trim() ?? "none",
          pathname: window.location.pathname,
          progress: document.querySelector(".KafkaOperationProgress")?.textContent?.trim() ?? "none",
          topicWorkspace: Boolean(document.querySelector('[data-testid="kafka-topic-workspace"]')),
        }));
        throw new Error(`Messages Browse did not settle: ${JSON.stringify(diagnostic)}`);
      }
      const browseState = await messagesBrowser.getAttribute("data-browse-state");
      if (browseState !== "complete") {
        throw new Error(`Messages Browse failed: ${await messagesBrowser.locator(".KafkaMessageError").innerText()}`);
      }
      await frame.waitForFunction(
        () => Boolean(document.querySelector(".KafkaMessageTable .TableRow[data-offset]")),
        undefined,
        { timeout: 30_000 },
      );
      expect(await messagesBrowser.getAttribute("data-browse-requests")).toBe("1");
      expect(await messagesBrowser.locator(".KafkaMessageTable .TableRow").count()).toBe(2);
      expect(await messagesBrowser.getByRole("button", { name: "Start tail" }).count()).toBe(1);
      const firstWindowOffsets = await messagesBrowser
        .locator(".KafkaMessageTable .TableRow")
        .evaluateAll((rows) => rows.map((row) => row.getAttribute("data-offset") ?? ""));
      expect(firstWindowOffsets).toHaveLength(2);
      expect(BigInt(firstWindowOffsets[1]) - BigInt(firstWindowOffsets[0])).toBe(1n);
      expect(await messagesBrowser.locator(".KafkaMessageRange").innerText()).toMatch(
        /2 records[\s\S]*Offsets \d+–\d+/,
      );

      await messagesBrowser.getByLabel("Filter message key").fill("order-json");
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("keyFilter") === "order-json");
      expect(await messagesBrowser.locator(".KafkaMessageTable .TableRow").count()).toBe(1);
      expect(await messagesBrowser.getAttribute("data-browse-requests")).toBe("1");

      await messagesBrowser.getByLabel("Filter message key").fill("");
      await messagesBrowser.getByLabel("Filter message value").fill("/created/i");
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("valueFilter") === "/created/i");
      expect(await messagesBrowser.locator(".KafkaMessageTable .TableRow").count()).toBe(1);

      await messagesBrowser.getByLabel("Filter message value").fill("");
      await messagesBrowser.getByLabel("Filter message header key").fill("trace");
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("headerKey") === "trace");
      expect(await messagesBrowser.locator(".KafkaMessageTable .TableRow").count()).toBe(1);
      await messagesBrowser.getByLabel("Filter message header key").fill("");

      expect(await frame.locator(".KafkaMessageDetailDrawer").count()).toBe(0);
      expect(await messagesBrowser.locator('[aria-label^="View details for offset "]').count()).toBe(2);

      const firstMessageRow = messagesBrowser.locator(
        `.KafkaMessageTable .TableRow[data-offset="${firstWindowOffsets[0]}"]`,
      );
      await firstMessageRow.click();
      const messageDetailDrawer = frame.getByTestId("kafka-message-detail");
      await messageDetailDrawer.waitFor({ state: "visible", timeout: 30_000 });
      expect(await messageDetailDrawer.locator(".KafkaMsgCodeBlock").nth(1).innerText()).toContain(
        '"state": "created"',
      );
      expect(await messageDetailDrawer.locator(".KafkaMsgHeaderList").innerText()).toMatch(
        /trace[\s\S]*first[\s\S]*second/,
      );
      await frame
        .locator(
          ".Drawer.KafkaMessageDetailDrawer .drawer-title [data-testid], .Drawer.KafkaMessageDetailDrawer .drawer-title .Icon",
        )
        .last()
        .click();
      expect(await frame.locator(".KafkaMessageDetailDrawer").count()).toBe(0);

      await messagesBrowser.getByRole("button", { name: "View details for offset 1" }).click();
      await messageDetailDrawer.waitFor({ state: "visible", timeout: 30_000 });
      expect(await messageDetailDrawer.innerText()).toContain("offset 1");
      await frame
        .locator(
          ".Drawer.KafkaMessageDetailDrawer .drawer-title [data-testid], .Drawer.KafkaMessageDetailDrawer .drawer-title .Icon",
        )
        .last()
        .click();
      expect(await frame.locator(".KafkaMessageDetailDrawer").count()).toBe(0);
      expect(frame.url()).not.toMatch(/order-json|created|ready.for.pickup|base64/i);

      await messagesBrowser.getByRole("button", { name: "Next window" }).click();
      await frame.waitForFunction(
        () => Boolean(document.querySelector(".KafkaMessageTable .TableRow[data-offset]")),
        undefined,
        { timeout: 120_000 },
      );
      expect(await messagesBrowser.getAttribute("data-browse-requests")).toBe("2");
      const secondWindowOffsets = await messagesBrowser
        .locator(".KafkaMessageTable .TableRow")
        .evaluateAll((rows) => rows.map((row) => row.getAttribute("data-offset") ?? ""));
      expect(secondWindowOffsets).toHaveLength(2);
      expect(BigInt(secondWindowOffsets[0]) - BigInt(firstWindowOffsets[1])).toBe(1n);
      expect(BigInt(secondWindowOffsets[1]) - BigInt(secondWindowOffsets[0])).toBe(1n);
      const binaryMessageRow = messagesBrowser.locator(
        `.KafkaMessageTable .TableRow[data-offset="${secondWindowOffsets[0]}"]`,
      );
      await binaryMessageRow.focus();
      await binaryMessageRow.press("Enter");
      await messageDetailDrawer.waitFor({ state: "visible", timeout: 30_000 });
      expect(await messageDetailDrawer.locator(".KafkaMsgCodeBlock").nth(1).getAttribute("data-format")).toBe("binary");
      expect(await messageDetailDrawer.locator(".KafkaMsgCodeBlock").nth(1).innerText()).toBe("/wB/");
      const nullMessageRow = messagesBrowser.locator(
        `.KafkaMessageTable .TableRow[data-offset="${secondWindowOffsets[1]}"]`,
      );
      await nullMessageRow.focus();
      await nullMessageRow.press(" ");
      expect(await messageDetailDrawer.locator(".KafkaMsgFormatBadge.format-null").nth(0).innerText()).toBe("null");
      await frame
        .locator(
          ".Drawer.KafkaMessageDetailDrawer .drawer-title [data-testid], .Drawer.KafkaMessageDetailDrawer .drawer-title .Icon",
        )
        .last()
        .click();
      expect(await frame.locator(".KafkaMessageDetailDrawer").count()).toBe(0);
      expect(await messagesBrowser.getByRole("button", { name: "Next window" }).isDisabled()).toBe(true);

      await messagesBrowser.getByRole("button", { name: "Timestamp" }).click();
      await messagesBrowser.getByLabel("Timestamp").evaluate((element) => {
        const input = element as HTMLInputElement;
        const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")?.set;
        setter?.call(input, "2023-11-14T22:13");
        input.dispatchEvent(new Event("input", { bubbles: true }));
        input.dispatchEvent(new Event("change", { bubbles: true }));
      });
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("timestamp") !== "");
      await messagesBrowser.getByRole("button", { name: "Browse messages" }).click();
      await frame.waitForFunction(
        () =>
          document.querySelector('[data-testid="kafka-messages-browser"]')?.getAttribute("data-browse-state") ===
          "complete",
        undefined,
        { timeout: 45_000 },
      );
      expect(await messagesBrowser.getAttribute("data-browse-requests")).toBe("3");
      expect(await messagesBrowser.locator(".KafkaMessageTable .TableRow").count()).toBeGreaterThan(0);

      await messagesBrowser.getByRole("button", { name: "Start tail" }).click();
      await frame.waitForFunction(() => Boolean(document.querySelector(".KafkaMessageTailStatus")), undefined, {
        timeout: 30_000,
      });
      expect(await messagesBrowser.getByRole("button", { name: "Stop tail" }).count()).toBe(1);
      expect(await messagesBrowser.locator(".KafkaMessageTailStatus").innerText()).toMatch(
        /Tailing partition 0[\s\S]*Buffer \d+\/2/,
      );
      expect(await messagesBrowser.getByRole("button", { name: "Next window" }).count()).toBe(0);
      await messagesBrowser.getByRole("button", { name: "Stop tail" }).click();
      await frame.waitForFunction(() => !document.querySelector(".KafkaMessageTailStatus"), undefined, {
        timeout: 30_000,
      });
      expect(await messagesBrowser.getByRole("button", { name: "Start tail" }).count()).toBe(1);

      await window.setViewportSize({ width: 760, height: 700 });
      expect(
        await frame.locator(".KafkaPageShell").evaluate((element) => element.scrollWidth <= element.clientWidth + 1),
      ).toBe(true);
      expect(await messagesBrowser.evaluate((element) => element.scrollWidth <= element.clientWidth + 1)).toBe(true);
      expect(await controlsGeometry()).toEqual({ selectContained: true, separated: true });
      expect(await startModeGeometry()).toMatchObject({
        containerNoScroll: true,
        display: "grid",
        noOverlap: true,
        pseudoElementsHidden: true,
        readable: true,
      });
      const messageTable = messagesBrowser.locator(".KafkaMessageTable");
      const messageTableCount = await messageTable.count();
      expect(
        messageTableCount === 0
          ? { noOverflow: true }
          : await messageTable.evaluate((element) => ({
              noOverflow: element.scrollWidth <= element.clientWidth + 1,
            })),
      ).toEqual({ noOverflow: true });
      await window.setViewportSize({ width: 1365, height: 839 });

      await topicWorkspace.getByRole("tab", { name: "Partitions" }).click();
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("view") === "partitions");
      const pagePartitionTable = topicWorkspace.locator(".KafkaPartitionTable");
      await pagePartitionTable.waitFor({ state: "visible", timeout: 30_000 });
      expect(await pagePartitionTable.locator(".TableRow").count()).toBe(3);
      expect(await pagePartitionTable.innerText()).toContain("Healthy");
      const partitionGeometry = async () =>
        pagePartitionTable.evaluate((table) => {
          const head = table.querySelector(".TableHead");
          const row = table.querySelector(".TableRow");
          const classes = [
            "partitionIdCell",
            "partitionLeaderCell",
            "partitionReplicasCell",
            "partitionIsrCell",
            "partitionHealthCell",
          ];
          const cells = (container: Element | null) =>
            classes.map((className) => {
              const element = container?.querySelector(`.${className}`);
              const rect = element?.getBoundingClientRect();
              return rect ? { left: rect.left, width: rect.width } : null;
            });
          const headerCells = cells(head);
          const rowCells = cells(row);
          const headerReadable = classes.slice(0, 4).every((className) => {
            const cell = head?.querySelector(`.${className}`);
            const content = cell?.querySelector(".content");
            return Boolean(content && content.scrollWidth <= content.clientWidth + 1);
          });
          return {
            aligned: headerCells.every((header, index) => {
              const data = rowCells[index];
              return Boolean(
                header && data && Math.abs(header.left - data.left) <= 1 && Math.abs(header.width - data.width) <= 1,
              );
            }),
            headerCells,
            headerReadable,
            rowCells,
          };
        });
      const desktopPartitionGeometry = await partitionGeometry();
      expect(desktopPartitionGeometry.aligned).toBe(true);
      expect(desktopPartitionGeometry.headerReadable).toBe(true);
      expect(desktopPartitionGeometry.headerCells[0]?.width).toBeGreaterThanOrEqual(108);
      expect(desktopPartitionGeometry.headerCells[1]?.width).toBeGreaterThanOrEqual(76);

      await window.setViewportSize({ width: 760, height: 700 });
      expect(
        await frame.locator(".KafkaPageShell").evaluate((element) => element.scrollWidth <= element.clientWidth + 1),
      ).toBe(true);
      expect(await pagePartitionTable.evaluate((element) => element.scrollWidth <= element.clientWidth + 1)).toBe(true);
      const compactPartitionGeometry = await partitionGeometry();
      expect(compactPartitionGeometry.aligned).toBe(true);
      expect(compactPartitionGeometry.headerReadable).toBe(true);
      expect(compactPartitionGeometry.headerCells[0]?.width).toBeGreaterThanOrEqual(96);

      await frame.locator('.KafkaPageHeader button:has-text("Topics")').click();
      await topicsPage.waitFor({ state: "visible", timeout: 30_000 });
      expect(new URL(frame.url()).searchParams.get("topic")).toBeNull();
      expect(new URL(frame.url()).searchParams.get("query")).toBe("orders");
      expect(await topicsPage.getByPlaceholder("Filter topics").inputValue()).toBe("orders");
    },
    10 * 60 * 1000,
  );

  it(
    "cross-links Topic Consumers to the Group Workspace",
    async () => {
      const testGroup = "freelens-orders-consumer";

      const directRow = await ensureKafkaClusterRow(DIRECT_KAFKA_BROKER);
      await directRow.focus();
      await directRow.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));

      await openKafkaMenuItem("kafka-topics");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-topics"));

      const topicsPage = frame.locator('[data-testid="kafka-topics-page"]');
      await topicsPage.waitFor({ state: "visible", timeout: 120_000 });
      await topicsPage.getByPlaceholder("Filter topics").fill("orders");

      const topicRow = topicsPage.locator('.KafkaTopicPageTable .TableRow[data-topic="freelens-orders"]');
      await topicRow.waitFor({ state: "visible", timeout: 30_000 });
      await topicRow.focus();
      await topicRow.press("Enter");

      const topicWorkspace = frame.locator('[data-testid="kafka-topic-workspace"]');
      await topicWorkspace.waitFor({ state: "visible", timeout: 120_000 });
      const targetId = new URL(frame.url()).searchParams.get("target");
      expect(targetId).toMatch(/^kafka-[a-f0-9]{16}$/);

      await topicWorkspace.getByRole("tab", { name: "Consumers" }).click();
      const groupRow = topicWorkspace.locator(`.KafkaTopicConsumersTable .TableRow[data-group="${testGroup}"]`);
      await groupRow.waitFor({ state: "visible", timeout: 30_000 });
      await groupRow.focus();
      await groupRow.press("Enter");

      await frame.waitForFunction(
        ({ group, target }) => {
          const url = new URL(window.location.href);
          return (
            window.location.pathname.endsWith("/kafka-groups") &&
            url.searchParams.get("group") === group &&
            url.searchParams.get("target") === target
          );
        },
        { group: testGroup, target: targetId },
        { timeout: 30_000 },
      );
      await frame.locator('[data-testid="kafka-group-workspace"]').waitFor({ state: "visible", timeout: 60_000 });
    },
    6 * 60 * 1000,
  );

  it(
    "tails new records and stops without late updates",
    async () => {
      const directRow = await ensureKafkaClusterRow(DIRECT_KAFKA_BROKER);
      await directRow.focus();
      await directRow.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));
      await openKafkaMenuItem("kafka-topics");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-topics"));

      const topicsPage = frame.locator('[data-testid="kafka-topics-page"]');
      await topicsPage.waitFor({ state: "visible", timeout: 120_000 });
      await topicsPage.getByPlaceholder("Filter topics").fill("orders");

      const topicRow = topicsPage.locator('.KafkaTopicPageTable .TableRow[data-topic="freelens-orders"]');
      await topicRow.waitFor({ state: "visible", timeout: 30_000 });
      await topicRow.focus();
      await topicRow.press("Enter");

      const topicWorkspace = frame.locator('[data-testid="kafka-topic-workspace"]');
      await topicWorkspace.waitFor({ state: "visible", timeout: 120_000 });
      await topicWorkspace.getByRole("tab", { name: "Messages" }).click();
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("view") === "messages");

      const messagesBrowser = topicWorkspace.getByTestId("kafka-messages-browser");
      await messagesBrowser.waitFor({ state: "visible", timeout: 30_000 });
      await messagesBrowser.getByRole("button", { name: "Latest window" }).click();
      await messagesBrowser.getByLabel("Record limit").fill("2");
      await messagesBrowser.getByRole("button", { name: "Browse messages" }).click();

      await frame.waitForFunction(
        () => {
          const state = document
            .querySelector('[data-testid="kafka-messages-browser"]')
            ?.getAttribute("data-browse-state");
          return state === "complete" || state === "error";
        },
        undefined,
        { timeout: 45_000 },
      );

      const browseState = await messagesBrowser.getAttribute("data-browse-state");
      if (browseState !== "complete") {
        throw new Error(
          `Messages Browse failed before tail lifecycle assertions: ${await messagesBrowser.locator(".KafkaMessageError").innerText()}`,
        );
      }

      const maxVisibleOffset = async () => {
        const offsets = await messagesBrowser
          .locator(".KafkaMessageTable .TableRow[data-offset]")
          .evaluateAll((rows) => rows.map((row) => row.getAttribute("data-offset") ?? ""));
        if (offsets.length === 0 || offsets[0] === "") {
          throw new Error("Expected at least one visible offset before tail assertions");
        }

        return offsets.reduce((max, current) => (BigInt(current) > BigInt(max) ? current : max));
      };

      const baselineOffset = await maxVisibleOffset();
      await messagesBrowser.getByRole("button", { name: "Start tail" }).click();
      await frame.waitForFunction(() => Boolean(document.querySelector(".KafkaMessageTailStatus")), undefined, {
        timeout: 30_000,
      });
      expect(await messagesBrowser.getByRole("button", { name: "Stop tail" }).count()).toBe(1);

      const token = `tail-lifecycle-${Date.now()}`;
      produceDirectTopicPartition0Messages([`${token}-one`, `${token}-two`]);

      await frame.waitForFunction(
        (baseline) => {
          const offsets = [...document.querySelectorAll(".KafkaMessageTable .TableRow[data-offset]")]
            .map((row) => row.getAttribute("data-offset"))
            .filter((offset): offset is string => Boolean(offset));
          if (offsets.length === 0) {
            return false;
          }

          const maxOffset = offsets.reduce((max, current) => (BigInt(current) > BigInt(max) ? current : max));

          return BigInt(maxOffset) >= BigInt(baseline) + 2n;
        },
        baselineOffset,
        { timeout: 45_000 },
      );

      expect(await messagesBrowser.locator(".KafkaMessageTable").innerText()).toContain(token);

      await messagesBrowser.getByRole("button", { name: "Stop tail" }).click();
      await frame.waitForFunction(() => !document.querySelector(".KafkaMessageTailStatus"), undefined, {
        timeout: 30_000,
      });
      expect(await messagesBrowser.getByRole("button", { name: "Start tail" }).count()).toBe(1);

      const offsetAtStop = await maxVisibleOffset();
      produceDirectTopicPartition0Messages([`${token}-post-stop`]);
      await frame.waitForTimeout(3_500);
      expect(await maxVisibleOffset()).toBe(offsetAtStop);
      expect(await messagesBrowser.locator(".KafkaMessageTable").innerText()).not.toContain(`${token}-post-stop`);
    },
    6 * 60 * 1000,
  );

  it(
    "clears topic workspace and tail state when switching Kafka cluster",
    async () => {
      const manualBootstrap = "127.0.0.2:19093";

      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));

      await frame.locator('button:has-text("Add endpoint")').click();
      await frame.getByPlaceholder("broker-1.example.com:9092,broker-2.example.com:9092").fill(manualBootstrap);
      await frame.locator('.KafkaManualEndpoint button:has-text("Add")').click();
      const manualRow = frame.locator(`.KafkaClusterTable .TableRow[data-bootstrap="${manualBootstrap}"]`);
      await manualRow.waitFor({ state: "visible", timeout: 30_000 });
      const manualClusterName = (await manualRow.locator(".nameCell").innerText()).trim();

      const directRow = await ensureKafkaClusterRow("127.0.0.1:19092");
      await directRow.focus();
      await directRow.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));

      await openKafkaMenuItem("kafka-brokers");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-brokers"));
      const brokersPage = frame.locator('[data-testid="kafka-brokers-page"]');
      await brokersPage.waitFor({ state: "visible", timeout: 120_000 });
      const brokerRow = brokersPage.locator(".KafkaBrokerPageTable .TableRow").first();
      await brokerRow.waitFor({ state: "visible", timeout: 30_000 });
      await brokerRow.focus();
      await brokerRow.press("Enter");
      await frame.locator('[data-testid="kafka-broker-workspace"]').waitFor({ state: "visible", timeout: 60_000 });

      await frame
        .locator(".KafkaPageShell .KafkaPageHeader .KafkaHeaderActions .KafkaClusterSelector .Select__control")
        .first()
        .click();
      await frame.locator(".Select__menu .Select__option").filter({ hasText: manualClusterName }).first().click();
      try {
        await frame.waitForFunction(
          () =>
            window.location.pathname.endsWith("/kafka-brokers") &&
            !new URL(window.location.href).searchParams.get("broker") &&
            !document.querySelector('[data-testid="kafka-broker-workspace"]'),
          undefined,
          { timeout: 60_000 },
        );
      } catch {
        const diagnostic = await frame.evaluate(() => {
          const url = new URL(window.location.href);
          return {
            broker: url.searchParams.get("broker"),
            error: document.querySelector(".KafkaPageState.error")?.textContent?.trim() ?? "none",
            pathname: window.location.pathname,
            selectedCluster:
              document.querySelector(".KafkaClusterSelector .Select__single-value")?.textContent?.trim() ?? "none",
            target: url.searchParams.get("target"),
            workspace: Boolean(document.querySelector('[data-testid="kafka-broker-workspace"]')),
          };
        });
        throw new Error(`Broker Workspace did not reset on cluster switch: ${JSON.stringify(diagnostic)}`);
      }

      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const directRowAgain = frame.locator('.KafkaClusterTable .TableRow[data-bootstrap="127.0.0.1:19092"]');
      await directRowAgain.waitFor({ state: "visible", timeout: 30_000 });
      await directRowAgain.focus();
      await directRowAgain.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));

      await frame.click('[data-testid="link-for-sidebar-item-freelensapp--kafka-extension-kafka-topics"]');
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-topics"));

      const topicsPage = frame.locator('[data-testid="kafka-topics-page"]');
      await topicsPage.waitFor({ state: "visible", timeout: 120_000 });
      await topicsPage.getByPlaceholder("Filter topics").fill("orders");

      const topicRow = topicsPage.locator('.KafkaTopicPageTable .TableRow[data-topic="freelens-orders"]');
      await topicRow.waitFor({ state: "visible", timeout: 30_000 });
      await topicRow.focus();
      await topicRow.press("Enter");

      const topicWorkspace = frame.locator('[data-testid="kafka-topic-workspace"]');
      await topicWorkspace.waitFor({ state: "visible", timeout: 120_000 });
      await topicWorkspace.getByRole("tab", { name: "Messages" }).click();
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("view") === "messages");

      const messagesBrowser = topicWorkspace.getByTestId("kafka-messages-browser");
      await messagesBrowser.waitFor({ state: "visible", timeout: 30_000 });
      await messagesBrowser.getByRole("button", { name: "Latest window" }).click();
      await messagesBrowser.getByLabel("Record limit").fill("2");
      await messagesBrowser.getByRole("button", { name: "Browse messages" }).click();
      await frame.waitForFunction(
        () =>
          document.querySelector('[data-testid="kafka-messages-browser"]')?.getAttribute("data-browse-state") ===
          "complete",
        undefined,
        { timeout: 45_000 },
      );

      await messagesBrowser.getByRole("button", { name: "Start tail" }).click();
      await frame.waitForFunction(() => Boolean(document.querySelector(".KafkaMessageTailStatus")), undefined, {
        timeout: 30_000,
      });

      await frame
        .locator(".KafkaPageShell .KafkaPageHeader .KafkaHeaderActions .KafkaClusterSelector .Select__control")
        .first()
        .click();
      await frame.locator(".Select__menu .Select__option").filter({ hasText: manualClusterName }).first().click();

      try {
        await frame.waitForFunction(
          () =>
            window.location.pathname.endsWith("/kafka-topics") &&
            !new URL(window.location.href).searchParams.get("topic") &&
            new URL(window.location.href).searchParams.get("view") === "overview" &&
            !document.querySelector('[data-testid="kafka-topic-workspace"]') &&
            !document.querySelector(".KafkaMessageTailStatus"),
          undefined,
          { timeout: 60_000 },
        );
      } catch {
        const diagnostic = await frame.evaluate(() => {
          const url = new URL(window.location.href);
          return {
            browseState:
              document.querySelector('[data-testid="kafka-messages-browser"]')?.getAttribute("data-browse-state") ??
              "missing",
            pathname: window.location.pathname,
            query: url.searchParams.get("query"),
            selectedCluster:
              document.querySelector(".KafkaClusterSelector .Select__single-value")?.textContent?.trim() ?? "none",
            tailVisible: Boolean(document.querySelector(".KafkaMessageTailStatus")),
            topic: url.searchParams.get("topic"),
            topicWorkspaceVisible: Boolean(document.querySelector('[data-testid="kafka-topic-workspace"]')),
            view: url.searchParams.get("view"),
          };
        });
        throw new Error(`Cluster switch did not reset Topic Workspace state: ${JSON.stringify(diagnostic)}`);
      }

      expect(await frame.locator('[data-testid="kafka-topic-workspace"]').count()).toBe(0);
      expect(await frame.locator(".KafkaMessageTailStatus").count()).toBe(0);
      expect(new URL(frame.url()).searchParams.get("topic")).toBeNull();
      expect(new URL(frame.url()).searchParams.get("view")).toBe("overview");
      expect(new URL(frame.url()).searchParams.get("query") ?? "").toBe("");
    },
    6 * 60 * 1000,
  );

  it(
    "executes produce and reset only after local confirmation",
    async () => {
      const testGroup = "freelens-orders-consumer";
      const directBootstrap = "127.0.0.1:19092";
      expect(directBootstrap === "127.0.0.1:19092" || directBootstrap === "::1").toBe(true);

      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const directRow = await ensureKafkaClusterRow(directBootstrap);
      await directRow.focus();
      await directRow.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));
      const localTargetId = new URL(frame.url()).searchParams.get("target");
      expect(localTargetId).toMatch(/^kafka-[a-f0-9]{16}$/);
      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const settingsRow = frame.locator(`.KafkaClusterTable .TableRow[data-bootstrap="${directBootstrap}"]`);
      await settingsRow.waitFor({ state: "visible", timeout: 30_000 });
      await settingsRow.locator(".KafkaIconButton").dispatchEvent("click");
      const settings = frame.locator('[data-testid="kafka-connection-settings"]');
      await settings.waitFor({ state: "visible", timeout: 30_000 });
      const writeToggle = frame.getByLabel("Enable write mode for this Kafka target");
      await writeToggle.click();
      await frame.waitForFunction(
        ({ key, targetId }) => {
          const values = JSON.parse(window.localStorage.getItem(key) ?? "[]") as unknown;
          return Array.isArray(values) && values.includes(targetId);
        },
        { key: "freelens-kafka.write-mode.v1", targetId: localTargetId },
        { timeout: 30_000 },
      );
      await settings.locator(".drawer-title .Icon").last().click();
      await settings.waitFor({ state: "hidden", timeout: 30_000 });

      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const localRow = frame.locator(`.KafkaClusterTable .TableRow[data-bootstrap="${directBootstrap}"]`);
      await localRow.waitFor({ state: "visible", timeout: 30_000 });
      await localRow.focus();
      await localRow.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));

      await openKafkaMenuItem("kafka-topics");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-topics"));
      const topicsPage = frame.locator('[data-testid="kafka-topics-page"]');
      try {
        await topicsPage.waitFor({ state: "visible", timeout: 30_000 });
      } catch (error) {
        const diagnostic = await frame
          .locator("body")
          .innerText()
          .catch(() => "<body unavailable>");
        throw new Error(`Topics page did not render at ${frame.url()}: ${diagnostic.slice(-4000)}`, { cause: error });
      }
      await topicsPage.getByPlaceholder("Filter topics").fill("orders");
      const topicRow = topicsPage.locator('.KafkaTopicPageTable .TableRow[data-topic="freelens-orders"]');
      await topicRow.waitFor({ state: "visible", timeout: 30_000 });
      await topicRow.focus();
      await topicRow.press("Enter");
      const topicWorkspace = frame.locator('[data-testid="kafka-topic-workspace"]');
      await topicWorkspace.waitFor({ state: "visible", timeout: 120_000 });
      await frame.getByTestId("kafka-produce-message-button").click();
      const produceDrawer = frame.locator('[data-testid="kafka-produce-message-drawer"]');
      await produceDrawer.waitFor({ state: "visible", timeout: 30_000 });
      await frame.getByLabel("Message value").fill(`e2e-write-${Date.now()}`);
      const produceButton = frame.getByRole("button", { name: "Send message" });
      expect(await produceButton.isDisabled()).toBe(true);
      await frame.getByLabel("Confirm produce message").check({ force: true });
      expect(await produceButton.isDisabled()).toBe(false);
      await produceButton.click();
      expect(await produceDrawer.getByRole("status").innerText()).toContain("Sent to partition");

      await openKafkaMenuItem("kafka-groups");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-groups"));
      const groupsPage = frame.locator('[data-testid="kafka-groups-page"]');
      await groupsPage.waitFor({ state: "visible", timeout: 120_000 });
      expect(await groupsPage.locator(".KafkaGroupPageTable .TableRow").count()).toBeLessThanOrEqual(100);
      const groupRow = groupsPage.locator(`.KafkaGroupPageTable .TableRow[data-group="${testGroup}"]`);
      await groupRow.waitFor({ state: "visible", timeout: 30_000 });
      await groupRow.focus();
      await groupRow.press("Enter");
      const groupWorkspace = frame.locator('[data-testid="kafka-group-workspace"]');
      await groupWorkspace.waitFor({ state: "visible", timeout: 60_000 });
      await frame.getByTestId("kafka-reset-offsets-button").click();
      const resetDrawer = frame.locator('[data-testid="kafka-reset-offsets-drawer"]');
      await resetDrawer.waitFor({ state: "visible", timeout: 30_000 });
      const resetButton = frame.getByRole("button", { name: "Reset offsets" }).nth(1);
      expect(await resetButton.isDisabled()).toBe(true);
      await frame.getByLabel("Type the exact resource name").fill(testGroup);
      await frame.getByLabel("Confirm reset offsets").check({ force: true });
      expect(await resetButton.isDisabled()).toBe(false);
      await resetButton.click();
      expect(await resetDrawer.getByRole("status").innerText()).toContain("Reset partition");
      execFileSync("pnpm", ["tsx", "test/e2e/setup-direct-group.ts"], {
        cwd: extensionRootForFixtureCommands,
        env: {
          ...process.env,
          ALLOW_LOCAL_MUTATING_KAFKA_FIXTURE: "1",
          KAFKA_LOCAL: directBootstrap,
        },
        stdio: "inherit",
      });
    },
    6 * 60 * 1000,
  );

  it(
    "deletes a topic only after the typed confirmation",
    async () => {
      // SPEC-009 REQ-194–REQ-196: disposable loopback topic only.
      expect(DIRECT_KAFKA_BROKER).toBe("127.0.0.1:19092");
      const disposableTopic = `freelens-e2e-delete-${Date.now()}`;
      const topicFixture = (action: "create" | "assert-absent") => {
        execFileSync("pnpm", ["tsx", "test/e2e/direct-topic-fixture.ts"], {
          cwd: extensionRootForFixtureCommands,
          env: {
            ...process.env,
            KAFKA_LOCAL: DIRECT_KAFKA_BROKER,
            KAFKA_TOPIC: disposableTopic,
            KAFKA_TOPIC_ACTION: action,
          },
          stdio: "inherit",
        });
      };
      topicFixture("create");

      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const settingsRow = await ensureKafkaClusterRow(DIRECT_KAFKA_BROKER);
      await settingsRow.locator(".KafkaIconButton").dispatchEvent("click");
      const settings = frame.locator('[data-testid="kafka-connection-settings"]');
      await settings.waitFor({ state: "visible", timeout: 30_000 });
      await frame.getByLabel("Enable write mode for this Kafka target").check({ force: true });
      await settings.locator(".drawer-title .Icon").last().click();
      await settings.waitFor({ state: "hidden", timeout: 30_000 });

      const directRow = await ensureKafkaClusterRow(DIRECT_KAFKA_BROKER);
      await directRow.focus();
      await directRow.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));
      await openKafkaMenuItem("kafka-topics");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-topics"));
      const topicsPage = frame.locator('[data-testid="kafka-topics-page"]');
      await topicsPage.waitFor({ state: "visible", timeout: 120_000 });
      // The topic was created after the cached metadata snapshot: reload the list.
      await frame.locator(".KafkaRefreshButton").click();
      await topicsPage.getByPlaceholder("Filter topics").fill("e2e-delete");
      const topicRow = topicsPage.locator(`.KafkaTopicPageTable .TableRow[data-topic="${disposableTopic}"]`);
      await topicRow.waitFor({ state: "visible", timeout: 60_000 });
      await topicRow.focus();
      await topicRow.press("Enter");
      const topicWorkspace = frame.locator('[data-testid="kafka-topic-workspace"]');
      await topicWorkspace.waitFor({ state: "visible", timeout: 120_000 });

      await frame.getByTestId("kafka-delete-topic-button").click();
      const deleteDrawer = frame.locator('[data-testid="kafka-delete-topic-drawer"]');
      await deleteDrawer.waitFor({ state: "visible", timeout: 30_000 });
      expect(await deleteDrawer.innerText()).toContain(disposableTopic);
      const submitDelete = frame.getByTestId("kafka-delete-topic-submit");
      expect(await submitDelete.isDisabled()).toBe(true);
      await frame.getByLabel("Confirm topic deletion").check({ force: true });
      expect(await submitDelete.isDisabled()).toBe(true);
      await frame.getByLabel("Type topic to confirm deletion").fill(`${disposableTopic}-wrong`);
      expect(await submitDelete.isDisabled()).toBe(true);
      await frame.getByLabel("Type topic to confirm deletion").fill(disposableTopic);
      expect(await submitDelete.isDisabled()).toBe(false);
      await submitDelete.click();

      await topicsPage.waitFor({ state: "visible", timeout: 60_000 });
      expect(await frame.locator('[data-testid="kafka-topic-workspace"]').count()).toBe(0);
      expect(await frame.getByTestId("kafka-topic-write-status").innerText()).toContain(
        `Deleted topic ${disposableTopic}`,
      );
      await topicsPage.getByPlaceholder("Filter topics").fill("e2e-delete");
      await frame.waitForFunction(
        (name) =>
          Boolean(document.querySelector(".KafkaTopicPageTable")) &&
          !document.querySelector(`.KafkaTopicPageTable .TableRow[data-topic="${name}"]`),
        disposableTopic,
        { timeout: 60_000 },
      );
      topicFixture("assert-absent");
    },
    6 * 60 * 1000,
  );

  it(
    "shows Consumer Groups detail tabs and Topic cross-link",
    async () => {
      const TEST_GROUP = "freelens-orders-consumer";

      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const directRow = await ensureKafkaClusterRow("127.0.0.1:19092");
      await directRow.focus();
      await directRow.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));

      await openKafkaMenuItem("kafka-groups");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-groups"));

      const groupsPage = frame.locator('[data-testid="kafka-groups-page"]');
      await groupsPage.waitFor({ state: "visible", timeout: 120_000 });

      const groupRow = groupsPage.locator(`.KafkaGroupPageTable .TableRow[data-group="${TEST_GROUP}"]`);
      await groupRow.waitFor({ state: "visible", timeout: 30_000 });
      expect(await groupRow.locator(".groupStateCell").innerText()).toMatch(/Empty|Stable/);
      expect(Number(await groupRow.locator(".groupMembersCell").innerText())).toBeGreaterThanOrEqual(0);

      await groupRow.focus();
      await groupRow.press("Enter");
      await frame.waitForFunction((g) => new URL(window.location.href).searchParams.get("group") === g, TEST_GROUP, {
        timeout: 30_000,
      });

      const groupWorkspace = frame.locator('[data-testid="kafka-group-workspace"]');
      await groupWorkspace.waitFor({ state: "visible", timeout: 60_000 });
      expect(await groupWorkspace.locator('[role="tab"]').allTextContents()).toEqual([
        "Offsets & Lag",
        "Members",
        "Topics",
      ]);

      const offsetsSection = frame.locator('[data-testid="kafka-group-offsets"]');
      await offsetsSection.waitFor({ state: "visible", timeout: 30_000 });

      const topicHeader = offsetsSection.locator(".KafkaGroupTopicName").first();
      await topicHeader.waitFor({ state: "visible", timeout: 10_000 });
      expect(await topicHeader.innerText()).toContain("freelens-orders");

      // Partition 0 was committed at offset 2 in the fixture.
      const rows = offsetsSection.locator(".KafkaOffsetTable .TableRow");
      const p0 = rows.first();
      await p0.waitFor({ state: "visible", timeout: 10_000 });
      expect(await p0.locator(".committedCell code").innerText()).toBe("2");
      const lagText = await p0.locator(".lagCell").innerText();
      expect(lagText).toMatch(/^(\d+|—)$/);

      await groupWorkspace.getByRole("tab", { name: "Members" }).click();
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("view") === "members");
      // Empty group shows empty state (no active members at commit time).
      await frame.waitForFunction(
        () =>
          Boolean(
            document.querySelector('[data-testid="kafka-group-members"]') ||
              document.querySelector(".KafkaPageState.empty"),
          ),
        undefined,
        { timeout: 15_000 },
      );

      const targetId = new URL(frame.url()).searchParams.get("target");
      expect(targetId).toMatch(/^kafka-[a-f0-9]{16}$/);
      await groupWorkspace.getByRole("tab", { name: "Topics" }).click();
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("view") === "topics");

      const topicsSection = groupWorkspace.getByTestId("kafka-group-topics");
      await topicsSection.waitFor({ state: "visible", timeout: 30_000 });
      const topicRow = topicsSection.locator('.KafkaGroupTopicsTable .TableRow[data-topic="freelens-orders"]');
      await topicRow.waitFor({ state: "visible", timeout: 30_000 });
      expect(await topicRow.locator(".groupTopicPartitionsCell").innerText()).toMatch(/^\d+$/);
      expect(await topicRow.locator(".groupTopicLagCell").innerText()).toMatch(/^\d+$/);

      await topicRow.focus();
      await topicRow.press("Enter");
      await frame.waitForFunction(
        ({ target, topic }) => {
          const url = new URL(window.location.href);
          return (
            window.location.pathname.endsWith("/kafka-topics") &&
            url.searchParams.get("target") === target &&
            url.searchParams.get("topic") === topic &&
            url.searchParams.get("view") === "overview"
          );
        },
        { target: targetId, topic: "freelens-orders" },
        { timeout: 30_000 },
      );
      await frame.locator('[data-testid="kafka-topic-workspace"]').waitFor({ state: "visible", timeout: 60_000 });

      await openKafkaMenuItem("kafka-groups");
      await groupsPage.waitFor({ state: "visible", timeout: 30_000 });
      expect(new URL(frame.url()).searchParams.get("group")).toBeFalsy();
    },
    6 * 60 * 1000,
  );

  it(
    "configures a local Schema Registry and browses subjects",
    async () => {
      const directBootstrap = "127.0.0.1:19092";
      expect(schemaRegistryFixture.url).toBe("http://127.0.0.1:18081");

      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const directRow = await ensureKafkaClusterRow(directBootstrap);
      await directRow.focus();
      await directRow.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));
      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const settingsRow = frame.locator(`.KafkaClusterTable .TableRow[data-bootstrap="${directBootstrap}"]`);
      await settingsRow.waitFor({ state: "visible", timeout: 30_000 });
      await settingsRow.locator(".KafkaIconButton").dispatchEvent("click");
      const settings = frame.locator('[data-testid="kafka-connection-settings"]');
      await settings.waitFor({ state: "visible", timeout: 30_000 });
      await frame.getByLabel("Schema Registry URL").fill(schemaRegistryFixture.url);
      await frame.getByLabel("Schema Registry username").fill("fixture-user");
      await settings.getByRole("button", { name: "Save Schema Registry settings" }).click();
      await frame.waitForFunction(() =>
        window.localStorage.getItem("freelens-kafka.schema-registry.v1")?.includes("18081"),
      );
      await settings.locator(".drawer-title .Icon").last().click();
      await settings.waitFor({ state: "hidden", timeout: 30_000 });

      const schemaMenu = frame.locator(
        '[data-testid="link-for-sidebar-item-freelensapp--kafka-extension-kafka-schema-registry"]',
      );
      await schemaMenu.waitFor({ state: "visible", timeout: 30_000 });
      await schemaMenu.click();
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-schema-registry"));
      const schemaPage = frame.getByTestId("kafka-schema-registry-page");
      await schemaPage.waitFor({ state: "visible", timeout: 60_000 });
      await schemaPage.getByPlaceholder("Filter subjects").fill("orders");
      const subjectRow = schemaPage.locator('.KafkaSchemaSubjectTable .TableRow[data-subject="orders-value"]');
      try {
        await subjectRow.waitFor({ state: "visible", timeout: 30_000 });
      } catch (error) {
        const diagnostic = await schemaPage.innerText();
        throw new Error(`Schema subjects did not load: ${diagnostic.slice(-3000)}`, { cause: error });
      }
      expect(await subjectRow.innerText()).toContain("Details on open");
      await subjectRow.click();
      const detail = frame.getByTestId("kafka-schema-subject-detail");
      await detail.waitFor({ state: "visible", timeout: 30_000 });
      expect(await detail.innerText()).toContain("Compatibility: BACKWARD");
      expect(await detail.innerText()).toContain("Version 1");
      expect(await detail.innerText()).toContain("Version 2");
      expect(await detail.innerText()).toContain("OrderV2");

      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const writeRow = frame.locator(`.KafkaClusterTable .TableRow[data-bootstrap="${directBootstrap}"]`);
      await writeRow.waitFor({ state: "visible", timeout: 30_000 });
      await writeRow.locator(".KafkaIconButton").dispatchEvent("click");
      const writeSettings = frame.locator('[data-testid="kafka-connection-settings"]');
      await writeSettings.waitFor({ state: "visible", timeout: 30_000 });
      await frame.getByLabel("Enable write mode for this Kafka target").check({ force: true });
      await writeSettings.locator(".drawer-title .Icon").last().click();
      await writeSettings.waitFor({ state: "hidden", timeout: 30_000 });
      await openKafkaMenuItem("kafka-schema-registry");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-schema-registry"));
      const writeSchemaPage = frame.getByTestId("kafka-schema-registry-page");
      await writeSchemaPage.waitFor({ state: "visible", timeout: 30_000 });
      await writeSchemaPage.getByPlaceholder("Filter subjects").fill("orders");
      await writeSchemaPage.locator('.KafkaSchemaSubjectTable .TableRow[data-subject="orders-value"]').click();
      const writeControls = frame.getByTestId("kafka-schema-write-controls");
      await writeControls.waitFor({ state: "visible", timeout: 30_000 });
      await frame.getByLabel("Schema definition").fill('{"type":"record","name":"OrderV3"}');
      await frame.getByLabel("Confirm schema registration").check({ force: true });
      await writeControls.getByRole("button", { name: "Register schema" }).click();
      expect(await frame.getByTestId("kafka-schema-write-status").innerText()).toContain("id 23");
      await frame.getByLabel("Type subject to confirm deletion").fill("orders-value");
      await frame.getByLabel("Confirm subject deletion").check({ force: true });
      await writeControls.getByRole("button", { name: "Delete subject" }).click();
      expect(await frame.getByTestId("kafka-schema-write-status").innerText()).toContain(
        "Deleted subject orders-value",
      );

      await openKafkaMenuItem("kafka-topics");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-topics"));
      const topicsPage = frame.getByTestId("kafka-topics-page");
      await topicsPage.waitFor({ state: "visible", timeout: 60_000 });
      await topicsPage.getByPlaceholder("Filter topics").fill("schema-orders");
      const schemaTopic = topicsPage.locator('.KafkaTopicPageTable .TableRow[data-topic="freelens-schema-orders"]');
      await schemaTopic.waitFor({ state: "visible", timeout: 30_000 });
      await schemaTopic.focus();
      await schemaTopic.press("Enter");
      const topicWorkspace = frame.getByTestId("kafka-topic-workspace");
      await topicWorkspace.waitFor({ state: "visible", timeout: 60_000 });
      await topicWorkspace.getByRole("tab", { name: "Messages" }).click();
      const browser = topicWorkspace.getByTestId("kafka-messages-browser");
      await browser.waitFor({ state: "visible", timeout: 30_000 });
      await browser.getByRole("button", { name: "Earliest" }).click();
      await browser.getByRole("button", { name: "Browse messages" }).click();
      try {
        await frame.waitForFunction(
          () => document.querySelector('.KafkaMessageTable .TableRow[data-offset="0"]') !== null,
          undefined,
          { timeout: 60_000 },
        );
      } catch (error) {
        throw new Error(`Avro browse did not settle: ${await browser.innerText()}`, { cause: error });
      }
      await browser.locator('.KafkaMessageTable .TableRow[data-offset="0"]').click();
      const messageDetail = frame.getByTestId("kafka-message-detail");
      await messageDetail.waitFor({ state: "visible", timeout: 30_000 });
      expect(await messageDetail.getByTestId("kafka-decoded-value").innerText()).toContain("decoded-order");
      await frame.locator(".Drawer.KafkaMessageDetailDrawer .drawer-title .Icon").last().click();
      await browser.locator('.KafkaMessageTable .TableRow[data-offset="1"]').click();
      await messageDetail.waitFor({ state: "visible", timeout: 30_000 });
      expect(await messageDetail.getByTestId("kafka-decode-warning").innerText()).toContain("schema 99");

      await frame.locator(".Drawer.KafkaMessageDetailDrawer .drawer-title .Icon").last().click();
      await frame.locator('.KafkaPageHeader button:has-text("Topics")').click();
      await frame.waitForFunction(() => {
        const url = new URL(window.location.href);
        return window.location.pathname.endsWith("/kafka-topics") && !url.searchParams.has("topic");
      });
      const paymentsPage = frame.getByTestId("kafka-topics-page");
      await paymentsPage.waitFor({ state: "visible", timeout: 60_000 });
      await paymentsPage.getByPlaceholder("Filter topics").fill("schema-payments");
      const paymentsTopic = paymentsPage.locator(
        '.KafkaTopicPageTable .TableRow[data-topic="freelens-schema-payments"]',
      );
      await paymentsTopic.waitFor({ state: "visible", timeout: 30_000 });
      await paymentsTopic.focus();
      await paymentsTopic.press("Enter");
      const paymentsWorkspace = frame.getByTestId("kafka-topic-workspace");
      await paymentsWorkspace.waitFor({ state: "visible", timeout: 60_000 });
      await paymentsWorkspace.getByRole("tab", { name: "Messages" }).click();
      const paymentsBrowser = paymentsWorkspace.getByTestId("kafka-messages-browser");
      await paymentsBrowser.getByRole("button", { name: "Earliest" }).click();
      await paymentsBrowser.getByRole("button", { name: "Browse messages" }).click();
      await frame.waitForFunction(
        () => document.querySelector('.KafkaMessageTable .TableRow[data-offset="0"]') !== null,
        undefined,
        { timeout: 60_000 },
      );
      await paymentsBrowser.locator('.KafkaMessageTable .TableRow[data-offset="0"]').click();
      const protobufDetail = frame.getByTestId("kafka-message-detail");
      await protobufDetail.waitFor({ state: "visible", timeout: 30_000 });
      expect(await protobufDetail.getByTestId("kafka-decoded-value").innerText()).toContain("decoded-payment");
    },
    6 * 60 * 1000,
  );

  it(
    "configures local Kafka Connect and shows connector failures",
    async () => {
      const directBootstrap = "127.0.0.1:19092";
      expect(kafkaConnectFixture.url).toBe("http://127.0.0.1:18083");
      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const row = await ensureKafkaClusterRow(directBootstrap);
      await row.locator(".KafkaIconButton").dispatchEvent("click");
      const settings = frame.locator('[data-testid="kafka-connection-settings"]');
      await settings.waitFor({ state: "visible", timeout: 30_000 });
      await frame.getByLabel("Kafka Connect URL").fill(kafkaConnectFixture.url);
      await settings.getByRole("button", { name: "Save Kafka Connect settings" }).click();
      await settings.locator(".drawer-title .Icon").last().click();
      await settings.waitFor({ state: "hidden", timeout: 30_000 });
      const menu = frame.locator('[data-testid="link-for-sidebar-item-freelensapp--kafka-extension-kafka-connect"]');
      await menu.waitFor({ state: "visible", timeout: 30_000 });
      await menu.click();
      const page = frame.getByTestId("kafka-connect-page");
      await page.waitFor({ state: "visible", timeout: 60_000 });
      await page.getByPlaceholder("Filter connectors").fill("payments");
      const connector = page.locator('.KafkaConnectTable .TableRow[data-connector="payments-sink"]');
      try {
        await connector.waitFor({ state: "visible", timeout: 30_000 });
      } catch (error) {
        throw new Error(`Kafka Connect list did not load: ${(await page.innerText()).slice(-3000)}`, { cause: error });
      }
      expect(await connector.innerText()).toContain("Details on open");
      await connector.click();
      const detail = frame.getByTestId("kafka-connect-detail");
      await detail.waitFor({ state: "visible", timeout: 30_000 });
      expect(await detail.innerText()).toContain("FAILED");
      expect(await detail.innerText()).toContain("full fixture failure trace");
      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const connectRow = frame.locator(`.KafkaClusterTable .TableRow[data-bootstrap="${directBootstrap}"]`);
      await connectRow.locator(".KafkaIconButton").dispatchEvent("click");
      const connectSettings = frame.locator('[data-testid="kafka-connection-settings"]');
      await connectSettings.waitFor({ state: "visible", timeout: 30_000 });
      await frame.getByLabel("Enable write mode for this Kafka target").check({ force: true });
      await connectSettings.locator(".drawer-title .Icon").last().click();
      await connectSettings.waitFor({ state: "hidden", timeout: 30_000 });
      await openKafkaMenuItem("kafka-connect");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-connect"));
      const writePage = frame.getByTestId("kafka-connect-page");
      await writePage.waitFor({ state: "visible", timeout: 30_000 });
      await writePage.getByPlaceholder("Filter connectors").fill("payments");
      await writePage.locator('.KafkaConnectTable .TableRow[data-connector="payments-sink"]').click();
      const writeControls = frame.getByTestId("kafka-connect-write-controls");
      await writeControls.waitFor({ state: "visible", timeout: 30_000 });
      const pause = writeControls.getByRole("button", { name: "Pause" });
      expect(await pause.isDisabled()).toBe(true);
      await frame.getByLabel("Confirm Connect action").check({ force: true });
      expect(await pause.isDisabled()).toBe(false);
      await pause.click();
      expect(await writeControls.innerText()).toContain("Paused connector");
      await writeControls.getByRole("button", { name: "Restart" }).click();
      expect(await writeControls.innerText()).toContain("Restarted connector");
      await frame
        .getByLabel("Connector JSON configuration")
        .first()
        .fill('{"name":"payments-sink","connector.class":"UpdatedSink"}');
      await writeControls.getByRole("button", { name: "Update connector" }).click();
      expect(await writeControls.innerText()).toContain("Updated connector");
      await frame.getByLabel("Type connector to confirm deletion").fill("payments-sink");
      await writeControls.getByRole("button", { name: "Delete" }).click();
      expect(await writeControls.innerText()).toContain("Deleted connector payments-sink");
      await frame
        .getByLabel("New connector JSON configuration")
        .fill('{"name":"created-source","connector.class":"ExampleSource"}');
      await frame.getByRole("button", { name: "Create connector" }).click();
      expect(await frame.locator('[data-testid="kafka-connect-page"]').innerText()).toContain("Created connector");
    },
    6 * 60 * 1000,
  );

  it(
    "offers ACLs only on an authorized broker and manages rules there",
    async () => {
      const directBootstrap = "127.0.0.1:19092";
      const aclBootstrap = ACL_KAFKA_BROKER;
      // SPEC-009 REQ-113: ACL writes are exercised only against a disposable loopback broker.
      expect(aclBootstrap.startsWith("127.0.0.1:")).toBe(true);
      const aclMenu = frame.locator('[data-testid="link-for-sidebar-item-freelensapp--kafka-extension-kafka-acls"]');

      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const directRow = await ensureKafkaClusterRow(directBootstrap);
      const directName = (await directRow.locator(".nameCell").innerText()).trim();
      await directRow.focus();
      await directRow.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));
      // REQ-136: a broker without an authorizer must not surface the ACL page at all.
      expect(await aclMenu.count()).toBe(0);

      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      await frame.locator('button:has-text("Add endpoint")').click();
      await frame.getByPlaceholder("broker-1.example.com:9092,broker-2.example.com:9092").fill(aclBootstrap);
      await frame.locator('.KafkaManualEndpoint button:has-text("Add")').click();
      const aclRow = frame.locator(`.KafkaClusterTable .TableRow[data-bootstrap="${aclBootstrap}"]`);
      await aclRow.waitFor({ state: "visible", timeout: 30_000 });
      await aclRow.locator(".KafkaIconButton").dispatchEvent("click");
      const settings = frame.locator('[data-testid="kafka-connection-settings"]');
      await settings.waitFor({ state: "visible", timeout: 30_000 });
      await frame.getByLabel("Enable write mode for this Kafka target").check({ force: true });
      await settings.locator(".drawer-title .Icon").last().click();
      await settings.waitFor({ state: "hidden", timeout: 30_000 });

      await aclRow.focus();
      await aclRow.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));
      await frame.getByTestId("kafka-overview-page").waitFor({ state: "visible", timeout: 120_000 });
      await aclMenu.waitFor({ state: "visible", timeout: 60_000 });

      await openKafkaMenuItem("kafka-acls");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-acls"));
      const page = frame.getByTestId("kafka-acl-page");
      await page.waitFor({ state: "visible", timeout: 60_000 });
      const writeControls = page.getByTestId("kafka-acl-write-controls");
      await writeControls.waitFor({ state: "visible", timeout: 60_000 });

      const principal = "User:acl-ui-e2e";
      const resourceName = "freelens-acl-ui";
      await page.getByLabel("ACL resource type", { exact: true }).selectOption("TOPIC");
      await page.getByLabel("ACL resource name", { exact: true }).fill(resourceName);
      await page.getByLabel("ACL pattern type", { exact: true }).selectOption("LITERAL");
      await page.getByLabel("ACL principal", { exact: true }).fill(principal);
      await page.getByLabel("ACL operation", { exact: true }).selectOption("READ");
      await page.getByLabel("ACL permission type", { exact: true }).selectOption("ALLOW");
      expect(await page.getByTestId("kafka-acl-rule-preview").innerText()).toBe(
        `ALLOW ${principal} READ on TOPIC LITERAL "${resourceName}" from *`,
      );

      const createButton = writeControls.getByRole("button", { name: "Create ACL" });
      expect(await createButton.isDisabled()).toBe(true);
      await page.getByLabel("Confirm ACL create").check({ force: true });
      expect(await createButton.isDisabled()).toBe(false);
      await createButton.click();

      const createdRow = page.locator(`.KafkaAclTable .TableRow[data-principal="${principal}"]`);
      await createdRow.waitFor({ state: "visible", timeout: 60_000 });
      expect(await createdRow.innerText()).toContain(resourceName);
      expect(await createdRow.innerText()).toContain("READ");
      expect(await page.getByTestId("kafka-acl-write-status").innerText()).toContain(`Created ACL for ${principal}`);

      await page.getByLabel("Filter ACL resource type").selectOption("TOPIC");
      expect(await page.locator('.KafkaAclTable .TableRow[data-resource-type="TOPIC"]').count()).toBeGreaterThan(0);
      await page.getByLabel("Filter ACL principal").fill("User:absent");
      expect(await page.locator(".KafkaAclTable .TableRow").count()).toBe(0);
      await page.getByLabel("Filter ACL principal").fill("");

      await createdRow.dispatchEvent("click");
      expect(await page.getByTestId("kafka-acl-selected-rule").innerText()).toContain(principal);
      const deleteButton = writeControls.getByRole("button", { name: "Delete ACL" });
      expect(await deleteButton.isDisabled()).toBe(true);
      await page.getByLabel("Type ACL resource to confirm deletion").fill(resourceName);
      await page.getByLabel("Confirm ACL deletion").check({ force: true });
      expect(await deleteButton.isDisabled()).toBe(false);
      await deleteButton.click();
      await createdRow.waitFor({ state: "detached", timeout: 60_000 });
      expect(await page.getByTestId("kafka-acl-write-status").innerText()).toContain(`Deleted ACL for ${principal}`);

      // SC-079: the same page degrades explicitly on a broker that does not expose ACLs.
      await frame
        .locator(".KafkaPageShell .KafkaPageHeader .KafkaHeaderActions .KafkaClusterSelector .Select__control")
        .first()
        .click();
      await frame.locator(".Select__menu .Select__option").filter({ hasText: directName }).first().click();
      const unavailable = page.getByTestId("kafka-acl-unavailable");
      await unavailable.waitFor({ state: "visible", timeout: 60_000 });
      expect(await unavailable.innerText()).toContain("ACL inspection is not available");
      expect(await page.getByTestId("kafka-acl-write-controls").count()).toBe(0);

      // Leave the cluster list as found, so later tests still see a single discovered target.
      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      await aclRow.locator(".KafkaIconButton").dispatchEvent("click");
      await settings.waitFor({ state: "visible", timeout: 30_000 });
      await settings.getByRole("button", { name: "Remove endpoint" }).click();
      await aclRow.waitFor({ state: "detached", timeout: 30_000 });
    },
    6 * 60 * 1000,
  );

  it(
    "paints persisted aggregate health before metadata after a full window reload",
    async () => {
      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const reloadClusterRow = await ensureKafkaClusterRow(DIRECT_KAFKA_BROKER);
      await reloadClusterRow.focus();
      await reloadClusterRow.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));
      const targetId = new URL(frame.url()).searchParams.get("target");
      if (!targetId) throw new Error("Overview target is unavailable before persisted health reload");
      await frame.evaluate(() => {
        (window as typeof window & { kafkaInitialHealthWaitStartedAt?: number }).kafkaInitialHealthWaitStartedAt =
          Date.now();
      });
      const initialHealthOutcome = await frame.waitForFunction(
        () => {
          const diagnosticWindow = window as typeof window & { kafkaInitialHealthWaitStartedAt?: number };
          const page = document.querySelector('[data-testid="kafka-overview-page"]');
          const elapsed = Date.now() - (diagnosticWindow.kafkaInitialHealthWaitStartedAt ?? Date.now());
          if (!page && elapsed < 30_000) return false;
          const state = page?.getAttribute("data-health-state") ?? "missing";
          if (state !== "ready" && state !== "unavailable" && elapsed < 30_000) return false;
          const progress = document.querySelector('.KafkaOperationProgress[data-operation="health"]');
          return {
            bodyText: document.body.textContent?.trim().slice(0, 500),
            elapsed,
            pageText: page?.textContent?.trim().slice(0, 500),
            phase: progress?.getAttribute("data-phase"),
            progress: progress?.getAttribute("data-progress"),
            route: window.location.pathname,
            source: page?.getAttribute("data-health-source"),
            state,
          };
        },
        undefined,
        { timeout: 35_000 },
      );
      const initialHealth = (await initialHealthOutcome.jsonValue()) as { state: string };
      if (initialHealth.state !== "ready") {
        throw new Error(`Initial packaged Health did not become ready: ${JSON.stringify(initialHealth)}`);
      }
      await waitForDurableHealthSnapshot(targetId);

      await window.addInitScript(() => {
        const timingWindow = window as typeof window & {
          kafkaClusterFrameReadyAt?: number;
          kafkaHealthNavigationStartedAt?: number;
          kafkaPreviousHealthCoverage?: string;
          kafkaPreviousHealthPaintAt?: number;
          kafkaPreviousHealthSource?: string;
        };
        const mark = () => {
          if (!timingWindow.kafkaClusterFrameReadyAt && document.querySelector('[data-testid="cluster-sidebar"]')) {
            timingWindow.kafkaClusterFrameReadyAt = Date.now();
          }
          if (!timingWindow.kafkaPreviousHealthPaintAt) {
            const healthPage = document.querySelector(
              '[data-testid="kafka-overview-page"][data-health-source="persisted"]',
            );
            const coverage = healthPage?.querySelector('[data-testid="kafka-health-coverage"]');
            if (healthPage && coverage) {
              timingWindow.kafkaPreviousHealthCoverage = coverage.getAttribute("data-coverage") ?? "missing";
              timingWindow.kafkaPreviousHealthPaintAt = Date.now();
              timingWindow.kafkaPreviousHealthSource = healthPage.getAttribute("data-health-source") ?? "missing";
            }
          }
        };
        new MutationObserver(mark).observe(document, { childList: true, subtree: true });
        document.addEventListener("DOMContentLoaded", mark);
      });

      await window.reload();
      await window.waitForSelector('[data-testid^="catalog-list-for-"]', { timeout: 30_000 });
      frame = await launchKindClusterFromCatalogStable(TEST_KIND_CLUSTER_NAME, window);
      await frame.evaluate(() => {
        (window as typeof window & { kafkaHealthNavigationStartedAt?: number }).kafkaHealthNavigationStartedAt =
          Date.now();
      });
      await openKafkaMenuItem("kafka-overview");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"), undefined, {
        timeout: 30_000,
      });
      const timing = await frame.waitForFunction(
        () => {
          const timingWindow = window as typeof window & {
            kafkaClusterFrameReadyAt?: number;
            kafkaHealthNavigationStartedAt?: number;
            kafkaPreviousHealthCoverage?: string;
            kafkaPreviousHealthPaintAt?: number;
            kafkaPreviousHealthSource?: string;
          };
          return timingWindow.kafkaHealthNavigationStartedAt && timingWindow.kafkaPreviousHealthPaintAt
            ? {
                frameReadyAt: timingWindow.kafkaClusterFrameReadyAt,
                navigationStartedAt: timingWindow.kafkaHealthNavigationStartedAt,
                healthCoverage: timingWindow.kafkaPreviousHealthCoverage,
                healthPaintAt: timingWindow.kafkaPreviousHealthPaintAt,
                healthSource: timingWindow.kafkaPreviousHealthSource,
              }
            : false;
        },
        undefined,
        { timeout: 30_000 },
      );
      const { navigationStartedAt, healthCoverage, healthPaintAt, healthSource } = (await timing.jsonValue()) as {
        frameReadyAt: number;
        healthCoverage: string;
        navigationStartedAt: number;
        healthPaintAt: number;
        healthSource: string;
      };
      expect(healthPaintAt - navigationStartedAt).toBeGreaterThanOrEqual(0);
      expect(healthPaintAt - navigationStartedAt).toBeLessThanOrEqual(500);
      expect(healthSource).toBe("persisted");
      expect(healthCoverage).toBe("exact");
      const persistedPage = frame.getByTestId("kafka-overview-page");
      expect(["persisted", "cache", "network"]).toContain(await persistedPage.getAttribute("data-health-source"));
      expect(
        await persistedPage
          .locator(".KafkaMetric")
          .filter({ hasText: "Consumer lag" })
          .locator(".KafkaMetricValue")
          .innerText(),
      ).toMatch(/^\d+$/);

      // T022: a sanitized persisted partial result paints truthfully after a real main-process restart.
      const integrationDirectory = process.env.FREELENS_INTEGRATION_TESTING_DIR;
      const executablePath = utils.appPaths[process.platform];
      if (!integrationDirectory) throw new Error("FREELENS_INTEGRATION_TESTING_DIR is unavailable");
      if (!executablePath) throw new Error(`Freelens executable is unavailable for ${process.platform}`);
      const initialCleanup = cleanup;
      await electronApp.close();
      writePartialHealthSnapshot(targetId);
      electronApp = await electron.launch({
        args: ["--integration-testing"],
        executablePath,
        bypassCSP: true,
        env: {
          ...process.env,
          FREELENS_INTEGRATION_TESTING_DIR: integrationDirectory,
          LOG_LEVEL: "debug",
        },
        timeout: 100_000,
      });
      window = await waitForFreelensMainWindow(electronApp);
      cleanup = async () => {
        await electronApp.close().catch(() => undefined);
        await initialCleanup?.();
      };
      await window.addInitScript(() => {
        const timingWindow = window as typeof window & {
          kafkaPartialHealthEvidence?: {
            aggregateUpdatedAt: number;
            at: number;
            coverage: string;
            lag: string;
            metadataFreshness: string;
            metadataUpdatedAt: number | null;
            notice: boolean;
            source: string | null;
            topologyUpdatedAt: number;
          };
          kafkaPartialHealthNavigationStartedAt?: number;
        };
        const mark = () => {
          if (timingWindow.kafkaPartialHealthEvidence) return;
          const page = document.querySelector('[data-testid="kafka-overview-page"][data-health-source="persisted"]');
          const coverage = page?.querySelector('[data-testid="kafka-health-coverage"][data-coverage="lower-bound"]');
          const lag = [...(page?.querySelectorAll(".KafkaMetric") ?? [])]
            .find((metric) => metric.textContent?.includes("Consumer lag"))
            ?.querySelector(".KafkaMetricValue");
          const metadataFreshness = page?.querySelector('[data-testid="kafka-metadata-freshness"]');
          if (page && coverage && lag && metadataFreshness) {
            timingWindow.kafkaPartialHealthEvidence = {
              aggregateUpdatedAt: Number(
                page.querySelector('[data-testid="kafka-aggregate-freshness"]')?.getAttribute("data-updated-at"),
              ),
              at: Date.now(),
              coverage: coverage.textContent?.trim() ?? "",
              lag: lag.textContent?.trim() ?? "",
              metadataFreshness: metadataFreshness.textContent?.trim() ?? "",
              metadataUpdatedAt: metadataFreshness.hasAttribute("data-updated-at")
                ? Number(metadataFreshness.getAttribute("data-updated-at"))
                : null,
              notice: Boolean(page.querySelector('[data-testid="kafka-health-notice"]')),
              source: page.getAttribute("data-health-source"),
              topologyUpdatedAt: Number(
                page.querySelector('[data-testid="kafka-topology-freshness"]')?.getAttribute("data-updated-at"),
              ),
            };
          }
        };
        new MutationObserver(mark).observe(document, { childList: true, subtree: true });
        document.addEventListener("DOMContentLoaded", mark);
      });
      await utils.clickWelcomeButton(window);
      frame = await launchKindClusterFromCatalogStable(TEST_KIND_CLUSTER_NAME, window);
      await frame.evaluate(() => {
        (
          window as typeof window & { kafkaPartialHealthNavigationStartedAt?: number }
        ).kafkaPartialHealthNavigationStartedAt = Date.now();
      });
      await openKafkaMenuItem("kafka-overview");
      const partialEvidence = await frame.waitForFunction(
        () => {
          const timingWindow = window as typeof window & {
            kafkaPartialHealthEvidence?: {
              aggregateUpdatedAt: number;
              at: number;
              coverage: string;
              lag: string;
              metadataFreshness: string;
              metadataUpdatedAt: number | null;
              notice: boolean;
              source: string | null;
              topologyUpdatedAt: number;
            };
            kafkaPartialHealthNavigationStartedAt?: number;
          };
          return timingWindow.kafkaPartialHealthEvidence && timingWindow.kafkaPartialHealthNavigationStartedAt
            ? {
                ...timingWindow.kafkaPartialHealthEvidence,
                startedAt: timingWindow.kafkaPartialHealthNavigationStartedAt,
              }
            : false;
        },
        undefined,
        { timeout: 30_000 },
      );
      const partial = (await partialEvidence.jsonValue()) as {
        aggregateUpdatedAt: number;
        at: number;
        coverage: string;
        lag: string;
        metadataFreshness: string;
        metadataUpdatedAt: number | null;
        notice: boolean;
        source: string;
        startedAt: number;
        topologyUpdatedAt: number;
      };
      expect(partial.at - partial.startedAt).toBeGreaterThanOrEqual(0);
      expect(partial.at - partial.startedAt).toBeLessThanOrEqual(500);
      expect(partial.source).toBe("persisted");
      expect(partial.lag).toBe("≥2");
      expect(partial.coverage).toContain("Lower bound · 0/1 groups");
      expect(partial.coverage).toContain("1 group unavailable");
      expect(partial.coverage).toContain("1 topic unavailable");
      expect(partial.metadataUpdatedAt).toBeNull();
      expect(partial.metadataFreshness).toMatch(/Refreshing|Waiting for first result/);
      expect(partial.topologyUpdatedAt).toBeGreaterThan(0);
      expect(partial.aggregateUpdatedAt).toBeGreaterThanOrEqual(partial.topologyUpdatedAt);
      expect(partial.notice).toBe(true);
    },
    6 * 60 * 1000,
  );

  it(
    "restores message filter URL state after a full Freelens window reload",
    async () => {
      produceDirectTopicPartition0Messages(["spec008-reload-fixture"]);
      await openKafkaMenuItem("kafka-clusters");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-clusters"));
      const reloadClusterRow = await ensureKafkaClusterRow(DIRECT_KAFKA_BROKER);
      await reloadClusterRow.focus();
      await reloadClusterRow.press("Enter");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-overview"));
      await openKafkaMenuItem("kafka-topics");
      await frame.waitForFunction(() => window.location.pathname.endsWith("/kafka-topics"));
      const topicsPage = frame.getByTestId("kafka-topics-page");
      await topicsPage.waitFor({ state: "visible", timeout: 60_000 });
      await topicsPage.getByPlaceholder("Filter topics").fill("orders");
      const topic = topicsPage.locator('.KafkaTopicPageTable .TableRow[data-topic="freelens-orders"]');
      await topic.waitFor({ state: "visible", timeout: 30_000 });
      await topic.focus();
      await topic.press("Enter");
      await frame.waitForFunction(() => new URL(window.location.href).searchParams.get("topic") === "freelens-orders");
      const workspace = frame.getByTestId("kafka-topic-workspace");
      await workspace.waitFor({ state: "visible", timeout: 60_000 });
      await workspace.getByRole("tab", { name: "Messages" }).click();
      const browser = workspace.getByTestId("kafka-messages-browser");
      await browser.getByRole("button", { name: "Earliest" }).click();
      await browser.getByRole("button", { name: "Browse messages" }).click();
      await frame.waitForFunction(
        () => document.querySelector(".KafkaMessageTable .TableRow[data-offset]") !== null,
        undefined,
        { timeout: 60_000 },
      );
      await browser.getByLabel("Filter message key").fill("order-json");
      await browser.getByLabel("Filter message value").fill("created");
      await browser.getByLabel("Filter message header key").fill("trace");
      await frame.waitForFunction(
        () => {
          const url = new URL(window.location.href);
          const snapshot = window.localStorage.getItem("freelens-kafka.reload-route.v1") ?? "";
          return (
            url.searchParams.get("keyFilter") === "order-json" &&
            url.searchParams.get("valueFilter") === "created" &&
            url.searchParams.get("headerKey") === "trace" &&
            snapshot.includes("order-json") &&
            snapshot.includes("created") &&
            snapshot.includes("trace")
          );
        },
        undefined,
        { timeout: 30_000 },
      );
      await window.reload();
      await window.waitForSelector('[data-testid^="catalog-list-for-"]', { timeout: 30_000 });
      const reloadedFrame = await launchKindClusterFromCatalogStable(TEST_KIND_CLUSTER_NAME, window);
      await reloadedFrame.waitForSelector("[data-testid=cluster-sidebar]", { timeout: 30_000 });
      await reloadedFrame.waitForFunction(() => window.location.pathname.endsWith("/kafka-topics"), undefined, {
        timeout: 30_000,
      });
      const reloadedBrowser = reloadedFrame.getByTestId("kafka-messages-browser");
      await reloadedBrowser.waitFor({ state: "visible", timeout: 30_000 });
      expect(await reloadedBrowser.getByLabel("Filter message key").inputValue()).toBe("order-json");
      expect(await reloadedBrowser.getByLabel("Filter message value").inputValue()).toBe("created");
      expect(await reloadedBrowser.getByLabel("Filter message header key").inputValue()).toBe("trace");
      expect(await reloadedBrowser.getAttribute("data-browse-requests")).toBe("0");
    },
    6 * 60 * 1000,
  );
});
