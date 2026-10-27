/**
 * Copyright (c) Freelens Authors. All rights reserved.
 * Licensed under MIT License. See LICENSE in root directory for more information.
 */

// The tunnels of the port-forward: `kubectl port-forward --address 127.0.0.1 --namespace <namespace>
// pod/<pod> :<port>`, run by the main process with the kubectl the host runs for the cluster
// (`Main.Shell.getKubectlPath`), the environment the host gives for it (`Main.Shell.getEnvironment`) as its whole
// environment, and the folder of the extension as its working directory. The environment reaches the cluster
// through the proxy of Freelens, with the identity of the host's connection: the extension runs no credential plugin
// of its own (SPEC-019, REQ-224). Adapted from the tunnels of freelensapp/freelens-velero-extension.
//
// kubectl reads the Pod and opens its stream to the API server before it writes its line of readiness,
// `Forwarding from 127.0.0.1:<port>`, whose port the forwarder pipes its sockets to. The wait for that line is
// counted once kubectl is prepared: the host may first download the kubectl of the version of the cluster.
//
// No tunnel outlives what it was opened for (REQ-225): kubectl is killed when its signal aborts (the session closes),
// when the extension is deactivated (`closeAll`), when the process exits and when the cluster leaves the connected
// state. A kubectl that has not exited some seconds after it was told to is killed for good.

import { type ChildProcess, type SpawnOptions, spawn as spawnProcess } from "node:child_process";

import type { PodPort } from "./types";

/** What the tunnels ask of the host for a cluster. */
export interface TunnelHost {
  /** The kubectl the host runs for the cluster, `Main.Shell.getKubectlPath(clusterId)`. */
  kubectl(clusterId: string, onPhase?: (message: string) => void): Promise<string>;
  /** The environment the host gives for the cluster, `Main.Shell.getEnvironment(clusterId)`. */
  environment(clusterId: string): Promise<Record<string, string>>;
  /**
   * Told the connection status of the cluster now and whenever it changes, until what it returns is called: a
   * `reaction` on `Main.Catalog.getClusterById(clusterId)?.status`. `undefined` when the cluster is not in the catalog.
   */
  watchStatus(clusterId: string, listener: (status: string | undefined) => void): () => void;
  /** The folder of the extension, the working directory of kubectl. */
  cwd(): string;
}

export type Spawn = (command: string, args: string[], options: SpawnOptions) => ChildProcess;

export interface PodTunnel {
  address: string;
  port: number;
  /** Resolves when kubectl has exited. */
  ended: Promise<void>;
  /** Kills kubectl and waits for it to exit. */
  close(): Promise<void>;
}

export type TunnelFailure = "cancelled" | "disconnected" | "forbidden" | "credential" | "deadline" | "ended";

/** Why a tunnel could not be opened, in words for the Kafka pages; nothing kubectl wrote is in the message. */
export class TunnelError extends Error {
  constructor(
    readonly reason: TunnelFailure,
    message: string,
  ) {
    super(message);
    this.name = "TunnelError";
  }
}

export const TUNNEL_BOUNDS = {
  /** From the end of the preparation of kubectl to its line of readiness. */
  readyMs: 15_000,
  /** From the moment kubectl is told to end to the moment it is killed for good. */
  exitMs: 5_000,
} as const;

export const CONNECTED = "connected";

const LOOPBACK = "127.0.0.1";
// The port is taken once what follows it is there: a line cut inside its port is not read as a shorter one.
const READY = /^Forwarding from 127\.0\.0\.1:(\d+) -> /m;
// The refusal of the API server, as kubectl writes it at its default verbosity: kubectl 1.31 and later asks a
// port-forward over a WebSocket first, with the verb get, and falls back to the older protocol, with create.
const PORT_FORWARD_REFUSED = /cannot create resource "pods\/portforward"/;
// A credential the cluster did not take.
const UNAUTHORIZED = /\bUnauthorized\b/;
// How much of what kubectl writes is kept to read its refusal: the end of it.
const KEPT = 16 * 1024;
const LABEL = /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/;
const SUBDOMAIN = /^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$/;

function kept(text: string): string {
  return text.length > KEPT ? text.slice(text.length - KEPT) : text;
}

function describe(pod: PodPort): string {
  return `${pod.namespace}/${pod.pod}:${pod.port}`;
}

// How kubectl that ended before its line of readiness ended.
function endOf(written: string, pod: PodPort): TunnelError {
  if (PORT_FORWARD_REFUSED.test(written)) {
    return new TunnelError(
      "forbidden",
      `Port-forward to ${describe(pod)} refused: the cluster connection may not create pods/portforward`,
    );
  }
  if (UNAUTHORIZED.test(written)) {
    return new TunnelError(
      "credential",
      `Port-forward to ${describe(pod)} refused: the cluster did not accept the credential`,
    );
  }
  return new TunnelError("ended", `kubectl port-forward to ${describe(pod)} ended before it was ready`);
}

function isPod(pod: PodPort): boolean {
  return (
    typeof pod?.namespace === "string" &&
    pod.namespace.length <= 63 &&
    LABEL.test(pod.namespace) &&
    typeof pod.pod === "string" &&
    pod.pod.length <= 253 &&
    SUBDOMAIN.test(pod.pod) &&
    Number.isInteger(pod.port) &&
    pod.port >= 1 &&
    pod.port <= 65535
  );
}

/** The kubectl of the extension: one for each tunnel, every one of them killed by `closeAll` and on process exit. */
export class KubectlTunnels {
  private readonly running = new Set<{ stop(): Promise<boolean> }>();
  private closed = false;
  private unhook?: () => void;

  constructor(
    private readonly host: TunnelHost,
    private readonly spawn: Spawn = spawnProcess,
    private readonly onExit: (listener: () => void) => () => void = (listener) => {
      process.once("exit", listener);
      return () => process.removeListener("exit", listener);
    },
  ) {}

  /** How many kubectl of the extension have not exited. */
  get count(): number {
    return this.running.size;
  }

  /** Every kubectl told to end, and no other started: the extension is deactivated. */
  closeAll(): void {
    this.closed = true;
    for (const run of [...this.running]) void run.stop();
    this.unhook?.();
    this.unhook = undefined;
  }

  async open(
    clusterId: string,
    pod: PodPort,
    signal: AbortSignal,
    onPhase?: (message: string) => void,
  ): Promise<PodTunnel> {
    if (signal.aborted || this.closed) throw new TunnelError("cancelled", "The port-forward was cancelled");
    if (!isPod(pod)) throw new TunnelError("ended", `Not a broker pod to port-forward to: ${describe(pod)}`);
    let kubectl: string;
    let environment: Record<string, string>;

    try {
      [kubectl, environment] = await Promise.all([
        this.host.kubectl(clusterId, onPhase),
        this.host.environment(clusterId),
      ]);
    } catch {
      // The host gives no kubectl and no environment for a cluster that is gone or not connected (C15).
      throw new TunnelError("disconnected", "The Kubernetes cluster is not connected");
    }
    if (signal.aborted || this.closed) throw new TunnelError("cancelled", "The port-forward was cancelled");
    if (typeof kubectl !== "string" || !kubectl || typeof environment !== "object" || environment === null) {
      throw new TunnelError("ended", "The host gave no kubectl for the cluster");
    }
    let child: ChildProcess;

    try {
      child = this.spawn(
        kubectl,
        ["port-forward", "--address", LOOPBACK, "--namespace", pod.namespace, `pod/${pod.pod}`, `:${pod.port}`],
        { cwd: this.host.cwd(), env: environment, stdio: ["ignore", "pipe", "pipe"], windowsHide: true },
      );
    } catch {
      throw new TunnelError("ended", "kubectl could not be started");
    }
    return this.follow(child, clusterId, pod, signal);
  }

  private hook(): void {
    if (this.unhook) return;
    this.unhook = this.onExit(() => {
      for (const run of this.running) void run.stop();
    });
  }

  // kubectl from its start to its end: ready on its line, or ended with what ended it; killed at every end.
  private follow(child: ChildProcess, clusterId: string, pod: PodPort, signal: AbortSignal): Promise<PodTunnel> {
    let exited = false;
    const ended = new Promise<void>((resolve) => {
      child.once("exit", () => {
        exited = true;
        resolve();
      });
      child.once("error", () => {
        if (child.pid !== undefined) return;
        exited = true;
        resolve();
      });
    });
    let stopping: Promise<boolean> | undefined;
    // Tells kubectl to end and waits for it: what has not exited within its bound is killed for good.
    const stop = (): Promise<boolean> => {
      stopping ??= (async () => {
        if (exited) return true;
        try {
          child.kill("SIGTERM");
        } catch {
          // It ended already.
        }
        let late: ReturnType<typeof setTimeout> | undefined;
        const inTime = await Promise.race([
          ended.then(() => true),
          new Promise<false>((resolve) => {
            late = setTimeout(() => resolve(false), TUNNEL_BOUNDS.exitMs);
          }),
        ]);

        clearTimeout(late);
        if (!inTime) {
          try {
            child.kill("SIGKILL");
          } catch {
            // Nothing more can be done of it.
          }
        }
        return inTime;
      })();
      return stopping;
    };
    const run = { stop };
    let ready = false;
    let settled = false;
    let written = "";
    let stdout = "";
    const afterReady = () => void stop();
    let unwatch: () => void = () => undefined;

    this.running.add(run);
    this.hook();
    void ended.then(() => {
      this.running.delete(run);
      signal.removeEventListener("abort", afterReady);
      unwatch();
    });
    return new Promise<PodTunnel>((resolve, reject) => {
      // The first end is the one that is said; kubectl is killed at every one.
      const fail = (error: TunnelError) => {
        if (!settled) {
          settled = true;
          clearTimeout(timer);
          signal.removeEventListener("abort", aborted);
          reject(error);
        }
        void stop();
      };
      const aborted = () => fail(new TunnelError("cancelled", "The port-forward was cancelled"));
      const timer = setTimeout(
        () =>
          fail(
            new TunnelError(
              "deadline",
              `kubectl port-forward to ${describe(pod)} was not ready within ${TUNNEL_BOUNDS.readyMs / 1000}s`,
            ),
          ),
        TUNNEL_BOUNDS.readyMs,
      );

      signal.addEventListener("abort", aborted, { once: true });
      // What kubectl writes is read for its line of readiness and for its refusal, to its end, so that kubectl never
      // waits for room to write.
      child.stdout?.on("data", (chunk: Buffer | string) => {
        if (ready || settled) return;
        stdout = kept(stdout + String(chunk));
        const line = READY.exec(stdout);

        if (!line) return;
        const port = Number(line[1]);

        if (!Number.isInteger(port) || port < 1 || port > 65535) {
          fail(new TunnelError("ended", `kubectl port-forward to ${describe(pod)} gave no usable port`));
          return;
        }
        ready = true;
        settled = true;
        stdout = "";
        written = "";
        clearTimeout(timer);
        signal.removeEventListener("abort", aborted);
        // Ready: the end of the session, whenever it comes, still kills it.
        signal.addEventListener("abort", afterReady, { once: true });
        resolve({
          address: LOOPBACK,
          port,
          ended,
          close: async () => {
            await stop();
          },
        });
      });
      child.stderr?.on("data", (chunk: Buffer | string) => {
        if (!ready) written = kept(written + String(chunk));
      });
      child.once("error", () => fail(new TunnelError("ended", "kubectl could not be started")));
      child.once("close", () =>
        fail(
          signal.aborted || this.closed
            ? new TunnelError("cancelled", "The port-forward was cancelled")
            : endOf(written, pod),
        ),
      );
      // The cluster leaves the connected state, or the catalog: kubectl is killed, whenever it is.
      try {
        unwatch = this.host.watchStatus(clusterId, (status) => {
          if (status === CONNECTED) return;
          fail(new TunnelError("disconnected", "The Kubernetes cluster is not connected"));
        });
      } catch {
        fail(new TunnelError("ended", "The status of the Kubernetes cluster could not be followed"));
      }
      if (exited) unwatch();
    });
  }
}
