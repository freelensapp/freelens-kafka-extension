import { KubeConfig, PortForward } from "@kubernetes/client-node";
import type { Duplex } from "node:stream";

import type { Forwarder } from "./port-forward-manager";
import type { PodPort } from "./types";

export interface KubeForwarderOptions {
  /**
   * Path to the kubeconfig file the Freelens cluster was added from (`kubeConfigPath` of the
   * catalog entry). Defaults to the standard resolution (`~/.kube/config` / `$KUBECONFIG`).
   */
  kubeConfigPath?: string;
  /** Context to use (`contextName` of the catalog entry). Defaults to the current context. */
  context?: string;
}

/** The part of `KubeConfig` the loader needs (injectable in unit tests). */
export interface KubeConfigLoader {
  loadFromFile(path: string): void;
  loadFromDefault(): void;
  setCurrentContext(context: string): void;
}

/**
 * Load the kubeconfig for a port-forward: the cluster's own file when given and readable,
 * otherwise the standard resolution. An unreadable file falls back to the standard resolution
 * (the pre-#22 behaviour) so an entry whose file moved still works when its context is also in
 * the default kubeconfig; the returned string says why the file was skipped, for logging.
 */
export function loadForwarderKubeConfig(kc: KubeConfigLoader, options: KubeForwarderOptions): string | undefined {
  let skipped: string | undefined;
  if (options.kubeConfigPath) {
    try {
      kc.loadFromFile(options.kubeConfigPath);
    } catch (error) {
      skipped = `${options.kubeConfigPath}: ${error instanceof Error ? error.message : String(error)}`;
      kc.loadFromDefault();
    }
  } else {
    kc.loadFromDefault();
  }
  if (options.context) kc.setCurrentContext(options.context);
  return skipped;
}

/**
 * Production {@link Forwarder} backed by the Kubernetes API (SPDY port-forward)
 * via `@kubernetes/client-node`, from the kubeconfig file and context of the Freelens
 * cluster the user selected (see {@link loadForwarderKubeConfig}).
 *
 * Note: exercised by the KinD integration test; unit tests inject a TCP-pipe forwarder.
 */
export function createKubeForwarder(options: KubeForwarderOptions = {}): Forwarder {
  const kc = new KubeConfig();
  const skipped = loadForwarderKubeConfig(kc, options);
  if (skipped) {
    console.warn(`[kafka] port-forward: kubeconfig file not readable, using the default kubeconfig (${skipped})`);
  }

  const portForward = new PortForward(kc);

  return (target: PodPort, socket: Duplex) => {
    // output = socket (pod -> client), err = null, input = socket (client -> pod)
    portForward.portForward(target.namespace, target.pod, [target.port], socket, null, socket).catch((err: unknown) => {
      socket.destroy(err instanceof Error ? err : new Error(String(err)));
    });
  };
}
