import { KubeConfig, PortForward } from "@kubernetes/client-node";
import type { Duplex } from "node:stream";

import type { Forwarder } from "./port-forward-manager";
import type { PodPort } from "./types";

export interface KubeForwarderOptions {
  /** Path to the kubeconfig (from `Catalog` `kubeConfigPath`). Defaults to the standard location. */
  kubeConfigPath?: string;
  /** Context to use (from `Catalog` `contextName`). Defaults to the current context. */
  context?: string;
}

/**
 * Production {@link Forwarder} backed by the Kubernetes API (SPDY port-forward)
 * via `@kubernetes/client-node`. By default it loads the standard kubeconfig
 * (`~/.kube/config` / `$KUBECONFIG`) and selects the given context — the catalog's
 * `kubeConfigPath` points at the Freelens proxy, which cannot tunnel a port-forward.
 *
 * Note: exercised by the KinD integration test; unit tests inject a TCP-pipe forwarder.
 */
export function createKubeForwarder(options: KubeForwarderOptions = {}): Forwarder {
  const kc = new KubeConfig();
  if (options.kubeConfigPath) kc.loadFromFile(options.kubeConfigPath);
  else kc.loadFromDefault();
  if (options.context) kc.setCurrentContext(options.context);

  const portForward = new PortForward(kc);

  return (target: PodPort, socket: Duplex) => {
    // output = socket (pod -> client), err = null, input = socket (client -> pod)
    portForward.portForward(target.namespace, target.pod, [target.port], socket, null, socket).catch((err: unknown) => {
      socket.destroy(err instanceof Error ? err : new Error(String(err)));
    });
  };
}
