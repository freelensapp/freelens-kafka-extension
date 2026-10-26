/**
 * Copyright (c) Freelens Authors. All rights reserved.
 * Licensed under MIT License. See LICENSE in root directory for more information.
 */

// The forwarder of one Kafka session on Freelens v2: each socket the `PortForwardManager` accepts for a broker pod is
// piped to the loopback port of a kubectl port-forward to that pod (SPEC-019, REQ-224). One kubectl serves every
// socket of its pod while it runs; when it exits, the next socket opens another. `close` kills them all: the session
// is closed (REQ-225).

import net from "node:net";
import type { Duplex } from "node:stream";

import type { KubectlTunnels, PodTunnel } from "./kubectl-tunnels";
import type { Forwarder } from "./port-forward-manager";
import type { PodPort } from "./types";

export type Connect = (port: number, host: string) => net.Socket;

export interface TunnelForwarder {
  forward: Forwarder;
  /** How many kubectl the forwarder keeps open or is opening. */
  readonly open: number;
  /** Every kubectl of the session killed; the sockets that come later are refused. */
  close(): Promise<void>;
}

export interface TunnelForwarderOptions {
  tunnels: Pick<KubectlTunnels, "open">;
  clusterId: string;
  /** Told what the host does before kubectl runs, such as its download. */
  onPhase?: (message: string) => void;
  /** Told why a tunnel could not be opened. */
  onError?: (error: unknown) => void;
  connect?: Connect;
}

function keyOf(target: PodPort): string {
  return `${target.namespace}/${target.pod}:${target.port}`;
}

export function createTunnelForwarder(options: TunnelForwarderOptions): TunnelForwarder {
  const controller = new AbortController();
  const tunnels = new Map<string, Promise<PodTunnel>>();
  const connect: Connect = options.connect ?? ((port, host) => net.connect(port, host));

  const tunnelOf = (target: PodPort): Promise<PodTunnel> => {
    const key = keyOf(target);
    const known = tunnels.get(key);

    if (known) return known;
    const opening = options.tunnels.open(options.clusterId, target, controller.signal, options.onPhase);

    tunnels.set(key, opening);
    opening.then(
      (tunnel) =>
        void tunnel.ended.then(() => {
          if (tunnels.get(key) === opening) tunnels.delete(key);
        }),
      (error: unknown) => {
        if (tunnels.get(key) === opening) tunnels.delete(key);
        options.onError?.(error);
      },
    );
    return opening;
  };

  const forward: Forwarder = (target, socket: Duplex) => {
    if (controller.signal.aborted) {
      socket.destroy();
      return;
    }
    tunnelOf(target).then(
      (tunnel) => {
        if (socket.destroyed) return;
        const upstream = connect(tunnel.port, tunnel.address);

        upstream.on("error", () => socket.destroy());
        socket.on("error", () => upstream.destroy());
        upstream.on("close", () => socket.destroy());
        socket.on("close", () => upstream.destroy());
        socket.pipe(upstream);
        upstream.pipe(socket);
      },
      // Why is told through `onError`: the socket only ends.
      () => socket.destroy(),
    );
  };

  return {
    forward,
    get open() {
      return tunnels.size;
    },
    close: async () => {
      controller.abort();
      const opened = await Promise.allSettled([...tunnels.values()]);

      tunnels.clear();
      await Promise.all(opened.map((result) => (result.status === "fulfilled" ? result.value.close() : undefined)));
    },
  };
}
