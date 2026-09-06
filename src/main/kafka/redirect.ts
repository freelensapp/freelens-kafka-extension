import net from "node:net";
import tls from "node:tls";

import type { AddressMap } from "./types";

export interface ResolvedTarget {
  host: string;
  port: number;
  redirected: boolean;
}

/**
 * Resolve an advertised `host:port` against the map. Pure and unit-testable.
 * Unmapped addresses pass through unchanged.
 */
export function resolveTarget(map: AddressMap, host: string, port: number): ResolvedTarget {
  const mapped = map.get(`${host}:${port}`);
  if (!mapped) return { host, port, redirected: false };
  const idx = mapped.lastIndexOf(":");
  return {
    host: mapped.slice(0, idx),
    port: Number(mapped.slice(idx + 1)),
    redirected: true,
  };
}

interface SocketFactoryArgs {
  host: string;
  port: number;
  ssl?: tls.ConnectionOptions;
  onConnect: () => void;
}

/**
 * Build a kafkajs `socketFactory` that redirects each outgoing broker
 * connection to its mapped local endpoint, preserving the original hostname for
 * TLS SNI / certificate validation. No Kafka wire-protocol rewriting required.
 */
export function createRedirectSocketFactory(map: AddressMap, onRedirect?: (from: string, to: string) => void) {
  return ({ host, port, ssl, onConnect }: SocketFactoryArgs) => {
    const target = resolveTarget(map, host, port);
    if (target.redirected) onRedirect?.(`${host}:${port}`, `${target.host}:${target.port}`);

    const socket = ssl
      ? tls.connect({ ...ssl, host: target.host, port: target.port, servername: host }, onConnect)
      : net.connect({ host: target.host, port: target.port }, onConnect);
    socket.setKeepAlive(true, 60_000);
    socket.setNoDelay(true);
    return socket;
  };
}

/**
 * Build a kafkajs `socketFactory` that always connects to a single fixed local
 * endpoint, regardless of the requested host/port (preserving the original host
 * for TLS SNI). Used in the two-phase connect to reach a seed broker before the
 * per-broker advertised addresses are known.
 */
export function createFixedSocketFactory(fixedHost: string, fixedPort: number) {
  return ({ host, ssl, onConnect }: SocketFactoryArgs) => {
    const socket = ssl
      ? tls.connect({ ...ssl, host: fixedHost, port: fixedPort, servername: host }, onConnect)
      : net.connect({ host: fixedHost, port: fixedPort }, onConnect);
    socket.setKeepAlive(true, 60_000);
    socket.setNoDelay(true);
    return socket;
  };
}

/**
 * Build a kafkajs `socketFactory` for a **direct** connection (no port-forward) that tracks the
 * sockets it opens so they can be force-closed on disconnect. kafkajs otherwise leaves the broker
 * socket open, which keeps the (Electron main) process alive after use. Returns the factory plus a
 * `closeAll` that destroys any still-open sockets.
 */
export function createDirectSocketFactory(): {
  socketFactory: (args: SocketFactoryArgs) => net.Socket | tls.TLSSocket;
  closeAll: () => void;
} {
  const sockets = new Set<net.Socket | tls.TLSSocket>();
  return {
    socketFactory: ({ host, port, ssl, onConnect }: SocketFactoryArgs) => {
      const socket = ssl
        ? tls.connect({ ...ssl, host, port, servername: host }, onConnect)
        : net.connect({ host, port }, onConnect);
      socket.setKeepAlive(true, 60_000);
      socket.setNoDelay(true);
      sockets.add(socket);
      socket.once("close", () => sockets.delete(socket));
      return socket;
    },
    closeAll: () => {
      for (const socket of sockets) socket.destroy();
      sockets.clear();
    },
  };
}
