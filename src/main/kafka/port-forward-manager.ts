import net from "node:net";
import type { Duplex } from "node:stream";

import type { AddressMap, BrokerRef, PodPort } from "./types";

/**
 * Wires an accepted local socket to a pod port. In production this is the
 * Kubernetes SPDY port-forward (see `createKubeForwarder`); in tests it can be
 * a plain TCP pipe, which keeps `PortForwardManager` independent of Kubernetes.
 */
export type Forwarder = (target: PodPort, socket: Duplex) => void;

/**
 * Opens one local TCP listener per broker pod and tunnels each incoming
 * connection through the injected `Forwarder`. Produces the
 * `advertised -> local` address map consumed by the redirect socketFactory.
 */
export class PortForwardManager {
  private servers: net.Server[] = [];

  constructor(
    private readonly forwarder: Forwarder,
    private readonly host: string = "127.0.0.1",
  ) {}

  /** Open a forward per broker; returns the advertised->local address map. */
  async open(brokers: BrokerRef[]): Promise<AddressMap> {
    const map: AddressMap = new Map();
    for (const broker of brokers) {
      const localPort = await this.openOne({
        namespace: broker.namespace,
        pod: broker.pod,
        port: broker.containerPort,
      });
      map.set(`${broker.advertisedHost}:${broker.advertisedPort}`, `${this.host}:${localPort}`);
    }
    return map;
  }

  private openOne(target: PodPort): Promise<number> {
    return new Promise<number>((resolve, reject) => {
      const server = net.createServer((socket) => {
        socket.on("error", () => socket.destroy());
        this.forwarder(target, socket);
      });
      server.once("error", reject);
      server.listen(0, this.host, () => {
        const addr = server.address();
        if (addr && typeof addr === "object") {
          this.servers.push(server);
          resolve(addr.port);
        } else {
          server.close();
          reject(new Error("failed to allocate a local port for port-forward"));
        }
      });
    });
  }

  /** Close every local listener. Call on disconnect. */
  closeAll(): void {
    for (const server of this.servers) server.close();
    this.servers = [];
  }
}
