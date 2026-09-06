import net from "node:net";
import { describe, expect, it } from "vitest";
import { type Forwarder, PortForwardManager } from "./port-forward-manager";

import type { BrokerRef } from "./types";

/** A stand-in "pod": a TCP echo server. */
function startEcho(): Promise<{ port: number; close: () => void }> {
  return new Promise((resolve) => {
    const server = net.createServer((socket) => socket.pipe(socket));
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address() as net.AddressInfo;
      resolve({ port: addr.port, close: () => server.close() });
    });
  });
}

function roundtrip(host: string, port: number, payload: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const client = net.connect(port, host, () => client.write(payload));
    client.on("data", (data) => {
      resolve(data.toString());
      client.destroy();
    });
    client.on("error", reject);
  });
}

describe("PortForwardManager", () => {
  it("opens a local forward per broker, builds the map, and pipes data through the forwarder", async () => {
    const echo = await startEcho();
    // Forwarder that simulates the kube tunnel by piping to the "pod" (echo server).
    const forwarder: Forwarder = (_target, socket) => {
      const upstream = net.connect(echo.port, "127.0.0.1");
      socket.pipe(upstream);
      upstream.pipe(socket);
      const done = () => {
        upstream.destroy();
        socket.destroy();
      };
      socket.on("error", done);
      upstream.on("error", done);
    };

    const manager = new PortForwardManager(forwarder);
    const brokers: BrokerRef[] = [
      {
        advertisedHost: "kafka-internal",
        advertisedPort: 9092,
        namespace: "default",
        pod: "kafka-0",
        containerPort: 9092,
      },
    ];

    const map = await manager.open(brokers);
    const local = map.get("kafka-internal:9092");
    expect(local).toBeDefined();

    const [host, portStr] = local!.split(":");
    expect(await roundtrip(host, Number(portStr), "ping")).toBe("ping");

    manager.closeAll();
    echo.close();
  });
});
