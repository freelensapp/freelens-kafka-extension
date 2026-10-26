// The forwarder of a session over fake tunnels whose port is a real loopback echo server: one tunnel per pod while it
// runs, a new one after it ends, sockets refused after close, and every tunnel closed with the session.

import net from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { TunnelError } from "./kubectl-tunnels";
import { createTunnelForwarder } from "./tunnel-forwarder";

import type { PodTunnel } from "./kubectl-tunnels";
import type { PodPort } from "./types";

const POD: PodPort = { namespace: "kafka", pod: "my-cluster-broker-0", port: 9092 };
const servers: net.Server[] = [];

async function echoServer(): Promise<number> {
  const server = net.createServer((socket) => socket.pipe(socket));
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return (server.address() as net.AddressInfo).port;
}

afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => new Promise((resolve) => server.close(resolve))));
});

function fakeTunnels(port: number) {
  const opened: { pod: PodPort; signal: AbortSignal; end: () => void; close: ReturnType<typeof vi.fn> }[] = [];
  const open = vi.fn(async (_clusterId: string, pod: PodPort, signal: AbortSignal): Promise<PodTunnel> => {
    let end!: () => void;
    const ended = new Promise<void>((resolve) => {
      end = resolve;
    });
    const close = vi.fn(async () => end());
    opened.push({ pod, signal, end, close });
    return { address: "127.0.0.1", port, ended, close };
  });
  return { open, opened };
}

// A socket pair: what the PortForwardManager accepted (`accepted`) and the client of it (`client`).
async function socketPair(): Promise<{ client: net.Socket; accepted: net.Socket; close: () => void }> {
  let accepted!: net.Socket;
  const listening = net.createServer((socket) => {
    accepted = socket;
  });
  await new Promise<void>((resolve) => listening.listen(0, "127.0.0.1", resolve));
  const client = net.connect((listening.address() as net.AddressInfo).port, "127.0.0.1");
  await vi.waitFor(() => expect(accepted).toBeDefined());
  return { client, accepted, close: () => listening.close() };
}

async function roundTrip(client: net.Socket, text: string): Promise<string> {
  return new Promise((resolve) => {
    client.once("data", (data) => resolve(String(data)));
    client.write(text);
  });
}

describe("createTunnelForwarder", () => {
  it("pipes each socket of a pod through one tunnel while it runs", async () => {
    const tunnels = fakeTunnels(await echoServer());
    const forwarder = createTunnelForwarder({ tunnels, clusterId: "cluster-a" });
    const first = await socketPair();
    const second = await socketPair();

    forwarder.forward(POD, first.accepted);
    forwarder.forward(POD, second.accepted);
    expect(await roundTrip(first.client, "ping")).toBe("ping");
    expect(await roundTrip(second.client, "pong")).toBe("pong");
    expect(tunnels.open).toHaveBeenCalledTimes(1);
    expect(tunnels.open).toHaveBeenCalledWith("cluster-a", POD, expect.any(AbortSignal), undefined);
    expect(forwarder.open).toBe(1);

    first.client.destroy();
    second.client.destroy();
    first.close();
    second.close();
    await forwarder.close();
  });

  it("opens another tunnel once the one of the pod has ended", async () => {
    const tunnels = fakeTunnels(await echoServer());
    const forwarder = createTunnelForwarder({ tunnels, clusterId: "cluster-a" });
    const first = await socketPair();

    forwarder.forward(POD, first.accepted);
    expect(await roundTrip(first.client, "a")).toBe("a");
    tunnels.opened[0].end();
    await vi.waitFor(() => expect(forwarder.open).toBe(0));

    const second = await socketPair();
    forwarder.forward(POD, second.accepted);
    expect(await roundTrip(second.client, "b")).toBe("b");
    expect(tunnels.open).toHaveBeenCalledTimes(2);

    first.client.destroy();
    second.client.destroy();
    first.close();
    second.close();
    await forwarder.close();
  });

  it("destroys the socket with the tunnel's refusal and tells it, then tries again on the next socket", async () => {
    const refusal = new TunnelError("forbidden", "Port-forward refused");
    const port = await echoServer();
    const open = vi
      .fn()
      .mockImplementationOnce(async () => {
        throw refusal;
      })
      .mockResolvedValue({
        address: "127.0.0.1",
        port,
        ended: new Promise<void>(() => undefined),
        close: async () => undefined,
      });
    const errors: unknown[] = [];
    const forwarder = createTunnelForwarder({
      tunnels: { open },
      clusterId: "cluster-a",
      onError: (e) => errors.push(e),
    });
    const first = await socketPair();
    const closed = new Promise((resolve) => first.client.once("close", resolve));

    forwarder.forward(POD, first.accepted);
    await closed;
    expect(errors).toEqual([refusal]);

    const second = await socketPair();
    forwarder.forward(POD, second.accepted);
    expect(await roundTrip(second.client, "again")).toBe("again");

    second.client.destroy();
    first.close();
    second.close();
    await forwarder.close();
  });

  it("aborts and closes every tunnel on close, and refuses the sockets that come later", async () => {
    const tunnels = fakeTunnels(await echoServer());
    const forwarder = createTunnelForwarder({ tunnels, clusterId: "cluster-a" });
    const pair = await socketPair();

    const other = await socketPair();

    forwarder.forward(POD, pair.accepted);
    forwarder.forward({ ...POD, pod: "my-cluster-broker-1" }, other.accepted);
    await vi.waitFor(() => expect(tunnels.opened).toHaveLength(2));
    await forwarder.close();

    expect(tunnels.opened.every(({ signal, close }) => signal.aborted && close.mock.calls.length === 1)).toBe(true);
    expect(forwarder.open).toBe(0);

    const late = await socketPair();
    const closed = new Promise((resolve) => late.client.once("close", resolve));
    forwarder.forward(POD, late.accepted);
    await closed;
    expect(tunnels.open).toHaveBeenCalledTimes(2);

    pair.client.destroy();
    other.client.destroy();
    pair.close();
    other.close();
    late.close();
  });
});
