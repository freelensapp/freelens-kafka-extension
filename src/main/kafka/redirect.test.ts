import net from "node:net";
import { describe, expect, it } from "vitest";
import { createDirectSocketFactory, resolveTarget } from "./redirect";

describe("resolveTarget", () => {
  it("passes through unmapped addresses unchanged", () => {
    const map = new Map<string, string>();
    expect(resolveTarget(map, "broker.example.com", 9092)).toEqual({
      host: "broker.example.com",
      port: 9092,
      redirected: false,
    });
  });

  it("redirects a mapped advertised address to its local endpoint", () => {
    const map = new Map([["kafka-internal:9092", "127.0.0.1:50001"]]);
    expect(resolveTarget(map, "kafka-internal", 9092)).toEqual({
      host: "127.0.0.1",
      port: 50001,
      redirected: true,
    });
  });

  it("only redirects the exact host:port key", () => {
    const map = new Map([["kafka-internal:9092", "127.0.0.1:50001"]]);
    expect(resolveTarget(map, "kafka-internal", 9093).redirected).toBe(false);
  });
});

describe("createDirectSocketFactory", () => {
  it("connects, tracks the socket, and closeAll destroys it", async () => {
    const server = net.createServer();
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
    const { port } = server.address() as net.AddressInfo;

    const { socketFactory, closeAll } = createDirectSocketFactory();
    const socket = socketFactory({ host: "127.0.0.1", port, onConnect: () => undefined });
    await new Promise<void>((resolve, reject) => {
      socket.once("connect", () => resolve());
      socket.once("error", reject);
    });

    expect(socket.destroyed).toBe(false);
    closeAll();
    expect(socket.destroyed).toBe(true);

    await new Promise<void>((resolve) => server.close(() => resolve()));
  });
});
