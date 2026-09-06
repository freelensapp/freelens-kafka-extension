import net from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { probeBootstrapReachable, probeTcp } from "./reachability";

describe("probeTcp / probeBootstrapReachable", () => {
  let server: net.Server | undefined;

  afterEach(async () => {
    if (server) {
      await new Promise<void>((resolve) => server?.close(() => resolve()));
      server = undefined;
    }
  });

  it("resolves true for a listening port and false for a closed one", async () => {
    server = net.createServer();
    await new Promise<void>((resolve) => server?.listen(0, "127.0.0.1", () => resolve()));
    const { port } = server.address() as net.AddressInfo;

    expect(await probeTcp("127.0.0.1", port, 1000)).toBe(true);
    expect(await probeBootstrapReachable(`127.0.0.1:${port}`, 1000)).toBe(true);

    await new Promise<void>((resolve) => server?.close(() => resolve()));
    server = undefined;
    expect(await probeTcp("127.0.0.1", port, 1000)).toBe(false);
  });
});
