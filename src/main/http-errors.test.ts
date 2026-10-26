// The failures of an undici fetch, as the host's `Main.Util.fetch` gives them, on real loopback sockets: a refused
// port is described in the words node:http used, and a connection the server drops is transient.

import { createServer, type Server } from "node:http";
import net from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { describeFetchError, isTransientConnectionError } from "./http-errors";
import { KafkaConnectClient } from "./kafka-connect/client";
import { SchemaRegistryClient } from "./schema-registry/client";

const servers: Server[] = [];

afterEach(async () => {
  await Promise.all(servers.splice(0).map((server) => new Promise((resolve) => server.close(resolve))));
});

async function listen(server: Server): Promise<string> {
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return `http://127.0.0.1:${(server.address() as net.AddressInfo).port}`;
}

async function closedPort(): Promise<number> {
  const probe = net.createServer();
  await new Promise<void>((resolve) => probe.listen(0, "127.0.0.1", resolve));
  const { port } = probe.address() as net.AddressInfo;
  await new Promise((resolve) => probe.close(resolve));
  return port;
}

// A server that drops its first `drops` connections before answering, then answers `body`.
function dropping(drops: number, body: unknown): { server: Server; requests: () => number } {
  let seen = 0;
  const server = createServer((request, response) => {
    seen += 1;
    if (seen <= drops) {
      request.socket.destroy();
      return;
    }
    response.setHeader("content-type", "application/json");
    response.end(JSON.stringify(body));
  });
  return { server, requests: () => seen };
}

describe("describeFetchError", () => {
  it("says why a fetch failed in the words of its cause", async () => {
    const port = await closedPort();
    const error = await fetch(`http://127.0.0.1:${port}/subjects`).catch((e: unknown) => e);

    expect(error).toBeInstanceOf(TypeError);
    expect((error as Error).message).toBe("fetch failed");
    const described = describeFetchError(error) as Error;
    expect(described.message).toContain("ECONNREFUSED");
    expect(described.message).toContain(`127.0.0.1:${port}`);
    expect(described.cause).toBe(error);
  });

  it("leaves any other error as it is", () => {
    const error = new Error("Schema Registry request failed (500) for /subjects");
    expect(describeFetchError(error)).toBe(error);
    expect(describeFetchError("text")).toBe("text");
  });
});

describe("isTransientConnectionError", () => {
  it("knows a connection the server dropped, through undici's causes", async () => {
    const { server } = dropping(1, []);
    const url = await listen(server);
    const error = await fetch(`${url}/connectors`).catch((e: unknown) => e);

    expect(isTransientConnectionError(error)).toBe(true);
  });

  it("knows node:http's reset and nothing else", () => {
    expect(isTransientConnectionError(Object.assign(new Error("socket hang up"), { code: "ECONNRESET" }))).toBe(true);
    expect(isTransientConnectionError(new Error("Kafka Connect request failed (500) for /connectors"))).toBe(false);
    expect(isTransientConnectionError(undefined)).toBe(false);
  });
});

describe("the HTTP clients on undici's fetch", () => {
  it("Kafka Connect reads once more after a dropped connection", async () => {
    const { server, requests } = dropping(1, ["orders-source"]);
    const client = new KafkaConnectClient({ baseUrl: await listen(server), fetchImpl: fetch });

    await expect(client.listConnectorNames()).resolves.toEqual(["orders-source"]);
    expect(requests()).toBe(2);
  });

  it("Kafka Connect and the Schema Registry say a refused port as such", async () => {
    const port = await closedPort();
    const connect = new KafkaConnectClient({ baseUrl: `http://127.0.0.1:${port}`, fetchImpl: fetch });
    const registry = new SchemaRegistryClient({ baseUrl: `http://127.0.0.1:${port}`, fetchImpl: fetch });

    await expect(connect.listConnectorNames()).rejects.toThrow(/ECONNREFUSED/);
    await expect(registry.listSubjects()).rejects.toThrow(/ECONNREFUSED/);
  });
});
