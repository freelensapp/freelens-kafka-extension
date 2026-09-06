import { createServer } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { KafkaConnectClient } from "./client";

let server: ReturnType<typeof createServer> | undefined;

async function startServer(): Promise<string> {
  server = createServer((request, response) => {
    response.setHeader("content-type", "application/json");
    if (request.url === "/connectors") return void response.end(JSON.stringify(["orders-source", "payments-sink"]));
    if (request.url === "/connectors/orders-source")
      return void response.end(
        JSON.stringify({ config: { name: "orders-source", "connector.class": "ExampleSource" } }),
      );
    if (request.url === "/connectors/orders-source/status")
      return void response.end(
        JSON.stringify({
          connector: { state: "RUNNING", worker_id: "worker-a" },
          tasks: [{ id: 0, state: "RUNNING" }],
        }),
      );
    if (request.url === "/connectors/payments-sink")
      return void response.end(JSON.stringify({ config: { name: "payments-sink", "connector.class": "ExampleSink" } }));
    if (request.url === "/connectors/payments-sink/status")
      return void response.end(
        JSON.stringify({
          connector: { state: "FAILED", worker_id: "worker-b" },
          tasks: [{ id: 0, state: "FAILED", trace: "full failure trace" }],
        }),
      );
    if (request.method === "POST" && request.url === "/connectors") {
      request.resume();
      return void request.on("end", () => response.end(JSON.stringify({ name: "new-connector" })));
    }
    if (request.method === "DELETE" || request.url?.endsWith("/pause") || request.url?.endsWith("/resume")) {
      request.resume();
      request.on("end", () => {
        response.statusCode = 204;
        response.end();
      });
      return;
    }
    response.statusCode = 404;
    response.end(JSON.stringify({ message: "not found" }));
  });
  await new Promise<void>((resolve) => server?.listen(18083, "127.0.0.1", resolve));
  return "http://127.0.0.1:18083";
}

afterEach(async () => new Promise<void>((resolve) => server?.close(() => resolve())));

describe("KafkaConnectClient", () => {
  it("lists connector state and tasks", async () => {
    const client = new KafkaConnectClient({ baseUrl: await startServer() });
    await expect(client.listConnectors()).resolves.toEqual([
      { name: "orders-source", type: "source", status: "RUNNING", taskCount: 1 },
      { name: "payments-sink", type: "sink", status: "FAILED", taskCount: 1 },
    ]);
    await expect(client.getConnector("payments-sink")).resolves.toMatchObject({
      tasks: [{ trace: "full failure trace" }],
    });
  });

  it("executes bounded lifecycle and management calls", async () => {
    const fetchImpl = async (_input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      const post = init?.method === "POST";
      return new Response(post ? JSON.stringify({ name: "new-connector" }) : undefined, {
        status: post ? 200 : 204,
        headers: { "content-type": "application/json" },
      });
    };
    const client = new KafkaConnectClient({ baseUrl: "http://127.0.0.1:18083", fetchImpl });
    await expect(client.pauseConnector("orders-source")).resolves.toBeUndefined();
    await expect(client.resumeConnector("orders-source")).resolves.toBeUndefined();
    await expect(client.deleteConnector("orders-source")).resolves.toBeUndefined();
    await expect(client.createConnector({ name: "new-connector" })).resolves.toEqual({ name: "new-connector" });
  });
});
