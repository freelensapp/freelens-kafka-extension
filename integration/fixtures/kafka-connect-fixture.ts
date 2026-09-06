import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

export interface KafkaConnectFixture {
  url: string;
  close: () => Promise<void>;
}
type Connector = { config: Record<string, string>; state: string; trace?: string };

function body(request: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    request.on("error", reject);
  });
}
function send(response: ServerResponse, value: unknown, status = 200): void {
  response.statusCode = status;
  response.end(value === undefined ? undefined : JSON.stringify(value));
}

export async function startKafkaConnectFixture(port = 18083): Promise<KafkaConnectFixture> {
  const connectors = new Map<string, Connector>([
    ["orders-source", { config: { name: "orders-source", "connector.class": "ExampleSource" }, state: "RUNNING" }],
    [
      "payments-sink",
      {
        config: { name: "payments-sink", "connector.class": "ExampleSink" },
        state: "FAILED",
        trace: "full fixture failure trace",
      },
    ],
  ]);
  const server: Server = createServer(async (request, response) => {
    response.setHeader("content-type", "application/json");
    const path = request.url ?? "/";
    const match = path.match(/^\/connectors\/([^/]+)(?:\/(config|status|pause|resume|restart))?$/);
    if (request.method === "GET" && path === "/connectors") return send(response, [...connectors.keys()]);
    if (match) {
      const name = decodeURIComponent(match[1]);
      const connector = connectors.get(name);
      if (!connector) return send(response, { message: "not found" }, 404);
      const action = match[2];
      if (request.method === "GET" && !action) return send(response, { config: connector.config });
      if (request.method === "GET" && action === "status")
        return send(response, {
          connector: { state: connector.state, worker_id: "worker-local" },
          tasks: [{ id: 0, state: connector.state, trace: connector.trace }],
        });
      if (request.method === "PUT" && action === "pause") {
        connector.state = "PAUSED";
        return send(response, undefined, 202);
      }
      if (request.method === "PUT" && action === "resume") {
        connector.state = "RUNNING";
        return send(response, undefined, 202);
      }
      if (request.method === "POST" && action === "restart") {
        connector.state = "RUNNING";
        connector.trace = undefined;
        return send(response, undefined, 202);
      }
      if (request.method === "PUT" && action === "config") {
        connector.config = JSON.parse(await body(request));
        connector.state = "RUNNING";
        return send(response, undefined, 200);
      }
      if (request.method === "DELETE" && !action) {
        connectors.delete(name);
        return send(response, undefined, 204);
      }
    }
    if (request.method === "POST" && path === "/connectors") {
      const config = JSON.parse(await body(request)).config as Record<string, string>;
      connectors.set(config.name, { config, state: "RUNNING" });
      return send(response, { name: config.name }, 201);
    }
    send(response, { message: "not found" }, 404);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", resolve);
  });
  return {
    url: `http://127.0.0.1:${port}`,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.closeAllConnections?.();
        server.close((error) => (error ? reject(error) : resolve()));
      }),
  };
}
