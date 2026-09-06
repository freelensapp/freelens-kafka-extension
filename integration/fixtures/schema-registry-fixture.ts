import { createServer, type Server } from "node:http";

export interface SchemaRegistryFixture {
  url: string;
  close: () => Promise<void>;
}

export async function startSchemaRegistryFixture(port = 18081): Promise<SchemaRegistryFixture> {
  let registered = false;
  let deleted = false;
  const server: Server = createServer((request, response) => {
    const path = request.url ?? "/";
    if (request.method === "POST" && path === "/subjects/orders-value/versions") {
      registered = true;
      response.setHeader("content-type", "application/json");
      return void response.end(JSON.stringify({ id: 23 }));
    }
    if (request.method === "DELETE" && path === "/subjects/orders-value") {
      deleted = true;
      response.setHeader("content-type", "application/json");
      return void response.end(JSON.stringify([1, 2, 3]));
    }
    response.setHeader("content-type", "application/json");
    if (path === "/subjects")
      return void response.end(JSON.stringify(deleted ? ["payments-value"] : ["orders-value", "payments-value"]));
    if (path === "/subjects/orders-value/versions")
      return void response.end(JSON.stringify(registered ? [1, 2, 3] : [1, 2]));
    if (path === "/subjects/payments-value/versions") return void response.end(JSON.stringify([1]));
    if (path === "/subjects/orders-value/versions/1") {
      return void response.end(
        JSON.stringify({
          id: 11,
          subject: "orders-value",
          schema: '{"type":"record","name":"Order"}',
          schemaType: "AVRO",
        }),
      );
    }
    if (path === "/subjects/orders-value/versions/2") {
      return void response.end(
        JSON.stringify({
          id: 12,
          subject: "orders-value",
          schema: '{"type":"record","name":"OrderV2"}',
          schemaType: "AVRO",
        }),
      );
    }
    if (path === "/subjects/payments-value/versions/1") {
      return void response.end(
        JSON.stringify({ id: 21, subject: "payments-value", schema: "message Payment {}", schemaType: "PROTOBUF" }),
      );
    }
    if (path === "/config/orders-value") return void response.end(JSON.stringify({ compatibilityLevel: "BACKWARD" }));
    if (path === "/config/payments-value") return void response.end(JSON.stringify({ compatibilityLevel: "FULL" }));
    if (path === "/schemas/ids/7") {
      return void response.end(
        JSON.stringify({ id: 7, schema: '{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}' }),
      );
    }
    if (path === "/schemas/ids/21") {
      return void response.end(
        JSON.stringify({
          id: 21,
          schemaType: "PROTOBUF",
          schema: 'syntax = "proto3"; message Payment { string id = 1; }',
        }),
      );
    }
    response.statusCode = 404;
    response.end(JSON.stringify({ error_code: 40401, message: "Not found" }));
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => resolve());
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
