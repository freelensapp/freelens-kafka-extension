import { describe, expect, it, vi } from "vitest";
import { SchemaRegistryClient } from "./client";

function response(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

describe("SchemaRegistryClient", () => {
  it("lists subjects and reads latest schema metadata without leaking credentials", async () => {
    const calls: Array<{ url: string; headers: HeadersInit | undefined }> = [];
    const fetchImpl = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ url: String(input), headers: init?.headers });
      if (String(input).endsWith("/subjects")) return response(["orders-value"]);
      if (String(input).endsWith("/versions")) return response([1, 2]);
      if (String(input).endsWith("/versions/2")) {
        return response({ id: 22, subject: "orders-value", schema: '{"type":"record"}', schemaType: "AVRO" });
      }
      if (String(input).endsWith("/config/orders-value")) return response({ compatibilityLevel: "BACKWARD" });
      throw new Error(`Unexpected URL ${String(input)}`);
    });
    const client = new SchemaRegistryClient({
      baseUrl: "http://127.0.0.1:18081/",
      username: "registry-user",
      password: "secret",
      fetchImpl,
    });

    await expect(client.listSubjects()).resolves.toEqual(["orders-value"]);
    await expect(client.getSubjectSummary("orders-value")).resolves.toEqual({
      subject: "orders-value",
      latestVersion: 2,
      schemaType: "AVRO",
      compatibility: "BACKWARD",
    });
    expect(calls.every(({ url }) => url.startsWith("http://127.0.0.1:18081/"))).toBe(true);
    expect(calls.every(({ headers }) => "Authorization" in (headers as Record<string, string>))).toBe(true);
    expect(calls.every(({ url }) => !url.includes("secret") && !url.includes("registry-user"))).toBe(true);
  });

  it("returns every version and tolerates subject-level compatibility being absent", async () => {
    const fetchImpl = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/versions")) return response([1, 2]);
      if (url.endsWith("/versions/1")) return response({ id: 11, schema: "{}", schemaType: "JSON" });
      if (url.endsWith("/versions/2")) return response({ id: 12, schema: "message Order {}", schemaType: "PROTOBUF" });
      if (url.endsWith("/config/orders-value")) return response({}, 404);
      throw new Error(`Unexpected URL ${url}`);
    });
    const client = new SchemaRegistryClient({ baseUrl: "http://127.0.0.1:18081", fetchImpl });

    await expect(client.getSubjectDetail("orders-value")).resolves.toEqual({
      subject: "orders-value",
      compatibility: undefined,
      versions: [
        { id: 11, version: 1, subject: "orders-value", schema: "{}", schemaType: "JSON", references: undefined },
        {
          id: 12,
          version: 2,
          subject: "orders-value",
          schema: "message Order {}",
          schemaType: "PROTOBUF",
          references: undefined,
        },
      ],
    });
  });

  it("looks up a schema by Confluent schema id", async () => {
    const fetchImpl = vi.fn(async () => response({ id: 7, schema: "{}" }));
    const client = new SchemaRegistryClient({ baseUrl: "http://127.0.0.1:18081", fetchImpl });

    await expect(client.getSchemaById(7)).resolves.toEqual({ id: 7, schema: "{}" });
  });

  it("registers a schema and deletes a subject using bounded HTTP requests", async () => {
    const calls: Array<{ method?: string; body?: string }> = [];
    const fetchImpl = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
      calls.push({ method: init?.method, body: init?.body as string | undefined });
      return response(init?.method === "DELETE" ? [1, 2] : { id: 23 });
    });
    const client = new SchemaRegistryClient({ baseUrl: "http://127.0.0.1:18081", fetchImpl });

    await expect(client.registerSchema("orders-value", "{}", "AVRO")).resolves.toEqual({ id: 23 });
    await expect(client.deleteSubject("orders-value")).resolves.toEqual([1, 2]);
    expect(calls).toEqual([
      { method: "POST", body: JSON.stringify({ schema: "{}", schemaType: "AVRO" }) },
      { method: "DELETE", body: undefined },
    ]);
  });

  it("fails with a bounded timeout", async () => {
    const fetchImpl = vi.fn(
      (_input: RequestInfo | URL, init?: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
        }),
    );
    const client = new SchemaRegistryClient({ baseUrl: "http://127.0.0.1:18081", fetchImpl, timeoutMs: 5 });

    await expect(client.listSubjects()).rejects.toThrow("timed out after 5ms");
  });
});
