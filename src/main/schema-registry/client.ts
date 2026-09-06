import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";

export type SchemaType = "AVRO" | "PROTOBUF" | "JSON" | string;

export interface SchemaRegistryClientOptions {
  baseUrl: string;
  username?: string;
  password?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}

export interface SchemaSubjectSummary {
  subject: string;
  latestVersion: number;
  schemaType: SchemaType;
  compatibility?: string;
}

export interface SchemaVersion {
  id: number;
  version: number;
  subject: string;
  schema: string;
  schemaType: SchemaType;
  references?: Array<{ name: string; subject: string; version: number }>;
}

export interface SchemaSubjectDetail {
  subject: string;
  compatibility?: string;
  versions: SchemaVersion[];
}

export interface SchemaRegistrationResult {
  id: number;
}

interface RegistrySchemaResponse {
  id: number;
  version?: number;
  subject?: string;
  schema: string;
  schemaType?: SchemaType;
  references?: Array<{ name: string; subject: string; version: number }>;
}

function normalizeBaseUrl(baseUrl: string): string {
  const parsed = new URL(baseUrl);
  parsed.pathname = parsed.pathname.replace(/\/$/, "");
  return parsed.toString().replace(/\/$/, "");
}

export class SchemaRegistryClient {
  private readonly baseUrl: string;
  private readonly fetchImpl?: typeof fetch;
  private readonly timeoutMs: number;
  private readonly authorization?: string;

  constructor(options: SchemaRegistryClientOptions) {
    this.baseUrl = normalizeBaseUrl(options.baseUrl);
    this.fetchImpl = options.fetchImpl;
    this.timeoutMs = options.timeoutMs ?? 30_000;
    if (options.username !== undefined && options.password !== undefined) {
      this.authorization = `Basic ${Buffer.from(`${options.username}:${options.password}`).toString("base64")}`;
    }
  }

  async listSubjects(): Promise<string[]> {
    return this.request<string[]>("/subjects");
  }

  async getSubjectSummary(subject: string): Promise<SchemaSubjectSummary> {
    const versions = await this.request<number[]>(`/subjects/${encodeURIComponent(subject)}/versions`);
    const latestVersion = Math.max(...versions);
    const schema = await this.request<RegistrySchemaResponse>(
      `/subjects/${encodeURIComponent(subject)}/versions/${latestVersion}`,
    );
    return {
      subject,
      latestVersion,
      schemaType: schema.schemaType ?? "AVRO",
      compatibility: await this.getCompatibility(subject),
    };
  }

  async getSubjectDetail(subject: string): Promise<SchemaSubjectDetail> {
    const versions = await this.request<number[]>(`/subjects/${encodeURIComponent(subject)}/versions`);
    const schemas = await Promise.all(
      versions.map(async (version) => {
        const schema = await this.request<RegistrySchemaResponse>(
          `/subjects/${encodeURIComponent(subject)}/versions/${version}`,
        );
        return {
          id: schema.id,
          version,
          subject: schema.subject ?? subject,
          schema: schema.schema,
          schemaType: schema.schemaType ?? "AVRO",
          references: schema.references,
        };
      }),
    );
    return { subject, compatibility: await this.getCompatibility(subject), versions: schemas };
  }

  async getSchemaById(id: number): Promise<RegistrySchemaResponse> {
    return this.request<RegistrySchemaResponse>(`/schemas/ids/${id}`);
  }

  async registerSchema(
    subject: string,
    schema: string,
    schemaType: SchemaType = "AVRO",
  ): Promise<SchemaRegistrationResult> {
    return this.request<SchemaRegistrationResult>(`/subjects/${encodeURIComponent(subject)}/versions`, {
      method: "POST",
      body: JSON.stringify({ schema, schemaType }),
    });
  }

  async deleteSubject(subject: string): Promise<number[]> {
    return this.request<number[]>(`/subjects/${encodeURIComponent(subject)}`, { method: "DELETE" });
  }

  async getCompatibility(subject: string): Promise<string | undefined> {
    const response = await this.request<{ compatibilityLevel?: string }>(`/config/${encodeURIComponent(subject)}`, {
      allowNotFound: true,
    });
    return response?.compatibilityLevel;
  }

  private async request<T>(
    path: string,
    options: { allowNotFound?: boolean; method?: string; body?: string } = {},
  ): Promise<T> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const headers: Record<string, string> = { Accept: "application/json" };
      if (options.body) headers["Content-Type"] = "application/vnd.schemaregistry.v1+json";
      if (this.authorization) headers.Authorization = this.authorization;
      if (this.fetchImpl) {
        const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
          method: options.method,
          body: options.body,
          headers,
          signal: controller.signal,
        });
        if (options.allowNotFound && response.status === 404) return undefined as T;
        if (!response.ok) throw new Error(`Schema Registry request failed (${response.status}) for ${path}`);
        return (await response.json()) as T;
      }
      const parsed = new URL(`${this.baseUrl}${path}`);
      const request = (parsed.protocol === "https:" ? httpsRequest : httpRequest)({
        protocol: parsed.protocol,
        hostname: parsed.hostname,
        port: parsed.port,
        path: `${parsed.pathname}${parsed.search}`,
        method: options.method ?? "GET",
        headers,
      });
      const body = await new Promise<{ statusCode?: number; body: string }>((resolve, reject) => {
        const chunks: Buffer[] = [];
        request.on("response", (response) => {
          response.on("data", (chunk: Buffer) => chunks.push(chunk));
          response.on("end", () =>
            resolve({ statusCode: response.statusCode, body: Buffer.concat(chunks).toString("utf8") }),
          );
        });
        request.on("error", reject);
        controller.signal.addEventListener("abort", () => request.destroy(new Error("aborted")), { once: true });
        if (options.body) request.write(options.body);
        request.end();
      });
      if (options.allowNotFound && body.statusCode === 404) return undefined as T;
      if (!body.statusCode || body.statusCode < 200 || body.statusCode >= 300) {
        throw new Error(`Schema Registry request failed (${body.statusCode ?? "unknown"}) for ${path}`);
      }
      return JSON.parse(body.body) as T;
    } catch (error) {
      if (controller.signal.aborted || (error instanceof DOMException && error.name === "AbortError")) {
        throw new Error(`Schema Registry request timed out after ${this.timeoutMs}ms: ${path}`);
      }
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }
}
