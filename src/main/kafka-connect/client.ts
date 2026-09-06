import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";

export interface KafkaConnectClientOptions {
  baseUrl: string;
  username?: string;
  password?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}

export interface ConnectorSummary {
  name: string;
  type?: "source" | "sink" | string;
  status: string;
  taskCount: number;
}

export interface ConnectorTask {
  id: number;
  state: string;
  workerId?: string;
  trace?: string;
}

export interface ConnectorDetail {
  name: string;
  config: Record<string, string>;
  connector: { state: string; workerId?: string; version?: string };
  tasks: ConnectorTask[];
}

function normalized(url: string): string {
  const parsed = new URL(url);
  parsed.pathname = parsed.pathname.replace(/\/$/, "");
  return parsed.toString().replace(/\/$/, "");
}

export class KafkaConnectClient {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;
  private readonly authorization?: string;
  private readonly fetchImpl?: typeof fetch;

  constructor(options: KafkaConnectClientOptions) {
    this.baseUrl = normalized(options.baseUrl);
    this.timeoutMs = options.timeoutMs ?? 30_000;
    this.fetchImpl = options.fetchImpl;
    if (options.username !== undefined && options.password !== undefined) {
      this.authorization = `Basic ${Buffer.from(`${options.username}:${options.password}`).toString("base64")}`;
    }
  }

  async listConnectors(): Promise<ConnectorSummary[]> {
    const names = await this.request<string[]>("/connectors");
    return Promise.all(
      names.map(async (name) => {
        const detail = await this.getConnector(name);
        return {
          name,
          type: detail.config["connector.class"]?.toLowerCase().includes("source") ? "source" : "sink",
          status: detail.connector.state,
          taskCount: detail.tasks.length,
        };
      }),
    );
  }

  async listConnectorNames(): Promise<string[]> {
    return this.request<string[]>("/connectors");
  }

  async getConnector(name: string): Promise<ConnectorDetail> {
    const encoded = encodeURIComponent(name);
    const [config, status] = await Promise.all([
      this.request<Record<string, string>>(`/connectors/${encoded}`),
      this.request<{ name: string; connector: ConnectorDetail["connector"]; tasks: ConnectorTask[] }>(
        `/connectors/${encoded}/status`,
      ),
    ]);
    const rawConfig = typeof config === "object" && config !== null && "config" in config ? config.config : config;
    return {
      name,
      config: rawConfig as unknown as Record<string, string>,
      connector: status.connector,
      tasks: status.tasks,
    };
  }

  async createConnector(config: Record<string, string>): Promise<{ name: string }> {
    return this.request<{ name: string }>("/connectors", {
      method: "POST",
      body: JSON.stringify({ name: config.name, config }),
    });
  }

  async updateConnector(name: string, config: Record<string, string>): Promise<void> {
    await this.request(`/connectors/${encodeURIComponent(name)}/config`, {
      method: "PUT",
      body: JSON.stringify(config),
    });
  }

  async deleteConnector(name: string): Promise<void> {
    await this.request(`/connectors/${encodeURIComponent(name)}`, { method: "DELETE" });
  }

  async pauseConnector(name: string): Promise<void> {
    await this.request(`/connectors/${encodeURIComponent(name)}/pause`, { method: "PUT" });
  }

  async resumeConnector(name: string): Promise<void> {
    await this.request(`/connectors/${encodeURIComponent(name)}/resume`, { method: "PUT" });
  }

  async restartConnector(name: string): Promise<void> {
    await this.request(`/connectors/${encodeURIComponent(name)}/restart`, { method: "POST" });
  }

  private async request<T>(path: string, options: { method?: string; body?: string } = {}): Promise<T> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const headers: Record<string, string> = { Accept: "application/json" };
      if (options.body) headers["Content-Type"] = "application/json";
      if (this.authorization) headers.Authorization = this.authorization;
      if (this.fetchImpl) {
        const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
          method: options.method,
          body: options.body,
          headers,
          signal: controller.signal,
        });
        if (!response.ok) throw new Error(`Kafka Connect request failed (${response.status}) for ${path}`);
        const body = await response.text();
        return body ? (JSON.parse(body) as T) : (undefined as T);
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
      const result = await new Promise<{ statusCode?: number; body: string }>((resolve, reject) => {
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
      if (!result.statusCode || result.statusCode < 200 || result.statusCode >= 300) {
        throw new Error(`Kafka Connect request failed (${result.statusCode ?? "unknown"}) for ${path}`);
      }
      return (result.body ? JSON.parse(result.body) : undefined) as T;
    } catch (error) {
      if (controller.signal.aborted)
        throw new Error(`Kafka Connect request timed out after ${this.timeoutMs}ms: ${path}`);
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }
}
