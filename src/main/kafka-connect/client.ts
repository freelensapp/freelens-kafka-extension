import { describeFetchError, isTransientConnectionError } from "../http-errors";

export interface KafkaConnectClientOptions {
  baseUrl: string;
  username?: string;
  password?: string;
  /** The host's HTTP client, `Main.Util.fetch`, so that the user's proxy and CA settings apply (SPEC-019, REQ-226). */
  fetchImpl: typeof fetch;
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
  private readonly fetchImpl: typeof fetch;

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
    const retry = (options.method ?? "GET") === "GET";
    try {
      try {
        return await this.requestOnce<T>(path, options);
      } catch (error) {
        if (!retry || !isTransientConnectionError(error)) throw error;
        return await this.requestOnce<T>(path, options);
      }
    } catch (error) {
      throw describeFetchError(error);
    }
  }

  private async requestOnce<T>(path: string, options: { method?: string; body?: string } = {}): Promise<T> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    try {
      const headers: Record<string, string> = { Accept: "application/json" };
      if (options.body) headers["Content-Type"] = "application/json";
      if (this.authorization) headers.Authorization = this.authorization;
      const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
        method: options.method,
        body: options.body,
        headers,
        signal: controller.signal,
      });
      if (!response.ok) throw new Error(`Kafka Connect request failed (${response.status}) for ${path}`);
      const body = await response.text();
      return body ? (JSON.parse(body) as T) : (undefined as T);
    } catch (error) {
      if (controller.signal.aborted)
        throw new Error(`Kafka Connect request timed out after ${this.timeoutMs}ms: ${path}`);
      throw error;
    } finally {
      clearTimeout(timeout);
    }
  }
}
