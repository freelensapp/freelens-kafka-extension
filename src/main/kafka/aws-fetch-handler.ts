// A request handler of the AWS SDK over a WHATWG fetch: the host's (`Main.Util.fetch`), so that the STS and SSO calls
// of the credential chain (a profile's `role_arn`, `sso_session`, a web identity token) go through the user's proxy and
// CA settings (SPEC-019, REQ-226). The SDK takes it as `clientConfig.requestHandler` of the clients it makes for those
// calls; the instance metadata and container endpoints of the chain take none and keep the SDK's own client.
//
// What the SDK gives and takes is described here by its shape: an `HttpRequest` of @smithy/protocol-http and an
// `HttpResponse` that its deserializers read (`statusCode`, `headers`, `body` as a Node stream).

import { Readable } from "node:stream";
import { formatUrl } from "@aws-sdk/util-format-url";

export interface AwsHttpRequest {
  method: string;
  protocol: string;
  hostname: string;
  port?: number;
  path: string;
  query?: Record<string, string | Array<string> | null>;
  headers: Record<string, string>;
  body?: unknown;
  username?: string;
  password?: string;
  fragment?: string;
}

export interface AwsHttpResponse {
  statusCode: number;
  reason?: string;
  headers: Record<string, string>;
  body: Readable;
}

export interface AwsFetchRequestHandler {
  handle(request: AwsHttpRequest, options?: { abortSignal?: AbortSignal }): Promise<{ response: AwsHttpResponse }>;
  updateHttpClientConfig(key: never, value: never): void;
  httpHandlerConfigs(): Record<string, never>;
  destroy(): void;
}

// Headers fetch sets itself from the URL and the body.
const SET_BY_FETCH = new Set(["host", "content-length", "transfer-encoding", "connection"]);

async function bodyOf(body: unknown): Promise<string | Uint8Array | undefined> {
  if (body === undefined || body === null) return undefined;
  if (typeof body === "string" || body instanceof Uint8Array) return body;
  if (body instanceof Readable) {
    const chunks: Buffer[] = [];
    for await (const chunk of body) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as string));
    return Buffer.concat(chunks);
  }
  throw new Error("The AWS SDK gave a request body the extension cannot send");
}

export function createAwsFetchRequestHandler(fetchImpl: typeof fetch): AwsFetchRequestHandler {
  return {
    async handle(request, options) {
      const headers: Record<string, string> = {};
      for (const [name, value] of Object.entries(request.headers)) {
        if (!SET_BY_FETCH.has(name.toLowerCase())) headers[name] = value;
      }
      const method = request.method.toUpperCase();
      const body = method === "GET" || method === "HEAD" ? undefined : await bodyOf(request.body);
      const response = await fetchImpl(formatUrl(request), {
        method,
        headers,
        body: body as NonNullable<RequestInit["body"]> | undefined,
        signal: options?.abortSignal,
      });
      const responseHeaders: Record<string, string> = {};
      response.headers.forEach((value, name) => {
        responseHeaders[name] = value;
      });
      // The answers of STS and SSO are small: they are read whole and handed over as a stream.
      const content = Buffer.from(await response.arrayBuffer());

      return {
        response: {
          statusCode: response.status,
          reason: response.statusText,
          headers: responseHeaders,
          body: Readable.from([content]),
        },
      };
    },
    updateHttpClientConfig() {
      // The host's client has its own settings: there is nothing of it to configure.
    },
    httpHandlerConfigs() {
      return {};
    },
    destroy() {
      // Nothing is held between requests.
    },
  };
}
