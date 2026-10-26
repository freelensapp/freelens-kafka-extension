// The AWS request handler over fetch: what it sends and gives back, and, with the SDK's own credential chain, that an
// assumed role is asked of STS through it. STS is a local fake; no request leaves the machine.

import { mkdtempSync, writeFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Readable } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createAwsFetchRequestHandler } from "./aws-fetch-handler";
import { awsCredentialsProvider } from "./msk-iam";
import type net from "node:net";

const servers: Server[] = [];

afterEach(async () => {
  vi.unstubAllEnvs();
  await Promise.all(servers.splice(0).map((server) => new Promise((resolve) => server.close(resolve))));
});

async function text(stream: Readable): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk as Buffer));
  return Buffer.concat(chunks).toString("utf8");
}

describe("createAwsFetchRequestHandler", () => {
  it("sends the request as a URL with its query, without the headers fetch sets, and gives the answer back", async () => {
    const fetchImpl = vi.fn<typeof fetch>(
      async () => new Response("<ok/>", { status: 200, statusText: "OK", headers: { "x-amzn-requestid": "r-1" } }),
    );
    const handler = createAwsFetchRequestHandler(fetchImpl);
    const { response } = await handler.handle({
      method: "POST",
      protocol: "https:",
      hostname: "sts.eu-west-1.amazonaws.com",
      path: "/",
      query: { Action: "AssumeRole", List: ["a", "b"] },
      headers: { host: "sts.eu-west-1.amazonaws.com", "content-length": "3", authorization: "AWS4-HMAC-SHA256 x" },
      body: "a=b",
    });

    const [url, init] = fetchImpl.mock.calls[0];
    expect(url).toBe("https://sts.eu-west-1.amazonaws.com/?Action=AssumeRole&List=a&List=b");
    expect(init).toMatchObject({ method: "POST", body: "a=b", headers: { authorization: "AWS4-HMAC-SHA256 x" } });
    expect(Object.keys(init?.headers ?? {})).toEqual(["authorization"]);
    expect(response).toMatchObject({ statusCode: 200, reason: "OK", headers: { "x-amzn-requestid": "r-1" } });
    expect(await text(response.body)).toBe("<ok/>");
  });

  it("sends a stream body whole and no body for GET", async () => {
    const fetchImpl = vi.fn<typeof fetch>(async () => new Response(null, { status: 204 }));
    const handler = createAwsFetchRequestHandler(fetchImpl);
    const base = { protocol: "http:", hostname: "127.0.0.1", port: 8080, path: "/x", headers: {} };

    await handler.handle({ ...base, method: "PUT", body: Readable.from([Buffer.from("ab"), Buffer.from("c")]) });
    await handler.handle({ ...base, method: "GET", body: "ignored" });

    expect(Buffer.from(fetchImpl.mock.calls[0][1]?.body as Uint8Array).toString()).toBe("abc");
    expect(fetchImpl.mock.calls[1][1]?.body).toBeUndefined();
    expect(fetchImpl.mock.calls[0][0]).toBe("http://127.0.0.1:8080/x");
  });

  it("carries the abort signal of the SDK", async () => {
    const fetchImpl = vi.fn<typeof fetch>(async () => new Response(""));
    const handler = createAwsFetchRequestHandler(fetchImpl);
    const controller = new AbortController();

    await handler.handle(
      { method: "GET", protocol: "http:", hostname: "localhost", path: "/", headers: {} },
      { abortSignal: controller.signal },
    );
    expect(fetchImpl.mock.calls[0][1]?.signal).toBe(controller.signal);
  });
});

describe("awsCredentialsProvider with the host's fetch", () => {
  it("asks STS for an assumed role through the given fetch", async () => {
    const actions: string[] = [];
    const server = createServer((request, response) => {
      let body = "";
      request.on("data", (chunk: Buffer) => {
        body += chunk.toString("utf8");
      });
      request.on("end", () => {
        actions.push(new URLSearchParams(body).get("Action") ?? "");
        response.setHeader("content-type", "text/xml");
        response.end(`<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>ASIAFAKEASSUMED</AccessKeyId>
      <SecretAccessKey>fake-secret</SecretAccessKey>
      <SessionToken>fake-session</SessionToken>
      <Expiration>2099-01-01T00:00:00Z</Expiration>
    </Credentials>
    <AssumedRoleUser><Arn>arn:aws:sts::123456789012:assumed-role/reader/s</Arn><AssumedRoleId>AROA:s</AssumedRoleId></AssumedRoleUser>
  </AssumeRoleResult>
  <ResponseMetadata><RequestId>r-1</RequestId></ResponseMetadata>
</AssumeRoleResponse>`);
      });
    });
    servers.push(server);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const sts = `http://127.0.0.1:${(server.address() as net.AddressInfo).port}`;
    const folder = mkdtempSync(join(tmpdir(), "kafka-aws-"));
    writeFileSync(
      join(folder, "config"),
      `[profile reader]\nrole_arn = arn:aws:iam::123456789012:role/reader\nsource_profile = base\nregion = eu-west-1\n\n[profile base]\nregion = eu-west-1\n`,
    );
    writeFileSync(
      join(folder, "credentials"),
      "[base]\naws_access_key_id = AKIAFAKEBASE\naws_secret_access_key = fake\n",
    );
    vi.stubEnv("AWS_CONFIG_FILE", join(folder, "config"));
    vi.stubEnv("AWS_SHARED_CREDENTIALS_FILE", join(folder, "credentials"));
    vi.stubEnv("AWS_ENDPOINT_URL_STS", sts);
    for (const name of ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE"]) {
      vi.stubEnv(name, undefined as unknown as string);
    }
    const fetchImpl = vi.fn<typeof fetch>((input, init) => fetch(input, init));

    const credentials = await awsCredentialsProvider("reader", fetchImpl)();

    expect(credentials).toMatchObject({ accessKeyId: "ASIAFAKEASSUMED", sessionToken: "fake-session" });
    expect(actions).toEqual(["AssumeRole"]);
    expect(fetchImpl).toHaveBeenCalledOnce();
    expect(String(fetchImpl.mock.calls[0][0])).toContain(sts);
  });
});
