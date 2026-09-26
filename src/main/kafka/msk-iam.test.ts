import { describe, expect, it } from "vitest";
import { createMskIamSasl, generateMskIamToken, mskIamHostname } from "./msk-iam";

const NOW = new Date("2026-09-26T10:00:00Z");
const staticCredentials = async () => ({
  accessKeyId: "AKIAEXAMPLEKEY",
  secretAccessKey: "secret",
  sessionToken: "session/token+value=",
});

function decode(token: string): URL {
  expect(token).toMatch(/^[A-Za-z0-9_-]+$/);
  return new URL(Buffer.from(token, "base64url").toString("utf8"));
}

describe("generateMskIamToken", () => {
  it("encodes a presigned kafka-cluster:Connect request for the region endpoint", async () => {
    const { token, expiresAt } = await generateMskIamToken({
      region: "eu-west-1",
      credentials: staticCredentials,
      now: NOW,
    });
    const url = decode(token);

    expect(url.origin).toBe("https://kafka.eu-west-1.amazonaws.com");
    expect(url.pathname).toBe("/");
    expect(url.searchParams.get("Action")).toBe("kafka-cluster:Connect");
    expect(url.searchParams.get("X-Amz-Algorithm")).toBe("AWS4-HMAC-SHA256");
    expect(url.searchParams.get("X-Amz-Credential")).toBe(
      "AKIAEXAMPLEKEY/20260926/eu-west-1/kafka-cluster/aws4_request",
    );
    expect(url.searchParams.get("X-Amz-Date")).toBe("20260926T100000Z");
    expect(url.searchParams.get("X-Amz-Expires")).toBe("900");
    expect(url.searchParams.get("X-Amz-Security-Token")).toBe("session/token+value=");
    expect(url.searchParams.get("X-Amz-SignedHeaders")).toBe("host");
    expect(url.searchParams.get("X-Amz-Signature")).toMatch(/^[a-f0-9]{64}$/);
    expect(url.searchParams.get("User-Agent")).toMatch(/^freelens-kafka-extension\/\d/);
    expect(expiresAt).toBe(NOW.getTime() + 900_000);
  });

  it("is deterministic for the same credentials and instant, and the query keys are sorted", async () => {
    const options = { region: "us-east-1", credentials: staticCredentials, now: NOW };
    const first = await generateMskIamToken(options);
    const second = await generateMskIamToken(options);
    expect(first.token).toBe(second.token);

    const keys = [...decode(first.token).searchParams.keys()];
    expect(keys).toEqual([...keys].sort());
  });

  it("shortens the token life to the expiry of temporary credentials", async () => {
    const { token, expiresAt } = await generateMskIamToken({
      region: "us-east-1",
      now: NOW,
      credentials: async () => ({
        accessKeyId: "AKIAEXAMPLEKEY",
        secretAccessKey: "secret",
        expiration: new Date(NOW.getTime() + 120_000),
      }),
    });
    expect(decode(token).searchParams.get("X-Amz-Expires")).toBe("120");
    expect(expiresAt).toBe(NOW.getTime() + 120_000);
  });

  it("rejects an empty region and empty credentials with a message that names the cause", async () => {
    await expect(generateMskIamToken({ region: " ", credentials: staticCredentials })).rejects.toThrow(
      "AWS region is required",
    );
    await expect(
      generateMskIamToken({
        region: "us-east-1",
        profile: "missing",
        credentials: async () => ({ accessKeyId: "", secretAccessKey: "" }),
      }),
    ).rejects.toThrow('profile "missing"');
  });

  it("derives the signing endpoint from the region only", () => {
    expect(mskIamHostname("ap-southeast-2")).toBe("kafka.ap-southeast-2.amazonaws.com");
  });
});

describe("createMskIamSasl", () => {
  it("is an OAUTHBEARER configuration that mints a token at every authentication", async () => {
    let resolutions = 0;
    const sasl = createMskIamSasl({
      region: "eu-central-1",
      credentials: async () => {
        resolutions += 1;
        return staticCredentials();
      },
    });

    expect(sasl.mechanism).toBe("oauthbearer");
    if (sasl.mechanism !== "oauthbearer") throw new Error("unreachable");
    const first = await sasl.oauthBearerProvider();
    const second = await sasl.oauthBearerProvider();
    expect(decode(first.value).origin).toBe("https://kafka.eu-central-1.amazonaws.com");
    expect(resolutions).toBe(2);
    expect(second.value).toMatch(/^[A-Za-z0-9_-]+$/);
  });
});
