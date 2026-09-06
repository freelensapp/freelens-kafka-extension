import { describe, expect, it } from "vitest";
import { parseClusterCa, parseMtls, parseScram, resolveStrimziCredentials } from "./credentials";

import type { KubeReader, KubeSecret } from "./kube-reader";

const b64 = (s: string): string => Buffer.from(s).toString("base64");

describe("credential parsers", () => {
  it("parseClusterCa reads ca.crt", () => {
    expect(parseClusterCa({ data: { "ca.crt": b64("CA-PEM") } })).toEqual({
      ca: ["CA-PEM"],
    });
  });

  it("parseScram reads the password", () => {
    expect(parseScram({ data: { password: b64("s3cr3t") } }, "my-user")).toEqual({
      mechanism: "scram-sha-512",
      username: "my-user",
      password: "s3cr3t",
    });
  });

  it("parseMtls reads cert+key (+ca)", () => {
    const secret: KubeSecret = {
      data: {
        "user.crt": b64("CERT"),
        "user.key": b64("KEY"),
        "ca.crt": b64("CA"),
      },
    };
    expect(parseMtls(secret)).toEqual({ cert: "CERT", key: "KEY", ca: ["CA"] });
  });

  it("parseMtls returns undefined without cert/key", () => {
    expect(parseMtls({ data: {} })).toBeUndefined();
  });
});

function fakeReader(secrets: Record<string, KubeSecret>): KubeReader {
  return {
    listCustomResources: async () => [],
    listPods: async () => [],
    listServices: async () => [],
    listWorkloads: async () => [],
    getConfigMap: async () => null,
    getSecret: async (namespace, name) => secrets[`${namespace}/${name}`] ?? null,
  };
}

describe("resolveStrimziCredentials", () => {
  it("resolves TLS cluster CA + SCRAM user", async () => {
    const reader = fakeReader({
      "kafka/my-cluster-cluster-ca-cert": { data: { "ca.crt": b64("CA") } },
      "kafka/my-user": { data: { password: b64("pw") } },
    });
    const creds = await resolveStrimziCredentials(reader, {
      namespace: "kafka",
      clusterName: "my-cluster",
      tls: true,
      user: "my-user",
    });
    expect(creds.ssl).toEqual({ ca: ["CA"] });
    expect(creds.sasl).toEqual({
      mechanism: "scram-sha-512",
      username: "my-user",
      password: "pw",
    });
  });

  it("merges an mTLS user cert into ssl (no sasl)", async () => {
    const reader = fakeReader({
      "kafka/my-cluster-cluster-ca-cert": { data: { "ca.crt": b64("CA") } },
      "kafka/tls-user": {
        data: { "user.crt": b64("C"), "user.key": b64("K") },
      },
    });
    const creds = await resolveStrimziCredentials(reader, {
      namespace: "kafka",
      clusterName: "my-cluster",
      tls: true,
      user: "tls-user",
    });
    expect(creds.ssl).toMatchObject({ ca: ["CA"], cert: "C", key: "K" });
    expect(creds.sasl).toBeUndefined();
  });

  it("returns empty creds for a plaintext cluster with no user", async () => {
    const creds = await resolveStrimziCredentials(fakeReader({}), {
      namespace: "kafka",
      clusterName: "my-cluster",
    });
    expect(creds).toEqual({});
  });
});
