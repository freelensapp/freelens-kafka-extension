import { describe, expect, it } from "vitest";
import { loadForwarderKubeConfig } from "./kube-forwarder";

function fakeKubeConfig(unreadable?: string) {
  const calls: string[] = [];
  return {
    calls,
    loadFromFile(path: string) {
      if (path === unreadable) throw new Error("ENOENT: no such file or directory");
      calls.push(`file:${path}`);
    },
    loadFromDefault() {
      calls.push("default");
    },
    setCurrentContext(context: string) {
      calls.push(`context:${context}`);
    },
  };
}

describe("loadForwarderKubeConfig", () => {
  it("loads the cluster's own kubeconfig file and selects its context", () => {
    const kc = fakeKubeConfig();
    expect(loadForwarderKubeConfig(kc, { kubeConfigPath: "/clusters/prod.yaml", context: "prod" })).toBeUndefined();
    expect(kc.calls).toEqual(["file:/clusters/prod.yaml", "context:prod"]);
  });

  it("uses the default resolution when no file is known", () => {
    const kc = fakeKubeConfig();
    expect(loadForwarderKubeConfig(kc, { context: "kind-kind" })).toBeUndefined();
    expect(loadForwarderKubeConfig(kc, {})).toBeUndefined();
    expect(kc.calls).toEqual(["default", "context:kind-kind", "default"]);
  });

  it("falls back to the default resolution when the file is not readable and reports why", () => {
    const kc = fakeKubeConfig("/gone.yaml");
    expect(loadForwarderKubeConfig(kc, { kubeConfigPath: "/gone.yaml", context: "prod" })).toBe(
      "/gone.yaml: ENOENT: no such file or directory",
    );
    expect(kc.calls).toEqual(["default", "context:prod"]);
  });
});
