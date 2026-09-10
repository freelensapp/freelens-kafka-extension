import { describe, expect, it } from "vitest";
import { resolveForwarderOptions } from "./forwarder-options";

const clusters = [
  { id: "a", contextName: "ctx-a", kubeConfigPath: "/home/u/.kube/config", isActive: false },
  { id: "b", contextName: "ctx-b", kubeConfigPath: "/home/u/clusters/prod.yaml", isActive: true },
  { id: "c", contextName: "ctx-c", kubeConfigPath: "", isActive: false },
];

describe("resolveForwarderOptions", () => {
  it("uses the kubeconfig file and context of the requested cluster", () => {
    expect(resolveForwarderOptions(clusters, { clusterId: "b" })).toEqual({
      kubeConfigPath: "/home/u/clusters/prod.yaml",
      context: "ctx-b",
    });
  });

  it("falls back to the active cluster, then to the first one", () => {
    expect(resolveForwarderOptions(clusters, {})).toEqual({
      kubeConfigPath: "/home/u/clusters/prod.yaml",
      context: "ctx-b",
    });
    expect(resolveForwarderOptions([clusters[0], clusters[2]], {})).toEqual({
      kubeConfigPath: "/home/u/.kube/config",
      context: "ctx-a",
    });
  });

  it("keeps the default kubeconfig when the catalog entry has no file path", () => {
    expect(resolveForwarderOptions(clusters, { clusterId: "c" })).toEqual({
      kubeConfigPath: undefined,
      context: "ctx-c",
    });
  });

  it("returns no options for an unknown cluster or an empty catalog", () => {
    expect(resolveForwarderOptions(clusters, { clusterId: "zzz" })).toEqual({});
    expect(resolveForwarderOptions([], {})).toEqual({});
  });

  it("lets an explicit request context bypass the catalog", () => {
    expect(resolveForwarderOptions(clusters, { clusterId: "b", context: "kind-kind" })).toEqual({
      context: "kind-kind",
    });
  });
});
