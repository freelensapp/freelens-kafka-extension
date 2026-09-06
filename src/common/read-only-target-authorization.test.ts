import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { authorizeReadOnlyTarget, selectSingleAuthorizedTarget } from "./read-only-target-authorization";

const hash = (value: string) => createHash("sha256").update(value).digest("hex");

describe("read-only target authorization", () => {
  it("requires opt-in, matching aliases and both pinned identities", () => {
    const pins = { contextSha256: hash("context-a"), targetHostSha256: hash("target.example") };
    const environment = {
      ALLOW_REAL_READ_ONLY: "1",
      KUBE_CONTEXT: "context-a",
      AUTHORIZED_KUBE_CONTEXT: "context-a",
      KAFKA_TARGET_HOST: "TARGET.EXAMPLE",
      AUTHORIZED_KAFKA_TARGET_HOST: "target.example",
    };
    expect(authorizeReadOnlyTarget(environment, pins)).toEqual({
      context: "context-a",
      targetHost: "target.example",
    });
    expect(() => authorizeReadOnlyTarget({ ...environment, KUBE_CONTEXT: "context-b" }, pins)).toThrow(
      "authorization refused",
    );
    expect(() => authorizeReadOnlyTarget({ ...environment, KAFKA_TARGET_HOST: "other.example" }, pins)).toThrow(
      "authorization refused",
    );
  });

  it("loads generic SHA-256 pins from the environment and fails closed when they are absent", () => {
    const environment = {
      ALLOW_REAL_READ_ONLY: "1",
      KUBE_CONTEXT: "context-a",
      AUTHORIZED_KUBE_CONTEXT: "context-a",
      AUTHORIZED_KUBE_CONTEXT_SHA256: hash("context-a"),
      KAFKA_TARGET_HOST: "target.example",
      AUTHORIZED_KAFKA_TARGET_HOST: "target.example",
      AUTHORIZED_KAFKA_TARGET_HOST_SHA256: hash("target.example"),
    };
    expect(authorizeReadOnlyTarget(environment)).toEqual({ context: "context-a", targetHost: "target.example" });
    const { AUTHORIZED_KUBE_CONTEXT_SHA256: _removed, ...missingContextPin } = environment;
    expect(() => authorizeReadOnlyTarget(missingContextPin)).toThrow("AUTHORIZED_KUBE_CONTEXT_SHA256");
  });

  it("requires exactly one discovered target to match the authorized host", () => {
    const candidates = [{ hosts: ["other.example"] }, { hosts: ["target.example"] }];
    const select = (values: typeof candidates) =>
      selectSingleAuthorizedTarget(values, "target.example", (candidate, host) => candidate.hosts.includes(host));
    expect(select(candidates)).toBe(candidates[1]);
    expect(() => select(candidates.slice(0, 1))).toThrow("not unique");
    expect(() => select([...candidates, { hosts: ["target.example"] }])).toThrow("not unique");
  });
});
