import { describe, expect, it, vi } from "vitest";
import { createAcl, deleteAcl, fetchAcls, probeAclSupport, toKafkaAclFilter } from "./acl";

// Kafka answers DescribeAcls with numeric enums, never with their names.
const describeAclsResponse = {
  resources: [
    {
      resourceType: 2,
      resourceName: "orders",
      resourcePatternType: 3,
      acls: [{ principal: "User:ops", host: "*", operation: 3, permissionType: 3 }],
    },
  ],
};

const readAcl = {
  resourceType: "TOPIC",
  resourceName: "orders",
  patternType: "LITERAL",
  principal: "User:ops",
  host: "*",
  operation: "READ",
  permissionType: "ALLOW",
};

function admin(overrides: Record<string, unknown> = {}) {
  return {
    connect: vi.fn(),
    disconnect: vi.fn(),
    describeAcls: vi.fn().mockResolvedValue(describeAclsResponse),
    createAcls: vi.fn().mockResolvedValue(undefined),
    deleteAcls: vi.fn().mockResolvedValue(undefined),
    ...overrides,
  };
}

describe("ACL enum mapping", () => {
  it("converts rule names to the numeric enums Kafka expects", () => {
    expect(toKafkaAclFilter(readAcl)).toEqual({
      resourceType: 2,
      resourceName: "orders",
      resourcePatternType: 3,
      principal: "User:ops",
      host: "*",
      operation: 3,
      permissionType: 3,
    });
  });

  it("rejects values Kafka would silently misinterpret", () => {
    expect(() => toKafkaAclFilter({ ...readAcl, operation: "SUDO" })).toThrow(/Unsupported Kafka operation "SUDO"/);
    expect(() => toKafkaAclFilter({ ...readAcl, resourceType: "TOPICS" })).toThrow(/resource type/);
  });
});

describe("fetchAcls", () => {
  it("maps numeric Kafka enums back to names and disconnects", async () => {
    const value = admin();
    await expect(fetchAcls(value as never)).resolves.toEqual({ available: true, acls: [readAcl] });
    expect(value.connect).toHaveBeenCalledOnce();
    expect(value.disconnect).toHaveBeenCalledOnce();
  });

  it("degrades gracefully when the broker refuses or does not support ACL inspection", async () => {
    for (const reason of ["CLUSTER_AUTHORIZATION_FAILED", "UNSUPPORTED_VERSION", "SECURITY_DISABLED"]) {
      const value = admin({ describeAcls: vi.fn().mockRejectedValue(new Error(reason)) });
      await expect(fetchAcls(value as never)).resolves.toEqual({
        available: false,
        acls: [],
        message: "ACL inspection is not available for this cluster.",
      });
      expect(value.disconnect).toHaveBeenCalledOnce();
    }
  });

  it("degrades on a real KafkaJS protocol error, whose message omits the code", async () => {
    const protocolError = Object.assign(new Error("Security features are disabled"), { type: "SECURITY_DISABLED" });
    const value = admin({ describeAcls: vi.fn().mockRejectedValue(protocolError) });
    await expect(fetchAcls(value as never)).resolves.toEqual({
      available: false,
      acls: [],
      message: "ACL inspection is not available for this cluster.",
    });
  });

  it("propagates unrelated failures instead of hiding them", async () => {
    const value = admin({ describeAcls: vi.fn().mockRejectedValue(new Error("ECONNREFUSED")) });
    await expect(fetchAcls(value as never)).rejects.toThrow("ECONNREFUSED");
    expect(value.disconnect).toHaveBeenCalledOnce();
  });
});

describe("probeAclSupport", () => {
  it("reports availability without throwing", async () => {
    await expect(probeAclSupport(admin() as never)).resolves.toBe(true);
    await expect(
      probeAclSupport(admin({ describeAcls: vi.fn().mockRejectedValue(new Error("SECURITY_DISABLED")) }) as never),
    ).resolves.toBe(false);
  });
});

describe("ACL writes", () => {
  it("sends numeric enums to CreateAcls and disconnects", async () => {
    const value = admin();
    await createAcl(value as never, readAcl);
    expect(value.createAcls).toHaveBeenCalledWith({
      acl: [
        {
          resourceType: 2,
          resourceName: "orders",
          resourcePatternType: 3,
          principal: "User:ops",
          host: "*",
          operation: 3,
          permissionType: 3,
        },
      ],
    });
    expect(value.disconnect).toHaveBeenCalledOnce();
  });

  it("sends numeric enums to DeleteAcls and disconnects", async () => {
    const value = admin();
    await deleteAcl(value as never, readAcl);
    expect(value.deleteAcls).toHaveBeenCalledWith({
      filters: [
        {
          resourceType: 2,
          resourceName: "orders",
          resourcePatternType: 3,
          principal: "User:ops",
          host: "*",
          operation: 3,
          permissionType: 3,
        },
      ],
    });
    expect(value.disconnect).toHaveBeenCalledOnce();
  });

  it("never opens a connection for a rule Kafka would reject", async () => {
    const value = admin();
    await expect(createAcl(value as never, { ...readAcl, permissionType: "MAYBE" })).rejects.toThrow(/permission type/);
    expect(value.connect).not.toHaveBeenCalled();
  });
});
