import { AclOperationTypes, AclPermissionTypes, AclResourceTypes, ResourcePatternTypes } from "kafkajs";

import type { Admin } from "kafkajs";

import type { KafkaAclDto, KafkaAclResultDto } from "../../common/ipc";

export type KafkaAclWriteInput = KafkaAclDto;

// The KafkaJS enums are plain runtime objects, so the numeric -> name direction must be built here.
function reverseOf(enumeration: Record<string, string | number>): Map<number, string> {
  return new Map(
    Object.entries(enumeration)
      .filter((entry): entry is [string, number] => typeof entry[1] === "number")
      .map(([name, value]) => [value, name]),
  );
}

const RESOURCE_TYPE_NAMES = reverseOf(AclResourceTypes as never);
const PATTERN_TYPE_NAMES = reverseOf(ResourcePatternTypes as never);
const OPERATION_NAMES = reverseOf(AclOperationTypes as never);
const PERMISSION_TYPE_NAMES = reverseOf(AclPermissionTypes as never);

/** Kafka reports ACL fields as numeric enums; the IPC contract and the UI use their names. */
function nameOf(names: Map<number, string>, value: number, field: string): string {
  const name = names.get(value);
  if (name === undefined) throw new Error(`Unknown Kafka ${field} value ${value}`);
  return name;
}

function valueOf(enumeration: Record<string, string | number>, name: string, field: string): number {
  const value = enumeration[name];
  if (typeof value !== "number") throw new Error(`Unsupported Kafka ${field} "${name}"`);
  return value;
}

const ANY_FILTER = {
  resourceType: AclResourceTypes.ANY,
  resourcePatternType: ResourcePatternTypes.ANY,
  operation: AclOperationTypes.ANY,
  permissionType: AclPermissionTypes.ANY,
};

export function toKafkaAclFilter(acl: KafkaAclWriteInput) {
  return {
    resourceType: valueOf(AclResourceTypes as never, acl.resourceType, "resource type"),
    resourceName: acl.resourceName,
    resourcePatternType: valueOf(ResourcePatternTypes as never, acl.patternType, "pattern type"),
    principal: acl.principal,
    host: acl.host,
    operation: valueOf(AclOperationTypes as never, acl.operation, "operation"),
    permissionType: valueOf(AclPermissionTypes as never, acl.permissionType, "permission type"),
  };
}

export function fromKafkaAcl(
  resource: { resourceType: number; resourceName: string; resourcePatternType: number },
  acl: { principal: string; host: string; operation: number; permissionType: number },
): KafkaAclDto {
  return {
    resourceType: nameOf(RESOURCE_TYPE_NAMES, resource.resourceType, "resource type"),
    resourceName: resource.resourceName,
    patternType: nameOf(PATTERN_TYPE_NAMES, resource.resourcePatternType, "pattern type"),
    principal: acl.principal,
    host: acl.host,
    operation: nameOf(OPERATION_NAMES, acl.operation, "operation"),
    permissionType: nameOf(PERMISSION_TYPE_NAMES, acl.permissionType, "permission type"),
  };
}

/** Brokers with no authorizer, or without permission to read ACLs, must degrade instead of failing. */
function unavailableMessage(error: unknown): string | undefined {
  // KafkaJS carries the protocol code in `type`; its message is prose such as "Security features are disabled".
  const { type, message } = (error ?? {}) as { type?: string; message?: string };
  const text = `${type ?? ""} ${message ?? String(error)}`;
  return /CLUSTER_AUTHORIZATION_FAILED|UNSUPPORTED_VERSION|SECURITY_DISABLED|Security features are disabled/i.test(text)
    ? "ACL inspection is not available for this cluster."
    : undefined;
}

export async function fetchAcls(admin: Admin): Promise<KafkaAclResultDto> {
  await admin.connect();
  try {
    const response = await admin.describeAcls(ANY_FILTER as never);
    return {
      available: true,
      acls: response.resources.flatMap((resource) =>
        resource.acls.map((acl) => fromKafkaAcl(resource as never, acl as never)),
      ),
    };
  } catch (error) {
    const message = unavailableMessage(error);
    if (message) return { available: false, acls: [], message };
    throw error;
  } finally {
    await admin.disconnect();
  }
}

/** Non-throwing probe deciding whether the ACL page is offered at all. */
export async function probeAclSupport(admin: Admin): Promise<boolean> {
  try {
    await admin.describeAcls(ANY_FILTER as never);
    return true;
  } catch {
    return false;
  }
}

export async function createAcl(admin: Admin, acl: KafkaAclWriteInput): Promise<void> {
  const entry = toKafkaAclFilter(acl);
  await admin.connect();
  try {
    await admin.createAcls({ acl: [entry as never] });
  } finally {
    await admin.disconnect();
  }
}

export async function deleteAcl(admin: Admin, acl: KafkaAclWriteInput): Promise<void> {
  const filter = toKafkaAclFilter(acl);
  await admin.connect();
  try {
    await admin.deleteAcls({ filters: [filter as never] });
  } finally {
    await admin.disconnect();
  }
}
