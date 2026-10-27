import type { KafkaAclDto } from "../common/ipc";

/**
 * The value of a filter that keeps every ACL. Kafka has an operation named `ALL`, so "any" cannot be spelled `ALL`:
 * that would show two `ALL` options and leave no way to list the ACLs whose operation is `ALL`.
 */
export const ANY_ACL_FILTER = "";

export interface KafkaAclFilter {
  principal: string;
  resourceType: string;
  operation: string;
}

export function filterKafkaAcls(acls: readonly KafkaAclDto[], filter: KafkaAclFilter): KafkaAclDto[] {
  const principal = filter.principal.toLowerCase();

  return acls.filter(
    (acl) =>
      (filter.resourceType === ANY_ACL_FILTER || acl.resourceType === filter.resourceType) &&
      (filter.operation === ANY_ACL_FILTER || acl.operation === filter.operation) &&
      acl.principal.toLowerCase().includes(principal),
  );
}
