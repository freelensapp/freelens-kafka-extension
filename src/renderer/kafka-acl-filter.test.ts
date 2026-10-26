import { describe, expect, it } from "vitest";
import { ANY_ACL_FILTER, filterKafkaAcls } from "./kafka-acl-filter";

import type { KafkaAclDto } from "../common/ipc";

const acl = (principal: string, resourceType: string, operation: string) =>
  ({ principal, resourceType, operation }) as KafkaAclDto;
const ACLS = [
  acl("User:alice", "TOPIC", "READ"),
  acl("User:alice", "TOPIC", "ALL"),
  acl("User:bob", "GROUP", "ALL"),
  acl("User:bob", "CLUSTER", "DESCRIBE"),
];
const any = { principal: "", resourceType: ANY_ACL_FILTER, operation: ANY_ACL_FILTER };

describe("filterKafkaAcls", () => {
  it("keeps every ACL with no filter", () => {
    expect(filterKafkaAcls(ACLS, any)).toEqual(ACLS);
  });

  it("lists the ACLs whose operation is Kafka's ALL, and only them", () => {
    expect(filterKafkaAcls(ACLS, { ...any, operation: "ALL" })).toEqual([ACLS[1], ACLS[2]]);
  });

  it("combines the resource type, the operation and the principal, case-insensitively", () => {
    expect(filterKafkaAcls(ACLS, { principal: "ALICE", resourceType: "TOPIC", operation: ANY_ACL_FILTER })).toEqual([
      ACLS[0],
      ACLS[1],
    ]);
    expect(filterKafkaAcls(ACLS, { principal: "bob", resourceType: "GROUP", operation: "ALL" })).toEqual([ACLS[2]]);
  });
});
