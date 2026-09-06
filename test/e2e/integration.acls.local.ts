import { connectDirect } from "../../src/main/kafka/kafka-connection";

const bootstrap = process.env.KAFKA_ACL ?? "127.0.0.1:19095";
const topic = "acl-e2e-topic";

function assertLocal(value: string): void {
  const host = value.split(",")[0]?.split(":")[0];
  if (host !== "127.0.0.1" && host !== "::1") throw new Error(`ACL test refuses non-local bootstrap: ${value}`);
}

async function main(): Promise<void> {
  assertLocal(bootstrap);
  const connection = await connectDirect({ bootstrap });
  try {
    const admin = connection.admin();
    await admin.connect();
    try {
      try {
        await admin.deleteAcls({
          filters: [
            {
              resourceType: 2 as never,
              resourceName: topic,
              resourcePatternType: 3 as never,
              operation: 3 as never,
              permissionType: 3 as never,
            },
          ],
        });
      } catch {}
      await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }], waitForLeaders: true });
      await admin.createAcls({
        acl: [
          {
            resourceType: 2 as never,
            resourceName: topic,
            resourcePatternType: 3 as never,
            principal: "User:acl-e2e",
            host: "*",
            operation: 3 as never,
            permissionType: 3 as never,
          },
        ],
      });
      const created = await admin.describeAcls({
        resourceType: 1 as never,
        resourcePatternType: 1 as never,
        operation: 1 as never,
        permissionType: 1 as never,
      });
      if (
        !created.resources.some(
          (resource) =>
            resource.resourceName === topic && resource.acls.some((acl) => acl.principal === "User:acl-e2e"),
        )
      )
        throw new Error("CreateAcls result was not returned by DescribeAcls");
      await admin.deleteAcls({
        filters: [
          {
            resourceType: 2 as never,
            resourceName: topic,
            resourcePatternType: 3 as never,
            principal: "User:acl-e2e",
            host: "*",
            operation: 3 as never,
            permissionType: 3 as never,
          },
        ],
      });
      const deleted = await admin.describeAcls({
        resourceType: 1 as never,
        resourcePatternType: 1 as never,
        operation: 1 as never,
        permissionType: 1 as never,
      });
      if (
        deleted.resources.some(
          (resource) =>
            resource.resourceName === topic && resource.acls.some((acl) => acl.principal === "User:acl-e2e"),
        )
      )
        throw new Error("DeleteAcls did not remove the ACL");
    } finally {
      await admin.disconnect();
    }
  } finally {
    await connection.disconnect();
  }
}
main()
  .then(() => process.exit(0))
  .catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(1);
  });
