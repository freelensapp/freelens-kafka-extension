/** Seed a test consumer group with committed offsets for the packaged Consumer Groups E2E test. */
import { execFileSync } from "node:child_process";
import { connectDirect } from "../../src/main/kafka/kafka-connection";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";

/** Stable group ID used by the packaged E2E Consumer Groups test. */
export const DIRECT_TEST_GROUP = "freelens-orders-consumer";

interface DockerContainerInspect {
  Config?: { Image?: string; Labels?: Record<string, string> };
  HostConfig?: { PortBindings?: Record<string, Array<{ HostIp?: string; HostPort?: string }> | null> };
  State?: { Running?: boolean };
}

function requireDisposableBroker(): void {
  if (process.env.ALLOW_LOCAL_MUTATING_KAFKA_FIXTURE !== "1" || BROKER !== "127.0.0.1:19092") {
    throw new Error("Refusing consumer-group seed outside the approved loopback fixture");
  }
  const inspected = JSON.parse(
    execFileSync("docker", ["inspect", "freelens-kafka-direct"], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }),
  ) as DockerContainerInspect[];
  const container = inspected[0];
  const labels = container?.Config?.Labels ?? {};
  const bindings = container?.HostConfig?.PortBindings?.["19092/tcp"] ?? [];
  const loopback = bindings.some(({ HostIp, HostPort }) => HostIp === "127.0.0.1" && HostPort === "19092");
  if (
    !container?.State?.Running ||
    container.Config?.Image !== "apache/kafka:3.9.0" ||
    labels["com.docker.compose.project"] !== "freelens-kafka-e2e-direct" ||
    labels["com.docker.compose.service"] !== "kafka-direct" ||
    !loopback
  ) {
    throw new Error("Approved loopback fixture identity is unavailable");
  }
}

async function main(): Promise<void> {
  requireDisposableBroker();
  const connection = await connectDirect({ bootstrap: BROKER });
  try {
    const admin = connection.admin();
    await admin.connect();
    try {
      // Remove stale group from a previous run if present.
      try {
        await admin.deleteGroups([DIRECT_TEST_GROUP]);
      } catch {
        // Non-fatal: group may not exist yet.
      }
      // Commit partition 0 at offset 2 and partition 1 at offset 0 so the Offsets & Lag
      // tab has deterministic data regardless of how many records the fixture produced.
      await admin.setOffsets({
        groupId: DIRECT_TEST_GROUP,
        topic: "freelens-orders",
        partitions: [
          { partition: 0, offset: "2" },
          { partition: 1, offset: "0" },
        ],
      });
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
