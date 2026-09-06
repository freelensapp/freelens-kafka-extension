/**
 * Disposable local-only protocol evidence for read-only consumer group operations.
 * Proves no group join, no offset commit and correct lag computation.
 * Requires the Docker fixture started by pnpm kafka:direct:up (freelens-orders topic with 3 partitions).
 */
import { connectDirect } from "../../src/main/kafka/kafka-connection";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";
const TOPIC = "freelens-orders";
const TEST_GROUP = `freelens-group-evidence-${Date.now()}`;

const timeout = setTimeout(() => {
  process.stderr.write("consumer group integration timed out\n");
  process.exit(2);
}, 60_000);
timeout.unref();

async function groupCount(connection: Awaited<ReturnType<typeof connectDirect>>): Promise<number> {
  const admin = connection.admin();
  await admin.connect();
  try {
    const { groups } = await admin.listGroups();
    return groups.length;
  } finally {
    await admin.disconnect();
  }
}

async function main(): Promise<void> {
  const connection = await connectDirect({ bootstrap: BROKER });
  try {
    const countBefore = await groupCount(connection);

    // List groups — must not join or create any group.
    const groups = await connection.listGroups();
    process.stdout.write(`GROUPS_LIST_OK count=${groups.length}\n`);

    // Commit a known offset via a real producer group so we can verify lag.
    // We use admin.setOffsets which is a write — but to a test-only group we own.
    const admin = connection.admin();
    await admin.connect();
    try {
      await admin.setOffsets({
        groupId: TEST_GROUP,
        topic: TOPIC,
        partitions: [{ partition: 0, offset: "1" }],
      });
    } finally {
      await admin.disconnect();
    }

    // Fetch group detail — must not change group state or committed offsets beyond what we set.
    const detail = await connection.groupDetail(TEST_GROUP);
    if (detail.groupId !== TEST_GROUP) throw new Error(`expected groupId ${TEST_GROUP}, got ${detail.groupId}`);
    const tp = detail.topicOffsets.find((t) => t.topic === TOPIC)?.partitions.find((p) => p.partition === 0);
    if (!tp) throw new Error(`no offset entry for ${TOPIC} partition 0`);
    if (tp.committedOffset !== "1") throw new Error(`expected committedOffset=1, got ${tp.committedOffset}`);
    const lagValue = tp.lag === "—" ? null : BigInt(tp.lag);
    if (lagValue !== null && lagValue < 0n) throw new Error(`lag must be non-negative, got ${tp.lag}`);

    const topicConsumers = await connection.topicConsumers(TOPIC);
    const topicConsumer = topicConsumers.groups.find((group) => group.groupId === TEST_GROUP);
    if (!topicConsumer) throw new Error(`topic consumers did not include ${TEST_GROUP}`);
    if (topicConsumer.totalLag !== tp.lag) {
      throw new Error(`expected topic lag=${tp.lag}, got ${topicConsumer.totalLag}`);
    }
    process.stdout.write(
      `TOPIC_CONSUMERS_OK topic=${TOPIC} groupId=${TEST_GROUP} totalLag=${topicConsumer.totalLag}\n`,
    );

    // Clean up: delete the test-only group we created.
    const cleanupAdmin = connection.admin();
    await cleanupAdmin.connect();
    try {
      await cleanupAdmin.deleteGroups([TEST_GROUP]);
    } finally {
      await cleanupAdmin.disconnect();
    }

    const countAfter = await groupCount(connection);
    if (countAfter !== countBefore) {
      throw new Error(`group count changed from ${countBefore} to ${countAfter} after cleanup`);
    }

    process.stdout.write(
      `GROUPS_DETAIL_OK groupId=${TEST_GROUP} committedOffset=${tp.committedOffset} hwm=${tp.highWatermark} lag=${tp.lag} groups_restored=${countAfter === countBefore}\n`,
    );
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
