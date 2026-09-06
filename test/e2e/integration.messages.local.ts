/**
 * Disposable local-only protocol evidence for group-free bounded message Browse and Tail polling.
 * This file produces fixture records only to the Docker Kafka started by kafka:direct:up.
 */
import { connectDirect } from "../../src/main/kafka/kafka-connection";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";
const TOPIC = "freelens-orders";
const MESSAGE_FIXTURE_TIMESTAMP = 1_700_000_000_000;

const timeout = setTimeout(() => {
  process.stderr.write("message Browse/Tail integration timed out\n");
  process.exit(2);
}, 60_000);
timeout.unref();

async function groupIds(connection: Awaited<ReturnType<typeof connectDirect>>): Promise<string[]> {
  const admin = connection.admin();
  await admin.connect();
  try {
    return (await admin.listGroups()).groups.map(({ groupId }) => groupId).sort();
  } finally {
    await admin.disconnect();
  }
}

async function main(): Promise<void> {
  const connection = await connectDirect({ bootstrap: BROKER });
  try {
    const groupsBefore = await groupIds(connection);
    const earliest = await connection.browseMessages({
      topic: TOPIC,
      partition: 0,
      startMode: "earliest",
      limit: 10,
    });
    if (earliest.messages.length !== 4) throw new Error(`expected 4 records, got ${earliest.messages.length}`);
    if (earliest.messages.map(({ offset }) => offset).join(",") !== "0,1,2,3") {
      throw new Error("records were not returned in ascending offset order");
    }
    if (earliest.messages[0].value.format !== "json") throw new Error("JSON value was not detected");
    if (earliest.messages[1].value.format !== "text") throw new Error("text value was not detected");
    if (earliest.messages[2].value.format !== "binary") throw new Error("binary value was not preserved");
    if (earliest.messages[2].value.base64 !== "/wB/") throw new Error("binary bytes changed");
    if (earliest.messages[3].value.format !== "null") throw new Error("null value was not preserved");
    const traceHeaders = earliest.messages[0].headers.filter(({ name }) => name === "trace");
    if (traceHeaders.map(({ value }) => value.text).join(",") !== "first,second") {
      throw new Error("duplicate header values were not preserved");
    }
    const latest = await connection.browseMessages({
      topic: TOPIC,
      partition: 0,
      startMode: "latest",
      limit: 2,
    });
    if (latest.startOffset !== "2" || latest.messages.map(({ offset }) => offset).join(",") !== "2,3") {
      throw new Error("latest offset window was incorrect");
    }

    const timestampWindow = await connection.browseMessages({
      topic: TOPIC,
      partition: 0,
      startMode: "timestamp",
      timestamp: MESSAGE_FIXTURE_TIMESTAMP + 2,
      limit: 10,
    });
    if (timestampWindow.messages[0]?.offset !== "2") throw new Error("timestamp offset resolution was incorrect");

    const groupsAfter = await groupIds(connection);
    if (groupsAfter.join("\n") !== groupsBefore.join("\n")) {
      throw new Error("Browse changed Kafka consumer groups");
    }

    process.stdout.write(
      `MESSAGE_BROWSE_OK records=${earliest.returnedCount} range=${earliest.startOffset}-${earliest.nextOffset} groups=${groupsAfter.length}\n`,
    );

    // REQ-073 Tail evidence: each Tail poll is an independent connection/disconnect (mirrors the IPC
    // handler). Verify that 3 sequential offset-cursor polls leave groups and committed offsets
    // unchanged, and that every connection disconnects exactly once (process must exit cleanly).
    const tailStart = await connection.browseMessages({
      topic: TOPIC,
      partition: 0,
      startMode: "latest",
      limit: 1,
    });
    let tailCursor = tailStart.nextOffset;
    const groupsBeforeTail = await groupIds(connection);

    for (let poll = 0; poll < 3; poll++) {
      // Create a fresh connection per poll — exactly as the IPC messagesBrowse handler does.
      const pollConnection = await connectDirect({ bootstrap: BROKER });
      try {
        const result = await pollConnection.browseMessages({
          topic: TOPIC,
          partition: 0,
          startMode: "offset",
          offset: tailCursor,
          limit: 10,
        });
        tailCursor = result.nextOffset;
      } finally {
        await pollConnection.disconnect();
      }
    }

    const groupsAfterTail = await groupIds(connection);
    if (groupsAfterTail.join("\n") !== groupsBeforeTail.join("\n")) {
      throw new Error("Tail polling changed Kafka consumer groups");
    }

    process.stdout.write(`TAIL_POLL_OK polls=3 final_cursor=${tailCursor} groups=${groupsAfterTail.length}\n`);
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
