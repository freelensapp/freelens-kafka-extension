import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import tls from "node:tls";
import { connectDirect } from "../../src/main/kafka/kafka-connection";

import type { SASLOptions } from "kafkajs";

async function overview(
  label: string,
  bootstrap: string,
  options: { ssl?: true | tls.ConnectionOptions; sasl?: SASLOptions },
) {
  const connection = await connectDirect({ bootstrap, ...options });
  try {
    const result = await connection.overview();
    if (result.brokers.length !== 1) throw new Error(`${label}: expected one broker, got ${result.brokers.length}`);
    console.log(`✓ ${label}: brokers=${result.brokers.length} topics=${result.topics.length}`);
  } finally {
    await connection.disconnect();
  }
}

function listen(server: net.Server | tls.Server, port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "127.0.0.1", () => resolve());
  });
}

async function main(): Promise<void> {
  await overview("PLAINTEXT no-auth", "127.0.0.1:19093", {});
  await overview("SASL/PLAIN", "127.0.0.1:19094", {
    sasl: { mechanism: "plain", username: "alice", password: "alice-secret" },
  });
  await overview("SCRAM-SHA-256", "127.0.0.1:19094", {
    sasl: { mechanism: "scram-sha-256", username: "alice", password: "alice-secret" },
  });
  await overview("SCRAM-SHA-512", "127.0.0.1:19094", {
    sasl: { mechanism: "scram-sha-512", username: "alice", password: "alice-secret" },
  });

  const directory = mkdtempSync(join(tmpdir(), "freelens-kafka-tls-"));
  try {
    const keyPath = join(directory, "key.pem");
    const certPath = join(directory, "cert.pem");
    execFileSync(
      "openssl",
      [
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-sha256",
        "-days",
        "1",
        "-subj",
        "/CN=127.0.0.1",
        "-addext",
        "subjectAltName=IP:127.0.0.1",
        "-keyout",
        keyPath,
        "-out",
        certPath,
      ],
      { stdio: "ignore" },
    );
    const cert = readFileSync(certPath, "utf8");
    const proxy = tls.createServer({ cert, key: readFileSync(keyPath) }, (client) => {
      const upstream = net.connect(19097, "127.0.0.1");
      client.pipe(upstream);
      upstream.pipe(client);
      const close = () => {
        client.destroy();
        upstream.destroy();
      };
      client.once("error", close);
      upstream.once("error", close);
    });
    await listen(proxy, 19096);
    try {
      await overview("TLS no-auth", "127.0.0.1:19096", { ssl: { ca: [cert] } });
    } finally {
      await new Promise<void>((resolve) => proxy.close(() => resolve()));
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
