import { Kafka, logLevel, type SASLOptions } from "kafkajs";
import { KafkaConnection } from "./kafka-connection";
import { type Forwarder, PortForwardManager } from "./port-forward-manager";
import { createFixedSocketFactory } from "./redirect";
import type { ConnectionOptions as TlsOptions } from "node:tls";

import type { DiscoveredKafka } from "./discovery";
import type { BrokerRef } from "./types";

export interface MetadataBroker {
  nodeId: number;
  host: string;
  port: number;
}

/**
 * Match Kafka metadata brokers to their backing pods. Pure.
 * Prefers the advertised-host prefix (Strimzi per-broker DNS embeds the pod
 * name, e.g. `my-cluster-kafka-0.<svc>...`); falls back to nodeId == brokerId.
 */
export function matchBrokersToPods(
  metadataBrokers: MetadataBroker[],
  brokerPods: { brokerId: number; pod: string }[],
  namespace: string,
): BrokerRef[] {
  return metadataBrokers.map((mb) => {
    const match =
      brokerPods.find((bp) => mb.host === bp.pod || mb.host.startsWith(`${bp.pod}.`)) ??
      brokerPods.find((bp) => bp.brokerId === mb.nodeId);
    if (!match) throw new Error(`no pod found for broker nodeId=${mb.nodeId} host=${mb.host}`);
    return {
      advertisedHost: mb.host,
      advertisedPort: mb.port,
      namespace,
      pod: match.pod,
      containerPort: mb.port,
    };
  });
}

export interface ConnectDiscoveredOptions {
  clientId?: string;
  discovered: DiscoveredKafka;
  forwarder: Forwarder;
  sasl?: SASLOptions;
  ssl?: TlsOptions | boolean;
  onRedirect?: (from: string, to: string) => void;
  onPhase?: (phase: "metadata" | "connect", info: string) => void;
}

function splitHostPort(addr: string): [string, number] {
  const i = addr.lastIndexOf(":");
  return [addr.slice(0, i), Number(addr.slice(i + 1))];
}

/**
 * Two-phase connect from a {@link DiscoveredKafka}:
 * 1. port-forward one seed broker pod and read cluster metadata to learn each
 *    broker's authoritative advertised address;
 * 2. match brokers to pods, open a per-broker forward, and connect through the
 *    exact `advertised -> local` redirect.
 *
 * This avoids guessing Strimzi's per-broker DNS: Kafka itself reports it.
 */
export async function connectDiscovered(options: ConnectDiscoveredOptions): Promise<KafkaConnection> {
  const { discovered } = options;
  if (discovered.brokerPods.length === 0) {
    throw new Error(`discovered kafka "${discovered.name}" has no broker pods to port-forward`);
  }
  const seed = discovered.brokerPods[0];

  // Phase 1 — metadata via a single seed forward (everything redirected to the seed).
  const phase1 = new PortForwardManager(options.forwarder);
  let metadataBrokers: MetadataBroker[];
  try {
    const map = await phase1.open([
      {
        advertisedHost: seed.pod,
        advertisedPort: discovered.port,
        namespace: discovered.namespace,
        pod: seed.pod,
        containerPort: discovered.port,
      },
    ]);
    const local = map.get(`${seed.pod}:${discovered.port}`);
    if (!local) throw new Error("failed to open the seed port-forward");
    const [host, port] = splitHostPort(local);
    const seedKafka = new Kafka({
      clientId: options.clientId ?? "freelens-kafka-metadata",
      brokers: [`${seed.pod}:${discovered.port}`],
      ssl: options.ssl,
      sasl: options.sasl,
      logLevel: logLevel.NOTHING,
      socketFactory: createFixedSocketFactory(host, port),
      connectionTimeout: 10_000,
      retry: { retries: 5 },
    });
    const admin = seedKafka.admin();
    await admin.connect();
    try {
      const cluster = await admin.describeCluster();
      metadataBrokers = cluster.brokers.map((b) => ({
        nodeId: b.nodeId,
        host: b.host,
        port: b.port,
      }));
    } finally {
      await admin.disconnect();
    }
  } finally {
    phase1.closeAll();
  }
  options.onPhase?.("metadata", `${metadataBrokers.length} broker(s)`);

  // Phase 2 — per-broker forwards + exact redirect.
  const brokers = matchBrokersToPods(metadataBrokers, discovered.brokerPods, discovered.namespace);
  const bootstrap = `${metadataBrokers[0].host}:${metadataBrokers[0].port}`;
  options.onPhase?.("connect", `${brokers.length} broker(s), bootstrap ${bootstrap}`);
  return KafkaConnection.connect({
    clientId: options.clientId,
    brokers,
    bootstrap,
    forwarder: options.forwarder,
    sasl: options.sasl,
    ssl: options.ssl,
    onRedirect: options.onRedirect,
  });
}
