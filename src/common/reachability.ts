/** Reachability + connection-strategy helpers shared by Main and Renderer (pure, no Node deps). */

/** Normalize a bootstrap (comma list, optional `scheme://`, optional port) into kafkajs `host:port`
 *  brokers. A missing port defaults to `defaultPort` — some workloads set the host without a port. Pure. */
export function splitBootstrap(bootstrap: string, defaultPort = 9092): string[] {
  return bootstrap
    .split(",")
    .map((part) => part.trim().replace(/^[a-z]+:\/\//i, ""))
    .filter(Boolean)
    .map((hostPort) => {
      const i = hostPort.lastIndexOf(":");
      if (i < 0) return `${hostPort}:${defaultPort}`;
      return Number.isFinite(Number(hostPort.slice(i + 1))) ? hostPort : `${hostPort.slice(0, i)}:${defaultPort}`;
    });
}

/** Host + port of the first entry of a (possibly comma-separated, possibly `scheme://`) bootstrap. Pure. */
export function firstBrokerAddress(bootstrap: string): { host: string; port: number } {
  const first = splitBootstrap(bootstrap)[0];
  if (!first) return { host: "", port: 9092 };
  const i = first.lastIndexOf(":");
  return { host: first.slice(0, i), port: Number(first.slice(i + 1)) };
}

/** Connection strategy for a discovered Kafka, from its source + whether this machine can reach it. Pure.
 *
 * - `portForward` — Strimzi (brokers are pods): tunnel each broker (works whether or not the PC can reach it).
 * - `direct` — reachable from this machine (public / VPN, e.g. MSK): kafkajs straight to the bootstrap.
 * - `relay` — reachable only from the cluster's pods: needs an in-cluster relay (not available yet, P3.2 i).
 */
export function chooseStrategy(source: string, pcReachable: boolean): "portForward" | "direct" | "relay" {
  if (source === "strimzi") return "portForward";
  if (pcReachable) return "direct";
  return "relay";
}
