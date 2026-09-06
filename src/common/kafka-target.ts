import { splitBootstrap } from "./reachability";

function hashIdentity(value: string): string {
  let first = 0x811c9dc5;
  let second = 0x9e3779b9;

  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    first = Math.imul(first ^ code, 0x01000193);
    second = Math.imul(second ^ code, 0x85ebca6b);
  }

  return [first, second].map((part) => (part >>> 0).toString(16).padStart(8, "0")).join("");
}

export function canonicalKafkaBootstrap(bootstrap: string): string {
  const brokers = splitBootstrap(bootstrap).map((broker) => {
    const separator = broker.lastIndexOf(":");
    const host = broker.slice(0, separator).trim().toLowerCase().replace(/\.$/, "");
    const port = broker.slice(separator + 1);
    return `${host}:${port}`;
  });

  return [...new Set(brokers)].sort().join(",");
}

export function createKafkaTargetId(bootstrap: string): string {
  const identity = canonicalKafkaBootstrap(bootstrap);
  if (!identity) throw new Error("Kafka bootstrap is required to create a target ID");
  return `kafka-${hashIdentity(identity)}`;
}
