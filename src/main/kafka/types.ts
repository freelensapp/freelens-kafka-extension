/**
 * A Kafka broker to reach, together with the pod that backs it.
 *
 * `advertisedHost`/`advertisedPort` are the addresses the broker announces in
 * its metadata (for in-cluster Strimzi these are internal pod DNS names that a
 * laptop cannot resolve). `namespace`/`pod`/`containerPort` identify the pod to
 * port-forward to.
 */
export interface BrokerRef {
  advertisedHost: string;
  advertisedPort: number;
  namespace: string;
  pod: string;
  containerPort: number;
}

/** A pod port to forward to. */
export interface PodPort {
  namespace: string;
  pod: string;
  port: number;
}

/** Map of advertised `"host:port"` -> reachable local `"host:port"`. */
export type AddressMap = Map<string, string>;
