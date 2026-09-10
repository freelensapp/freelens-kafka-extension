import type { KubeForwarderOptions } from "./kafka/kube-forwarder";

/** The subset of Freelens `ClusterInfo` the port-forward needs. */
export interface ForwarderClusterInfo {
  id: string;
  contextName: string;
  /** The kubeconfig file the cluster was added to Freelens from. */
  kubeConfigPath?: string;
  isActive: boolean;
}

export interface ForwarderRequest {
  clusterId?: string;
  /** Explicit kubeconfig context (tests only): keeps the default kubeconfig resolution. */
  context?: string;
}

/**
 * The kubeconfig file and context the broker port-forward must use for a request: those of
 * the Freelens cluster the request names (or the one the user is viewing, or the first one).
 *
 * Freelens keeps one catalog entry per kubeconfig file and context, and the file may be
 * outside the default `~/.kube/config` / `$KUBECONFIG` resolution (one file per cluster is a
 * common layout), so the file path must travel with the context (#22).
 */
export function resolveForwarderOptions(
  clusters: readonly ForwarderClusterInfo[],
  request: ForwarderRequest,
): KubeForwarderOptions {
  if (request.context) return { context: request.context };
  const cluster = request.clusterId
    ? clusters.find((c) => c.id === request.clusterId)
    : (clusters.find((c) => c.isActive) ?? clusters[0]);
  if (!cluster) return {};
  return {
    kubeConfigPath: cluster.kubeConfigPath || undefined,
    context: cluster.contextName,
  };
}
