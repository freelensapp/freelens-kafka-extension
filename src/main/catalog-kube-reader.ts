import { Main } from "@freelensapp/extensions";

import type { CustomResourceRef, KubeConfigMap, KubeObject, KubeReader, KubeSecret } from "./kafka/kube-reader";

/**
 * A {@link KubeReader} backed by Freelens's already-connected cluster via `Main.K8s`.
 *
 * Reads go through Freelens's cluster connection, so no raw kubeconfig is loaded — the
 * catalog's kubeconfig points at the Freelens proxy, which raw `@kubernetes/client-node`
 * cannot reach (that is the `ECONNREFUSED 127.0.0.1:<proxy>` seen otherwise).
 */
export function createCatalogKubeReader(clusterId: string): KubeReader {
  return {
    async listCustomResources(ref: CustomResourceRef, namespace) {
      // Querying a not-installed CR type returns a generic "Unknown error" (and a GET on a
      // missing CRD 404s the same way), so instead LIST the CRDs (always succeeds) and check
      // the target CRD is present — a missing operator (e.g. no Strimzi) then reads as empty.
      const crdName = `${ref.plural}.${ref.group}`;
      const crds = await Main.K8s.queryCluster<KubeObject>(clusterId, {
        apiVersion: "apiextensions.k8s.io/v1",
        kind: "CustomResourceDefinition",
      });
      if (!crds.some((crd) => crd.metadata?.name === crdName)) return [];
      return Main.K8s.queryCluster<KubeObject>(clusterId, {
        apiVersion: `${ref.group}/${ref.version}`,
        kind: ref.kind,
        namespace,
      });
    },
    async listPods(namespace, labelSelector) {
      return Main.K8s.queryCluster<KubeObject>(clusterId, {
        apiVersion: "v1",
        kind: "Pod",
        namespace,
        labelSelector,
      });
    },
    async listServices(namespace) {
      return Main.K8s.queryCluster<KubeObject>(clusterId, {
        apiVersion: "v1",
        kind: "Service",
        namespace,
      });
    },
    async listWorkloads(namespace) {
      const lists = await Promise.all(
        ["Deployment", "StatefulSet", "DaemonSet"].map(async (kind) =>
          (
            await Main.K8s.queryCluster<KubeObject>(clusterId, {
              apiVersion: "apps/v1",
              kind,
              namespace,
            })
          ).map((o) => ({ ...o, kind: o.kind ?? kind })),
        ),
      );
      return lists.flat();
    },
    async getConfigMap(namespace, name) {
      return Main.K8s.getResource<KubeConfigMap>(clusterId, {
        apiVersion: "v1",
        kind: "ConfigMap",
        namespace,
        name,
      });
    },
    async getSecret(namespace, name) {
      return Main.K8s.getResource<KubeSecret>(clusterId, {
        apiVersion: "v1",
        kind: "Secret",
        namespace,
        name,
      });
    },
  };
}
