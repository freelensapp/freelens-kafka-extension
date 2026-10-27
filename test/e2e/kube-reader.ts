import { AppsV1Api, CoreV1Api, CustomObjectsApi, KubeConfig } from "@kubernetes/client-node";

import type { KubeConfigMap, KubeObject, KubeReader, KubeSecret } from "../../src/main/kafka/kube-reader";

export interface KubeReaderOptions {
  /** From `Main.Catalog` `kubeConfigPath` (defaults to the standard location). */
  kubeConfigPath?: string;
  /** From `Main.Catalog` `contextName` (defaults to the current context). */
  context?: string;
}

function isNotFound(err: unknown): boolean {
  const code =
    (err as { code?: number; statusCode?: number } | null)?.code ?? (err as { statusCode?: number } | null)?.statusCode;
  return code === 404;
}

/** The {@link KubeReader} of the itests, backed by `@kubernetes/client-node` (same kubeconfig as the port-forward). */
export function createKubeReader(options: KubeReaderOptions = {}): KubeReader {
  const kc = new KubeConfig();
  if (options.kubeConfigPath) kc.loadFromFile(options.kubeConfigPath);
  else kc.loadFromDefault();
  if (options.context) kc.setCurrentContext(options.context);

  const core = kc.makeApiClient(CoreV1Api);
  const custom = kc.makeApiClient(CustomObjectsApi);
  const apps = kc.makeApiClient(AppsV1Api);

  return {
    async listCustomResources(ref, namespace) {
      const { group, version, plural } = ref;
      try {
        const res = namespace
          ? await custom.listNamespacedCustomObject({
              group,
              version,
              namespace,
              plural,
            })
          : await custom.listClusterCustomObject({ group, version, plural });
        return ((res as { items?: KubeObject[] }).items ?? []) as KubeObject[];
      } catch (err) {
        // A cluster without the CRD (e.g. no Strimzi on EKS) 404s — that reads as "none", not an error.
        if (isNotFound(err)) return [];
        throw err;
      }
    },
    async listPods(namespace, labelSelector) {
      const res = await core.listNamespacedPod({ namespace, labelSelector });
      return (res.items ?? []) as unknown as KubeObject[];
    },
    async listServices(namespace) {
      const res = namespace
        ? await core.listNamespacedService({ namespace })
        : await core.listServiceForAllNamespaces();
      return (res.items ?? []) as unknown as KubeObject[];
    },
    async listWorkloads(namespace) {
      const [deployments, statefulSets, daemonSets] = await Promise.all([
        namespace ? apps.listNamespacedDeployment({ namespace }) : apps.listDeploymentForAllNamespaces(),
        namespace ? apps.listNamespacedStatefulSet({ namespace }) : apps.listStatefulSetForAllNamespaces(),
        namespace ? apps.listNamespacedDaemonSet({ namespace }) : apps.listDaemonSetForAllNamespaces(),
      ]);
      const tag = (items: unknown[] | undefined, kind: string): KubeObject[] =>
        ((items ?? []) as KubeObject[]).map((o) => ({ ...o, kind: o.kind ?? kind }));
      return [
        ...tag(deployments.items, "Deployment"),
        ...tag(statefulSets.items, "StatefulSet"),
        ...tag(daemonSets.items, "DaemonSet"),
      ];
    },
    async getConfigMap(namespace, name) {
      try {
        const res = await core.readNamespacedConfigMap({ name, namespace });
        return res as unknown as KubeConfigMap;
      } catch (err) {
        if (isNotFound(err)) return null;
        throw err;
      }
    },
    async getSecret(namespace, name) {
      try {
        const res = await core.readNamespacedSecret({ name, namespace });
        return res as unknown as KubeSecret;
      } catch (err) {
        if (isNotFound(err)) return null;
        throw err;
      }
    },
  };
}
