import type { KubeObject, KubeReader } from "./kube-reader";

interface EnvVar {
  name?: string;
  value?: string;
  valueFrom?: {
    configMapKeyRef?: { name?: string; key?: string };
    secretKeyRef?: { name?: string; key?: string };
  };
}

interface EnvFromSource {
  prefix?: string;
  configMapRef?: { name?: string };
  secretRef?: { name?: string };
}

export interface WorkloadContainer {
  name?: string;
  env?: EnvVar[];
  envFrom?: EnvFromSource[];
}

/** Kafka bootstrap environment/config keys used across common client frameworks. */
export function isBootstrapKey(key: string): boolean {
  return /bootstrap[._-]?servers?$/i.test(key) || /^kafka[._-]?brokers?$/i.test(key);
}

/** App + init containers of a workload (or a Pod-shaped object). */
export function workloadContainers(workload: KubeObject): WorkloadContainer[] {
  const spec = workload.spec?.template?.spec ?? workload.spec;
  const containers = Array.isArray(spec?.containers) ? (spec.containers as WorkloadContainer[]) : [];
  const initContainers = Array.isArray(spec?.initContainers) ? (spec.initContainers as WorkloadContainer[]) : [];
  return [...containers, ...initContainers];
}

const WORKLOAD_SHORT: Record<string, string> = {
  deployment: "deploy",
  statefulset: "sts",
  daemonset: "ds",
  replicaset: "rs",
  pod: "pod",
  job: "job",
  cronjob: "cronjob",
};

export function workloadLabel(workload: KubeObject): string {
  const namespace = workload.metadata?.namespace ?? "default";
  const kind = (workload.kind ?? "workload").toLowerCase();
  return `${namespace}/${WORKLOAD_SHORT[kind] ?? kind}/${workload.metadata?.name ?? "?"}`;
}

export async function forEachConcurrent<T>(
  items: T[],
  concurrency: number,
  visit: (item: T) => Promise<void>,
): Promise<void> {
  let nextIndex = 0;
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (nextIndex < items.length) {
      const index = nextIndex++;
      await visit(items[index]);
    }
  });
  await Promise.all(workers);
}

/** Cached resolver for one discovery/credential scan. Secret values stay inside Main. */
export function createWorkloadEnvironmentResolver(reader: KubeReader): {
  resolve: (
    workload: KubeObject,
    container: WorkloadContainer,
    includeName?: (name: string) => boolean,
  ) => Promise<Record<string, string>>;
} {
  const configMaps = new Map<string, Promise<Record<string, string> | null>>();
  const secrets = new Map<string, Promise<Record<string, string> | null>>();

  const configMapData = async (namespace: string, name: string): Promise<Record<string, string> | null> => {
    const cacheKey = `${namespace}/${name}`;
    let pending = configMaps.get(cacheKey);
    if (!pending) {
      pending = reader
        .getConfigMap(namespace, name)
        .then((configMap) => configMap?.data ?? null)
        .catch(() => null);
      configMaps.set(cacheKey, pending);
    }
    return pending;
  };

  const secretData = async (namespace: string, name: string): Promise<Record<string, string> | null> => {
    const cacheKey = `${namespace}/${name}`;
    let pending = secrets.get(cacheKey);
    if (!pending) {
      pending = reader
        .getSecret(namespace, name)
        .then((secret) => {
          const raw = secret?.data;
          return raw
            ? Object.fromEntries(
                Object.entries(raw).map(([key, value]) => [key, Buffer.from(value, "base64").toString("utf8")]),
              )
            : null;
        })
        .catch(() => null);
      secrets.set(cacheKey, pending);
    }
    return pending;
  };

  return {
    async resolve(workload, container, includeName) {
      const namespace = workload.metadata?.namespace ?? "default";
      const values: Record<string, string> = {};

      for (const from of container.envFrom ?? []) {
        const prefix = from.prefix ?? "";
        const source = from.configMapRef?.name
          ? await configMapData(namespace, from.configMapRef.name)
          : from.secretRef?.name
            ? await secretData(namespace, from.secretRef.name)
            : null;
        for (const [key, value] of Object.entries(source ?? {})) {
          const name = `${prefix}${key}`;
          if (!includeName || includeName(name)) values[name] = value;
        }
      }

      // Explicit env entries override envFrom, matching Kubernetes semantics.
      for (const env of container.env ?? []) {
        if (!env.name) continue;
        if (includeName && !includeName(env.name)) continue;
        if (typeof env.value === "string") {
          values[env.name] = env.value;
          continue;
        }
        if (env.valueFrom?.configMapKeyRef?.name && env.valueFrom.configMapKeyRef.key) {
          const value = (await configMapData(namespace, env.valueFrom.configMapKeyRef.name))?.[
            env.valueFrom.configMapKeyRef.key
          ];
          if (value !== undefined) values[env.name] = value;
        } else if (env.valueFrom?.secretKeyRef?.name && env.valueFrom.secretKeyRef.key) {
          const value = (await secretData(namespace, env.valueFrom.secretKeyRef.name))?.[
            env.valueFrom.secretKeyRef.key
          ];
          if (value !== undefined) values[env.name] = value;
        }
      }

      return values;
    },
  };
}
