/** A loosely-typed Kubernetes object — enough for discovery/credential parsing and easy to fixture. */
export interface KubeObject {
  apiVersion?: string;
  kind?: string;
  metadata?: {
    name?: string;
    namespace?: string;
    labels?: Record<string, string>;
    annotations?: Record<string, string>;
  };
  spec?: any;
  status?: any;
}

export interface KubeSecret extends KubeObject {
  type?: string;
  /** base64-encoded values, as returned by the API. */
  data?: Record<string, string>;
}

export interface KubeConfigMap extends KubeObject {
  /** plain-text values, as returned by the API. */
  data?: Record<string, string>;
}

/**
 * Read-only Kubernetes access needed by discovery and credential resolution.
 * A seam: the extension reads through the host (`createCatalogKubeReader`),
 * the KinD and real-cluster itests through client-node
 * (`test/e2e/kube-reader.ts`), and unit tests through an in-memory fake.
 */
/** Identifies a custom resource type: client-node needs the `plural`, Freelens's K8s API needs the `kind`. */
export interface CustomResourceRef {
  group: string;
  version: string;
  plural: string;
  kind: string;
}

export interface KubeReader {
  listCustomResources(ref: CustomResourceRef, namespace?: string): Promise<KubeObject[]>;
  listPods(namespace: string, labelSelector?: string): Promise<KubeObject[]>;
  listServices(namespace?: string): Promise<KubeObject[]>;
  /** Deployments + StatefulSets + DaemonSets (each tagged with its `kind`), for workload-config discovery. */
  listWorkloads(namespace?: string): Promise<KubeObject[]>;
  getConfigMap(namespace: string, name: string): Promise<KubeConfigMap | null>;
  getSecret(namespace: string, name: string): Promise<KubeSecret | null>;
}
