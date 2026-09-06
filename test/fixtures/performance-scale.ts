import type { KubeObject, KubeReader } from "../../src/main/kafka/kube-reader";

export interface FakePerformanceCallLog {
  listWorkloads: number;
  getConfigMap: number;
  getSecret: number;
  adminConnect: number;
  adminQueries: number;
  adminDisconnect: number;
}

export function createPerformanceScaleReader(workloadCount = 235): {
  reader: KubeReader;
  calls: FakePerformanceCallLog;
} {
  const calls: FakePerformanceCallLog = {
    listWorkloads: 0,
    getConfigMap: 0,
    getSecret: 0,
    adminConnect: 0,
    adminQueries: 0,
    adminDisconnect: 0,
  };
  const workloads: KubeObject[] = Array.from({ length: workloadCount }, (_, index) => ({
    kind: "Deployment",
    metadata: { name: `workload-${index}`, namespace: "perf" },
    spec: { template: { spec: { containers: [{ env: [{ name: "KAFKA_BOOTSTRAP_SERVERS", value: "kafka:9092" }] }] } } },
  }));

  return {
    calls,
    reader: {
      listCustomResources: async () => [],
      listPods: async () => [],
      listServices: async () => [],
      listWorkloads: async () => {
        calls.listWorkloads++;
        return workloads;
      },
      getConfigMap: async () => {
        calls.getConfigMap++;
        return null;
      },
      getSecret: async () => {
        calls.getSecret++;
        return null;
      },
    },
  };
}

export function createPerformanceAdminCallLog(): FakePerformanceCallLog {
  return {
    listWorkloads: 0,
    getConfigMap: 0,
    getSecret: 0,
    adminConnect: 0,
    adminQueries: 0,
    adminDisconnect: 0,
  };
}
