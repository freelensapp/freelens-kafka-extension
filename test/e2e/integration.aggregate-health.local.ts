import { KafkaAggregateHealthManager } from "../../src/main/kafka/aggregate-health-manager";
import { connectDirect } from "../../src/main/kafka/kafka-connection";

import type { Admin } from "kafkajs";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";
const TOPIC = "freelens-orders";
const KAFKA_JS_CREATE_CLUSTER_SYMBOL = "private:Kafka:createCluster";

interface InstrumentedBroker {
  [symbol: symbol]: unknown;
}

interface InstrumentedCluster {
  findBroker(request: { nodeId: string }): Promise<InstrumentedBroker>;
}

const timeout = setTimeout(() => {
  process.stderr.write("aggregate health worker integration timed out\n");
  process.exit(2);
}, 60_000);
timeout.unref();

function privateSymbol(value: object, description: string): symbol | undefined {
  let candidate: object | null = value;
  while (candidate) {
    const symbol = Object.getOwnPropertySymbols(candidate).find((item) => item.description === description);
    if (symbol) return symbol;
    candidate = Object.getPrototypeOf(candidate) as object | null;
  }
  return undefined;
}

async function main(): Promise<void> {
  const connection = await connectDirect({ bootstrap: BROKER });
  const kafka = connection.client;
  const originalAdmin = kafka.admin.bind(kafka);
  const privateKafka = kafka as unknown as Record<symbol, unknown>;
  const createClusterSymbol = Object.getOwnPropertySymbols(kafka).find(
    (symbol) => symbol.description === KAFKA_JS_CREATE_CLUSTER_SYMBOL,
  );
  if (!createClusterSymbol) throw new Error("KafkaJS private cluster factory is unavailable");
  const originalCreateCluster = privateKafka[createClusterSymbol] as (options: unknown) => InstrumentedCluster;
  let fetchOffsetsFallbacks = 0;
  let findCoordinatorRequests = 0;
  let healthLoads = 0;
  let offsetFetchRequests = 0;

  privateKafka[createClusterSymbol] = (options: unknown) => {
    const cluster = originalCreateCluster(options);
    const originalFindBroker = cluster.findBroker.bind(cluster);
    const instrumentedBrokers = new WeakSet<object>();
    cluster.findBroker = async (request) => {
      const broker = await originalFindBroker(request);
      if (!instrumentedBrokers.has(broker)) {
        instrumentedBrokers.add(broker);
        const sendRequestSymbol = privateSymbol(broker, "private:Broker:sendRequest");
        if (!sendRequestSymbol) throw new Error("KafkaJS broker request adapter is unavailable");
        const originalSendRequest = broker[sendRequestSymbol] as (protocol: {
          request?: { apiName?: string };
        }) => Promise<unknown>;
        broker[sendRequestSymbol] = async (protocol) => {
          if (protocol.request?.apiName === "FindCoordinator") findCoordinatorRequests++;
          if (protocol.request?.apiName === "OffsetFetch") offsetFetchRequests++;
          return originalSendRequest.call(broker, protocol);
        };
      }
      return broker;
    };
    return cluster;
  };

  kafka.admin = () => {
    const admin = originalAdmin();
    return new Proxy(admin, {
      get(target, property, receiver) {
        const value = Reflect.get(target, property, receiver);
        if (typeof value !== "function") return value;
        return (...args: unknown[]) => {
          if (property === "fetchOffsets") fetchOffsetsFallbacks++;
          return (value as (...values: unknown[]) => unknown).apply(target, args);
        };
      },
    }) as Admin;
  };

  try {
    const manager = new KafkaAggregateHealthManager();
    const loader = (report: Parameters<typeof connection.health>[0], signal: AbortSignal) => {
      healthLoads++;
      return connection.health(report, signal);
    };
    const [first, second, consumers] = await Promise.all([
      manager.load("target", loader),
      manager.load("target", loader),
      connection.topicConsumers(TOPIC),
    ]);

    if (JSON.stringify(first) !== JSON.stringify(second)) throw new Error("coalesced health results differ");
    if (consumers.groups.length === 0) throw new Error("Topic Consumers returned no fixture group");
    if (
      healthLoads !== 1 ||
      findCoordinatorRequests !== 1 ||
      offsetFetchRequests !== 1 ||
      fetchOffsetsFallbacks !== 0
    ) {
      throw new Error(
        `unexpected shared worker requests: health=${healthLoads} findCoordinator=${findCoordinatorRequests} offsetFetch=${offsetFetchRequests} fallback=${fetchOffsetsFallbacks}`,
      );
    }

    process.stdout.write(
      `AGGREGATE_WORKER_OK healthLoads=${healthLoads} subscribers=2 topicConsumers=1 findCoordinator=${findCoordinatorRequests} offsetFetch=${offsetFetchRequests} publicFallbacks=${fetchOffsetsFallbacks}\n`,
    );
  } finally {
    kafka.admin = originalAdmin;
    privateKafka[createClusterSymbol] = originalCreateCluster;
    await connection.disconnect();
  }
}

main()
  .then(() => process.exit(0))
  .catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(1);
  });
