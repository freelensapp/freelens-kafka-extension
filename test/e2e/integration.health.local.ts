import { connectDirect } from "../../src/main/kafka/kafka-connection";

import type { Admin } from "kafkajs";

const BROKER = process.env.KAFKA_LOCAL ?? "127.0.0.1:19092";
const KAFKA_JS_CREATE_CLUSTER_SYMBOL = "private:Kafka:createCluster";

interface ListOffsetsBroker {
  listOffsets(request: { topics: Array<{ partitions: Array<{ timestamp: number }> }> }): Promise<unknown>;
  [symbol: symbol]: unknown;
}

interface InstrumentedCluster {
  findBroker(request: { nodeId: string }): Promise<ListOffsetsBroker>;
}

const timeout = setTimeout(() => {
  process.stderr.write("cluster health integration timed out\n");
  process.exit(2);
}, 60_000);
timeout.unref();

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
  let fetchOffsetsCalls = 0;
  let fetchTopicOffsetsCalls = 0;
  let findCoordinatorBatchCalls = 0;
  let listOffsetsCalls = 0;
  let nonHighOffsetRequests = 0;
  let offsetFetchBatchCalls = 0;
  let offsetFetchBatchGroups = 0;

  privateKafka[createClusterSymbol] = (options: unknown) => {
    const cluster = originalCreateCluster(options);
    const originalFindBroker = cluster.findBroker.bind(cluster);
    const instrumentedBrokers = new WeakSet<object>();
    cluster.findBroker = async (request) => {
      const broker = await originalFindBroker(request);
      if (!instrumentedBrokers.has(broker)) {
        instrumentedBrokers.add(broker);
        let brokerPrototype: object | null = broker;
        let sendRequestSymbol: symbol | undefined;
        while (brokerPrototype && !sendRequestSymbol) {
          sendRequestSymbol = Object.getOwnPropertySymbols(brokerPrototype).find(
            (symbol) => symbol.description === "private:Broker:sendRequest",
          );
          brokerPrototype = Object.getPrototypeOf(brokerPrototype) as object | null;
        }
        if (!sendRequestSymbol) throw new Error("KafkaJS broker request adapter is unavailable");
        const originalSendRequest = broker[sendRequestSymbol] as (protocol: {
          groupIds?: string[];
          request?: { apiName?: string };
        }) => Promise<unknown>;
        broker[sendRequestSymbol] = async (protocol) => {
          if (protocol.request?.apiName === "FindCoordinator") findCoordinatorBatchCalls++;
          if (protocol.request?.apiName === "OffsetFetch") {
            offsetFetchBatchCalls++;
            offsetFetchBatchGroups += protocol.groupIds?.length ?? 0;
          }
          return originalSendRequest.call(broker, protocol);
        };
        const originalListOffsets = broker.listOffsets.bind(broker);
        broker.listOffsets = async (listOffsetsRequest) => {
          listOffsetsCalls++;
          nonHighOffsetRequests += listOffsetsRequest.topics
            .flatMap(({ partitions }) => partitions)
            .filter(({ timestamp }) => timestamp !== -1).length;
          return originalListOffsets(listOffsetsRequest);
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
          if (property === "fetchOffsets") fetchOffsetsCalls++;
          if (property === "fetchTopicOffsets") fetchTopicOffsetsCalls++;
          return (value as (...values: unknown[]) => unknown).apply(target, args);
        };
      },
    }) as Admin;
  };

  try {
    const progress: Array<{ completed?: number; phase: string; total?: number }> = [];
    const health = await connection.health(({ completed, phase, total }) => {
      progress.push({ completed, phase, total });
    });

    if (health.onlineBrokers !== 1) throw new Error(`expected one online broker, got ${health.onlineBrokers}`);
    if (!/^(?:≥)?\d+$/.test(health.consumerGroupLag)) {
      throw new Error(`expected numeric aggregate lag, got ${health.consumerGroupLag}`);
    }
    if (fetchOffsetsCalls !== 0) {
      throw new Error(`health used ${fetchOffsetsCalls} public per-group offset fallback call(s)`);
    }
    if (fetchTopicOffsetsCalls !== 0) {
      throw new Error(`cold health used ${fetchTopicOffsetsCalls} public per-topic offset fallback call(s)`);
    }
    if (listOffsetsCalls !== 1) throw new Error(`expected one broker ListOffsets batch, got ${listOffsetsCalls}`);
    if (nonHighOffsetRequests !== 0) {
      throw new Error(`health issued ${nonHighOffsetRequests} non-high ListOffsets partition request(s)`);
    }
    if (findCoordinatorBatchCalls !== 1 || offsetFetchBatchCalls !== 1 || offsetFetchBatchGroups !== 1) {
      throw new Error(
        `unexpected group batch shape: findCoordinator=${findCoordinatorBatchCalls} offsetFetch=${offsetFetchBatchCalls} groups=${offsetFetchBatchGroups}`,
      );
    }

    const topologyIndex = progress.findIndex(({ phase }) => phase === "topology");
    const groupsIndex = progress.findIndex(
      ({ completed, phase, total }) => phase === "groups" && total !== undefined && completed === total,
    );
    const watermarksIndex = progress.findIndex(
      ({ completed, phase, total }) => phase === "watermarks" && total !== undefined && completed === total,
    );
    const completeIndex = progress.findIndex(({ phase }) => phase === "complete");
    if (!(topologyIndex < groupsIndex && groupsIndex < watermarksIndex && watermarksIndex < completeIndex)) {
      throw new Error(`unexpected health progress order: ${progress.map(({ phase }) => phase).join(",")}`);
    }

    process.stdout.write(
      `HEALTH_BATCH_OK lag=${health.consumerGroupLag} findCoordinator=${findCoordinatorBatchCalls} offsetFetch=${offsetFetchBatchCalls} groups=${offsetFetchBatchGroups} publicGroupFallbacks=${fetchOffsetsCalls} listOffsets=${listOffsetsCalls} lowOffsets=${nonHighOffsetRequests} topicFallbacks=${fetchTopicOffsetsCalls}\n`,
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
