import { describe, expect, it } from "vitest";
import { isInternalTopic, toTopicDetail } from "./topic-metadata";

describe("toTopicDetail", () => {
  it("normalizes partition topology and health", () => {
    expect(
      toTopicDetail({
        name: "orders",
        partitions: [
          {
            partitionErrorCode: 5,
            partitionId: 2,
            leader: -1,
            replicas: [1, 2, 3],
            isr: [1, 2],
            offlineReplicas: [3],
          },
          {
            partitionErrorCode: 0,
            partitionId: 0,
            leader: 1,
            replicas: [1, 2, 3],
            isr: [1, 2, 3],
          },
        ],
      }),
    ).toEqual({
      name: "orders",
      internal: false,
      partitions: [
        {
          partitionId: 0,
          leader: 1,
          replicas: [1, 2, 3],
          isr: [1, 2, 3],
          offlineReplicas: [],
          errorCode: 0,
          underReplicated: false,
          unavailable: false,
        },
        {
          partitionId: 2,
          leader: -1,
          replicas: [1, 2, 3],
          isr: [1, 2],
          offlineReplicas: [3],
          errorCode: 5,
          underReplicated: true,
          unavailable: true,
        },
      ],
      partitionCount: 2,
      replicationFactor: 3,
      underReplicatedPartitions: 1,
      unavailablePartitions: 1,
    });
  });

  it("identifies Kafka internal topics", () => {
    expect(isInternalTopic("__consumer_offsets")).toBe(true);
    expect(isInternalTopic("orders")).toBe(false);
  });
});
