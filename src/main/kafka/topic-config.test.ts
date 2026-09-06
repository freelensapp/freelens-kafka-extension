import { ConfigResourceTypes } from "kafkajs";
import { describe, expect, it, vi } from "vitest";
import { fetchTopicConfig } from "./topic-config";

import type { Admin } from "kafkajs";

function createAdmin(response: unknown): Admin {
  return {
    describeConfigs: vi.fn().mockResolvedValue(response),
  } as unknown as Admin;
}

describe("fetchTopicConfig", () => {
  it("maps and sorts topic config entries", async () => {
    const admin = createAdmin({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: "orders",
          errorCode: 0,
          errorMessage: "",
          configEntries: [
            {
              configName: "retention.ms",
              configValue: "604800000",
              isDefault: false,
              configSource: 1,
              isSensitive: false,
              readOnly: false,
              configSynonyms: [],
            },
            {
              configName: "cleanup.policy",
              configValue: "delete",
              isDefault: false,
              configSource: 5,
              isSensitive: false,
              readOnly: false,
              configSynonyms: [],
            },
          ],
        },
      ],
      throttleTime: 0,
    });

    await expect(fetchTopicConfig(admin, "orders")).resolves.toEqual({
      topic: "orders",
      entries: [
        {
          name: "cleanup.policy",
          value: "delete",
          source: "DEFAULT",
          readOnly: false,
          sensitive: false,
        },
        {
          name: "retention.ms",
          value: "604800000",
          source: "DYNAMIC_TOPIC_CONFIG",
          readOnly: false,
          sensitive: false,
        },
      ],
    });

    expect(admin.describeConfigs).toHaveBeenCalledWith({
      resources: [{ type: ConfigResourceTypes.TOPIC, name: "orders" }],
      includeSynonyms: false,
    });
  });

  it("masks sensitive values", async () => {
    const admin = createAdmin({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: "payments",
          errorCode: 0,
          errorMessage: "",
          configEntries: [
            {
              configName: "ssl.keystore.password",
              configValue: "secret",
              isDefault: false,
              configSource: 2,
              isSensitive: true,
              readOnly: true,
              configSynonyms: [],
            },
          ],
        },
      ],
      throttleTime: 0,
    });

    await expect(fetchTopicConfig(admin, "payments")).resolves.toEqual({
      topic: "payments",
      entries: [
        {
          name: "ssl.keystore.password",
          value: "****",
          source: "DYNAMIC_BROKER_CONFIG",
          readOnly: true,
          sensitive: true,
        },
      ],
    });
  });

  it("throws when Kafka returns a topic-level error", async () => {
    const admin = createAdmin({
      resources: [
        {
          resourceType: ConfigResourceTypes.TOPIC,
          resourceName: "missing-topic",
          errorCode: 3,
          errorMessage: "UNKNOWN_TOPIC_OR_PARTITION",
          configEntries: [],
        },
      ],
      throttleTime: 0,
    });

    await expect(fetchTopicConfig(admin, "missing-topic")).rejects.toThrow("UNKNOWN_TOPIC_OR_PARTITION");
  });
});
