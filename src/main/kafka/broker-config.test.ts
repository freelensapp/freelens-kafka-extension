import { ConfigResourceTypes } from "kafkajs";
import { describe, expect, it, vi } from "vitest";
import { fetchBrokerConfig } from "./broker-config";

import type { Admin } from "kafkajs";

function createAdmin(response: unknown): Admin {
  return { describeConfigs: vi.fn().mockResolvedValue(response) } as unknown as Admin;
}

describe("fetchBrokerConfig", () => {
  it("maps, masks and sorts broker config entries", async () => {
    const admin = createAdmin({
      resources: [
        {
          resourceType: ConfigResourceTypes.BROKER,
          resourceName: "2",
          errorCode: 0,
          errorMessage: "",
          configEntries: [
            {
              configName: "num.partitions",
              configValue: "3",
              isDefault: false,
              configSource: 4,
              isSensitive: false,
              readOnly: false,
              configSynonyms: [],
            },
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

    await expect(fetchBrokerConfig(admin, 2)).resolves.toEqual({
      brokerId: 2,
      entries: [
        {
          name: "num.partitions",
          value: "3",
          source: "STATIC_BROKER_CONFIG",
          readOnly: false,
          sensitive: false,
        },
        {
          name: "ssl.keystore.password",
          value: "****",
          source: "DYNAMIC_BROKER_CONFIG",
          readOnly: true,
          sensitive: true,
        },
      ],
    });
    expect(admin.describeConfigs).toHaveBeenCalledWith({
      resources: [{ type: ConfigResourceTypes.BROKER, name: "2" }],
      includeSynonyms: false,
    });
  });

  it("throws when Kafka returns a broker-level error", async () => {
    const admin = createAdmin({
      resources: [
        {
          resourceType: ConfigResourceTypes.BROKER,
          resourceName: "9",
          errorCode: 42,
          errorMessage: "INVALID_REQUEST",
          configEntries: [],
        },
      ],
      throttleTime: 0,
    });

    await expect(fetchBrokerConfig(admin, 9)).rejects.toThrow("INVALID_REQUEST");
  });

  it("preserves an empty config response for the workspace empty state", async () => {
    const admin = createAdmin({
      resources: [
        {
          resourceType: ConfigResourceTypes.BROKER,
          resourceName: "1",
          errorCode: 0,
          errorMessage: "",
          configEntries: [],
        },
      ],
      throttleTime: 0,
    });

    await expect(fetchBrokerConfig(admin, 1)).resolves.toEqual({ brokerId: 1, entries: [] });
  });
});
