import { ConfigResourceTypes, type DescribeConfigResponse } from "kafkajs";
import { toConfigEntries } from "./config-entry";

import type { Admin } from "kafkajs";

import type { BrokerConfigDto } from "../../common/ipc";

function findBrokerResource(response: DescribeConfigResponse, brokerId: number) {
  const name = String(brokerId);
  return response.resources.find(
    (resource) => resource.resourceType === ConfigResourceTypes.BROKER && resource.resourceName === name,
  );
}

export async function fetchBrokerConfig(admin: Admin, brokerId: number): Promise<BrokerConfigDto> {
  const response = await admin.describeConfigs({
    resources: [{ type: ConfigResourceTypes.BROKER, name: String(brokerId) }],
    includeSynonyms: false,
  });
  const resource = findBrokerResource(response, brokerId);

  if (!resource) {
    throw new Error(`Broker config for "${brokerId}" was not returned by Kafka`);
  }
  if (resource.errorCode !== 0) {
    throw new Error(
      resource.errorMessage || `Broker config request for "${brokerId}" failed with error code ${resource.errorCode}`,
    );
  }

  return {
    brokerId,
    entries: toConfigEntries(resource.configEntries),
  };
}
