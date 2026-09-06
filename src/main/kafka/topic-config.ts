import { ConfigResourceTypes, type DescribeConfigResponse } from "kafkajs";
import { toConfigEntries } from "./config-entry";

import type { Admin } from "kafkajs";

import type { TopicConfigDto } from "../../common/ipc";

function findTopicResource(response: DescribeConfigResponse, topic: string) {
  return response.resources.find(
    (resource) => resource.resourceType === ConfigResourceTypes.TOPIC && resource.resourceName === topic,
  );
}

export async function fetchTopicConfig(admin: Admin, topic: string): Promise<TopicConfigDto> {
  const response = await admin.describeConfigs({
    resources: [{ type: ConfigResourceTypes.TOPIC, name: topic }],
    includeSynonyms: false,
  });

  const resource = findTopicResource(response, topic);

  if (!resource) {
    throw new Error(`Topic config for "${topic}" was not returned by Kafka`);
  }

  if (resource.errorCode !== 0) {
    throw new Error(
      resource.errorMessage || `Topic config request for "${topic}" failed with error code ${resource.errorCode}`,
    );
  }

  return {
    topic,
    entries: toConfigEntries(resource.configEntries),
  };
}
