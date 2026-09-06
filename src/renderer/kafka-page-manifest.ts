import { KAFKA_PAGE_IDS } from "./kafka-navigation";

export const KAFKA_MENU_IDS = {
  root: "kafka",
  clusters: "kafka-clusters",
  overview: "kafka-overview",
  topics: "kafka-topics",
  groups: "kafka-groups",
  brokers: "kafka-brokers",
  schemas: "kafka-schema-registry",
  connect: "kafka-connect",
  acls: "kafka-acls",
} as const;

export const KAFKA_ROUTE_OWNER_MENU_MANIFEST = [
  { id: "kafka-overview-route-owner", pageId: KAFKA_PAGE_IDS.overview },
  { id: "kafka-topics-route-owner", pageId: KAFKA_PAGE_IDS.topics },
  { id: "kafka-groups-route-owner", pageId: KAFKA_PAGE_IDS.groups },
  { id: "kafka-brokers-route-owner", pageId: KAFKA_PAGE_IDS.brokers },
  { id: "kafka-schema-registry-route-owner", pageId: KAFKA_PAGE_IDS.schemas },
  { id: "kafka-connect-route-owner", pageId: KAFKA_PAGE_IDS.connect },
  { id: "kafka-acls-route-owner", pageId: KAFKA_PAGE_IDS.acls },
] as const;

export const KAFKA_CLUSTER_PAGE_MANIFEST = [
  {
    id: KAFKA_PAGE_IDS.clusters,
    params: { query: "" },
  },
  {
    id: KAFKA_PAGE_IDS.overview,
    params: { target: "" },
  },
  {
    id: KAFKA_PAGE_IDS.topics,
    params: {
      target: "",
      query: "",
      topic: "",
      view: "overview",
      keyFilter: "",
      valueFilter: "",
      headerKey: "",
      headerValue: "",
      timestamp: "",
    },
  },
  {
    id: KAFKA_PAGE_IDS.groups,
    params: { target: "", query: "", group: "", view: "offsets" },
  },
  {
    id: KAFKA_PAGE_IDS.brokers,
    params: { target: "", broker: "" },
  },
  {
    id: KAFKA_PAGE_IDS.schemas,
    params: { target: "", query: "", subject: "" },
  },
  {
    id: KAFKA_PAGE_IDS.connect,
    params: { target: "", query: "", connector: "" },
  },
  { id: KAFKA_PAGE_IDS.acls, params: { target: "" } },
] as const;

export const KAFKA_CLUSTER_MENU_MANIFEST = [
  {
    id: KAFKA_MENU_IDS.root,
    title: "Apache Kafka",
    pageId: KAFKA_PAGE_IDS.clusters,
  },
  {
    id: KAFKA_MENU_IDS.clusters,
    parentId: KAFKA_MENU_IDS.root,
    title: "Clusters",
    pageId: KAFKA_PAGE_IDS.clusters,
  },
  {
    id: KAFKA_MENU_IDS.overview,
    parentId: KAFKA_MENU_IDS.root,
    title: "Overview",
    pageId: KAFKA_PAGE_IDS.overview,
  },
  {
    id: KAFKA_MENU_IDS.topics,
    parentId: KAFKA_MENU_IDS.root,
    title: "Topics",
    pageId: KAFKA_PAGE_IDS.topics,
  },
  {
    id: KAFKA_MENU_IDS.groups,
    parentId: KAFKA_MENU_IDS.root,
    title: "Consumer Groups",
    pageId: KAFKA_PAGE_IDS.groups,
  },
  {
    id: KAFKA_MENU_IDS.brokers,
    parentId: KAFKA_MENU_IDS.root,
    title: "Brokers",
    pageId: KAFKA_PAGE_IDS.brokers,
  },
  {
    id: KAFKA_MENU_IDS.schemas,
    parentId: KAFKA_MENU_IDS.root,
    title: "Schema Registry",
    pageId: KAFKA_PAGE_IDS.schemas,
  },
  {
    id: KAFKA_MENU_IDS.connect,
    parentId: KAFKA_MENU_IDS.root,
    title: "Kafka Connect",
    pageId: KAFKA_PAGE_IDS.connect,
  },
  { id: KAFKA_MENU_IDS.acls, parentId: KAFKA_MENU_IDS.root, title: "ACLs", pageId: KAFKA_PAGE_IDS.acls },
] as const;
