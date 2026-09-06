import { describe, expect, it } from "vitest";
import { KAFKA_PAGE_IDS } from "./kafka-navigation";
import {
  KAFKA_CLUSTER_MENU_MANIFEST,
  KAFKA_CLUSTER_PAGE_MANIFEST,
  KAFKA_MENU_IDS,
  KAFKA_ROUTE_OWNER_MENU_MANIFEST,
} from "./kafka-page-manifest";

describe("Kafka page registration manifest", () => {
  it("registers functional resource pages with URL-backed Topic Workspace state", () => {
    expect(KAFKA_CLUSTER_PAGE_MANIFEST).toEqual([
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
      {
        id: KAFKA_PAGE_IDS.acls,
        params: { target: "" },
      },
    ]);
  });

  it("registers only implemented resource children", () => {
    expect(KAFKA_CLUSTER_MENU_MANIFEST).toEqual([
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
      {
        id: KAFKA_MENU_IDS.acls,
        parentId: KAFKA_MENU_IDS.root,
        title: "ACLs",
        pageId: KAFKA_PAGE_IDS.acls,
      },
    ]);
    expect(KAFKA_CLUSTER_MENU_MANIFEST.map((item) => item.title)).toEqual(expect.arrayContaining(["Schema Registry"]));
    expect(KAFKA_CLUSTER_MENU_MANIFEST.filter((menu) => menu.pageId === KAFKA_PAGE_IDS.topics)).toHaveLength(1);
  });

  it("targets only registered pages", () => {
    const pageIds = new Set(KAFKA_CLUSTER_PAGE_MANIFEST.map((page) => page.id));
    expect(KAFKA_CLUSTER_MENU_MANIFEST.every((menu) => pageIds.has(menu.pageId))).toBe(true);
  });

  it("owns nested routes outside the visible hierarchy to suppress duplicate sibling tabs", () => {
    const nestedPageIds = KAFKA_CLUSTER_MENU_MANIFEST.filter(
      (menu) => "parentId" in menu && menu.pageId !== KAFKA_PAGE_IDS.clusters,
    ).map((menu) => menu.pageId);
    expect(KAFKA_ROUTE_OWNER_MENU_MANIFEST.map((owner) => owner.pageId)).toEqual(nestedPageIds);
    expect(KAFKA_ROUTE_OWNER_MENU_MANIFEST.every((owner) => !("parentId" in owner))).toBe(true);
  });
});
