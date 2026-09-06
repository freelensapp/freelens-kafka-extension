import { describe, expect, it } from "vitest";
import {
  forgetKafkaClusterSelection,
  implementedKafkaTopicView,
  KAFKA_PAGE_IDS,
  KAFKA_RELOAD_ROUTE_KEY,
  openKafkaCluster,
  openKafkaTopic,
  readKafkaReloadRoute,
  rememberedKafkaTargetId,
  rememberKafkaClusterSelection,
  resolveKafkaClusterSelection,
  saveKafkaReloadRoute,
  switchKafkaCluster,
  updateKafkaClusterQuery,
} from "./kafka-navigation";

class MemoryStorage {
  readonly values = new Map<string, string>();

  getItem(key: string): string | null {
    return this.values.get(key) ?? null;
  }

  setItem(key: string, value: string): void {
    this.values.set(key, value);
  }

  removeItem(key: string): void {
    this.values.delete(key);
  }
}

const orders = { targetId: "kafka-1111111111111111", name: "orders" };
const payments = { targetId: "kafka-2222222222222222", name: "payments" };

describe("Kafka cluster selection", () => {
  it("requires an explicit choice for multiple clusters without valid state", () => {
    expect(resolveKafkaClusterSelection([orders, payments])).toEqual({ state: "required" });
    expect(resolveKafkaClusterSelection([orders, payments], "kafka-stale", "kafka-missing")).toEqual({
      state: "required",
    });
  });

  it("prefers a valid requested cluster, then remembered state, then one unique cluster", () => {
    expect(resolveKafkaClusterSelection([orders, payments], payments.targetId, orders.targetId)).toMatchObject({
      state: "selected",
      target: payments,
      source: "requested",
    });
    expect(resolveKafkaClusterSelection([orders, payments], "kafka-stale", orders.targetId)).toMatchObject({
      state: "selected",
      target: orders,
      source: "remembered",
    });
    expect(resolveKafkaClusterSelection([orders])).toMatchObject({
      state: "selected",
      target: orders,
      source: "single",
    });
    expect(resolveKafkaClusterSelection([])).toEqual({ state: "empty" });
  });

  it("persists only target IDs and isolates Kubernetes clusters", () => {
    const storage = new MemoryStorage();
    rememberKafkaClusterSelection(storage, "kind-kind", orders);
    rememberKafkaClusterSelection(storage, "eks-dev", payments);
    expect(rememberedKafkaTargetId(storage, "kind-kind")).toBe(orders.targetId);
    expect(rememberedKafkaTargetId(storage, "eks-dev")).toBe(payments.targetId);
    expect([...storage.values.values()][0]).not.toContain("orders");

    forgetKafkaClusterSelection(storage, "kind-kind", payments.targetId);
    expect(rememberedKafkaTargetId(storage, "kind-kind")).toBe(orders.targetId);
    forgetKafkaClusterSelection(storage, "kind-kind", orders.targetId);
    expect(rememberedKafkaTargetId(storage, "kind-kind")).toBeUndefined();
    expect(rememberedKafkaTargetId(storage, "eks-dev")).toBe(payments.targetId);
  });
});

describe("Kafka navigation targets", () => {
  it("restores reload routes only in a new renderer session", () => {
    const storage = new MemoryStorage();
    const route = {
      pageId: KAFKA_PAGE_IDS.topics,
      params: { target: orders.targetId, topic: "orders", view: "messages" },
    };
    saveKafkaReloadRoute(route, storage, "session-a", 1_000);

    expect(readKafkaReloadRoute(storage, "session-a", 1_001)).toBeUndefined();
    expect(storage.getItem(KAFKA_RELOAD_ROUTE_KEY)).toBeNull();

    saveKafkaReloadRoute(route, storage, "session-a", 2_000);
    expect(readKafkaReloadRoute(storage, "session-b", 2_001)).toEqual(route);
  });

  it("drops expired reload routes", () => {
    const storage = new MemoryStorage();
    saveKafkaReloadRoute({ pageId: KAFKA_PAGE_IDS.topics, params: {} }, storage, "session-a", 1_000);
    expect(readKafkaReloadRoute(storage, "session-b", 400_001)).toBeUndefined();
    expect(storage.getItem(KAFKA_RELOAD_ROUTE_KEY)).toBeNull();
  });

  it("opens cluster and topic workspaces with non-secret route state", () => {
    expect(openKafkaCluster(orders.targetId)).toEqual({
      pageId: KAFKA_PAGE_IDS.overview,
      params: { target: orders.targetId },
    });
    expect(openKafkaTopic(orders.targetId, "orders.events", "partitions")).toEqual({
      pageId: KAFKA_PAGE_IDS.topics,
      params: { target: orders.targetId, topic: "orders.events", view: "partitions" },
    });
  });

  it("exposes only implemented Topic Workspace views", () => {
    expect(implementedKafkaTopicView("overview")).toBe("overview");
    expect(implementedKafkaTopicView("messages")).toBe("messages");
    expect(implementedKafkaTopicView("partitions")).toBe("partitions");
    expect(implementedKafkaTopicView("configuration")).toBe("configuration");
    expect(implementedKafkaTopicView("consumers")).toBe("consumers");
    expect(implementedKafkaTopicView("not-a-view")).toBe("overview");
  });

  it("keeps resource pages but clears stale topic identity when switching cluster", () => {
    expect(switchKafkaCluster(KAFKA_PAGE_IDS.brokers, payments.targetId)).toEqual({
      pageId: KAFKA_PAGE_IDS.brokers,
      params: { target: payments.targetId },
    });
    expect(switchKafkaCluster(KAFKA_PAGE_IDS.topics, payments.targetId)).toEqual({
      pageId: KAFKA_PAGE_IDS.topics,
      params: { target: payments.targetId },
    });
  });

  it("replaces filter history while typing", () => {
    const calls: unknown[] = [];
    updateKafkaClusterQuery(
      {
        set: (...args) => calls.push(args),
      },
      "orders",
    );
    expect(calls).toEqual([["orders", { replaceHistory: true }]]);
    expect(() => updateKafkaClusterQuery(undefined, "payments")).not.toThrow();
  });
});
