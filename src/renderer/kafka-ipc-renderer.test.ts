import { beforeEach, describe, expect, it, vi } from "vitest";

const invoke = vi.fn();

vi.mock("@freelensapp/extensions", () => ({
  Renderer: {
    Ipc: class {
      invoke = invoke;
    },
  },
}));

import { type DeleteTopicsRequest, KAFKA_IPC } from "../common/ipc";
import { KafkaIpcRenderer } from "./kafka-ipc";

const request: DeleteTopicsRequest = {
  targetId: "broker-1",
  namespace: "kafka",
  clusterName: "demo",
  topics: ["orders", "payments"],
};

function client(): KafkaIpcRenderer {
  return new KafkaIpcRenderer({} as ConstructorParameters<typeof KafkaIpcRenderer>[0]);
}

describe("KafkaIpcRenderer.deleteTopics", () => {
  beforeEach(() => {
    invoke.mockReset();
  });

  it("uses the batch IPC handler when it is available", async () => {
    invoke.mockResolvedValue({ deleted: ["orders", "payments"], failed: [] });

    await expect(client().deleteTopics(request)).resolves.toEqual({
      deleted: ["orders", "payments"],
      failed: [],
    });

    expect(invoke).toHaveBeenCalledExactlyOnceWith(KAFKA_IPC.deleteTopics, request);
  });

  it("uses the single-topic handler when an extension update has not reloaded Main", async () => {
    invoke
      .mockRejectedValueOnce(new Error("No handler registered for 'extensions@old-version:kafka:topics:delete'"))
      .mockResolvedValueOnce({ topic: "orders" })
      .mockRejectedValueOnce(new Error("Topic authorization failed"));

    await expect(client().deleteTopics(request)).resolves.toEqual({
      deleted: ["orders"],
      failed: [{ topic: "payments", error: "Topic authorization failed" }],
    });

    expect(invoke.mock.calls).toEqual([
      [KAFKA_IPC.deleteTopics, request],
      [KAFKA_IPC.deleteTopic, { ...request, topic: "orders" }],
      [KAFKA_IPC.deleteTopic, { ...request, topic: "payments" }],
    ]);
  });

  it("does not hide errors from a registered batch handler", async () => {
    const error = new Error("Kafka broker unavailable");
    invoke.mockRejectedValue(error);

    await expect(client().deleteTopics(request)).rejects.toBe(error);
    expect(invoke).toHaveBeenCalledExactlyOnceWith(KAFKA_IPC.deleteTopics, request);
  });
});
