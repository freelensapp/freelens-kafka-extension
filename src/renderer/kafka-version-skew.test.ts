import { describe, expect, it, vi } from "vitest";
import { describeVersionSkew, detectVersionSkew, KafkaVersionSkewStore } from "./kafka-version-skew";

describe("detectVersionSkew", () => {
  it("is silent when both halves run the same version", () => {
    expect(detectVersionSkew("1.3.1", { version: "1.3.1" })).toBeUndefined();
  });

  it("reports the version the main process still runs", () => {
    expect(detectVersionSkew("1.4.0", { version: "1.3.1" })).toEqual({
      rendererVersion: "1.4.0",
      mainVersion: "1.3.1",
    });
  });

  it("reports a main process that predates the handshake", () => {
    expect(detectVersionSkew("1.3.1", { missing: true })).toEqual({ rendererVersion: "1.3.1" });
  });
});

describe("describeVersionSkew", () => {
  it("names both versions", () => {
    const { title, detail } = describeVersionSkew({ rendererVersion: "1.4.0", mainVersion: "1.3.1" });
    expect(title).toBe("Restart Freelens to finish the Kafka extension update");
    expect(detail).toContain("version 1.4.0");
    expect(detail).toContain("background part of version 1.3.1");
  });

  it("does not invent a version for an older main process", () => {
    expect(describeVersionSkew({ rendererVersion: "1.3.1" }).detail).toContain("background part of a previous version");
  });
});

describe("KafkaVersionSkewStore", () => {
  it("probes once and notifies the subscribers of a skew", async () => {
    const store = new KafkaVersionSkewStore();
    const listener = vi.fn();
    store.subscribe(listener);
    const probe = vi.fn().mockResolvedValue({ missing: true });

    await store.check("1.3.1", probe);
    await store.check("1.3.1", probe);

    expect(probe).toHaveBeenCalledTimes(1);
    expect(listener).toHaveBeenCalledTimes(1);
    expect(store.get()).toEqual({ rendererVersion: "1.3.1" });
  });

  it("stays silent when the versions match", async () => {
    const store = new KafkaVersionSkewStore();
    const listener = vi.fn();
    store.subscribe(listener);

    await store.check("1.3.1", async () => ({ version: "1.3.1" }));

    expect(listener).not.toHaveBeenCalled();
    expect(store.get()).toBeUndefined();
  });

  it("shows nothing when the probe fails for another reason", async () => {
    const store = new KafkaVersionSkewStore();

    await store.check("1.3.1", () => Promise.reject(new Error("ipc channel closed")));

    expect(store.get()).toBeUndefined();
  });

  it("stops notifying an unsubscribed listener", async () => {
    const store = new KafkaVersionSkewStore();
    const listener = vi.fn();
    store.subscribe(listener)();

    await store.check("1.3.1", async () => ({ missing: true }));

    expect(listener).not.toHaveBeenCalled();
  });
});
