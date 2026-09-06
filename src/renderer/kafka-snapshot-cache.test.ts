import { describe, expect, it, vi } from "vitest";
import { KafkaSnapshotCache } from "./kafka-snapshot-cache";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

describe("KafkaSnapshotCache", () => {
  it("returns stale data while coalescing one background revalidation", async () => {
    let now = 1_000;
    const cache = new KafkaSnapshotCache<string>({ now: () => now, ttlMs: 100, maxEntries: 5 });
    await cache.load("overview", async () => "first");
    now += 101;
    const pending = deferred<string>();
    const loader = vi.fn(() => pending.promise);
    const first = cache.load("overview", loader);
    const second = cache.load("overview", loader);

    await Promise.resolve();
    expect(cache.read("overview")).toMatchObject({ data: "first", stale: true, loading: true });
    expect(loader).toHaveBeenCalledTimes(1);
    pending.resolve("second");
    await expect(Promise.all([first, second])).resolves.toEqual(["second", "second"]);
    expect(cache.read("overview")).toMatchObject({ data: "second", fresh: true, stale: false });
  });

  it("retains stale data and exposes refresh errors", async () => {
    let now = 1_000;
    const cache = new KafkaSnapshotCache<string>({ now: () => now, ttlMs: 100, maxEntries: 5 });
    await cache.load("overview", async () => "kept");
    now += 101;
    await expect(cache.load("overview", async () => Promise.reject(new Error("offline")))).rejects.toThrow("offline");
    expect(cache.read("overview")).toMatchObject({ data: "kept", stale: true, loading: false, error: "offline" });
  });

  it("starts a replacement loader after invalidating an in-flight generation", async () => {
    const cache = new KafkaSnapshotCache<string>({ ttlMs: 100 });
    let resolveFirst!: (value: string) => void;
    const firstPending = new Promise<string>((resolve) => {
      resolveFirst = resolve;
    });
    const first = cache.load("target", () => firstPending);
    cache.invalidate("target");
    const secondLoader = vi.fn(async () => "second");

    await expect(cache.load("target", secondLoader)).resolves.toBe("second");
    resolveFirst("first");
    await expect(first).resolves.toBe("first");

    expect(secondLoader).toHaveBeenCalledOnce();
    expect(cache.read("target")).toMatchObject({ data: "second", loading: false });
  });

  it("evicts the least recently used idle entry at the configured bound", async () => {
    let now = 1_000;
    const cache = new KafkaSnapshotCache<string>({ now: () => now, ttlMs: 10_000, maxEntries: 2 });
    await cache.load("one", async () => "one");
    now += 1;
    await cache.load("two", async () => "two");
    now += 1;
    cache.read("one");
    now += 1;
    await cache.load("three", async () => "three");
    expect(cache.size()).toBe(2);
    expect(cache.read("one").data).toBe("one");
    expect(cache.read("two").data).toBeUndefined();
  });

  it("stays at its configured memory bound after 1,000 resource snapshots", async () => {
    let now = 1_000;
    const cache = new KafkaSnapshotCache<string>({ now: () => now++, ttlMs: 10_000, maxEntries: 100 });

    for (let index = 0; index < 1_000; index++) {
      await cache.load(`resource-${index}`, async () => `value-${index}`);
    }

    expect(cache.size()).toBe(100);
    expect(cache.read("resource-0").data).toBeUndefined();
    expect(cache.read("resource-999").data).toBe("value-999");
  });
});
