import { describe, expect, it, vi } from "vitest";
import { KafkaSessionManager } from "./session-manager";

import type { KafkaConnection } from "./kafka-connection";

function fakeConnection(): KafkaConnection & { disconnect: ReturnType<typeof vi.fn> } {
  return { disconnect: vi.fn(async () => undefined) } as unknown as KafkaConnection & {
    disconnect: ReturnType<typeof vi.fn>;
  };
}

describe("KafkaSessionManager", () => {
  it("coalesces connection setup and closes an idle session once", async () => {
    let now = 100;
    const timers: Array<() => void> = [];
    const connection = fakeConnection();
    const connect = vi.fn(async () => connection);
    const manager = new KafkaSessionManager({
      now: () => now,
      idleTimeoutMs: 50,
      setTimer: (callback) => {
        timers.push(callback);
        return timers.length as never;
      },
      clearTimer: vi.fn(),
    });

    const [first, second] = await Promise.all([manager.acquire("target", connect), manager.acquire("target", connect)]);
    expect(connect).toHaveBeenCalledTimes(1);
    expect(manager.size()).toBe(1);
    await first.release();
    await second.release();
    expect(connection.disconnect).not.toHaveBeenCalled();
    now += 50;
    timers[0]();
    expect(connection.disconnect).toHaveBeenCalledTimes(1);
  });

  it("evicts the least recently used idle session but never an active lease", async () => {
    const connections = [fakeConnection(), fakeConnection(), fakeConnection()];
    let index = 0;
    const manager = new KafkaSessionManager({ maxSessions: 2, idleTimeoutMs: 100_000 });
    const first = await manager.acquire("one", async () => connections[index++]);
    await first.release();
    const second = await manager.acquire("two", async () => connections[index++]);
    const third = await manager.acquire("three", async () => connections[index++]);
    expect(manager.size()).toBe(2);
    expect(connections[0].disconnect).toHaveBeenCalledTimes(1);
    await second.release();
    await third.release();
  });

  it("coalesces 1,000 concurrent leases for one target into one connection", async () => {
    const connection = fakeConnection();
    const connect = vi.fn(async () => connection);
    const manager = new KafkaSessionManager({
      setTimer: () => 1 as never,
      clearTimer: vi.fn(),
    });

    const leases = await Promise.all(Array.from({ length: 1_000 }, () => manager.acquire("shared", connect)));

    expect(connect).toHaveBeenCalledTimes(1);
    expect(manager.size()).toBe(1);
    await Promise.all(leases.map((lease) => lease.release()));
    expect(connection.disconnect).not.toHaveBeenCalled();
  });

  it("keeps 200 sequential targets within the configured three-session bound", async () => {
    const connections: Array<ReturnType<typeof fakeConnection>> = [];
    const manager = new KafkaSessionManager({
      maxSessions: 3,
      setTimer: () => 1 as never,
      clearTimer: vi.fn(),
    });

    for (let index = 0; index < 200; index++) {
      const connection = fakeConnection();
      connections.push(connection);
      const lease = await manager.acquire(`target-${index}`, async () => connection);
      await lease.release();
    }

    expect(manager.size()).toBe(3);
    expect(connections.filter((connection) => connection.disconnect.mock.calls.length === 1)).toHaveLength(197);
    expect(connections.slice(-3).every((connection) => connection.disconnect.mock.calls.length === 0)).toBe(true);
  });

  it("invalidates and closes a connection that finishes opening during shutdown", async () => {
    const connection = fakeConnection();
    let resolveConnect!: (connection: KafkaConnection) => void;
    const pendingConnection = new Promise<KafkaConnection>((resolve) => {
      resolveConnect = resolve;
    });
    const manager = new KafkaSessionManager();
    const acquiring = manager.acquire("target", () => pendingConnection);

    const clearing = manager.clear();
    resolveConnect(connection);

    await expect(acquiring).rejects.toThrow("invalidated while connecting");
    await clearing;
    expect(connection.disconnect).toHaveBeenCalledOnce();
    expect(manager.size()).toBe(0);
  });

  it("never evicts a connection before its concurrent acquire returns a lease", async () => {
    const firstConnection = fakeConnection();
    const secondConnection = fakeConnection();
    let resolveFirst!: (connection: KafkaConnection) => void;
    let resolveSecond!: (connection: KafkaConnection) => void;
    const firstPending = new Promise<KafkaConnection>((resolve) => {
      resolveFirst = resolve;
    });
    const secondPending = new Promise<KafkaConnection>((resolve) => {
      resolveSecond = resolve;
    });
    const manager = new KafkaSessionManager({
      maxSessions: 1,
      setTimer: () => 1 as never,
      clearTimer: vi.fn(),
    });

    const firstAcquire = manager.acquire("first", () => firstPending);
    const secondAcquire = manager.acquire("second", () => secondPending);
    resolveFirst(firstConnection);
    resolveSecond(secondConnection);
    const [first, second] = await Promise.all([firstAcquire, secondAcquire]);

    expect(firstConnection.disconnect).not.toHaveBeenCalled();
    expect(secondConnection.disconnect).not.toHaveBeenCalled();
    await first.release();
    await second.release();
    expect(manager.size()).toBe(1);
    expect(firstConnection.disconnect.mock.calls.length + secondConnection.disconnect.mock.calls.length).toBe(1);
  });
});
