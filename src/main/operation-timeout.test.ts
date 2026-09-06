import { afterEach, describe, expect, it, vi } from "vitest";
import { withFinalizer, withTimeout } from "./operation-timeout";

function deferred<T>() {
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((_resolve, next) => {
    reject = next;
  });
  return { promise, reject };
}

describe("withTimeout", () => {
  afterEach(() => vi.useRealTimers());

  it("waits for cooperative teardown before rejecting a timed-out operation", async () => {
    vi.useFakeTimers();
    const pending = deferred<never>();
    const teardown = vi.fn(async () => {
      pending.reject(new Error("cancelled"));
      await pending.promise.catch(() => undefined);
    });
    const operation = withTimeout(pending.promise, 100, "operation", teardown);
    const assertion = expect(operation).rejects.toThrow("operation timed out after 100ms");

    await vi.advanceTimersByTimeAsync(100);

    await assertion;
    expect(teardown).toHaveBeenCalledOnce();
  });

  it("runs a late operation finalizer once after the caller has timed out", async () => {
    vi.useFakeTimers();
    let resolve!: (value: string) => void;
    const pending = new Promise<string>((next) => {
      resolve = next;
    });
    const finalize = vi.fn(async () => undefined);
    const finalized = withFinalizer(pending, finalize);
    const timed = withTimeout(finalized, 100, "operation");
    const assertion = expect(timed).rejects.toThrow("operation timed out after 100ms");

    await vi.advanceTimersByTimeAsync(100);
    await assertion;
    expect(finalize).not.toHaveBeenCalled();
    resolve("done");
    await expect(finalized).resolves.toBe("done");
    expect(finalize).toHaveBeenCalledOnce();
  });

  it("does not settle the timeout until asynchronous teardown releases the resource", async () => {
    vi.useFakeTimers();
    let releaseTeardown!: () => void;
    const teardownGate = new Promise<void>((resolve) => {
      releaseTeardown = resolve;
    });
    const never = new Promise<never>(() => undefined);
    let settled = false;
    const timed = withTimeout(never, 100, "health", async () => teardownGate).finally(() => {
      settled = true;
    });
    const assertion = expect(timed).rejects.toThrow("health timed out after 100ms");

    await vi.advanceTimersByTimeAsync(100);
    expect(settled).toBe(false);
    releaseTeardown();
    await assertion;
    expect(settled).toBe(true);
  });
});
