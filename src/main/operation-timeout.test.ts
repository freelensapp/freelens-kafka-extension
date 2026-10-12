import { afterEach, describe, expect, it, vi } from "vitest";
import { withDeadline, withFinalizer, withTimeout } from "./operation-timeout";

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

describe("withDeadline", () => {
  afterEach(() => vi.useRealTimers());

  it("gives the operation a deadline that falls the margin before the timeout", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1_000_000);
    const limits: Array<{ signal: AbortSignal; deadline: number }> = [];

    await expect(
      withDeadline(
        async (given) => {
          limits.push(given);
          return "sizes";
        },
        30_000,
        3_000,
        "Topic sizes",
      ),
    ).resolves.toBe("sizes");

    expect(limits.map(({ deadline }) => deadline)).toEqual([1_027_000]);
    expect(limits[0].signal.aborted).toBe(false);
  });

  it("lets an operation that ends at its deadline report its own failure", async () => {
    vi.useFakeTimers();
    const operation = withDeadline(
      ({ deadline }) =>
        new Promise<never>((_resolve, reject) => {
          setTimeout(() => reject(new Error("reading the cluster metadata: no answer")), deadline - Date.now());
        }),
      30_000,
      3_000,
      "Topic sizes",
    );
    const assertion = expect(operation).rejects.toThrow("reading the cluster metadata: no answer");

    await vi.advanceTimersByTimeAsync(27_000);

    await assertion;
  });

  it("aborts the signal of an operation that is still going on at the timeout", async () => {
    vi.useFakeTimers();
    let signal: AbortSignal | undefined;
    const operation = withDeadline(
      (given) => {
        signal = given.signal;
        return new Promise<never>(() => undefined);
      },
      30_000,
      3_000,
      "Topic sizes",
    );
    const assertion = expect(operation).rejects.toThrow("Topic sizes timed out after 30000ms");

    await vi.advanceTimersByTimeAsync(29_999);
    expect(signal?.aborted).toBe(false);
    await vi.advanceTimersByTimeAsync(1);

    await assertion;
    expect(signal?.aborted).toBe(true);
  });
});
