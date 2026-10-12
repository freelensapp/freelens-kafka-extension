export function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
  onTimeout?: () => Promise<void> | void,
): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  let timedOut = false;
  const bounded = new Promise<T>((resolve, reject) => {
    promise.then(
      (value) => {
        if (!timedOut) resolve(value);
      },
      (error: unknown) => {
        if (!timedOut) reject(error);
      },
    );
    timeout = setTimeout(() => {
      timedOut = true;
      Promise.resolve(onTimeout?.()).then(
        () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
        () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
      );
    }, timeoutMs);
  });
  return bounded.finally(() => {
    if (timeout) clearTimeout(timeout);
  });
}

/**
 * Run an operation under a timeout it can beat: it gets a deadline (`Date.now()` milliseconds) that
 * falls `marginMs` before the timeout, to end by itself and say where it stopped, and a signal
 * that aborts when the timeout fires all the same, to stop what is still going on.
 */
export function withDeadline<T>(
  run: (limits: { signal: AbortSignal; deadline: number }) => Promise<T>,
  timeoutMs: number,
  marginMs: number,
  label: string,
): Promise<T> {
  const abandoned = new AbortController();
  const deadline = Date.now() + timeoutMs - marginMs;
  return withTimeout(run({ signal: abandoned.signal, deadline }), timeoutMs, label, () => abandoned.abort());
}

export function withFinalizer<T>(operation: Promise<T>, finalize: () => Promise<void> | void): Promise<T> {
  return operation.then(
    async (value) => {
      await finalize();
      return value;
    },
    async (error: unknown) => {
      await finalize();
      throw error;
    },
  );
}
