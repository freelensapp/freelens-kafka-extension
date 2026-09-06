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
