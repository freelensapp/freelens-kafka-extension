export type IpcRequest = object | undefined;

function requestWithoutOperationId(request: IpcRequest): IpcRequest {
  if (!request || Array.isArray(request)) return request;
  const { operationId: _operationId, ...parameters } = request as Record<string, unknown>;
  return parameters;
}

function stableRequestKey(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableRequestKey).join(",")}]`;
  if (value && typeof value === "object") {
    return `{${Object.entries(value)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, entry]) => `${JSON.stringify(key)}:${stableRequestKey(entry)}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

export function createIpcRequestDeduper() {
  const inFlight = new Map<string, Promise<unknown>>();

  return function dedupe<T>(channel: string, request: IpcRequest, invoke: () => Promise<T>): Promise<T> {
    const key = `${channel}:${stableRequestKey(requestWithoutOperationId(request))}`;
    const existing = inFlight.get(key);
    if (existing) return existing as Promise<T>;

    const promise = Promise.resolve()
      .then(invoke)
      .finally(() => inFlight.delete(key));
    inFlight.set(key, promise);
    return promise;
  };
}
