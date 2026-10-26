// Failures of the host's fetch (`Main.Util.fetch`, undici): the error says "fetch failed" and keeps why in its
// `cause`, down a chain such as TypeError → SocketError → Error with a `code`.

function* chain(error: unknown): Generator<Error> {
  const seen = new Set<unknown>();
  let current = error;

  while (current instanceof Error && !seen.has(current)) {
    seen.add(current);
    yield current;
    current = current.cause;
  }
}

function codeOf(error: Error): string | undefined {
  const code = (error as { code?: unknown }).code;
  return typeof code === "string" ? code : undefined;
}

/** A connection the server closed or reset: worth one more try for a request that changes nothing. */
export function isTransientConnectionError(error: unknown): boolean {
  for (const link of chain(error)) {
    const message = link.message.toLowerCase();
    const code = codeOf(link);

    if (message.includes("socket hang up") || message.includes("other side closed")) return true;
    if (code === "ECONNRESET" || code === "UND_ERR_SOCKET") return true;
  }
  return false;
}

/** The error of a failed fetch in the words of its cause, as `node:http` said it: "connect ECONNREFUSED …". */
export function describeFetchError(error: unknown): unknown {
  if (!(error instanceof Error) || error.message !== "fetch failed") return error;
  const causes = [...chain(error)].slice(1);
  const deepest = causes[causes.length - 1];

  if (!deepest) return error;
  const code = causes.map(codeOf).find((value) => value !== undefined);
  const message = code && !deepest.message.includes(code) ? `${deepest.message} (${code})` : deepest.message;

  return new Error(message, { cause: error });
}
