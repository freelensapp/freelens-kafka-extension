// Electron rejects an invoke whose handler threw with "Error invoking remote method '<channel>': <Name>: <message>":
// the channel and the error's class name are of no use on a page.
const PREFIX = /^Error invoking remote method '[^']*': (?:[A-Za-z]*Error: )?/;

/** The error of a failed invoke, with the message main gave and nothing of Electron's wording. */
export function withoutIpcPrefix(error: unknown): unknown {
  if (!(error instanceof Error) || !PREFIX.test(error.message)) return error;
  const cleaned = new Error(error.message.replace(PREFIX, ""), { cause: error });

  cleaned.stack = error.stack;
  return cleaned;
}
