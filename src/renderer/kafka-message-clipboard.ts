import type { KafkaMessageBytesDto, KafkaMessageHeaderDto } from "../common/ipc";

/** The bytes as the inspector displays them: pretty JSON, the text, or the base64 preview. */
export function messageBytesText(bytes: KafkaMessageBytesDto): string {
  if (bytes.format === "null") return "null";
  if (bytes.format === "json" && !bytes.truncated && bytes.text !== undefined) {
    try {
      return JSON.stringify(JSON.parse(bytes.text), null, 2);
    } catch {
      return bytes.text;
    }
  }
  return bytes.text ?? bytes.base64 ?? "";
}

type HeaderJsonValue = string | null;

/** A header value for the JSON export: the text when UTF-8, the base64 preview for binary, null for null. */
function headerJsonValue(bytes: KafkaMessageBytesDto): HeaderJsonValue {
  if (bytes.format === "null") return null;
  return bytes.text ?? bytes.base64 ?? "";
}

/**
 * All headers of a record as a pretty-printed JSON object, paste-friendly for editors, bug
 * reports and producer tooling. A name that repeats collects its values in an array, in order.
 */
export function messageHeadersJson(headers: readonly KafkaMessageHeaderDto[]): string {
  // Null prototype: a header named `__proto__` must become an own property, not the prototype.
  const object: Record<string, HeaderJsonValue | HeaderJsonValue[]> = Object.create(null);
  for (const header of headers) {
    const value = headerJsonValue(header.value);
    if (!Object.hasOwn(object, header.name)) {
      object[header.name] = value;
      continue;
    }
    const existing = object[header.name];
    if (Array.isArray(existing)) existing.push(value);
    else object[header.name] = [existing, value];
  }
  return JSON.stringify(object, null, 2);
}

function copyWithSelection(text: string): void {
  const textarea = document.createElement("textarea");
  textarea.value = text;
  textarea.setAttribute("readonly", "");
  textarea.setAttribute("aria-hidden", "true");
  textarea.style.position = "fixed";
  textarea.style.top = "0";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  try {
    textarea.select();
    if (!document.execCommand("copy")) throw new Error("the clipboard is not available");
  } finally {
    textarea.remove();
  }
}

/**
 * Put text on the system clipboard from a click handler. The async Clipboard API is tried
 * first; a cluster page runs in an iframe whose permissions policy may refuse it, so the
 * selection-based copy command is the fallback while the click gesture is still active.
 */
export async function copyTextToClipboard(text: string): Promise<void> {
  if (navigator.clipboard) {
    try {
      await navigator.clipboard.writeText(text);
      return;
    } catch {
      // fall through to the selection-based copy
    }
  }
  copyWithSelection(text);
}
