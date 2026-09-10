import { describe, expect, it } from "vitest";
import { messageBytesText, messageHeadersJson } from "./kafka-message-clipboard";

import type { KafkaMessageBytesDto } from "../common/ipc";

const text = (value: string, format: KafkaMessageBytesDto["format"] = "text"): KafkaMessageBytesDto => ({
  format,
  byteLength: value.length,
  truncated: false,
  base64: Buffer.from(value).toString("base64"),
  text: value,
});
const nullBytes: KafkaMessageBytesDto = { format: "null", byteLength: 0, truncated: false };
const binary: KafkaMessageBytesDto = { format: "binary", byteLength: 3, truncated: false, base64: "/wB/" };

describe("messageBytesText", () => {
  it("pretty-prints complete JSON and keeps invalid JSON verbatim", () => {
    expect(messageBytesText(text('{"id":1,"state":"created"}', "json"))).toBe('{\n  "id": 1,\n  "state": "created"\n}');
    expect(messageBytesText(text("{oops", "json"))).toBe("{oops");
  });

  it("returns text, the base64 preview for binary and null for Kafka null", () => {
    expect(messageBytesText(text("ready for pickup"))).toBe("ready for pickup");
    expect(messageBytesText(binary)).toBe("/wB/");
    expect(messageBytesText(nullBytes)).toBe("null");
  });

  it("does not pretty-print a truncated JSON preview", () => {
    expect(messageBytesText({ ...text('{"id":1', "json"), truncated: true })).toBe('{"id":1');
  });
});

describe("messageHeadersJson", () => {
  it("builds a JSON object with one entry per header name, in order", () => {
    expect(
      JSON.parse(
        messageHeadersJson([
          { name: "contentType", value: text("application/json") },
          { name: "empty", value: nullBytes },
          { name: "checksum", value: binary },
        ]),
      ),
    ).toEqual({ contentType: "application/json", empty: null, checksum: "/wB/" });
  });

  it("collects the values of a repeated header name into an array", () => {
    expect(
      JSON.parse(
        messageHeadersJson([
          { name: "trace", value: text("first") },
          { name: "trace", value: text("second") },
          { name: "trace", value: nullBytes },
        ]),
      ),
    ).toEqual({ trace: ["first", "second", null] });
  });

  it("is pretty-printed and safe for prototype-like names", () => {
    const json = messageHeadersJson([{ name: "__proto__", value: text("x") }]);
    expect(json).toContain("\n  ");
    expect(JSON.parse(json)).toEqual(JSON.parse('{"__proto__":"x"}'));
    expect(messageHeadersJson([])).toBe("{}");
  });
});
