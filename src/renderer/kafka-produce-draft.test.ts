import { describe, expect, it } from "vitest";
import { describeInvalidHeaderLines, parseProduceHeaders, parseProducePartition } from "./kafka-produce-draft";

describe("parseProduceHeaders", () => {
  it("reads one header per line", () => {
    expect(parseProduceHeaders("source=freelens\ntrace-id=42")).toEqual({
      headers: { source: "freelens", "trace-id": "42" },
      invalidLines: [],
    });
  });

  it("keeps every character after the first equals sign", () => {
    expect(parseProduceHeaders("signature=YWJjZA==\nquery=a=1&b=2").headers).toEqual({
      signature: "YWJjZA==",
      query: "a=1&b=2",
    });
  });

  it("skips blank lines, trims keys and accepts Windows line endings", () => {
    expect(parseProduceHeaders("\r\n  source  =freelens\r\n\r\n")).toEqual({
      headers: { source: "freelens" },
      invalidLines: [],
    });
  });

  it("accepts an empty value and keeps the last value of a repeated key", () => {
    expect(parseProduceHeaders("flag=\nretry=1\nretry=2").headers).toEqual({ flag: "", retry: "2" });
  });

  it("reports the lines without a key or without a separator", () => {
    expect(parseProduceHeaders("source=freelens\nno-separator\n=value-only\n\ntrace=1")).toEqual({
      headers: { source: "freelens", trace: "1" },
      invalidLines: [2, 3],
    });
  });

  it("returns no header for an empty field", () => {
    expect(parseProduceHeaders("")).toEqual({ headers: {}, invalidLines: [] });
  });
});

describe("parseProducePartition", () => {
  it("leaves the partition to the default partitioner when the field is empty", () => {
    expect(parseProducePartition("")).toEqual({});
    expect(parseProducePartition("   ", 3)).toEqual({});
  });

  it("accepts a partition of the topic", () => {
    expect(parseProducePartition("0", 3)).toEqual({ partition: 0 });
    expect(parseProducePartition(" 2 ", 3)).toEqual({ partition: 2 });
    expect(parseProducePartition("7")).toEqual({ partition: 7 });
  });

  it("refuses anything that is not a whole number", () => {
    for (const text of ["abc", "-1", "1.5", "1e2", "0x1"]) {
      expect(parseProducePartition(text, 3).error).toBe("Partition must be a whole number.");
    }
  });

  it("refuses a partition the topic does not have", () => {
    expect(parseProducePartition("3", 3)).toEqual({ error: "This topic has partitions 0 to 2." });
  });
});

describe("describeInvalidHeaderLines", () => {
  it("is silent when every line is valid", () => {
    expect(describeInvalidHeaderLines([])).toBeUndefined();
  });

  it("names the offending lines", () => {
    expect(describeInvalidHeaderLines([2])).toBe("Line 2 is not in the key=value form.");
    expect(describeInvalidHeaderLines([2, 5])).toBe("Lines 2, 5 are not in the key=value form.");
  });
});
