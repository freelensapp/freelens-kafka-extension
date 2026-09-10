import { describe, expect, it } from "vitest";
import { formatBytes, sumBytes } from "./format-bytes";

describe("formatBytes", () => {
  it("uses binary units with a precision that keeps three significant digits", () => {
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes("1023")).toBe("1023 B");
    expect(formatBytes(1024)).toBe("1.00 KiB");
    expect(formatBytes(1536)).toBe("1.50 KiB");
    expect(formatBytes(10 * 1024 * 1024)).toBe("10.0 MiB");
    expect(formatBytes(5_000_000_000)).toBe("4.66 GiB");
    expect(formatBytes(123456789012345n)).toBe("112 TiB");
  });

  it("reports invalid values as n/a", () => {
    expect(formatBytes("abc")).toBe("n/a");
    expect(formatBytes("")).toBe("n/a");
    expect(formatBytes(-1)).toBe("n/a");
  });
});

describe("sumBytes", () => {
  it("adds decimal strings and skips the invalid ones", () => {
    expect(sumBytes(["1", "2", undefined, "x", "-5", "9007199254740993"])).toBe("9007199254740996");
    expect(sumBytes([])).toBe("0");
  });
});
