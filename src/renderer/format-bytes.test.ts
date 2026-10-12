import { describe, expect, it } from "vitest";
import { formatBytes, sumBytes, topicSizeText, topicSizeTitle } from "./format-bytes";

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

describe("topicSizeTitle", () => {
  const size = { leaderBytes: "1536", replicaBytes: "4608", exact: true };

  it("shows the leader and replica footprint of an exact size", () => {
    expect(topicSizeTitle(size)).toBe("Leader replicas 1.50 KiB, all replicas 4.50 KiB");
    expect(topicSizeTitle(size, [])).toBe("Leader replicas 1.50 KiB, all replicas 4.50 KiB");
  });

  it("warns about a lower bound and says what the size reader worked around", () => {
    expect(topicSizeTitle({ ...size, exact: false })).toBe(
      "Leader replicas 1.50 KiB, all replicas 4.50 KiB (lower bound: a broker did not report)",
    );
    expect(
      topicSizeTitle({ ...size, exact: false }, [
        "broker 2: KafkaJSConnectionClosedError: Closed connection; reconnecting and retrying once",
        "broker 2 left out: KafkaJSConnectionClosedError: Closed connection, on a fresh connection too",
      ]),
    ).toBe(
      "Leader replicas 1.50 KiB, all replicas 4.50 KiB (lower bound: a broker did not report). Size reader: broker 2: KafkaJSConnectionClosedError: Closed connection; reconnecting and retrying once; broker 2 left out: KafkaJSConnectionClosedError: Closed connection, on a fresh connection too",
    );
  });
});

describe("topicSizeText", () => {
  const orders = { leaderBytes: "1536", replicaBytes: "4608", exact: true, partitions: [] };
  const read = { supported: true, unavailableBrokers: [], topics: { orders } };

  it("shows the size with its hover text", () => {
    expect(topicSizeText({ data: read }, "orders")).toEqual({
      text: "1.50 KiB",
      title: "Leader replicas 1.50 KiB, all replicas 4.50 KiB",
      placeholder: false,
    });
    expect(
      topicSizeText(
        { data: { ...read, topics: { orders: { ...orders, exact: false } }, notes: ["broker 2 left out: boom"] } },
        "orders",
      ),
    ).toEqual({
      text: "≥ 1.50 KiB",
      title:
        "Leader replicas 1.50 KiB, all replicas 4.50 KiB (lower bound: a broker did not report). Size reader: broker 2 left out: boom",
      placeholder: false,
    });
  });

  it("tells a running read from a failed one, an unsupported cluster and a topic without a size", () => {
    expect(topicSizeText(undefined, "orders")).toEqual({ text: "…", placeholder: true });
    expect(topicSizeText({}, "orders")).toEqual({ text: "…", placeholder: true });
    expect(topicSizeText({ error: "asking the brokers: broker 1: boom" }, "orders")).toEqual({
      text: "n/a",
      title: "asking the brokers: broker 1: boom",
      placeholder: true,
    });
    expect(topicSizeText({ data: { ...read, supported: false } }, "orders")).toEqual({
      text: "n/a",
      title: "The brokers do not offer DescribeLogDirs",
      placeholder: true,
    });
    expect(topicSizeText({ data: read }, "deleted-meanwhile")).toEqual({ text: "—", placeholder: true });
  });
});

describe("sumBytes", () => {
  it("adds decimal strings and skips the invalid ones", () => {
    expect(sumBytes(["1", "2", undefined, "x", "-5", "9007199254740993"])).toBe("9007199254740996");
    expect(sumBytes([])).toBe("0");
  });
});
