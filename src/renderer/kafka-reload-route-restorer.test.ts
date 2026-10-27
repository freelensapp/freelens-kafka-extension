import { describe, expect, it } from "vitest";
import { nextRestoreStep } from "./kafka-reload-route-restorer";

const base = { startPathname: "/", pageId: "kafka-topics", attempts: 0, maxAttempts: 100 };

describe("nextRestoreStep", () => {
  it("navigates to the saved page while the frame is still where it started", () => {
    expect(nextRestoreStep({ ...base, pathname: "/" })).toBe("navigate");
    expect(nextRestoreStep({ ...base, pathname: "/", attempts: 99 })).toBe("navigate");
  });

  it("is done once the frame shows the saved page", () => {
    expect(nextRestoreStep({ ...base, pathname: "/extension/abc/kafka-topics" })).toBe("done");
  });

  it("gives up when the user went to another page first", () => {
    expect(nextRestoreStep({ ...base, pathname: "/extension/abc/kafka-overview" })).toBe("give-up");
  });

  it("gives up after its last attempt", () => {
    expect(nextRestoreStep({ ...base, pathname: "/", attempts: 100 })).toBe("give-up");
  });
});
