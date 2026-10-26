import { describe, expect, it } from "vitest";
import { withoutIpcPrefix } from "./kafka-ipc-error";

describe("withoutIpcPrefix", () => {
  it("keeps the message main gave, without Electron's wording", () => {
    const raw = new Error(
      "Error invoking remote method 'extensions@b739:kafka:produce': Error: Produce message refused: write mode is not enabled",
    );
    const cleaned = withoutIpcPrefix(raw) as Error;

    expect(cleaned.message).toBe("Produce message refused: write mode is not enabled");
    expect(cleaned.cause).toBe(raw);
  });

  it("drops the class name of the error main threw", () => {
    const raw = new Error(
      "Error invoking remote method 'extensions@b739:kafka:overview': TunnelError: The cluster is not connected",
    );

    expect((withoutIpcPrefix(raw) as Error).message).toBe("The cluster is not connected");
  });

  it("keeps a missing handler recognisable by its channel", () => {
    const raw = new Error(
      "Error invoking remote method 'extensions@old:kafka:topics:delete': Error: No handler registered for 'extensions@old:kafka:topics:delete'",
    );

    expect((withoutIpcPrefix(raw) as Error).message).toBe(
      "No handler registered for 'extensions@old:kafka:topics:delete'",
    );
  });

  it("leaves any other error as it is", () => {
    const other = new Error("Topic authorization failed");

    expect(withoutIpcPrefix(other)).toBe(other);
    expect(withoutIpcPrefix("text")).toBe("text");
  });
});
