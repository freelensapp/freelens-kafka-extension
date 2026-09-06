import { describe, expect, it } from "vitest";
import { canSubmitWriteAction, getWriteConfirmationLabel } from "./kafka-write-policy";

describe("kafka-write-policy", () => {
  it("allows a normal confirmation without a typed resource name", () => {
    expect(
      canSubmitWriteAction({ confirmationAccepted: true, requiredResourceName: undefined, enteredResourceName: "" }),
    ).toBe(true);
  });

  it("requires the exact resource name for destructive operations", () => {
    expect(
      canSubmitWriteAction({
        confirmationAccepted: true,
        requiredResourceName: "orders-topic",
        enteredResourceName: "orders-topic",
      }),
    ).toBe(true);

    expect(
      canSubmitWriteAction({
        confirmationAccepted: true,
        requiredResourceName: "orders-topic",
        enteredResourceName: "other-topic",
      }),
    ).toBe(false);
  });

  it("surfaces the correct confirmation label for destructive writes", () => {
    expect(getWriteConfirmationLabel({ destructive: true, resourceName: "orders-topic" })).toBe(
      "Type orders-topic to confirm",
    );
    expect(getWriteConfirmationLabel({ destructive: false, resourceName: "orders-topic" })).toBe("Confirm once");
  });
});
