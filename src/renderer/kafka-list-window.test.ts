import { describe, expect, it } from "vitest";
import { kafkaListWindow } from "./kafka-list-window";

describe("kafkaListWindow", () => {
  it("limits mounted items and clamps pages", () => {
    const items = Array.from({ length: 251 }, (_, index) => index);
    expect(kafkaListWindow(items, 1, 100)).toMatchObject({
      items: Array.from({ length: 100 }, (_, index) => index + 100),
      page: 1,
      pageCount: 3,
      total: 251,
    });
    expect(kafkaListWindow(items, 99, 100).page).toBe(2);
    expect(kafkaListWindow(items, 99, 100).items).toHaveLength(51);
  });

  it("keeps a 10,000-item collection bounded to 100 mounted rows", () => {
    const items = Array.from({ length: 10_000 }, (_, index) => `topic-${index}`);
    const window = kafkaListWindow(items, 50);

    expect(window).toMatchObject({ page: 50, pageCount: 100, total: 10_000 });
    expect(window.items).toHaveLength(100);
    expect(window.items[0]).toBe("topic-5000");
  });
});
