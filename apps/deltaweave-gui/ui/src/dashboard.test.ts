import { describe, it, expect } from "vitest";
import { renderDashboard } from "./dashboard";

describe("dashboard", () => {
  it("shows transfer and conflict panes together", () => {
    const html = renderDashboard({
      bytesPerSecond: 86_000_000,
      currentPath: "VMware.iso",
      percent: 62,
      conflicts: [{ path: "VMware.iso", localNewer: true }],
    });
    expect(html).toContain("충돌");
    expect(html).toContain("86");
    expect(html.toLowerCase()).not.toContain("prefers-color-scheme: dark");
  });
});
