import { describe, it, expect } from "vitest";
import { renderDashboard } from "./dashboard";

describe("dashboard", () => {
  it("shows transfer and conflict panes together", () => {
    const html = renderDashboard({
      bytesPerSecond: 86_000_000,
      currentPath: "VMware.iso",
      percent: 62,
      jobs: [{ id: "job-a", name: "ISOs", local_root: "C:\\ISOs", paused: false }],
      conflicts: [{ jobId: "job-a", path: "VMware.iso", localNewer: true }],
      localEndpointId: "aa".repeat(32),
    });
    expect(html).toContain("충돌");
    expect(html).toContain("이 PC");
    expect(html).toContain("aa".repeat(32));
    expect(html).toContain("86");
    expect(html).toContain('data-job-id="job-a"');
    expect(html).toContain("Keep this PC");
    expect(html).toContain("ISOs");
    expect(html).toContain('data-action="sync-job"');
    expect(html.toLowerCase()).not.toContain("prefers-color-scheme: dark");
  });
});
