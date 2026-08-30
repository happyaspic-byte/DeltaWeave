import { describe, expect, it } from "vitest";
import {
  buildResolveConflict,
  commandFromButtonDataset,
  renderConflictPane,
  submitConflictResolution,
} from "./conflicts";

const conflict = {
  jobId: "job-a",
  path: "VMware.iso",
  conflictPath: "VMware.iso.conflict-abcd",
  winnerHash: "11".repeat(32),
  loserHash: "22".repeat(32),
};

describe("conflict pane", () => {
  it("offers all three explicit resolution actions", () => {
    const html = renderConflictPane([conflict]);
    expect(html).toContain("Keep this PC");
    expect(html).toContain("Keep peer");
    expect(html).toContain("Keep both");
    expect(html).toContain("VMware.iso");
  });

  it("builds ResolveConflict commands without exposing hashes", () => {
    expect(buildResolveConflict(conflict, "keep_remote")).toEqual({
      type: "resolve_conflict",
      id: "job-a",
      path: "VMware.iso",
      action: "keep_remote",
    });
    expect(renderConflictPane([conflict])).not.toContain(conflict.winnerHash);
    expect(renderConflictPane([conflict])).not.toContain(conflict.loserHash);
  });

  it("turns Keep peer clicks into ResolveConflict", () => {
    expect(
      commandFromButtonDataset({
        action: "resolve-conflict",
        jobId: "job-a",
        path: "VMware.iso",
        kind: "keep_remote",
      }),
    ).toEqual({
      type: "resolve_conflict",
      id: "job-a",
      path: "VMware.iso",
      action: "keep_remote",
    });
    expect(() =>
      commandFromButtonDataset({
        action: "resolve-conflict",
        jobId: "",
        path: "VMware.iso",
        kind: "keep_remote",
      }),
    ).toThrow("충돌 작업이 없습니다");
  });

  it("sends the ResolveConflict command instead of discarding it", async () => {
    const sent: ReturnType<typeof commandFromButtonDataset>[] = [];
    await submitConflictResolution(
      {
        action: "resolve-conflict",
        jobId: "job-a",
        path: "VMware.iso",
        kind: "keep_both",
      },
      (command) => {
        sent.push(command);
      },
    );
    expect(sent).toEqual([
      {
        type: "resolve_conflict",
        id: "job-a",
        path: "VMware.iso",
        action: "keep_both",
      },
    ]);
  });
});
