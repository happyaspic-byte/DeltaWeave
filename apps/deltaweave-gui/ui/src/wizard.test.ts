import { describe, expect, it } from "vitest";
import {
  advanceWizard,
  buildCreateJob,
  createWizardState,
  renderWizard,
  type WizardState,
} from "./wizard";

describe("add folder wizard", () => {
  it("requires all four stages before creating a job", () => {
    const state: WizardState = {
      ...createWizardState(),
      stage: "preview",
      folder: "C:\\DeltaWeave-Private",
      peerEndpointId: "aa".repeat(32),
      direction: "bidirectional",
      preview: { sends: 4, receives: 2, deletes: 0, conflicts: 1 },
      previewConfirmed: false,
    };

    expect(() => buildCreateJob(state)).toThrow("미리보기를 확인");
    expect(buildCreateJob({ ...state, previewConfirmed: true })).toEqual({
      type: "create_job",
      name: "DeltaWeave-Private",
      local_root: "C:\\DeltaWeave-Private",
      peer_endpoint_id: "aa".repeat(32),
      direction: "bidirectional",
      preview_confirmed: true,
    });
  });

  it("renders folder, peer, direction, and preview stages", () => {
    const state = createWizardState();
    expect(renderWizard(state)).toContain("폴더 선택");
    expect(renderWizard({ ...state, stage: "peer" })).toContain("LAN에서 찾기");
    expect(renderWizard({ ...state, stage: "peer" })).toContain("dwpair1:");
    expect(renderWizard({ ...state, stage: "peer" })).toContain("고급");
    expect(renderWizard({ ...state, stage: "direction" })).toContain("양방향");
    expect(renderWizard({ ...state, stage: "preview" })).toContain("미리보기 확인");
  });

  it("blocks next until the current stage is complete", () => {
    const state = createWizardState();
    expect(() => advanceWizard(state)).toThrow("폴더를 선택");
    expect(advanceWizard({ ...state, folder: "C:\\Sync" }).stage).toBe("peer");
    expect(() =>
      advanceWizard({ ...state, stage: "peer", folder: "C:\\Sync" }),
    ).toThrow("피어를 선택");
    expect(
      advanceWizard({
        ...state,
        stage: "peer",
        folder: "C:\\Sync",
        peerEndpointId: "aa".repeat(32),
      }).stage,
    ).toBe("direction");
    expect(
      advanceWizard({
        ...state,
        stage: "peer",
        folder: "C:\\Sync",
        ticket: "dwpair1:deadbeef",
      }).stage,
    ).toBe("direction");
  });

  it("submits CreateJob after preview confirmation", async () => {
    const { submitCreateJob } = await import("./wizard");
    const sent: ReturnType<typeof buildCreateJob>[] = [];
    const command = await submitCreateJob(
      {
        ...createWizardState(),
        stage: "preview",
        folder: "C:\\DeltaWeave-Private",
        peerEndpointId: "aa".repeat(32),
        direction: "bidirectional",
        preview: { sends: 1, receives: 0, deletes: 0, conflicts: 0 },
        previewConfirmed: true,
      },
      (job) => {
        sent.push(job);
      },
    );
    expect(command.preview_confirmed).toBe(true);
    expect(sent).toHaveLength(1);
  });
});
