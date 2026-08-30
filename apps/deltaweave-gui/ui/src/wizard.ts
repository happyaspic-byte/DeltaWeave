export type WizardStage = "folder" | "peer" | "direction" | "preview";
export type SyncDirection = "bidirectional" | "send_only" | "receive_only";

export type PreviewSummary = {
  sends: number;
  receives: number;
  deletes: number;
  conflicts: number;
};

export type DiscoveredPeer = {
  endpointId: string;
  name: string;
  fingerprint: string;
};

export type WizardState = {
  stage: WizardStage;
  folder: string;
  peers: DiscoveredPeer[];
  peerEndpointId: string;
  peerAddress: string | null;
  ticket: string;
  manualAddress: string;
  manualPort: string;
  direction: SyncDirection;
  preview: PreviewSummary | null;
  previewConfirmed: boolean;
};

export type CreateJobCommand = {
  type: "create_job";
  name: string;
  local_root: string;
  peer_endpoint_id: string;
  peer_address: string | null;
  direction: SyncDirection;
  preview_confirmed: true;
};

export function createWizardState(): WizardState {
  return {
    stage: "folder",
    folder: "",
    peers: [],
    peerEndpointId: "",
    peerAddress: null,
    ticket: "",
    manualAddress: "",
    manualPort: "",
    direction: "bidirectional",
    preview: null,
    previewConfirmed: false,
  };
}

export function advanceWizard(state: WizardState): WizardState {
  if (state.stage === "folder") {
    if (!state.folder.trim()) {
      throw new Error("폴더를 선택하세요");
    }
    return { ...state, stage: "peer" };
  }
  if (state.stage === "peer") {
    if (!state.peerEndpointId && !state.ticket.startsWith("dwpair1:")) {
      throw new Error("피어를 선택하세요");
    }
    return { ...state, stage: "direction" };
  }
  if (state.stage === "direction") {
    return { ...state, stage: "preview", previewConfirmed: false };
  }
  return state;
}

export function buildCreateJob(state: WizardState): CreateJobCommand {
  if (!state.folder || !state.peerEndpointId || !state.preview) {
    throw new Error("마법사의 모든 단계를 완료하세요");
  }
  if (!state.previewConfirmed) {
    throw new Error("미리보기를 확인하세요");
  }
  return {
    type: "create_job",
    name: folderName(state.folder),
    local_root: state.folder,
    peer_endpoint_id: state.peerEndpointId,
    peer_address: state.peerAddress,
    direction: state.direction,
    preview_confirmed: true,
  };
}

export async function submitCreateJob(
  state: WizardState,
  send: (command: CreateJobCommand) => Promise<void> | void,
): Promise<CreateJobCommand> {
  const command = buildCreateJob(state);
  await send(command);
  return command;
}

export function renderWizard(state: WizardState): string {
  const stages: Record<WizardStage, () => string> = {
    folder: () => `
      <h2>폴더 선택</h2>
      <p>동기화할 로컬 폴더를 선택하세요.</p>
      <label>폴더 경로 <input name="folder" value="${escapeHtml(state.folder)}" /></label>
      <button type="button" data-action="browse-folder">찾아보기</button>`,
    peer: () => `
      <h2>피어 선택</h2>
      <section aria-label="LAN 피어">
        <h3>LAN에서 찾기</h3>
        ${renderPeers(state.peers, state.peerEndpointId)}
      </section>
      <label>페어링 티켓 <input name="ticket" placeholder="dwpair1:" value="${escapeHtml(state.ticket)}" /></label>
      <button type="button" data-action="issue-ticket">이 PC의 페어링 티켓 만들기</button>
      <details>
        <summary>고급</summary>
        <label>IP 주소 <input name="manual-address" value="${escapeHtml(state.manualAddress)}" /></label>
        <label>포트 <input name="manual-port" inputmode="numeric" value="${escapeHtml(state.manualPort)}" /></label>
      </details>`,
    direction: () => `
      <h2>동기화 방향</h2>
      ${directionOption("bidirectional", "양방향", state.direction)}
      ${directionOption("send_only", "보내기만", state.direction)}
      ${directionOption("receive_only", "받기만", state.direction)}`,
    preview: () => `
      <h2>미리보기 확인</h2>
      ${renderPreview(state.preview)}
      <label><input type="checkbox" name="preview-confirmed"${state.previewConfirmed ? " checked" : ""} /> 이 변경 내용을 확인했습니다.</label>
      <button type="button" data-action="create-job"${state.previewConfirmed ? "" : " disabled"}>동기화 시작</button>`,
  };

  return `<section class="wizard" aria-label="폴더 추가">
    <p class="wizard-step">${stageNumber(state.stage)} / 4</p>
    ${stages[state.stage]()}
    ${state.stage === "preview" ? "" : '<button type="button" data-action="next-stage">다음</button>'}
  </section>`;
}

function renderPeers(peers: DiscoveredPeer[], selected: string): string {
  if (peers.length === 0) {
    return "<p>검색된 피어가 없습니다.</p>";
  }
  return peers
    .map(
      (peer) => `<label><input type="radio" name="peer" value="${escapeHtml(peer.endpointId)}"${peer.endpointId === selected ? " checked" : ""} /> ${escapeHtml(peer.name)} <small>${escapeHtml(peer.fingerprint)}</small></label>`,
    )
    .join("");
}

function directionOption(value: SyncDirection, label: string, selected: SyncDirection): string {
  return `<label><input type="radio" name="direction" value="${value}"${value === selected ? " checked" : ""} /> ${label}</label>`;
}

function renderPreview(preview: PreviewSummary | null): string {
  if (!preview) {
    return "<p>미리보기를 실행하는 중입니다.</p>";
  }
  return `<dl>
    <dt>보내기</dt><dd>${preview.sends}</dd>
    <dt>받기</dt><dd>${preview.receives}</dd>
    <dt>삭제</dt><dd>${preview.deletes}</dd>
    <dt>충돌</dt><dd>${preview.conflicts}</dd>
  </dl>`;
}

function stageNumber(stage: WizardStage): number {
  return ["folder", "peer", "direction", "preview"].indexOf(stage) + 1;
}

function folderName(path: string): string {
  const trimmed = path.replace(/[\\/]+$/, "");
  return trimmed.split(/[\\/]/).at(-1) || "DeltaWeave";
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
