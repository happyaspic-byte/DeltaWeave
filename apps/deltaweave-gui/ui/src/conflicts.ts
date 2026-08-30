export type ConflictItem = {
  jobId: string;
  path: string;
  conflictPath?: string;
  winnerHash?: string;
  loserHash?: string;
};

export type ConflictAction = "keep_local" | "keep_remote" | "keep_both";

export type ResolveConflictCommand = {
  type: "resolve_conflict";
  id: string;
  path: string;
  action: ConflictAction;
};

export function renderConflictPane(conflicts: ConflictItem[]): string {
  const rows = conflicts
    .map(
      (conflict) => `<li>
        <p>${escapeHtml(conflict.path)}</p>
        <button type="button" data-action="resolve-conflict" data-job-id="${escapeHtml(conflict.jobId)}" data-path="${escapeHtml(conflict.path)}" data-kind="keep_local">Keep this PC</button>
        <button type="button" data-action="resolve-conflict" data-job-id="${escapeHtml(conflict.jobId)}" data-path="${escapeHtml(conflict.path)}" data-kind="keep_remote">Keep peer</button>
        <button type="button" data-action="resolve-conflict" data-job-id="${escapeHtml(conflict.jobId)}" data-path="${escapeHtml(conflict.path)}" data-kind="keep_both">Keep both</button>
      </li>`,
    )
    .join("");
  return `<section class="conflicts">
    <h2>충돌</h2>
    <ul>${rows}</ul>
  </section>`;
}

export function buildResolveConflict(
  conflict: ConflictItem,
  action: ConflictAction,
): ResolveConflictCommand {
  if (!conflict.jobId || !conflict.path) {
    throw new Error("충돌 작업이 없습니다");
  }
  return {
    type: "resolve_conflict",
    id: conflict.jobId,
    path: conflict.path,
    action,
  };
}

export function commandFromButtonDataset(dataset: {
  action?: string;
  jobId?: string;
  path?: string;
  kind?: string;
}): ResolveConflictCommand {
  if (dataset.action !== "resolve-conflict") {
    throw new Error("충돌 작업이 없습니다");
  }
  const kind = dataset.kind;
  if (kind !== "keep_local" && kind !== "keep_remote" && kind !== "keep_both") {
    throw new Error("충돌 작업이 없습니다");
  }
  return buildResolveConflict(
    { jobId: dataset.jobId ?? "", path: dataset.path ?? "" },
    kind,
  );
}

export async function submitConflictResolution(
  dataset: {
    action?: string;
    jobId?: string;
    path?: string;
    kind?: string;
  },
  send: (command: ResolveConflictCommand) => Promise<void> | void,
): Promise<ResolveConflictCommand> {
  const command = commandFromButtonDataset(dataset);
  await send(command);
  return command;
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
