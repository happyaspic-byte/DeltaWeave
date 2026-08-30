import {
  submitConflictResolution,
  type ResolveConflictCommand,
} from "./conflicts";
import { renderDashboard, type DashboardJob } from "./dashboard";
import {
  advanceWizard,
  createWizardState,
  renderWizard,
  submitCreateJob,
  type CreateJobCommand,
  type WizardState,
} from "./wizard";

const root = document.getElementById("app");
let wizard: WizardState = createWizardState();
let jobs: DashboardJob[] = [];
let conflicts: { jobId: string; path: string; localNewer: boolean }[] = [];

function paint(): void {
  if (!root) {
    return;
  }
  root.innerHTML = [
    renderDashboard({
      bytesPerSecond: 0,
      currentPath: "",
      percent: 0,
      jobs,
      conflicts,
    }),
    renderWizard(wizard),
  ].join("");
}

root?.addEventListener("click", (event) => {
  const target = event.target;
  if (!(target instanceof HTMLElement)) {
    return;
  }
  if (target.dataset.action === "next-stage") {
    void (async () => {
      try {
        wizard = readWizard(root);
        if (wizard.stage === "peer" && wizard.ticket.startsWith("dwpair1:")) {
          const redeemed = await invokeDaemon<{
            peer_endpoint_id?: string;
            server_direct_address?: string;
          }>("redeem_ticket", { code: wizard.ticket });
          wizard = {
            ...wizard,
            peerEndpointId: redeemed.peer_endpoint_id ?? wizard.peerEndpointId,
            peerAddress: redeemed.server_direct_address ?? wizard.peerAddress,
          };
        } else if (
          wizard.stage === "peer" &&
          wizard.manualAddress &&
          wizard.manualPort
        ) {
          wizard = {
            ...wizard,
            peerAddress: `${wizard.manualAddress}:${wizard.manualPort}`,
          };
        }
        wizard = advanceWizard(wizard);
        if (wizard.stage === "preview" && !wizard.preview) {
          if (!wizard.peerEndpointId || !wizard.peerAddress) {
            throw new Error("피어 주소가 없습니다");
          }
          const preview = await invokeDaemon<{
            sends?: number;
            receives?: number;
            deletes?: number;
            conflicts?: number;
          }>("preview_job", {
            local_root: wizard.folder,
            peer_endpoint_id: wizard.peerEndpointId,
            peer_address: wizard.peerAddress,
          });
          wizard = {
            ...wizard,
            preview: {
              sends: preview.sends ?? 0,
              receives: preview.receives ?? 0,
              deletes: preview.deletes ?? 0,
              conflicts: preview.conflicts ?? 0,
            },
          };
        }
        paint();
      } catch (error) {
        window.alert(error instanceof Error ? error.message : String(error));
      }
    })();
    return;
  }
  if (target.dataset.action === "browse-folder") {
    void invokeDaemon<string | null>("browse_folder", {}).then((folder) => {
      if (folder) {
        wizard = { ...readWizard(root), folder };
        paint();
      }
    });
    return;
  }
  if (target.dataset.action === "create-job") {
    void submitCreateJob(readWizard(root), sendCreateJob)
      .then(async () => {
        wizard = createWizardState();
        await refreshJobs();
        paint();
      })
      .catch((error: unknown) => {
        window.alert(error instanceof Error ? error.message : String(error));
      });
    return;
  }
  if (target.dataset.action === "sync-job" && target.dataset.jobId) {
    void invokeDaemon("sync_now", { id: target.dataset.jobId })
      .then(refreshJobs)
      .then(paint)
      .catch((error: unknown) => {
        window.alert(error instanceof Error ? error.message : String(error));
      });
    return;
  }
  if (target.dataset.action === "pause-job" && target.dataset.jobId) {
    void invokeDaemon("pause_job", { id: target.dataset.jobId })
      .then(refreshJobs)
      .then(paint)
      .catch((error: unknown) => {
        window.alert(error instanceof Error ? error.message : String(error));
      });
    return;
  }
  if (target.dataset.action === "resume-job" && target.dataset.jobId) {
    void invokeDaemon("resume_job", { id: target.dataset.jobId })
      .then(refreshJobs)
      .then(paint)
      .catch((error: unknown) => {
        window.alert(error instanceof Error ? error.message : String(error));
      });
    return;
  }
  if (target.dataset.action === "issue-ticket") {
    void invokeDaemon<{ code?: string }>("issue_ticket", { ttl_seconds: 600 })
      .then((issued) => {
        if (issued.code) {
          window.alert(`페어링 티켓:\n${issued.code}`);
        }
      })
      .catch((error: unknown) => {
        window.alert(error instanceof Error ? error.message : String(error));
      });
    return;
  }
  if (target.dataset.action === "resolve-conflict") {
    void submitConflictResolution(
      {
        action: target.dataset.action,
        jobId: target.dataset.jobId,
        path: target.dataset.path,
        kind: target.dataset.kind,
      },
      sendResolveConflict,
    ).catch((error: unknown) => {
      window.alert(error instanceof Error ? error.message : String(error));
    });
  }
});

root?.addEventListener("change", () => {
  if (root) {
    wizard = readWizard(root);
  }
});

function readWizard(host: HTMLElement): WizardState {
  const folder = inputValue(host, "folder");
  const ticket = inputValue(host, "ticket");
  const peer = radioValue(host, "peer");
  const direction =
    radioValue(host, "direction") === "send_only"
      ? "send_only"
      : radioValue(host, "direction") === "receive_only"
        ? "receive_only"
        : wizard.direction;
  return {
    ...wizard,
    folder: folder || wizard.folder,
    ticket: ticket || wizard.ticket,
    peerEndpointId: peer || wizard.peerEndpointId,
    manualAddress: inputValue(host, "manual-address") || wizard.manualAddress,
    manualPort: inputValue(host, "manual-port") || wizard.manualPort,
    direction,
    previewConfirmed: checkboxChecked(host, "preview-confirmed"),
  };
}

function inputValue(host: HTMLElement, name: string): string {
  const field = host.querySelector(`[name="${name}"]`);
  return field instanceof HTMLInputElement ? field.value : "";
}

function radioValue(host: HTMLElement, name: string): string {
  const field = host.querySelector(`input[name="${name}"]:checked`);
  return field instanceof HTMLInputElement ? field.value : "";
}

function checkboxChecked(host: HTMLElement, name: string): boolean {
  const field = host.querySelector(`input[name="${name}"]`);
  return field instanceof HTMLInputElement ? field.checked : false;
}

function sendResolveConflict(command: ResolveConflictCommand): Promise<void> {
  return invokeDaemon("resolve_conflict", {
    id: command.id,
    path: command.path,
    action: command.action,
  }).then(() => undefined);
}

function sendCreateJob(command: CreateJobCommand): Promise<void> {
  return invokeDaemon("create_job", command).then(() => undefined);
}

async function refreshJobs(): Promise<void> {
  const listed = await invokeDaemon<{ jobs?: DashboardJob[] }>("list_jobs", {});
  jobs = listed.jobs ?? [];
  const nextConflicts: { jobId: string; path: string; localNewer: boolean }[] = [];
  for (const job of jobs) {
    const listedConflicts = await invokeDaemon<{
      conflicts?: { path?: string }[];
    }>("list_conflicts", { id: job.id });
    for (const conflict of listedConflicts.conflicts ?? []) {
      if (conflict.path) {
        nextConflicts.push({ jobId: job.id, path: conflict.path, localNewer: true });
      }
    }
  }
  conflicts = nextConflicts;
}

async function invokeDaemon<T>(command: string, args: object): Promise<T> {
  const invoke = (
    globalThis as {
      __TAURI__?: { core?: { invoke?: (cmd: string, args: object) => Promise<T> } };
    }
  ).__TAURI__?.core?.invoke;
  if (typeof invoke !== "function") {
    return {} as T;
  }
  return invoke(command, args);
}

void refreshJobs().then(paint).catch(paint);
