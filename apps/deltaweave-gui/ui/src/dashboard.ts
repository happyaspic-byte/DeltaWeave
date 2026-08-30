import { renderConflictPane } from "./conflicts";

export type ConflictPane = {
  jobId: string;
  path: string;
  localNewer: boolean;
};

export type DashboardJob = {
  id: string;
  name: string;
  local_root: string;
  paused: boolean;
};

export type DashboardState = {
  bytesPerSecond: number;
  currentPath: string;
  percent: number;
  jobs: DashboardJob[];
  conflicts: ConflictPane[];
};

export function renderDashboard(state: DashboardState): string {
  const megabytes = Math.round(state.bytesPerSecond / 1_000_000);
  return [
    `<section class="transfer">`,
    `<p class="speed">${megabytes} MB/s</p>`,
    `<p class="file">${escapeHtml(state.currentPath)}</p>`,
    `<p class="percent">${state.percent}%</p>`,
    `</section>`,
    `<section class="jobs"><h2>동기화 폴더</h2>${renderJobs(state.jobs)}</section>`,
    renderConflictPane(
      state.conflicts.map((conflict) => ({
        jobId: conflict.jobId,
        path: conflict.path,
      })),
    ),
  ].join("");
}

function renderJobs(jobs: DashboardJob[]): string {
  if (jobs.length === 0) {
    return "<p>연결된 폴더가 없습니다.</p>";
  }
  return jobs
    .map(
      (job) => `<article class="job"><strong>${escapeHtml(job.name)}</strong><small>${escapeHtml(job.local_root)}</small><span>${job.paused ? "일시정지" : "동기화 중"}</span><button type="button" data-action="sync-job" data-job-id="${escapeHtml(job.id)}">지금 동기화</button><button type="button" data-action="${job.paused ? "resume-job" : "pause-job"}" data-job-id="${escapeHtml(job.id)}">${job.paused ? "다시 시작" : "일시정지"}</button></article>`,
    )
    .join("");
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
