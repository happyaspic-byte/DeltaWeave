import { renderConflictPane } from "./conflicts";

export type ConflictPane = {
  jobId: string;
  path: string;
  localNewer: boolean;
};

export type DashboardState = {
  bytesPerSecond: number;
  currentPath: string;
  percent: number;
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
    renderConflictPane(
      state.conflicts.map((conflict) => ({
        jobId: conflict.jobId,
        path: conflict.path,
      })),
    ),
  ].join("");
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
