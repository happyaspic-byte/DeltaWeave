export type ConflictPane = {
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
  const rows = state.conflicts
    .map((conflict) => `<li>${escapeHtml(conflict.path)}</li>`)
    .join("");
  return [
    `<section class="transfer">`,
    `<p class="speed">${megabytes} MB/s</p>`,
    `<p class="file">${escapeHtml(state.currentPath)}</p>`,
    `<p class="percent">${state.percent}%</p>`,
    `</section>`,
    `<section class="conflicts">`,
    `<h2>충돌</h2>`,
    `<ul>${rows}</ul>`,
    `</section>`,
  ].join("");
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}
