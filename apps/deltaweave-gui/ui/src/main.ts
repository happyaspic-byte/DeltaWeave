import { renderDashboard } from "./dashboard";

const root = document.getElementById("app");
if (root) {
  root.innerHTML = renderDashboard({
    bytesPerSecond: 0,
    currentPath: "",
    percent: 0,
    conflicts: [],
  });
}
