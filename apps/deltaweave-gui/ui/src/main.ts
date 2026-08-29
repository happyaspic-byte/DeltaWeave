import { renderDashboard } from "./dashboard";
import {
  advanceWizard,
  createWizardState,
  renderWizard,
  type WizardState,
} from "./wizard";

const root = document.getElementById("app");
let wizard: WizardState = createWizardState();

function paint(): void {
  if (!root) {
    return;
  }
  root.innerHTML = [
    renderDashboard({
      bytesPerSecond: 0,
      currentPath: "",
      percent: 0,
      conflicts: [],
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
    try {
      wizard = advanceWizard(readWizard(root));
      paint();
    } catch (error) {
      window.alert(error instanceof Error ? error.message : String(error));
    }
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

paint();
