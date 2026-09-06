import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it, vi } from "vitest";
import {
  CopyButton,
  Login,
  Modal,
  FolderForm,
  FolderControls,
} from "./components";
import type { FolderView } from "./types";

const folder: FolderView = {
  id: "folder-1",
  name: "작업 문서",
  root: "/data/docs",
  role: "sync",
  state_path: "/private/state",
  identity_path: "/private/id",
  device_id: "desktop",
  peer_endpoint_id: "abc",
  direct_addresses: ["127.0.0.1:9000"],
  allowed_peers: [],
  bind: "0.0.0.0:0",
  enabled: true,
  interval_seconds: 30,
  max_connections: 8,
  min_free_space_mib: 128,
  endpoint_id: "xyz",
  addresses: [],
  status: "idle",
  phase: null,
  current_path: null,
  last_sync_at: null,
  last_error: null,
  retry_at: null,
  files_count: 0,
  total_bytes: 0,
  last_report: null,
};

it("keeps a rejected login visible with the actual error", async () => {
  render(
    <Login
      onLogin={async () => {
        throw new Error("접근 키를 확인해 주세요.");
      }}
    />,
  );
  await userEvent.type(screen.getByLabelText("관리자 접근 키"), "bad-key");
  await userEvent.click(screen.getByRole("button", { name: "콘솔에 로그인" }));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "접근 키를 확인해 주세요.",
  );
  expect(screen.getByRole("button", { name: "콘솔에 로그인" })).toBeEnabled();
});

it("keeps folder form open and prevents duplicate submit until server acknowledges", async () => {
  let resolve!: () => void;
  const pending = new Promise<void>((r) => {
    resolve = r;
  });
  const saved = vi.fn();
  render(
    <FolderForm
      folder={folder}
      devices={[]}
      onSubmit={async () => pending}
      onSaved={saved}
      browse={async () => ({ path: "/", parent: null, entries: [] })}
    />,
  );
  await userEvent.click(screen.getByRole("button", { name: "변경 사항 저장" }));
  expect(screen.getByRole("button", { name: "저장 중…" })).toBeDisabled();
  expect(saved).not.toHaveBeenCalled();
  resolve();
  await waitFor(() => expect(saved).toHaveBeenCalledOnce());
});

it("shows folder validation errors and preserves entered values", async () => {
  render(
    <FolderForm
      folder={folder}
      devices={[]}
      onSubmit={async () => {
        throw new Error("경로가 겹칩니다.");
      }}
      onSaved={() => {}}
      browse={async () => ({ path: "/", parent: null, entries: [] })}
    />,
  );
  await userEvent.click(screen.getByRole("button", { name: "변경 사항 저장" }));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "경로가 겹칩니다.",
  );
  expect(screen.getByLabelText("폴더 이름")).toHaveValue("작업 문서");
});

it("issues pause and prevents another command while the server is responding", async () => {
  let resolve!: () => void;
  const action = vi.fn(
    async () =>
      new Promise<void>((r) => {
        resolve = r;
      }),
  );
  render(<FolderControls folder={folder} onCommand={action} />);
  await userEvent.click(
    screen.getByRole("button", { name: "작업 문서 일시정지" }),
  );
  expect(action).toHaveBeenCalledWith("pause");
  expect(
    screen.getByRole("button", { name: "작업 문서 지금 동기화" }),
  ).toBeDisabled();
  resolve();
  await waitFor(() =>
    expect(
      screen.getByRole("button", { name: "작업 문서 지금 동기화" }),
    ).toBeEnabled(),
  );
});

it("explains pause pending without allowing resume before pause finishes", () => {
  render(
    <FolderControls
      folder={{ ...folder, status: "pausing" }}
      onCommand={async () => {}}
    />,
  );
  expect(screen.getByText("현재 작업 종료 후 정지")).toBeVisible();
  expect(
    screen.queryByRole("button", { name: "작업 문서 재개" }),
  ).not.toBeInTheDocument();
});

it("rejects wrongly typed imported configuration before overwriting valid form values", async () => {
  render(
    <FolderForm
      folder={folder}
      devices={[]}
      onSubmit={async () => {}}
      onSaved={() => {}}
      browse={async () => ({ path: "/", parent: null, entries: [] })}
    />,
  );
  await userEvent.click(
    screen.getByText("기존 설정 · 공개 연결 정보 가져오기"),
  );
  await userEvent.selectOptions(screen.getByLabelText("가져올 정보"), "config");
  await userEvent.click(screen.getByLabelText("설정 JSON"));
  await userEvent.paste(
    JSON.stringify({ name: 42, root: "/new", role: "sync" }),
  );
  await userEvent.click(screen.getByRole("button", { name: "폼에 가져오기" }));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "name 값은 문자열이어야 합니다.",
  );
  expect(screen.getByLabelText("폴더 이름")).toHaveValue("작업 문서");
});

it("allows pause before a manual sync response completes and keeps its pending state visible", async () => {
  let resolveSync!: () => void;
  let resolvePause!: () => void;
  const syncResponse = new Promise<void>((resolve) => {
    resolveSync = resolve;
  });
  const pauseResponse = new Promise<void>((resolve) => {
    resolvePause = resolve;
  });
  const commands: string[] = [];
  const onCommand = async (command: "sync" | "pause" | "resume") => {
    commands.push(command);
    await (command === "sync" ? syncResponse : pauseResponse);
  };
  const { rerender } = render(
    <FolderControls folder={folder} onCommand={onCommand} />,
  );
  await userEvent.click(
    screen.getByRole("button", { name: "작업 문서 지금 동기화" }),
  );
  rerender(
    <FolderControls
      folder={{ ...folder, status: "syncing" }}
      onCommand={onCommand}
    />,
  );
  expect(
    screen.getByRole("button", { name: "작업 문서 지금 동기화" }),
  ).toBeDisabled();
  expect(
    screen.getByRole("button", { name: "작업 문서 일시정지" }),
  ).toBeEnabled();
  await userEvent.click(
    screen.getByRole("button", { name: "작업 문서 일시정지" }),
  );
  expect(commands).toEqual(["sync", "pause"]);
  expect(screen.getByText("일시정지 요청 중…")).toBeVisible();
  expect(
    screen.getByRole("button", { name: "작업 문서 일시정지" }),
  ).toBeDisabled();
  rerender(
    <FolderControls
      folder={{ ...folder, status: "pausing" }}
      onCommand={onCommand}
    />,
  );
  expect(screen.getByText("현재 작업 종료 후 정지")).toBeVisible();
  await act(async () => {
    resolveSync();
    await syncResponse;
  });
  expect(
    screen.getByRole("button", { name: "작업 문서 지금 동기화" }),
  ).toBeDisabled();
  expect(screen.getByText("현재 작업 종료 후 정지")).toBeVisible();
  await act(async () => {
    resolvePause();
    await pauseResponse;
  });
});

it("restores focus after a fallback copy so Escape still closes its dialog", async () => {
  const user = userEvent.setup();
  const clipboardDescriptor = Object.getOwnPropertyDescriptor(
    navigator,
    "clipboard",
  );
  const originalExecCommand = document.execCommand;
  Object.defineProperty(navigator, "clipboard", {
    configurable: true,
    value: undefined,
  });
  // jsdom select() omits the focus transfer performed by real browsers.
  const select = vi
    .spyOn(HTMLTextAreaElement.prototype, "select")
    .mockImplementation(function (this: HTMLTextAreaElement) {
      this.focus();
    });
  let copied = "";
  document.execCommand = () => {
    copied = (document.activeElement as HTMLTextAreaElement).value;
    return true;
  };
  try {
    const view = render(
      <Modal
        title="공개 연결 정보"
        onClose={() => view.rerender(<p>창 닫힘</p>)}
      >
        <CopyButton value="public-connection-json" />
      </Modal>,
    );
    await user.click(
      screen.getByRole("button", { name: "공개 연결 정보 복사" }),
    );
    const copyButton = screen.getByRole("button", { name: "복사됨" });
    expect(copied).toBe("public-connection-json");
    expect(copyButton).toHaveFocus();
    expect(document.querySelector("textarea")).not.toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.getByText("창 닫힘")).toBeVisible();
  } finally {
    select.mockRestore();
    document.execCommand = originalExecCommand;
    if (clipboardDescriptor)
      Object.defineProperty(navigator, "clipboard", clipboardDescriptor);
    else Reflect.deleteProperty(navigator, "clipboard");
  }
});

it("recovers outside focus for Tab and Escape after an asynchronous dialog action", async () => {
  const user = userEvent.setup();
  const view = render(
    <>
      <button>배경 메뉴</button>
      <Modal
        title="폴더 탐색"
        onClose={() => view.rerender(<p>탐색 창 닫힘</p>)}
      >
        <button>찾아보기</button>
        <button>폴더 선택</button>
      </Modal>
      <button>배경 다음 작업</button>
    </>,
  );
  function loseFocus() {
    // Browsers can move focus to BODY after the clicked async control is disabled.
    const temporaryControl = document.createElement("button");
    document.body.append(temporaryControl);
    temporaryControl.focus();
    temporaryControl.remove();
    expect(document.body).toHaveFocus();
  }
  loseFocus();
  await user.tab();
  expect(screen.getByRole("button", { name: "닫기" })).toHaveFocus();
  loseFocus();
  await user.tab({ shift: true });
  expect(screen.getByRole("button", { name: "폴더 선택" })).toHaveFocus();
  loseFocus();
  await user.keyboard("{Escape}");
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(screen.getByText("탐색 창 닫힘")).toBeVisible();
});

it("handles Escape only for the topmost dialog when focus is outside both", async () => {
  const user = userEvent.setup();
  const outerClose = vi.fn();
  const innerClose = vi.fn();
  render(
    <>
      <Modal title="바깥 창" onClose={outerClose}>
        <button>바깥 작업</button>
      </Modal>
      <Modal title="안쪽 창" onClose={innerClose}>
        <button>안쪽 작업</button>
      </Modal>
    </>,
  );
  const temporaryControl = document.createElement("button");
  document.body.append(temporaryControl);
  temporaryControl.focus();
  temporaryControl.remove();
  await user.keyboard("{Escape}");
  expect(innerClose).toHaveBeenCalledOnce();
  expect(outerClose).not.toHaveBeenCalled();
});
