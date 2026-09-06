import { render, screen, act, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import App from "./App";
import type { AppSnapshot } from "./types";

const snapshot: AppSnapshot = {
  node: {
    name: "실제 테스트 장치",
    version: "0.1.0",
    platform: "linux",
    started_at: 1788610000000,
    uptime_seconds: 123,
  },
  folders: [],
  devices: [
    {
      id: "registered",
      name: "등록한 장치",
      endpoint_id: "abc",
      address: "127.0.0.1:9000",
      added_at: 1788610000000,
      last_seen_at: null,
    },
  ],
  activities: [],
  history: [],
  totals: {
    folders: 0,
    active_folders: 0,
    files: 0,
    bytes: 0,
    pushed_bytes: 0,
    pulled_bytes: 0,
    conflicts: 0,
  },
  settings: {
    node_name: "실제 테스트 장치",
    poll_interval_seconds: 30,
    history_limit: 300,
  },
  revision: 7,
};
class BrowserEvents {
  static current: BrowserEvents;
  onopen: (() => void) | null = null;
  onerror: (() => void) | null = null;
  listener: ((event: { data: string }) => void) | null = null;
  constructor() {
    BrowserEvents.current = this;
  }
  addEventListener(_name: string, callback: (event: { data: string }) => void) {
    this.listener = callback;
  }
  close() {}
  send(state: AppSnapshot) {
    this.listener?.({ data: JSON.stringify(state) });
  }
}
function install(state: AppSnapshot = snapshot) {
  vi.stubGlobal("EventSource", BrowserEvents);
  vi.stubGlobal(
    "fetch",
    async (url: string) =>
      new Response(
        JSON.stringify(
          url.endsWith("/session")
            ? { authenticated: true, csrf_token: "csrf" }
            : state,
        ),
        { status: 200 },
      ),
  );
  vi.stubGlobal("scrollTo", () => {});
}
afterEach(() => vi.unstubAllGlobals());

it("shows honest empty folders and no successful response for a merely registered device", async () => {
  install();
  render(<App />);
  expect(await screen.findByText("첫 번째 폴더를 연결해 보세요")).toBeVisible();
  expect(screen.getByText("아직 응답 기록 없음")).toBeVisible();
  expect(
    screen.getByText("첫 동기화가 끝나면 그래프가 시작됩니다."),
  ).toBeVisible();
});
it("keeps the last snapshot visible with a disconnected transport warning", async () => {
  install();
  render(<App />);
  await screen.findByText("첫 번째 폴더를 연결해 보세요");
  act(() => BrowserEvents.current.onerror?.());
  expect(screen.getByText("실시간 연결 끊김")).toBeVisible();
  expect(
    screen.getByText(
      "실시간 연결이 끊겼습니다. 마지막 상태를 표시하며 자동으로 다시 연결합니다.",
    ),
  ).toBeVisible();
  expect(screen.getByText("등록한 장치")).toBeVisible();
});
it("recovers a full snapshot when a restarted server resets its revision", async () => {
  install();
  render(<App />);
  await screen.findByText("첫 번째 폴더를 연결해 보세요");
  act(() =>
    BrowserEvents.current.send({
      ...snapshot,
      node: {
        ...snapshot.node,
        name: "재시작한 장치",
        started_at: snapshot.node.started_at + 60000,
      },
      revision: 1,
    }),
  );
  expect((await screen.findAllByText("재시작한 장치"))[0]).toBeVisible();
});
it("preserves settings edits when the server rejects the save", async () => {
  install();
  render(<App />);
  await screen.findByText("첫 번째 폴더를 연결해 보세요");
  await userEvent.click(screen.getByRole("button", { name: "설정" }));
  const name = screen.getByLabelText("장치 표시 이름");
  await userEvent.clear(name);
  await userEvent.type(name, "수정한 장치");
  vi.stubGlobal(
    "fetch",
    async () =>
      new Response(JSON.stringify({ error: "설정을 저장할 수 없습니다." }), {
        status: 422,
      }),
  );
  await userEvent.click(screen.getByRole("button", { name: "변경 사항 저장" }));
  expect(await screen.findByRole("alert")).toHaveTextContent(
    "설정을 저장할 수 없습니다.",
  );
  expect(name).toHaveValue("수정한 장치");
  expect(screen.queryByText("설정을 저장했습니다.")).not.toBeInTheDocument();
});
it("opens server directory browsing from a blank path without sending an empty path parameter", async () => {
  install();
  const original = globalThis.fetch;
  const calls: string[] = [];
  vi.stubGlobal("fetch", async (url: string, init?: RequestInit) => {
    calls.push(url);
    return url.startsWith("/api/v1/browse")
      ? new Response(
          JSON.stringify({ path: "/server", parent: "/", entries: [] }),
          { status: 200 },
        )
      : original(url, init);
  });
  render(<App />);
  await screen.findByText("첫 번째 폴더를 연결해 보세요");
  await userEvent.click(screen.getByRole("button", { name: "폴더 추가" }));
  await userEvent.click(screen.getByRole("button", { name: "찾아보기" }));
  await waitFor(() => expect(calls).toContain("/api/v1/browse"));
  expect(screen.getByText("/server")).toBeVisible();
});

it("moves mobile menu focus into navigation and dismisses it from outside", async () => {
  install();
  const user = userEvent.setup();
  const { container } = render(<App />);
  await screen.findByText("첫 번째 폴더를 연결해 보세요");
  const toggle = container.querySelector<HTMLButtonElement>(".mobile-toggle")!;
  const nav = screen.getByRole("navigation", { name: "주 메뉴" });
  const currentPage = nav.querySelector<HTMLButtonElement>(
    '[aria-current="page"]',
  )!;
  const main = screen.getByRole("main");

  toggle.focus();
  await user.keyboard("{Enter}");
  expect(toggle).toHaveAttribute("aria-expanded", "true");
  expect(currentPage).toHaveFocus();
  await user.tab();
  expect(currentPage.nextElementSibling).toHaveFocus();

  main.focus();
  await user.keyboard("{Escape}");
  expect(toggle).toHaveAttribute("aria-expanded", "false");
  expect(toggle).toHaveFocus();

  await user.keyboard("{Enter}");
  expect(currentPage).toHaveFocus();
  await user.click(main);
  expect(toggle).toHaveAttribute("aria-expanded", "false");
});

it("pages activities, returns to the first filtered page, and clamps after a live shrink", async () => {
  const state: AppSnapshot = {
    ...snapshot,
    activities: Array.from({ length: 60 }, (_, index) => ({
      id: `activity-${index}`,
      folder_id: null,
      kind: "file_sent",
      title: `검증 기록 ${index}`,
      detail: index < 30 ? "검색그룹" : "나머지 기록",
      timestamp: snapshot.node.started_at + index * 1000,
      pushed_bytes: index,
      pulled_bytes: 0,
      path: null,
    })),
  };
  install(state);
  const user = userEvent.setup();
  const { container } = render(<App />);
  await screen.findByText("첫 번째 폴더를 연결해 보세요");
  await user.click(screen.getByRole("button", { name: "활동" }));
  expect(
    container.querySelectorAll(".full-activities .activity-row"),
  ).toHaveLength(25);
  expect(screen.getByText("검증 기록 59")).toBeVisible();
  expect(screen.queryByText("검증 기록 34")).not.toBeInTheDocument();

  await user.click(screen.getByRole("button", { name: "다음 활동 페이지" }));
  expect(screen.getByText("검증 기록 34")).toBeVisible();
  expect(screen.getByText("26–50 / 60건")).toBeVisible();
  await user.type(screen.getByLabelText("활동 검색"), "검색그룹");
  expect(screen.getByText("1–25 / 30건")).toBeVisible();
  expect(
    container.querySelectorAll(".full-activities .activity-row"),
  ).toHaveLength(25);

  await user.clear(screen.getByLabelText("활동 검색"));
  await user.click(screen.getByRole("button", { name: "다음 활동 페이지" }));
  act(() =>
    BrowserEvents.current.send({
      ...state,
      revision: 8,
      activities: state.activities.slice(0, 4),
    }),
  );
  expect(screen.getByText("1–4 / 4건")).toBeVisible();
  expect(
    screen.getByRole("button", { name: "이전 활동 페이지" }),
  ).toBeDisabled();
  expect(
    container.querySelectorAll(".full-activities .activity-row"),
  ).toHaveLength(4);
});
