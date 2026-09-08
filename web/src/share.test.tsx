import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";
import { Api, ApiError, OperationRequest } from "./api";
import {
  ManagedSharesBoard,
  ShareCreateFlow,
  ShareDetailFlow,
  ShareJoinFlow,
  MAX_EXPIRY_TIMER_MS,
  managedStatusLabels,
  scheduleExpiry,
  safeShareError,
} from "./share";
import type {
  Directory,
  IssuedKey,
  JoinResult,
  KeyPreview,
  ManagedShareView,
} from "./types";

const shareId = "a".repeat(64);
const invitationId = "b".repeat(64);
const memberId = "c".repeat(64);

const share: ManagedShareView = {
  share_id: shareId,
  name: "팀 문서",
  role: "owner",
  permission: null,
  root: "/srv/team-docs",
  status: "complete",
  phase: "complete",
  last_sync_at: 1_788_610_000,
  retry_at: null,
  files_count: 12,
  total_bytes: 4096,
  transferred_bytes: 2048,
  speed_bps: 512,
  active_peer_count: 1,
  connected_devices: [
    {
      member_id: memberId,
      permission: "read_only",
      active_operations: 1,
      last_seen_at: 1_788_610_000,
    },
  ],
  last_error: null,
};

const preview: KeyPreview = {
  share_id: shareId,
  name: "팀 문서",
  permission: "read_only",
  invitation_id: invitationId,
  expires_at: null,
  signature_valid: true,
  issuance: "not_checked",
};

const joined: JoinResult = {
  request_id: "join-request",
  share_id: shareId,
  enrollment: "enrolled",
  status: "initial_sync",
  permission: "read_only",
  member_id: memberId,
};

const directory: Directory = {
  path: "/srv/receiver",
  parent: "/srv",
  entries: [],
};

function configureJoinApi(
  api: Api,
  validate: ReturnType<typeof vi.fn> = vi.fn(async () => ({ ...preview, issuance: "validated" as const })),
) {
  vi.spyOn(api, "previewShareKey").mockResolvedValue(preview);
  vi.spyOn(api, "validateShareKey").mockImplementation(validate);
  vi.spyOn(api, "joinShare").mockResolvedValue(joined);
}

afterEach(() => vi.restoreAllMocks());

describe("managed share web contract", () => {
  it("reuses a request id only when the operation fingerprint is unchanged", () => {
    const operation = new OperationRequest();
    const first = operation.begin("share\u001froot-a");
    expect(operation.retry("share\u001froot-a")).toBe(first);
    expect(operation.begin("share\u001froot-b")).not.toBe(first);
    operation.reset();
    expect(operation.begin("share\u001froot-b")).not.toBe(first);
  });

  it("uses fixed safe copy for classified failures and never renders the bearer", () => {
    const secret = "opaque-test-key";
    const error = new ApiError(`server leaked ${secret}`, 500, "internal_error");
    expect(safeShareError(error, secret)).toBe(
      "공유 요청을 처리하지 못했습니다. 잠시 후 다시 시도하세요.",
    );
    expect(safeShareError(new Error(`bad ${secret}`), secret)).not.toContain(secret);
  });

  it("runs local preview and online validation from one key check before joining", async () => {
    const user = userEvent.setup();
    const api = new Api();
    configureJoinApi(api);
    const onComplete = vi.fn(async () => {});
    render(
      <ShareJoinFlow
        api={api}
        browse={async () => directory}
        onClose={() => {}}
        onComplete={onComplete}
      />,
    );

    await user.type(screen.getByLabelText("공유 키"), "opaque-test-key");
    await user.click(screen.getByRole("button", { name: "키 확인" }));
    await waitFor(() => expect(api.validateShareKey).toHaveBeenCalledOnce());
    expect(api.previewShareKey).toHaveBeenCalledOnce();
    expect(screen.getByText("팀 문서")).toBeVisible();
    expect(screen.getByText(/소유자 발급 기록이 확인되었습니다/)).toBeVisible();

    await user.click(screen.getByRole("button", { name: "찾아보기" }));
    await user.click(screen.getByRole("button", { name: "이 폴더 선택" }));
    await user.click(screen.getByRole("button", { name: "공유에 가입" }));
    await waitFor(() => expect(api.joinShare).toHaveBeenCalledOnce());
    expect(api.joinShare).toHaveBeenCalledWith(
      expect.any(String),
      "opaque-test-key",
      "/srv/receiver",
    );
    expect(onComplete).toHaveBeenCalledOnce();
    expect(screen.queryByDisplayValue("opaque-test-key")).not.toBeInTheDocument();
    expect(screen.queryByText("opaque-test-key")).not.toBeInTheDocument();
  });

  it("keeps offline validation explicit and stores a pending join", async () => {
    const user = userEvent.setup();
    const api = new Api();
    const offline = vi.fn(async () => {
      throw new ApiError("owner unavailable", 503, "offline");
    });
    configureJoinApi(api, offline);
    const pending: JoinResult = { ...joined, enrollment: "waiting", status: "waiting" };
    vi.spyOn(api, "joinShare").mockResolvedValue(pending);
    render(
      <ShareJoinFlow
        api={api}
        browse={async () => directory}
        onClose={() => {}}
        onComplete={async () => {}}
      />,
    );
    await user.type(screen.getByLabelText("공유 키"), "opaque-test-key");
    await user.click(screen.getByRole("button", { name: "키 확인" }));
    expect(await screen.findByText(/소유자가 오프라인입니다/)).toBeVisible();
    await user.click(screen.getByRole("button", { name: "찾아보기" }));
    await user.click(screen.getByRole("button", { name: "이 폴더 선택" }));
    expect(screen.getByRole("button", { name: "가입 대기로 저장" })).toBeEnabled();
    await user.click(screen.getByRole("button", { name: "가입 대기로 저장" }));
    expect(await screen.findByText("가입 요청을 안전하게 저장했습니다.")).toBeVisible();
    expect(api.joinShare).toHaveBeenCalledOnce();
  });

  it("blocks a revoked key and keeps join unavailable", async () => {
    const user = userEvent.setup();
    const api = new Api();
    const revoked = vi.fn(async () => {
      throw new ApiError("revoked", 422, "invitation_revoked");
    });
    configureJoinApi(api, revoked);
    render(
      <ShareJoinFlow
        api={api}
        browse={async () => directory}
        onClose={() => {}}
        onComplete={async () => {}}
      />,
    );
    await user.type(screen.getByLabelText("공유 키"), "opaque-test-key");
    await user.click(screen.getByRole("button", { name: "키 확인" }));
    expect(await screen.findByText("철회된 공유 키입니다.")).toBeVisible();
    expect(screen.getByRole("button", { name: "공유에 가입" })).toBeDisabled();
    expect(screen.queryByRole("button", { name: "찾아보기" })).not.toBeInTheDocument();
  });

  it("retries a lost join response with the same request id", async () => {
    const user = userEvent.setup();
    const api = new Api();
    configureJoinApi(api);
    const join = vi
      .spyOn(api, "joinShare")
      .mockRejectedValueOnce(new ApiError("timeout", 503, "offline"))
      .mockResolvedValueOnce(joined);
    render(
      <ShareJoinFlow
        api={api}
        browse={async () => directory}
        onClose={() => {}}
        onComplete={async () => {}}
      />,
    );
    await user.type(screen.getByLabelText("공유 키"), "opaque-test-key");
    await user.click(screen.getByRole("button", { name: "키 확인" }));
    await user.click(screen.getByRole("button", { name: "찾아보기" }));
    await user.click(screen.getByRole("button", { name: "이 폴더 선택" }));
    await user.click(screen.getByRole("button", { name: "공유에 가입" }));
    expect(await screen.findByText(/소유자에 연결할 수 없습니다/)).toBeVisible();
    await user.click(screen.getByRole("button", { name: "같은 요청 재시도" }));
    await waitFor(() => expect(join).toHaveBeenCalledTimes(2));
    expect(join.mock.calls[1][0]).toBe(join.mock.calls[0][0]);
    expect(screen.queryByDisplayValue("opaque-test-key")).not.toBeInTheDocument();
  });

  it("issues a display-once key without querying the key list", async () => {
    const user = userEvent.setup();
    const api = new Api();
    const created = vi.spyOn(api, "createShare").mockResolvedValue(share);
    const listKeys = vi.spyOn(api, "listKeys").mockResolvedValue([]);
    const issued: IssuedKey = {
      request_id: "issue-request",
      share_id: shareId,
      invitation_id: invitationId,
      permission: "read_only",
      expires_at: null,
      key: "opaque-issued-key",
    };
    const issue = vi.spyOn(api, "issueKey").mockResolvedValue(issued);
    render(
      <ShareCreateFlow
        api={api}
        browse={async () => directory}
        onClose={() => {}}
        onComplete={async () => {}}
      />,
    );
    await user.type(screen.getByLabelText("공유 이름"), "팀 문서");
    await user.type(screen.getByPlaceholderText("폴더를 찾아 선택하세요"), "/srv/team-docs");
    await user.click(screen.getByRole("button", { name: "폴더 공유 만들기" }));
    await waitFor(() => expect(created).toHaveBeenCalledOnce());
    await user.click(screen.getByRole("button", { name: /읽기 전용 키/ }));
    expect(await screen.findByText("opaque-issued-key")).toBeVisible();
    expect(issue).toHaveBeenCalledWith(shareId, expect.any(String), "read_only");
    expect(listKeys).not.toHaveBeenCalled();
  });

  it("uses a fresh request id for each acknowledged key issuance", async () => {
    const user = userEvent.setup();
    const api = new Api();
    const created = vi.spyOn(api, "createShare").mockResolvedValue(share);
    const issued = vi
      .spyOn(api, "issueKey")
      .mockResolvedValueOnce({
        request_id: "issue-one",
        share_id: shareId,
        invitation_id: invitationId,
        permission: "read_only",
        expires_at: null,
        key: "opaque-issued-key-one",
      })
      .mockResolvedValueOnce({
        request_id: "issue-two",
        share_id: shareId,
        invitation_id: invitationId,
        permission: "read_only",
        expires_at: null,
        key: "opaque-issued-key-two",
      });
    render(
      <ShareCreateFlow
        api={api}
        browse={async () => directory}
        onClose={() => {}}
        onComplete={async () => {}}
      />,
    );
    await user.type(screen.getByLabelText("공유 이름"), "팀 문서");
    await user.type(screen.getByPlaceholderText("폴더를 찾아 선택하세요"), "/srv/team-docs");
    await user.click(screen.getByRole("button", { name: "폴더 공유 만들기" }));
    await waitFor(() => expect(created).toHaveBeenCalledOnce());

    const issueButton = () => screen.getByRole("button", { name: /읽기 전용 키/ });
    await user.click(issueButton());
    expect(await screen.findByText("opaque-issued-key-one")).toBeVisible();
    await user.click(issueButton());
    expect(await screen.findByText("opaque-issued-key-two")).toBeVisible();

    expect(issued).toHaveBeenCalledTimes(2);
    expect(issued.mock.calls[1][1]).not.toBe(issued.mock.calls[0][1]);
  });

  it("uses a fresh request id for each acknowledged detail key issuance", async () => {
    const user = userEvent.setup();
    const api = new Api();
    vi.spyOn(api, "listKeys").mockResolvedValue([]);
    vi.spyOn(api, "listMembers").mockResolvedValue([]);
    const issued = vi
      .spyOn(api, "issueKey")
      .mockResolvedValueOnce({
        request_id: "detail-issue-one",
        share_id: shareId,
        invitation_id: invitationId,
        permission: "read_only",
        expires_at: null,
        key: "opaque-detail-key-one",
      })
      .mockResolvedValueOnce({
        request_id: "detail-issue-two",
        share_id: shareId,
        invitation_id: invitationId,
        permission: "read_only",
        expires_at: null,
        key: "opaque-detail-key-two",
      });
    render(
      <ShareDetailFlow
        share={share}
        api={api}
        onClose={() => {}}
        onRefresh={async () => {}}
      />,
    );

    const issueButton = () => screen.getByRole("button", { name: "읽기 전용 키 발급" });
    await user.click(issueButton());
    expect(await screen.findByText("opaque-detail-key-one")).toBeVisible();
    await user.click(issueButton());
    expect(await screen.findByText("opaque-detail-key-two")).toBeVisible();

    expect(issued).toHaveBeenCalledTimes(2);
    expect(issued.mock.calls[1][1]).not.toBe(issued.mock.calls[0][1]);
  });

  it("uses three request ids for pause, resume, and pause after acknowledgement", async () => {
    const user = userEvent.setup();
    const api = new Api();
    const command = vi.spyOn(api, "shareCommand").mockResolvedValue(share);
    const onRefresh = vi.fn(async () => {});
    const { rerender } = render(
      <ManagedSharesBoard
        shares={[share]}
        pending={[]}
        api={api}
        onCreate={() => {}}
        onJoin={() => {}}
        onDetail={() => {}}
        onRefresh={onRefresh}
      />,
    );

    await user.click(screen.getByRole("button", { name: "팀 문서 일시정지" }));
    await waitFor(() => expect(command).toHaveBeenCalledOnce());
    rerender(
      <ManagedSharesBoard
        shares={[{ ...share, status: "paused", phase: "paused" }]}
        pending={[]}
        api={api}
        onCreate={() => {}}
        onJoin={() => {}}
        onDetail={() => {}}
        onRefresh={onRefresh}
      />,
    );
    await user.click(screen.getByRole("button", { name: "팀 문서 재개" }));
    await waitFor(() => expect(command).toHaveBeenCalledTimes(2));
    rerender(
      <ManagedSharesBoard
        shares={[share]}
        pending={[]}
        api={api}
        onCreate={() => {}}
        onJoin={() => {}}
        onDetail={() => {}}
        onRefresh={onRefresh}
      />,
    );
    await user.click(screen.getByRole("button", { name: "팀 문서 일시정지" }));
    await waitFor(() => expect(command).toHaveBeenCalledTimes(3));

    const ids = command.mock.calls.map((call) => call[2]);
    expect(new Set(ids).size).toBe(3);
  });

  it("retries a lost command response with the same request id", async () => {
    const user = userEvent.setup();
    const api = new Api();
    const command = vi
      .spyOn(api, "shareCommand")
      .mockRejectedValueOnce(new ApiError("timeout", 503, "offline"))
      .mockResolvedValueOnce(share)
      .mockResolvedValueOnce(share);
    const { rerender } = render(
      <ManagedSharesBoard
        shares={[share]}
        pending={[]}
        api={api}
        onCreate={() => {}}
        onJoin={() => {}}
        onDetail={() => {}}
        onRefresh={async () => {}}
      />,
    );

    const pause = () => screen.getByRole("button", { name: "팀 문서 일시정지" });
    await user.click(pause());
    expect(await screen.findByText("소유자에 연결할 수 없습니다. 잠시 후 다시 시도하세요.")).toBeVisible();
    await user.click(pause());
    await waitFor(() => expect(command).toHaveBeenCalledTimes(2));
    expect(command.mock.calls[1][2]).toBe(command.mock.calls[0][2]);

    rerender(
      <ManagedSharesBoard
        shares={[share]}
        pending={[]}
        api={api}
        onCreate={() => {}}
        onJoin={() => {}}
        onDetail={() => {}}
        onRefresh={async () => {}}
      />,
    );
    await user.click(pause());
    await waitFor(() => expect(command).toHaveBeenCalledTimes(3));
    expect(command.mock.calls[2][2]).not.toBe(command.mock.calls[1][2]);
  });

  it("renders all eight managed statuses and separates registrations from live peers", () => {
    const statuses = Object.keys(managedStatusLabels) as Array<keyof typeof managedStatusLabels>;
    const shares = statuses.map((status, index) => ({
      ...share,
      share_id: `${index}`.padStart(64, "0"),
      name: `공유 ${status}`,
      status,
      connected_devices: index === 0 ? share.connected_devices : [],
      active_peer_count: index === 0 ? 1 : 0,
    }));
    const api = new Api();
    vi.spyOn(api, "shareCommand").mockResolvedValue(share);
    render(
      <ManagedSharesBoard
        shares={shares}
        pending={[]}
        api={api}
        onCreate={() => {}}
        onJoin={() => {}}
        onDetail={() => {}}
        onRefresh={async () => {}}
      />,
    );
    for (const status of statuses) expect(screen.getByText(managedStatusLabels[status])).toBeVisible();
    expect(screen.getAllByText("등록 멤버 수는 상세에서 확인 · 현재 연결과 별도 집계")).toHaveLength(8);
    expect(screen.getByText("현재 1대 연결 중")).toBeVisible();
  });

  it("rechecks long expiries after a bounded timer instead of overflowing", () => {
    vi.useFakeTimers();
    try {
      const start = new Date("2026-09-08T00:00:00Z");
      vi.setSystemTime(start);
      const expiresAt = Math.floor(start.getTime() / 1000) + 25 * 24 * 60 * 60;
      const onExpire = vi.fn();
      const cleanup = scheduleExpiry(expiresAt, onExpire);

      vi.advanceTimersByTime(MAX_EXPIRY_TIMER_MS);
      expect(onExpire).not.toHaveBeenCalled();

      vi.setSystemTime(expiresAt * 1000);
      vi.runOnlyPendingTimers();
      expect(onExpire).toHaveBeenCalledOnce();
      cleanup();
    } finally {
      vi.useRealTimers();
    }
  });
});
