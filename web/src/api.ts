import type {
  AppSnapshot,
  IssuedKey,
  JoinResult,
  KeyPreview,
  KeySummary,
  MemberView,
  MutationResult,
  Permission,
  Session,
  ShareCommandResult,
  ShareView,
} from "./types";

export class ApiError extends Error {
  constructor(
    message: string,
    public status: number,
    public code?: string,
    public requestId?: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

let requestSequence = 0;

/** Creates an opaque operation id without using storage, URL state, or logs. */
export function newRequestId(prefix = "qsync") {
  requestSequence += 1;
  const random =
    typeof globalThis.crypto?.randomUUID === "function"
      ? globalThis.crypto.randomUUID().replaceAll("-", "")
      : Math.random().toString(36).slice(2);
  return `${prefix}-${Date.now().toString(36)}-${requestSequence.toString(36)}-${random}`;
}

/** Keeps one request id for retries of an unchanged operation. */
export class OperationRequest {
  private id = "";
  private fingerprint = "";

  begin(fingerprint: string) {
    if (this.id && this.fingerprint === fingerprint) return this.id;
    this.id = newRequestId();
    this.fingerprint = fingerprint;
    return this.id;
  }
  retry(fingerprint: string) {
    return this.begin(fingerprint);
  }
  reset() {
    this.id = "";
    this.fingerprint = "";
  }
}

export class Api {
  private csrf = "";
  onUnauthorized?: () => void;
  async session(): Promise<Session> {
    const session = await this.request<Session>("/session");
    this.csrf = session.csrf_token ?? "";
    return session;
  }
  async login(token: string): Promise<Session> {
    const session = await this.request<Session>("/session", {
      method: "POST",
      body: JSON.stringify({ token }),
    });
    this.csrf = session.csrf_token ?? "";
    if (!session.authenticated)
      throw new Error("관리자 접근 키를 확인해 주세요.");
    return session;
  }
  async logout() {
    await this.request("/session", { method: "DELETE" });
    this.csrf = "";
  }
  async request<T = unknown>(path: string, init: RequestInit = {}): Promise<T> {
    const headers = new Headers(init.headers);
    if (init.body) headers.set("Content-Type", "application/json");
    if (
      init.method &&
      !["GET", "HEAD"].includes(init.method.toUpperCase()) &&
      this.csrf
    )
      headers.set("X-DeltaWeave-CSRF", this.csrf);
    const response = await fetch(`/api/v1${path}`, {
      ...init,
      headers,
      credentials: "same-origin",
    });
    const text = await response.text();
    let data: unknown;
    try {
      data = text ? JSON.parse(text) : undefined;
    } catch {
      data = undefined;
    }
    if (!response.ok) {
      if (response.status === 401 && path !== "/session")
        this.onUnauthorized?.();
      const payload =
        data && typeof data === "object"
          ? (data as Record<string, unknown>)
          : null;
      const error =
        payload && typeof payload.error === "string"
          ? payload.error
          : `요청을 처리하지 못했습니다. (${response.status})`;
      throw new ApiError(
        error,
        response.status,
        typeof payload?.error_code === "string"
          ? payload.error_code
          : undefined,
        typeof payload?.request_id === "string"
          ? payload.request_id
          : undefined,
      );
    }
    return data as T;
  }
  async command(id: string, command: "sync" | "pause" | "resume") {
    return this.request<{ accepted: boolean }>(
      `/folders/${encodeURIComponent(id)}/${command}`,
      { method: "POST" },
    );
  }

  async state() {
    return this.request<AppSnapshot>("/state");
  }
  async listShares() {
    return this.request<ShareView[]>("/shares");
  }
  async createShare(input: {
    request_id: string;
    name: string;
    root: string;
    min_free_space_mib?: number;
  }) {
    return this.request<ShareView>("/shares", {
      method: "POST",
      body: JSON.stringify(input),
    });
  }
  async previewShareKey(request_id: string, key: string) {
    return this.request<KeyPreview>("/shares/preview", {
      method: "POST",
      body: JSON.stringify({ request_id, key }),
    });
  }
  async validateShareKey(request_id: string, key: string) {
    return this.request<KeyPreview>("/shares/validate", {
      method: "POST",
      body: JSON.stringify({ request_id, key }),
    });
  }
  async joinShare(request_id: string, key: string, destination_root: string) {
    return this.request<JoinResult>("/shares/join", {
      method: "POST",
      body: JSON.stringify({ request_id, key, destination_root }),
    });
  }
  async resumeMembership(request_id: string, share_id: string) {
    return this.request<JoinResult>("/shares/resume", {
      method: "POST",
      body: JSON.stringify({ request_id, share_id }),
    });
  }
  async share(shareId: string) {
    return this.request<ShareView>(`/shares/${encodeURIComponent(shareId)}`);
  }
  async listKeys(shareId: string) {
    return this.request<KeySummary[]>(
      `/shares/${encodeURIComponent(shareId)}/keys`,
    );
  }
  async issueKey(
    shareId: string,
    request_id: string,
    permission: Permission,
    expires_at?: number,
  ) {
    return this.request<IssuedKey>(
      `/shares/${encodeURIComponent(shareId)}/keys`,
      {
        method: "POST",
        body: JSON.stringify({
          request_id,
          permission,
          ...(expires_at === undefined ? {} : { expires_at }),
        }),
      },
    );
  }
  async rotateKey(
    shareId: string,
    invitationId: string,
    request_id: string,
    expires_at?: number,
  ) {
    return this.request<IssuedKey>(
      `/shares/${encodeURIComponent(shareId)}/keys/${encodeURIComponent(invitationId)}/rotate`,
      {
        method: "POST",
        body: JSON.stringify({
          request_id,
          ...(expires_at === undefined ? {} : { expires_at }),
        }),
      },
    );
  }
  async revokeKey(shareId: string, invitationId: string, request_id: string) {
    return this.request<MutationResult>(
      `/shares/${encodeURIComponent(shareId)}/keys/${encodeURIComponent(invitationId)}/revoke`,
      { method: "POST", body: JSON.stringify({ request_id }) },
    );
  }
  async listMembers(shareId: string) {
    return this.request<MemberView[]>(
      `/shares/${encodeURIComponent(shareId)}/members`,
    );
  }
  async revokeMember(shareId: string, memberId: string, request_id: string) {
    return this.request<MutationResult>(
      `/shares/${encodeURIComponent(shareId)}/members/${encodeURIComponent(memberId)}/revoke`,
      { method: "POST", body: JSON.stringify({ request_id }) },
    );
  }
  async removeShare(shareId: string, request_id: string) {
    return this.request<MutationResult>(
      `/shares/${encodeURIComponent(shareId)}`,
      { method: "DELETE", body: JSON.stringify({ request_id }) },
    );
  }
  async shareCommand(
    shareId: string,
    command: "sync" | "pause" | "resume",
    request_id: string,
  ) {
    return this.request<ShareCommandResult>(
      `/shares/${encodeURIComponent(shareId)}/${command}`,
      { method: "POST", body: JSON.stringify({ request_id }) },
    );
  }
}

export async function consumeBootstrap(api: Api): Promise<Session | null> {
  const params = new URLSearchParams(location.hash.slice(1));
  const token = params.get("bootstrap");
  if (!token) return null;
  // Clear the one-use key before an asynchronous operation or error can leave it visible.
  history.replaceState(
    history.state,
    "",
    `${location.pathname}${location.search}`,
  );
  return api.login(token);
}
