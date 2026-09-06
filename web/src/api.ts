import type { Session } from "./types";

export class ApiError extends Error {
  constructor(
    message: string,
    public status: number,
  ) {
    super(message);
    this.name = "ApiError";
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
      const error =
        data && typeof data === "object" && "error" in data
          ? String(data.error)
          : `요청을 처리하지 못했습니다. (${response.status})`;
      throw new ApiError(error, response.status);
    }
    return data as T;
  }
  async command(id: string, command: "sync" | "pause" | "resume") {
    return this.request<{ accepted: boolean }>(
      `/folders/${encodeURIComponent(id)}/${command}`,
      { method: "POST" },
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
