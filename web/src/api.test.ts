import { afterEach, describe, expect, it, vi } from "vitest";
import { Api } from "./api";

afterEach(() => vi.unstubAllGlobals());
describe("authenticated API", () => {
  it("exchanges an access key and uses returned CSRF for folder controls", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    vi.stubGlobal("fetch", async (url: string, init?: RequestInit) => {
      calls.push({ url, init });
      return new Response(
        JSON.stringify(
          url.endsWith("/session")
            ? { authenticated: true, csrf_token: "csrf-example" }
            : { accepted: true },
        ),
        { status: 200 },
      );
    });
    const api = new Api();
    await api.login("administrator-key");
    await api.command("folder/one", "pause");
    expect(calls[0].init?.body).toBe(
      JSON.stringify({ token: "administrator-key" }),
    );
    expect(calls[1].url).toBe("/api/v1/folders/folder%2Fone/pause");
    expect(new Headers(calls[1].init?.headers).get("X-DeltaWeave-CSRF")).toBe(
      "csrf-example",
    );
    expect(calls.every((c) => c.init?.credentials === "same-origin")).toBe(
      true,
    );
    expect(localStorage.length).toBe(0);
  });
  it("reports structured server validation errors instead of treating rejection as success", async () => {
    vi.stubGlobal(
      "fetch",
      async () =>
        new Response(JSON.stringify({ error: "관리 폴더 경로가 겹칩니다." }), {
          status: 422,
        }),
    );
    await expect(
      new Api().request("/folders", { method: "POST", body: "{}" }),
    ).rejects.toThrow("관리 폴더 경로가 겹칩니다.");
  });

  it("keeps managed route names, key body mapping, encoded ids, and CSRF stable", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    vi.stubGlobal("fetch", async (url: string, init?: RequestInit) => {
      calls.push({ url, init });
      const body = url.endsWith("/session")
        ? { authenticated: true, csrf_token: "csrf-managed" }
        : {};
      return new Response(JSON.stringify(body), { status: 200 });
    });
    const api = new Api();
    await api.login("administrator-key");
    await api.previewShareKey("preview-request", "opaque-key");
    await api.validateShareKey("validate-request", "opaque-key");
    await api.joinShare("join-request", "opaque-key", "/srv/receiver");
    await api.resumeMembership("resume-request", "share/id");
    await api.shareCommand("share/id", "pause", "pause-request");
    await api.rotateKey("share/id", "invitation/id", "rotate-request");

    expect(calls.map(({ url }) => url)).toEqual([
      "/api/v1/session",
      "/api/v1/shares/preview",
      "/api/v1/shares/validate",
      "/api/v1/shares/join",
      "/api/v1/shares/resume",
      "/api/v1/shares/share%2Fid/pause",
      "/api/v1/shares/share%2Fid/keys/invitation%2Fid/rotate",
    ]);
    expect(JSON.parse(String(calls[1].init?.body))).toEqual({
      request_id: "preview-request",
      key: "opaque-key",
    });
    expect(JSON.parse(String(calls[3].init?.body))).toEqual({
      request_id: "join-request",
      key: "opaque-key",
      destination_root: "/srv/receiver",
    });
    for (const call of calls.slice(1)) {
      expect(new Headers(call.init?.headers).get("X-DeltaWeave-CSRF")).toBe(
        "csrf-managed",
      );
      expect(call.init?.credentials).toBe("same-origin");
    }
  });
});
