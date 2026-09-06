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
});
