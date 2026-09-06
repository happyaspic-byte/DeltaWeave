import { expect, it, vi, afterEach } from "vitest";
import { Api, consumeBootstrap } from "./api";
afterEach(() => {
  vi.unstubAllGlobals();
  history.replaceState(null, "", "/");
});
it("removes bootstrap secret before exchanging it and never keeps it in browser storage", async () => {
  history.replaceState(null, "", "/#bootstrap=single-use-key");
  const login = vi.fn(async (_token: string) => {
    expect(location.hash).toBe("");
    return { authenticated: true, csrf_token: "csrf" };
  });
  expect(await consumeBootstrap({ login } as unknown as Api)).toEqual({
    authenticated: true,
    csrf_token: "csrf",
  });
  expect(login).toHaveBeenCalledWith("single-use-key");
  expect(localStorage.length).toBe(0);
});
it("treats unauthorized state as session expiration", async () => {
  vi.stubGlobal(
    "fetch",
    async () =>
      new Response(JSON.stringify({ error: "Session expired" }), {
        status: 401,
      }),
  );
  await expect(new Api().request("/state")).rejects.toMatchObject({
    status: 401,
  });
});
