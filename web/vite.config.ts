import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8390",
        changeOrigin: true,
        configure(proxy) {
          // The dev server presents the UI and API as one origin. Keep the
          // upstream request on the control server's explicit local origin.
          proxy.on("proxyReq", (request, incoming) => {
            if (
              incoming.headers.origin === `http://${incoming.headers.host}`
            ) {
              request.setHeader("Origin", "http://127.0.0.1:8390");
            }
          });
        },
      },
    },
  },
  test: {
    environment: "jsdom",
    setupFiles: ["./src/test-setup.ts"],
    restoreMocks: true,
  },
});
