import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

const apiPaths = ["/mcp", "/health", "/search", "/traverse", "/path", "/graph", "/map/points", "/map/places", "/verse", "/chapter", "/reader", "/place"];

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const proxyTarget = env.API_PROXY_TARGET || "http://127.0.0.1:8000";

  return {
    plugins: [react(), tailwindcss()],
    server: {
      watch: {
        usePolling: env.VITE_USE_POLLING === "true",
      },
      proxy: Object.fromEntries(
        apiPaths.map((path) => [path, proxyTarget]),
      ),
    },
  };
});
