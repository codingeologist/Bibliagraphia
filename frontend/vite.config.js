import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

const apiPaths = ["/health", "/search", "/traverse", "/path", "/graph", "/map", "/verse", "/chapter", "/reader"];

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: Object.fromEntries(
      apiPaths.map((path) => [path, "http://127.0.0.1:8000"]),
    ),
  },
});
