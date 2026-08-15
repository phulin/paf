import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const apiTarget = process.env.PAF_API_URL ?? "http://127.0.0.1:8000";

export default defineConfig({
  plugins: [react()],
  build: {
    // The release bundle is package data. `npm run build` is intentionally the
    // only step that needs Node; installing and running the Python package does
    // not rebuild the UI.
    outDir: "../src/paf/web_dist",
    emptyOutDir: true,
  },
  server: {
    port: 5173,
    proxy: { "/api": { target: apiTarget, changeOrigin: true } },
  },
  preview: {
    port: 4173,
    proxy: { "/api": { target: apiTarget, changeOrigin: true } },
  },
});
