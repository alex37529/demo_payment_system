import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// phase-6 spec §4.5: port 5173 (NOT 3000 — 3000 is Grafana, tz_v2 §4.1).
// Dev: /v1 is proxied to the gateway (8000) so the browser never crosses
// origins (pitfall #8: no CORS needed in dev). Prod: nginx.conf does the same.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/v1": { target: "http://localhost:8000", changeOrigin: true },
    },
  },
  build: { outDir: "dist" },
});
