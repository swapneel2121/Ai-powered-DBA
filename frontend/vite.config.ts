import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
  build: {
    chunkSizeWarningLimit: 900,
    rollupOptions: {
      output: {
        // Split heavy vendors into their own content-hashed chunks so the
        // browser caches them separately and reloads only re-fetch app code.
        manualChunks: {
          react: ["react", "react-dom"],
          charts: ["recharts"],
          query: ["@tanstack/react-query"],
          icons: ["lucide-react"],
        },
      },
    },
  },
});
