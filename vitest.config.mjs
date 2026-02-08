import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      "open-sspm-app/": path.join(__dirname, "web/static/app/"),
    },
  },
  test: {
    environment: "jsdom",
    include: ["web/static/app/**/*.test.js"],
  },
});
