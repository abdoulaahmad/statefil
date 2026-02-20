import { defineConfig } from "vitest/config";
import path from "node:path";
import { fileURLToPath } from "node:url";

const dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      "@statefabric/core": path.resolve(dirname, "../core/src/index.ts")
    }
  },
  test: {
    include: ["test/**/*.spec.ts"]
  }
});
