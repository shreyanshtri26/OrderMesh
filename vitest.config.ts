import { defineConfig } from "vitest/config";

// Two projects:
//   unit         pure logic, no network, always runs (pnpm test)
//   integration  needs a real Postgres in TEST_DATABASE_URL (pnpm test:integration)
export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: "unit",
          environment: "node",
          include: ["packages/*/test/**/*.test.ts", "services/*/test/**/*.test.ts"],
          exclude: ["**/*.int.test.ts"],
        },
      },
      {
        test: {
          name: "integration",
          environment: "node",
          include: ["packages/*/test/**/*.int.test.ts", "services/*/test/**/*.int.test.ts"],
          globalSetup: ["./test/postgres.global-setup.ts"],
          exclude: ["**/node_modules/**"],
          fileParallelism: false,
        },
      },
    ],
  },
});
