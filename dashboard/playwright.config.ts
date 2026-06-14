import { defineConfig, devices } from "@playwright/test";

// The dashboard talks to Hookline only through its server proxy at
// /api/hookline/*. The e2e tests intercept those proxy calls with fixture data,
// so they exercise the full UI (rendering, filtering, replay) deterministically
// without needing a running Hookline backend.
export default defineConfig({
  testDir: "./e2e",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? "github" : "list",
  use: {
    baseURL: "http://localhost:3001",
    trace: "on-first-retry",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: "npm run dev",
    url: "http://localhost:3001",
    reuseExistingServer: !process.env.CI,
    timeout: 120_000,
  },
});
