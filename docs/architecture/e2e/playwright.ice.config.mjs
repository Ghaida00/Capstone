/** @type {import('@playwright/test').PlaywrightTestConfig} */
export default {
  testDir: ".",
  testMatch: "architecture-tour.spec.mjs",
  use: {
    baseURL: "http://127.0.0.1:9876",
    viewport: { width: 1440, height: 900 },
  },
  webServer: {
    command: "npx --yes serve -l 9876 .",
    cwd: "..",
    url: "http://127.0.0.1:9876/architecture-tour.html",
    reuseExistingServer: true,
    timeout: 120_000,
  },
  timeout: 45_000,
};
