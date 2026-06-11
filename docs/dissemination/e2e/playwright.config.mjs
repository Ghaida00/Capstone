/** @type {import('@playwright/test').PlaywrightTestConfig} */
export default {
  testDir: '.',
  testMatch: '*.spec.mjs',
  use: {
    baseURL: 'http://127.0.0.1:8765',
    viewport: { width: 1280, height: 900 },
  },
  webServer: {
    command: 'python -m http.server 8765',
    cwd: '../../..',
    url: 'http://127.0.0.1:8765/docs/dissemination/preview.html',
    reuseExistingServer: true,
    timeout: 120_000,
  },
};
