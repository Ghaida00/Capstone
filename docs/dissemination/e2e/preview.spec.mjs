// Run from repo root: npx playwright test docs/dissemination/e2e/preview.spec.mjs
// Requires: npm init -y && npx playwright install chromium (one-time)
import { test, expect } from '@playwright/test';

const PREVIEW = '/docs/dissemination/preview.html';

test.describe('Peakload dissemination preview', () => {
  test.use({ baseURL: 'http://127.0.0.1:8765' });

  test('feature tabs switch content', async ({ page }) => {
    await page.goto(`${PREVIEW}#fitur`);
    await expect(page.locator('#pl-tab-core')).toHaveAttribute('aria-selected', 'true');
    await expect(page.locator('#pl-panel-core')).toBeVisible();
    await expect(page.locator('#pl-panel-core h3')).toHaveText('Core Transaction');

    await page.locator('#pl-tab-resilience').click();
    await expect(page.locator('#pl-tab-resilience')).toHaveAttribute('aria-selected', 'true');
    await expect(page.locator('#pl-panel-resilience')).toBeVisible();
    await expect(page.locator('#pl-panel-resilience h3')).toHaveText('Resilience & HA');
    await expect(page.locator('#pl-panel-resilience img')).toHaveAttribute('src', /docker-containers/);

    await page.locator('#pl-tab-ops').click();
    await expect(page.locator('#pl-tab-ops')).toHaveAttribute('aria-selected', 'true');
    await expect(page.locator('#pl-panel-ops')).toBeVisible();
    await expect(page.locator('#pl-panel-ops h3')).toHaveText('Ops & Observability');
    await expect(page.locator('#pl-panel-ops img')).toHaveAttribute('src', /grafana-dashboard/);
  });

  test('repo cards layout and local links', async ({ page }) => {
    await page.goto(`${PREVIEW}#demo`);
    await expect(page.locator('.pl-repo-item')).toHaveCount(4);
    await expect(page.locator('a.pl-repo-card a')).toHaveCount(0);
    await expect(page.locator('a[data-pl-local="swagger-preview.html"]')).toHaveAttribute(
      'href',
      'swagger-preview.html'
    );
    const code = page.locator('.pl-code');
    await expect(code).toContainText('git clone');
    await expect(code).toContainText('docker compose up');
  });

  test('all curated images return 200', async ({ page, request }) => {
    await page.goto(PREVIEW);
    const srcs = await page.locator('.pl-page img').evaluateAll((imgs) =>
      [...new Set(imgs.map((i) => i.getAttribute('src')))]
    );
    for (const src of srcs) {
      const res = await request.get(`/docs/dissemination/${src}`);
      expect(res.status(), src).toBe(200);
    }
  });

  test('swagger preview loads', async ({ page }) => {
    await page.goto('/docs/dissemination/swagger-preview.html');
    await expect(page.locator('#swagger-ui')).toBeVisible();
    await expect(page.locator('.opblock').first()).toBeVisible();
  });
});
