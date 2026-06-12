// Audits every <img> on the dissemination preview page.
import { test, expect } from '@playwright/test';

const PREVIEW = '/docs/dissemination/preview.html';
const ASSET_PREFIX = '/docs/dissemination/';

/** Assets referenced in wordpress-blocks.html (unique). */
const EXPECTED_ASSETS = [
  'assets/curated/postman-202.png',
  'assets/curated/dashboard-metrics.png',
  'assets/curated/docker-containers.png',
  'assets/curated/grafana-dashboard.png',
  'assets/curated/prometheus-targets.png',
  'assets/curated/dashboard-submit.png',
  'assets/curated/github-repo.png',
];

test.describe('Dissemination page — image audit', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(PREVIEW);
    await page.waitForSelector('.pl-page');
  });

  test('every img returns HTTP 200 and renders with non-zero dimensions', async ({ page, request }) => {
    // Unhide tab panels so lazy-loaded tab images are in DOM and can load.
    await page.evaluate(() => {
      document.querySelectorAll('[data-pl-panel]').forEach((p) => {
        p.hidden = false;
      });
    });

    const imgs = page.locator('.pl-page img');
    const count = await imgs.count();
    expect(count, 'expected at least one image on page').toBeGreaterThan(0);

    const seen = new Set();
    const report = [];

    for (let i = 0; i < count; i += 1) {
      const img = imgs.nth(i);
      await img.scrollIntoViewIfNeeded();

      const meta = await img.evaluate((el) => ({
        src: el.getAttribute('src'),
        alt: el.getAttribute('alt'),
        section: el.closest('section')?.id ?? 'unknown',
      }));

      const rel = meta.src.replace(/^https?:\/\/[^/]+\/docs\/dissemination\//, '');
      seen.add(rel);

      const url = ASSET_PREFIX + rel;
      const res = await request.get(url);
      expect(res.status(), `${rel} HTTP status`).toBe(200);

      const body = await res.body();
      expect(body.length, `${rel} body size`).toBeGreaterThan(1000);
      expect(body[0], `${rel} PNG magic byte`).toBe(0x89);

      await expect(img, `${rel} visible`).toBeVisible();

      const dims = await img.evaluate((el) => ({
        complete: el.complete,
        w: el.naturalWidth,
        h: el.naturalHeight,
      }));

      expect(dims.complete, `${rel} img.complete`).toBe(true);
      expect(dims.w, `${rel} naturalWidth`).toBeGreaterThan(0);
      expect(dims.h, `${rel} naturalHeight`).toBeGreaterThan(0);

      report.push({ ...meta, rel, ...dims, bytes: body.length });
    }

    expect(seen.size, 'unique asset count').toBe(EXPECTED_ASSETS.length);
    for (const asset of EXPECTED_ASSETS) {
      expect(seen.has(asset), `missing on page: ${asset}`).toBe(true);
    }

    // eslint-disable-next-line no-console
    console.log(JSON.stringify({ total: count, unique: seen.size, report }, null, 2));
  });

  test('hidden tab panel images load when tabs are activated', async ({ page }) => {
    await page.goto(`${PREVIEW}#fitur`);

    for (const tab of ['core', 'resilience', 'ops']) {
      await page.locator(`#pl-tab-${tab}`).click();
      const panel = page.locator(`#pl-panel-${tab}`);
      await expect(panel).toBeVisible();

      const img = panel.locator('img');
      await img.scrollIntoViewIfNeeded();
      await expect(img).toBeVisible();

      const dims = await img.evaluate((el) => ({
        w: el.naturalWidth,
        h: el.naturalHeight,
      }));
      expect(dims.w, `${tab} tab image width`).toBeGreaterThan(0);
      expect(dims.h, `${tab} tab image height`).toBeGreaterThan(0);
    }
  });

  test('curated assets on disk match HTML references', async ({ request }) => {
    for (const rel of EXPECTED_ASSETS) {
      const res = await request.get(ASSET_PREFIX + rel);
      expect(res.status(), rel).toBe(200);
      const ct = res.headers()['content-type'] ?? '';
      expect(ct, `${rel} content-type`).toMatch(/image\/png/);
    }
  });
});
