// explicit index.mjs: bare directory import fails under Node ESM here
import { test, expect } from "../../dissemination/e2e/node_modules/@playwright/test/index.mjs";
const URL = process.env.ICEPANEL_URL || "http://127.0.0.1:9876/peakload-icepanel.html";
test.beforeEach(async ({ page }) => {
  page.on("pageerror", e => { throw new Error("pageerror: " + e.message); });
  await page.goto(URL, { waitUntil: "load" });
});

test("T1: chrome renders, camera pans and zooms", async ({ page }) => {
  await expect(page.locator(".topbar")).toBeVisible();
  await expect(page.locator(".canvas-card")).toBeVisible();
  await expect(page.locator("#stage")).toHaveCount(1);
  await expect(page.locator("#fx")).toHaveCount(1);          // particle canvas
  const before = await page.evaluate(() => icepanel.cam.get());
  await page.mouse.move(700, 450); await page.mouse.down();
  await page.mouse.move(800, 500); await page.mouse.up();
  const after = await page.evaluate(() => icepanel.cam.get());
  expect(after.x).not.toBe(before.x);
  await page.mouse.wheel(0, -240);
  const zoomed = await page.evaluate(() => icepanel.cam.get());
  expect(zoomed.scale).toBeGreaterThan(after.scale);
});
