import { copyFileSync, mkdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { expect, test, type Route } from '@playwright/test'

// The Platform page shows each class's scale state (stardelt-nova#1061): the page #1059's
// five-minute error sends an administrator to. The rows come from `status.scale` on
// `GET /api/warehouses` (#1054); the words are #1059's.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

/** An absolute path, so the screenshot lands in the repository from any working directory. */
const here = (path: string) => fileURLToPath(new URL(path, import.meta.url))

const ME = { user: 'bob', auth_mode: 'oidc', demo: false, warehouse: 'warehouse', is_admin: true, groups: ['admins'] }

const ROWS = [
  {
    name: 'small', displayName: null, size: 'xs', running: 1, queued: 0, memoryShare: '10%',
    default: true, canUse: true, yours: true,
    scale: { phase: 'Stopped', workers: 0, queriesInFlight: 0, lastQueryAt: '2026-11-16T06:00:00Z' },
  },
  {
    name: 'large', displayName: 'Large', size: 'large', running: 1, queued: 0, memoryShare: '40%',
    default: false, canUse: true, yours: false,
    scale: { phase: 'Starting', workers: 1, queriesInFlight: 2 },
  },
]

test('the Platform page shows each class its scale state', async ({ page }) => {
  await page.route('**/api/me', (route) => json(route, ME))
  await page.route('**/api/warehouses', (route) => json(route, ROWS))
  // The screenshot is the dark theme at 1280 wide, as the other page shots are.
  await page.setViewportSize({ width: 1280, height: 800 })
  await page.addInitScript(() => {
    try {
      localStorage.setItem('stardelt-nova:theme', 'dark')
    } catch {
      /* ignore */
    }
  })
  await page.goto('/platform')
  const main = page.locator('main')
  await expect(main.getByRole('heading', { name: 'Platform' })).toBeVisible()
  await expect(main.getByText('Stopped', { exact: true })).toBeVisible()
  await expect(main.getByText('Starting', { exact: true })).toBeVisible()
  await expect(main.getByText('2026-11-16T06:00:00Z')).toBeVisible()
  await expect(main.getByRole('cell', { name: 'Large' })).toBeVisible()
  // The shell scrolls `main` itself, so `fullPage` still shows one viewport: bring the
  // Query engine card into it before the shot.
  await main.getByRole('heading', { name: 'Query engine' }).scrollIntoViewIfNeeded()
  mkdirSync(here('../../docs/screenshots/1061'), { recursive: true })
  await page.screenshot({ path: here('test-results/platform-warehouses-1280-dark.png'), fullPage: true })
  copyFileSync(here('test-results/platform-warehouses-1280-dark.png'), here('../../docs/screenshots/1061/platform-warehouses-1280-dark.png'))
})
