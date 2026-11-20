import { copyFileSync, mkdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { expect, test, type Route } from '@playwright/test'

// The page against the real backend in its no-cluster dev mode (`playwright.config.ts`):
// `state.kube` is None, so `GET /api/compute` answers the one synthetic `shared` row.

test('the Compute page lists the shared cluster and offers no buttons for it', async ({ page }) => {
  await page.goto('/compute')
  // The header names the page (#337): without the map entry it reads "Not found".
  await expect(page.locator('header')).toContainText('Compute')
  const main = page.locator('main')
  await expect(main.getByRole('heading', { name: 'Compute' })).toBeVisible()
  await expect(main.getByText('Shared', { exact: true })).toBeVisible()
  // The page's own sentence also says "always on", so the card's line is the assertion.
  await expect(main.getByText('Shared · always on · scales executors to zero after 60 s idle')).toBeVisible()
  await expect(page.getByRole('button', { name: 'Start' })).toHaveCount(0)
})

test('the sidebar links to Compute', async ({ page }) => {
  await page.goto('/')
  await page.locator('#primary-nav').getByRole('link', { name: 'Compute' }).click()
  await expect(page.locator('main').getByRole('heading', { name: 'Compute' })).toBeVisible()
})

test('an administrator opens the new-cluster dialog and it validates a bad name', async ({ page }) => {
  await page.goto('/compute')
  await page.getByRole('button', { name: 'New cluster' }).click()
  const dialog = page.locator('dialog')
  await expect(dialog.getByRole('heading', { name: 'New cluster' })).toBeVisible()
  await dialog.getByLabel('Name', { exact: true }).fill('Bad Name')
  await expect(dialog.getByText(/use lower-case letters, digits and dashes/)).toBeVisible()
  await expect(dialog.getByRole('button', { name: 'Save' })).toBeDisabled()
  await page.keyboard.press('Escape')
  await expect(dialog).toHaveCount(0)
})

test('the new-cluster dialog validates the viewers field', async ({ page }) => {
  await page.goto('/compute')
  await page.getByRole('button', { name: 'New cluster' }).click()
  const dialog = page.locator('dialog')
  await dialog.getByLabel('Viewers').fill('not a group')
  await expect(dialog.getByText(/a group of your sign-in provider/)).toBeVisible()
  await expect(dialog.getByRole('button', { name: 'Save' })).toBeDisabled()
  await dialog.getByLabel('Viewers').fill('visitors, analytics')
  await expect(dialog.getByText(/a group of your sign-in provider/)).toHaveCount(0)
  await page.keyboard.press('Escape')
})

test('the shared card is editable but not deletable', async ({ page }) => {
  await page.goto('/compute')
  await page.getByRole('button', { name: 'Edit' }).click()
  const dialog = page.locator('dialog')
  await expect(dialog.getByRole('heading', { name: 'Edit shared' })).toBeVisible()
  await expect(dialog.getByRole('button', { name: 'Delete' })).toHaveCount(0)
  await dialog.getByRole('button', { name: 'Cancel' }).click()
  await expect(dialog).toHaveCount(0)
})

// Compute → SQL (#605): the class this person's queries run in, and every class the engine
// defines. The sidebar card no longer carries the class.

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

test('the SQL tab lists the classes, yours first', async ({ page }) => {
  await page.route('**/api/me', (route) => json(route, ME))
  await page.route('**/api/warehouses', (route) => json(route, ROWS))
  await page.setViewportSize({ width: 1280, height: 800 })
  await page.addInitScript(() => {
    try {
      localStorage.setItem('stardelt-nova:theme', 'dark')
    } catch {
      /* ignore */
    }
  })
  await page.goto('/compute?tab=sql')
  const main = page.locator('main')
  await expect(page.getByRole('tab', { name: 'SQL' })).toHaveAttribute('aria-selected', 'true')
  await expect(main.getByText('The SQL engine and the classes your queries run in.')).toBeVisible()
  const small = main.getByRole('row', { name: /^small/ })
  await expect(small.getByText('yours', { exact: true })).toBeVisible()
  await expect(small.getByText('Stopped', { exact: true })).toBeVisible()
  const large = main.getByRole('row', { name: /Large/ })
  await expect(large.getByText('Starting', { exact: true })).toBeVisible()
  await expect(main.getByText('Start now')).toBeVisible()
  mkdirSync(here('../../docs/screenshots/compute-sql'), { recursive: true })
  await page.screenshot({ path: here('test-results/compute-sql-1280-dark.png'), fullPage: true })
  copyFileSync(here('test-results/compute-sql-1280-dark.png'), here('../../docs/screenshots/compute-sql/compute-sql-1280-dark.png'))
})

test('the sidebar card no longer shows the SQL class', async ({ page }) => {
  await page.goto('/')
  await expect(page.locator('#primary-nav').locator('..').getByText(/^SQL: /)).toHaveCount(0)
})
