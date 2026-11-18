import { copyFileSync, mkdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { test, expect, type Page, type Route } from '@playwright/test'

// A stopped warehouse says so (stardelt-nova#1059): the SQL editor names the engine's state,
// offers **Start now** ahead of the first query, and says how long starting takes. The words
// are fixed in the PO review on platform#131.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

/** An absolute path, so the screenshot lands in the repository from any working directory. */
const here = (path: string) => fileURLToPath(new URL(path, import.meta.url))

const NOW = Date.UTC(2026, 10, 16, 9, 0, 0)

const ME = {
  user: 'alice', auth_mode: 'oidc', demo: false, warehouse: 'warehouse',
  is_admin: false, groups: ['analysts'], via: 'session',
}

/** The caller's class, with the scaler's word on the shared engine (`scale`). */
const warehouse = (scale: unknown) => [
  {
    name: 'small', displayName: 'Small', size: 'small', running: 10, queued: 100,
    memoryShare: '20%', default: true, canUse: true, yours: true,
    scale,
  },
]

/** `Stopped`, `Starting` and `Running` as the scaler writes them (`status.scale`). */
const STOPPED = {
  phase: 'Stopped', workers: 0, queriesInFlight: 0,
  lastQueryAt: new Date(NOW - 3_600_000).toISOString(),
}
const STARTING = { phase: 'Starting', workers: 0, queriesInFlight: 0, lastQueryAt: STOPPED.lastQueryAt }

/**
 * One page with `/api/warehouses` answered by `rows` and the start call recorded in `posted`.
 * A start has a short delay so its `Starting…` button can be seen (the real one waits for the
 * scaler's tick, up to two minutes).
 */
async function open(page: Page, rows: unknown[], posted: string[] = []) {
  await page.route('**/api/**', async (r) => {
    const url = new URL(r.request().url())
    if (url.pathname === '/api/me') return json(r, ME)
    if (url.pathname === '/api/warehouses') return json(r, rows)
    if (r.request().method() === 'POST' && url.pathname === '/api/warehouses/small/start') {
      posted.push(url.pathname)
      await new Promise((resolve) => setTimeout(resolve, 300))
      return json(r, { name: 'small', phase: 'Starting' }, 202)
    }
    return json(r, [])
  })
  await page.setViewportSize({ width: 1280, height: 800 })
  await page.emulateMedia({ colorScheme: 'dark' })
  await page.addInitScript(() => {
    try {
      localStorage.setItem('stardelt-nova:theme', 'dark')
    } catch {
      /* ignore */
    }
  })
  await page.goto('/query')
}

test('a stopped engine says it starts on the first query, and Start now sends the call', async ({ page }) => {
  const posted: string[] = []
  await open(page, warehouse(STOPPED), posted)

  const note = page.locator('main').getByTitle('10 running, 100 queued, 20% memory')
  await expect(note).toContainText('Warehouse: Small')
  await expect(note).toContainText('Stopped (starts on the first query)')
  const start = page.getByRole('button', { name: 'Start now' })
  await expect(start).toBeVisible()

  mkdirSync(here('../../docs/screenshots/1059'), { recursive: true })
  await page.screenshot({ path: here('screenshots/warehouse-stopped-1280-dark.png') })
  copyFileSync(here('screenshots/warehouse-stopped-1280-dark.png'), here('../../docs/screenshots/1059/warehouse-stopped-1280-dark.png'))

  await start.click()
  // The call is in flight: the button says so instead of taking a second click.
  await expect(page.getByRole('button', { name: 'Starting…' })).toBeVisible()
  expect(posted).toEqual(['/api/warehouses/small/start'])
})

test('a starting engine says how long it takes', async ({ page }) => {
  await open(page, warehouse(STARTING))
  const note = page.locator('main').getByTitle('10 running, 100 queued, 20% memory')
  await expect(note).toContainText('Starting — this can take up to 3 minutes')
  await expect(page.getByRole('button', { name: 'Start now' })).toHaveCount(0)
})

test('an engine the scaler has not reached shows the plain line only', async ({ page }) => {
  await open(page, warehouse(null))
  const note = page.locator('main').getByTitle('10 running, 100 queued, 20% memory')
  await expect(note).toHaveText('Warehouse: Small')
  await expect(page.getByRole('button', { name: 'Start now' })).toHaveCount(0)
  await expect(page.locator('main')).not.toContainText('Stopped')
})
