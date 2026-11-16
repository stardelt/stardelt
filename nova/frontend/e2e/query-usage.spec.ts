import { copyFileSync, mkdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { test, expect, type Route } from '@playwright/test'

// The Usage view of the Query log (platform#138 slice 2): who used the most compute, how it
// adds up per day, and the window's totals — under the same scope switch and time range as
// the queries themselves.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

/** An absolute path, so the screenshot lands in the repository from any working directory. */
const here = (path: string) => fileURLToPath(new URL(path, import.meta.url))

const NOW = Date.UTC(2026, 10, 16, 9, 0, 0)
const FROM = NOW - 7 * 86_400_000

const row = (key: string, queries: number, failed: number, cpuMs: number, inputBytes: number, writtenBytes: number) => ({
  key, queries, failed, cpuMs, wallMs: cpuMs * 2, inputBytes, writtenBytes,
})

/** Two people, as the screen shows them: alice used the most compute. */
const BY_PRINCIPAL = {
  by: 'principal',
  from: FROM,
  to: NOW,
  items: [
    row('alice', 812, 3, 2 * 3_600_000 + 60_000, 8_100_000_000, 1_200_000_000),
    row('svc-shop', 301, 2, 58 * 60_000, 3_900_000_000, 4_400_000_000),
  ],
  total: row('total', 1113, 5, 2 * 3_600_000 + 60_000 + 58 * 60_000, 12_000_000_000, 5_600_000_000),
}

/** Three UTC days; the middle one used the most CPU. */
const BY_DAY = {
  by: 'day',
  from: FROM,
  to: NOW,
  items: [
    row('2026-11-14', 40, 0, 20 * 60_000, 300_000_000, 0),
    row('2026-11-15', 120, 1, 41 * 60_000, 900_000_000, 100_000_000),
    row('2026-11-16', 30, 0, 10 * 60_000, 200_000_000, 0),
  ],
  total: row('total', 190, 1, 71 * 60_000, 1_400_000_000, 100_000_000),
}

const EMPTY = {
  by: 'principal',
  from: FROM,
  to: NOW,
  items: [],
  total: row('total', 0, 0, 0, 0, 0),
}

/** The administrator's Everyone view; `usage` replaces either the principal or the day answer. */
async function stub(page: import('@playwright/test').Page, usage: { principal?: unknown; day?: unknown } = {}) {
  const principal = usage.principal ?? BY_PRINCIPAL
  const day = usage.day ?? BY_DAY
  await page.route('**/api/**', async (r) => {
    const url = new URL(r.request().url())
    if (url.pathname === '/api/me') {
      return json(r, { user: 'bob', auth_mode: 'oidc', demo: false, warehouse: 'warehouse', is_admin: true, groups: ['admins'] })
    }
    if (url.pathname === '/api/query-log/usage') {
      return json(r, url.searchParams.get('by') === 'day' ? day : principal)
    }
    if (url.pathname === '/api/query-log') return json(r, { items: [], nextCursor: null })
    return json(r, {})
  })
}

/** Dark at the requested width, as the other conformance screenshots are taken. */
async function dark(page: import('@playwright/test').Page, width: number) {
  await page.setViewportSize({ width, height: 800 })
  await page.emulateMedia({ colorScheme: 'dark' })
  await page.addInitScript(() => {
    try {
      localStorage.setItem('stardelt-nova:theme', 'dark')
    } catch {
      /* ignore */
    }
  })
}

test('the usage view answers who used the most compute, and the row is there', async ({ page }) => {
  await dark(page, 1280)
  await stub(page)
  await page.goto('/queries?view=usage')

  // Administrators get Everyone; the first column is a person then.
  await page.getByRole('button', { name: 'Everyone' }).click()
  await expect(page.getByRole('heading', { name: 'Who used the most compute' })).toBeVisible()
  await expect(page.getByRole('cell', { name: 'alice', exact: true })).toBeVisible()
  await expect(page.getByRole('cell', { name: 'svc-shop', exact: true })).toBeVisible()
  // The total line sums the window: 1,113 queries and 5 failed. CPU reads in hours: the
  // window's minutes are the noise this view drops.
  await expect(page.getByText('1,113 queries · 3.0 h CPU · 11 GiB read · 5 failed')).toBeVisible()
  // One bar per day, each with its own words for a screen reader, and its day under it.
  await expect(page.getByRole('heading', { name: 'CPU time per day' })).toBeVisible()
  await expect(page.getByRole('img', { name: 'Sun 15 Nov · 41 m CPU · 120 queries' })).toBeVisible()
  await expect(page.getByText('Sun 15', { exact: true })).toBeVisible()

  mkdirSync(here('../../docs/screenshots/1060'), { recursive: true })
  await page.screenshot({ path: here('screenshots/query-usage-1280-dark.png') })
  copyFileSync(here('screenshots/query-usage-1280-dark.png'), here('../../docs/screenshots/1060/query-usage-1280-dark.png'))

  // The same view on a phone. The navigation collapses to a phone layout on load only,
  // so reload at the narrow width and take the scope again.
  await page.setViewportSize({ width: 390, height: 800 })
  await page.goto('/queries?view=usage')
  await page.getByRole('button', { name: 'Everyone' }).click()
  await expect(page.getByRole('heading', { name: 'Who used the most compute' })).toBeVisible()
  // At 390 px the table keeps Person, Queries, CPU time and Data read; the two columns that
  // made it scroll are gone (`hidden sm:table-cell`).
  await expect(page.getByRole('columnheader', { name: 'Data read' })).toBeVisible()
  await expect(page.getByRole('columnheader', { name: 'Data written' })).toBeHidden()
  await expect(page.getByRole('columnheader', { name: 'Failed' })).toBeHidden()
  // The card must not be a scroll container at 390 px (#1064): the CPU bar is hidden below sm.
  const card = page.locator('div.card', { has: page.getByRole('table') })
  const overflow = await card.evaluate((el) => el.scrollWidth - el.clientWidth)
  expect(overflow, 'the usage table card must not scroll at 390 px').toBeLessThanOrEqual(1)
  await page.screenshot({ path: here('screenshots/query-usage-390-dark.png') })
  copyFileSync(here('screenshots/query-usage-390-dark.png'), here('../../docs/screenshots/1060/query-usage-390-dark.png'))
})

test('Queries takes the view out of the address', async ({ page }) => {
  await stub(page)
  await page.goto('/queries?view=usage')
  await expect(page.getByRole('heading', { name: 'By warehouse' })).toBeVisible()

  await page.getByRole('group', { name: 'View' }).getByRole('button', { name: 'Queries' }).click()
  await expect(page).not.toHaveURL(/view=/)
  await expect(page.getByRole('group', { name: 'View' }).getByRole('button', { name: 'Queries' })).toHaveAttribute('aria-pressed', 'true')
  await expect(page.getByRole('group', { name: 'Status' })).toBeVisible()
})

test('an empty window says so', async ({ page }) => {
  await stub(page, { principal: EMPTY, day: { ...EMPTY, by: 'day' } })
  await page.goto('/queries?view=usage')

  await expect(page.getByText('No queries in this window.')).toBeVisible()
  await expect(page.getByRole('table')).toHaveCount(0)
})
