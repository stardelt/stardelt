import { test, expect, type Route } from '@playwright/test'

// The ⌘K palette offers the five actions: four navigate, and "Switch theme" runs the
// header's own toggle (stardelt-nova#615, slice c of #509). Every `/api/**` call is
// stubbed, so the test needs no cluster.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const ME = {
  user: 'alice',
  auth_mode: 'oidc',
  demo: false,
  warehouse: 'warehouse',
  is_admin: false,
  groups: ['analytics'],
}

const CATALOGS = { catalogs: [{ name: 'lakehouse' }] }

/** A ready endpoint: purpose ask, reachable, answers in JSON (the shell's own rule). */
const ENDPOINT = {
  name: 'lab', model: 'llama-3.3-70b', location: 'on-premise', purposes: ['ask'],
  sendRows: false, reachable: true, toolCalling: false, jsonOutput: true,
  contextTokens: 32768, checkedAt: '2026-11-15T09:00:00Z', message: null,
}

const ANSWER = {
  endpoint: 'lab', model: 'llama-3.3-70b', callId: 44,
  sql: 'SELECT count(*) AS orders FROM lakehouse.staging.orders',
  sentence: 'How many orders the staging table holds.',
  tables: [{ catalog: 'lakehouse', namespace: 'staging', table: 'orders' }],
  explain: 'passed', error: null,
}

/** Every API call the shell and the SQL editor make, plus the settings writes the theme makes. */
async function stubApi(page: import('@playwright/test').Page) {
  await page.route('**/api/**', (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    if (path === '/api/me') return json(route, ME)
    if (path === '/api/settings') return json(route, {})
    if (path.startsWith('/api/settings/')) return json(route, { key: path.split('/').pop(), value: null })
    if (path === '/api/catalog/catalogs') return json(route, CATALOGS)
    if (path === '/api/catalog/namespaces') return json(route, { namespaces: [] })
    if (path === '/api/apps/links') return json(route, [])
    if (path === '/api/saved-queries') return json(route, [])
    if (path === '/api/model-endpoints') return json(route, [ENDPOINT])
    if (path === '/api/ask') return json(route, ANSWER)
    if (path === '/api/ask/history') return json(route, { items: [] })
    return json(route, {})
  })
}

test('an action opens the SQL editor', async ({ page }) => {
  await stubApi(page)
  await page.goto('/')
  // The title is set by an effect once `/api/me` resolves: wait so the shell's own
  // keydown listener is attached before the first key press.
  await expect(page).toHaveTitle('Overview · stardelt Nova')
  await page.keyboard.press('Control+k')
  const input = page.getByRole('combobox', { name: 'Search pages, catalogs, schemas, tables and queries' })
  await input.fill('new q')
  const action = page.getByRole('dialog').getByRole('option', { name: /New query/ })
  await expect(action).toBeVisible()
  await action.click()
  await expect(page).toHaveURL(/\/query/)
})

test('Switch theme flips the paired theme and closes the palette', async ({ page }) => {
  await stubApi(page)
  await page.goto('/')
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'graphite')
  // The title is set by an effect once `/api/me` resolves: wait so the shell's own
  // keydown listener is attached before the first key press.
  await expect(page).toHaveTitle('Overview · stardelt Nova')
  await page.keyboard.press('Control+k')
  const input = page.getByRole('combobox', { name: 'Search pages, catalogs, schemas, tables and queries' })
  await input.fill('switch')
  await page.getByRole('dialog').getByRole('option', { name: /Switch theme/ }).click()
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'silver')
  await expect(page.getByRole('dialog')).toHaveCount(0)
})

test('a typed question is the last row, and Enter asks it in the panel', async ({ page }) => {
  await stubApi(page)
  await page.goto('/')
  await expect(page).toHaveTitle('Overview · stardelt Nova')
  await page.keyboard.press('Control+k')
  const input = page.getByRole('combobox', { name: 'Search pages, catalogs, schemas, tables and queries' })
  // An empty query keeps the Ask row away: it would shadow every page match.
  await expect(page.getByRole('dialog').getByRole('option', { name: /^Ask: / })).toHaveCount(0)
  await input.fill('how many orders')
  const options = page.getByRole('dialog').getByRole('option')
  await expect(options.last()).toHaveText(/Ask: how many orders/)
  // Nothing else matches the typed words, so Enter takes the last—and only—row.
  await expect(options).toHaveCount(1)
  await input.press('Enter')
  await expect(page.getByRole('dialog')).toHaveCount(0)
  const panel = page.locator('#ask-panel')
  await expect(panel).toBeVisible()
  await expect(panel.getByText('how many orders', { exact: true })).toBeVisible()
  await expect(panel.getByText('How many orders the staging table holds.')).toBeVisible()
})
