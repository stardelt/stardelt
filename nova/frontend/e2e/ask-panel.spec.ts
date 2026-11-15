import { test, expect, type Page, type Route } from '@playwright/test'

// Ask — the panel (stardelt-nova#1012, design nova#891 slice b): a question in, the
// checked SQL card out, with its sources and a vote. Every `/api/**` call is stubbed, so
// the spec shows exactly what the panel draws for one answer — the panel is the evidence
// until a lab endpoint is Ready.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const ME = {
  user: 'alice', auth_mode: 'oidc', demo: false, warehouse: 'warehouse',
  is_admin: false, groups: ['analytics'],
}

/** A ready endpoint: purpose ask, reachable, answers in JSON. */
const ENDPOINT = {
  name: 'lab', model: 'llama-3.3-70b', location: 'on-premise', purposes: ['ask'],
  sendRows: false, reachable: true, toolCalling: false, jsonOutput: true,
  contextTokens: 32768, checkedAt: '2026-11-15T09:00:00Z', message: null,
}

const READ_TABLES = {
  tables: [
    { namespace: 'gold.shop', name: 'customer_ltv', queries: 12, lastRead: '2026-11-14T09:00:00Z' },
    { namespace: 'lakehouse.staging', name: 'orders', queries: 9, lastRead: '2026-11-14T08:00:00Z' },
    { namespace: 'lakehouse.staging', name: 'trips_clean', queries: 5, lastRead: '2026-11-13T08:00:00Z' },
  ],
}

const EMPTY =
  'Ask a question about the tables you can see. stardelt writes SQL, checks it as you, and never runs it without you.'

const SQL = "SELECT count(*) AS rows\nFROM gold.shop.customer_ltv\nWHERE read_at >= current_date - INTERVAL '1' DAY"

const PASSED = {
  endpoint: 'lab', model: 'llama-3.3-70b', callId: 42, sql: SQL,
  sentence: 'How many customer rows the shop read yesterday.',
  tables: [{ catalog: 'gold', namespace: 'shop', table: 'customer_ltv' }],
  explain: 'passed', error: null,
}

const FAILED = {
  ...PASSED,
  explain: 'failed',
  error: 'Table gold.shop.customer_ltv does not exist',
}

const HISTORY_SQL = "SELECT count(*) AS orders\nFROM lakehouse.staging.orders\nWHERE ordered_at >= current_date - INTERVAL '7' DAY"

/** Two stored conversations, as `GET /api/ask/history` answers them: newest first. */
const HISTORY = {
  items: [
    {
      id: 2, question: 'How many orders came in this week?', endpoint: 'lab', model: 'llama-3.3-70b',
      callId: 43, sql: HISTORY_SQL, sentence: 'Orders placed in the last seven days.',
      tables: [{ catalog: 'lakehouse', namespace: 'staging', table: 'orders' }],
      explain: 'passed', error: null, createdAt: '2026-11-15T10:00:00Z',
    },
    {
      id: 1, question: 'What did the shop read yesterday?', endpoint: 'lab', model: 'llama-3.3-70b',
      callId: 41, sql: SQL, sentence: 'Customer rows the shop read yesterday.',
      tables: [{ catalog: 'gold', namespace: 'shop', table: 'customer_ltv' }],
      explain: 'failed', error: 'Table gold.shop.customer_ltv does not exist',
      createdAt: '2026-11-14T10:00:00Z',
    },
  ],
}

type Options = {
  /** What `/api/model-endpoints` answers; one ready row by default. */
  endpoints?: unknown[]
  /** What `/api/ask` answers; the passed answer by default. */
  ask?: (route: Route) => void
  /** What `/api/ask/feedback` answers; a 204 by default. */
  feedback?: (route: Route) => void
  /** What `/api/ask/history` answers; two stored conversations, newest first, by default. */
  history?: (route: Route) => void
  /** Whether the shell is expected to show the Ask button (it is, unless no endpoint is ready). */
  ready?: boolean
}

const METRICS = {
  catalogs: [],
  totals: { catalogs: 0, schemas: 0, tables: 0, records: 0, sizeBytes: 0 },
  generatedAt: 0,
}

/** Every `/api/**` call the shell and the panel make. */
async function stub(page: Page, options: Options = {}) {
  await page.route('**/api/**', (route) => {
    const { pathname } = new URL(route.request().url())
    if (pathname === '/api/me') return json(route, ME)
    if (pathname === '/api/settings') return json(route, {})
    if (pathname === '/api/apps/links') return json(route, [])
    if (pathname === '/api/catalog/metrics') return json(route, METRICS)
    if (pathname === '/api/model-endpoints') return json(route, options.endpoints ?? [ENDPOINT])
    if (pathname === '/api/lineage/read-tables') return json(route, READ_TABLES)
    if (pathname === '/api/ask') return options.ask ? options.ask(route) : json(route, PASSED)
    if (pathname === '/api/ask/history') return options.history ? options.history(route) : json(route, HISTORY)
    if (pathname === '/api/ask/feedback') {
      if (options.feedback) return options.feedback(route)
      return route.fulfill({ status: 204 })
    }
    return json(route, [])
  })
  await page.goto('/')
  if (options.ready !== false) {
    await expect(page.getByRole('button', { name: 'Ask about your data' })).toBeVisible()
  }
}

/** One theme, one width: the panel drawn the way the screenshots show it. */
async function view(page: Page, theme: 'dark' | 'light', width: number, height: number) {
  await page.setViewportSize({ width, height })
  await page.emulateMedia({ colorScheme: theme })
  await page.evaluate((value) => localStorage.setItem('stardelt-nova:theme', value), theme)
  await page.reload()
  // The endpoint list arrives after the first paint; the key binding exists with it.
  await expect(page.getByRole('button', { name: 'Ask about your data' })).toBeVisible()
  await page.keyboard.press('Control+J')
  await expect(page.locator('#ask-panel')).toBeVisible()
}

async function ask(page: Page, question: string) {
  await page.getByRole('textbox', { name: 'Ask a question' }).fill(question)
  await page.keyboard.press('Enter')
}

test('no ready endpoint, no Ask button', async ({ page }) => {
  await stub(page, { endpoints: [], ready: false })
  await expect(page.getByRole('button', { name: 'Ask about your data' })).toHaveCount(0)
  await expect(page.locator('#ask-panel')).toHaveCount(0)
})

test('⌘J opens the panel on the promise and three examples, Escape closes it', async ({ page }) => {
  await stub(page)
  const panel = page.locator('#ask-panel')
  await expect(panel).toHaveCount(0)
  await page.keyboard.press('Control+J')
  await expect(panel).toBeVisible()
  await expect(panel.getByText(EMPTY)).toBeVisible()
  await expect(panel.getByRole('button', { name: /^How many rows did / })).toHaveCount(3)
  await expect(
    panel.getByRole('button', { name: 'How many rows did gold.shop.customer_ltv get yesterday?' }),
  ).toBeVisible()
  await page.keyboard.press('Escape')
  await expect(panel).toHaveCount(0)
})

test('an answer renders its statement, its check line and its sources', async ({ page }) => {
  await stub(page)
  await view(page, 'dark', 1440, 900)
  await ask(page, 'How many customer rows did the shop read yesterday?')

  await expect(page.getByTestId('ask-sql')).toHaveText(SQL)
  await expect(page.getByText('✓ Checked: EXPLAIN passed as you')).toBeVisible()
  await expect(page.locator('#ask-panel').getByRole('link', { name: 'gold.shop.customer_ltv' }))
    .toHaveAttribute('href', /\/catalog\?catalog=gold&ns=shop&tbl=customer_ltv$/)
  await expect(page.getByText('How many customer rows the shop read yesterday.')).toBeVisible()
  await page.screenshot({ path: '../docs/screenshots/1012/ask-panel-1440-dark.png' })

  await view(page, 'light', 1440, 900)
  await ask(page, 'How many customer rows did the shop read yesterday?')
  await expect(page.getByTestId('ask-sql')).toHaveText(SQL)
  await page.screenshot({ path: '../docs/screenshots/1012/ask-panel-1440-light.png' })

  await view(page, 'dark', 390, 844)
  await ask(page, 'How many customer rows did the shop read yesterday?')
  await expect(page.getByTestId('ask-sql')).toHaveText(SQL)
  await page.screenshot({ path: '../docs/screenshots/1012/ask-panel-390-dark.png' })

  // The statement is the person's to run, in the editor, with the editor's own Run.
  await page.getByRole('button', { name: 'Open in SQL editor' }).click()
  await expect(page).toHaveURL(/\/query\?/)
  expect(new URL(page.url()).searchParams.get('sql')).toBe(SQL)
})

test('a failed check is honest, and the statement still opens in the editor', async ({ page }) => {
  await stub(page, { ask: (route) => json(route, FAILED) })
  await view(page, 'dark', 1440, 900)
  await ask(page, 'How many customer rows did the shop read yesterday?')
  await expect(
    page.getByText('✕ Not runnable: Table gold.shop.customer_ltv does not exist. The model tried twice.'),
  ).toBeVisible()
  await expect(page.getByText('✓ Checked: EXPLAIN passed as you')).toHaveCount(0)
  await expect(page.getByRole('button', { name: 'Open in SQL editor' })).toBeVisible()
  await page.screenshot({ path: '../docs/screenshots/1012/ask-panel-not-runnable-1440-dark.png' })
})

test('a vote reaches the row the answer wrote', async ({ page }) => {
  let resolveVote: (body: unknown) => void = () => {}
  const voted = new Promise<unknown>((resolve) => { resolveVote = resolve })
  await stub(page, {
    feedback: async (route) => {
      resolveVote(route.request().postDataJSON())
      await route.fulfill({ status: 204 })
    },
  })
  await page.keyboard.press('Control+J')
  await ask(page, 'How many customer rows did the shop read yesterday?')
  await expect(page.getByTestId('ask-sql')).toBeVisible()
  await page.getByRole('button', { name: 'This answer helped' }).click()
  expect(await voted).toEqual({ callId: 42, vote: 'up' })
  await expect(page.getByRole('button', { name: 'This answer helped' })).toBeDisabled()
  await expect(page.getByRole('button', { name: 'This answer was wrong' })).toBeDisabled()
})

test('a spent budget is a calm notice, not an error', async ({ page }) => {
  await stub(page, {
    ask: (route) => json(route, { error: 'Your group used its 2 M tokens for November' }, 429),
  })
  await page.keyboard.press('Control+J')
  await ask(page, 'How many customer rows did the shop read yesterday?')
  const panel = page.locator('#ask-panel')
  await expect(panel.getByText('Your group used its 2 M tokens for November')).toBeVisible()
  await expect(panel.locator('[role="alert"]')).toHaveCount(0)
  await expect(panel.locator('[role="status"]')).toHaveCount(1)
})

test('History lists the last conversations, newest first, with their check line', async ({ page }) => {
  await stub(page)
  await page.keyboard.press('Control+J')
  const panel = page.locator('#ask-panel')
  await panel.getByRole('button', { name: 'History' }).click()
  const rows = panel.locator('#ask-history button')
  await expect(rows).toHaveCount(2)
  await expect(rows.nth(0)).toContainText('How many orders came in this week?')
  await expect(rows.nth(0)).toContainText('✓ checked')
  await expect(rows.nth(1)).toContainText('What did the shop read yesterday?')
  await expect(rows.nth(1)).toContainText('✕ not runnable')
})

test('opening a stored conversation shows it, and the model is not asked again', async ({ page }) => {
  let asked = 0
  await stub(page, {
    ask: (route) => {
      asked += 1
      return json(route, PASSED)
    },
  })
  await page.keyboard.press('Control+J')
  const panel = page.locator('#ask-panel')
  await panel.getByRole('button', { name: 'History' }).click()
  await panel.locator('#ask-history button').first().click()

  await expect(panel.getByTestId('ask-sql')).toHaveText(HISTORY_SQL)
  await expect(panel.getByText('Orders placed in the last seven days.')).toBeVisible()
  await expect(panel.getByText('✓ Checked: EXPLAIN passed as you')).toBeVisible()
  expect(asked).toBe(0)
  await expect(panel.locator('#ask-history')).toHaveCount(0)
})

test('a person with no history is told so', async ({ page }) => {
  await stub(page, { history: (route) => json(route, { items: [] }) })
  await page.keyboard.press('Control+J')
  const panel = page.locator('#ask-panel')
  await panel.getByRole('button', { name: 'History' }).click()
  await expect(panel.locator('#ask-history').getByText('No questions yet.')).toBeVisible()
})

test('a history that cannot be read shows the API sentence, never an empty list', async ({ page }) => {
  await stub(page, { history: (route) => json(route, { error: 'the history was not read' }, 503) })
  await page.keyboard.press('Control+J')
  const panel = page.locator('#ask-panel')
  await panel.getByRole('button', { name: 'History' }).click()
  await expect(panel.getByRole('status').filter({ hasText: 'the history was not read' })).toBeVisible()
  await expect(panel.locator('#ask-history').getByText('No questions yet.')).toHaveCount(0)
})
