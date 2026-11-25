import { test, expect, type Locator, type Page, type Route } from '@playwright/test'

// The Overview's merged timeline (stardelt-nova#761, slice 1 of #510): pipeline runs, queries
// and loads in one list, newest first. Every `/api/**` answer the card reads is stubbed here
// (`page.route`), so the merge — not any server state — is what this spec measures.

const AT = (iso: string) => Date.parse(iso)

const QUERY_ITEMS = [
  {
    id: 'q-newest',
    state: 'FINISHED',
    user: 'demo',
    userName: 'Demo',
    source: 'nova-sql-editor',
    warehouse: 'warehouse',
    queryType: 'SELECT',
    sql: 'select payment_type, count(*) from stardelt.taxi.trips group by 1',
    createdAt: AT('2026-11-08T10:00:00Z'),
    wallMs: 412,
    outputRows: 4,
    errorCode: null,
  },
  {
    id: 'q-middle',
    state: 'FAILED',
    user: 'demo',
    userName: 'Demo',
    source: 'nova-sql-editor',
    warehouse: 'warehouse',
    queryType: 'SELECT',
    sql: 'select * from stardelt.taxi.no_such_table',
    createdAt: AT('2026-11-08T07:30:00Z'),
    wallMs: 88,
    outputRows: null,
    errorCode: 'TABLE_NOT_FOUND',
  },
]

const json = (route: Route, body: unknown) =>
  route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) })

/** One run row of `GET /api/runs`, at an absolute moment. */
const runAt = (runId: string, name: string, state: string, at: number) => ({
  namespace: 'default',
  name,
  ownerGroup: null,
  kind: 'pipeline' as const,
  runId,
  state,
  type: 'scheduled',
  logicalDate: new Date(at).toISOString(),
  startedAt: new Date(at).toISOString(),
  endedAt: null,
  durationSeconds: null,
  triggeredBy: 'scheduler',
  note: null,
})

const RUNS = {
  runs: [runAt('r-1', 'ingest_orders', 'running', AT('2026-11-08T09:00:00Z'))],
  groups: [],
}

/** One load, older than both the run and the query above. */
const JOBS = [
  {
    id: 'job-1',
    kind: 'load',
    title: 'shop.orders',
    status: 'succeeded',
    started_at: AT('2026-11-08T08:00:00Z'),
    detail: '1,204 rows via csv',
    rows: 1204,
    user: 'e2e',
  },
]

async function open(page: Page) {
  await page.route('**/api/query-log**', (route) =>
    json(route, { items: QUERY_ITEMS, nextCursor: null }),
  )
  await page.route('**/api/runs**', (route) => json(route, RUNS))
  await page.route('**/api/jobs**', (route) => json(route, JOBS))
  await page.goto('/')
  const card = page.locator('div.card', { has: page.getByRole('heading', { name: 'Recent activity' }) })
  // The card is below the fold and reads its two lists only when it comes near (nova#859):
  // bring it into view, as a person would.
  await page.getByRole('heading', { name: 'Recent activity' }).scrollIntoViewIfNeeded()
  await expect(card.locator('a[data-at]').first()).toBeVisible()
  // The two reads start together but answer one after the other, so the card can show the
  // loads alone for a moment: wait for a row of each gated source before a test reads the list.
  await expect(card.locator('a[href^="/pipelines/"]').first()).toBeVisible()
  await expect(card.locator('a[href^="/queries?id="]').first()).toBeVisible()
  return card
}

/** Every row's `at`, in the order the card renders them. */
async function rowTimes(card: Locator) {
  const raw = await card.locator('a[data-at]').evaluateAll((rows) =>
    rows.map((row) => Number(row.getAttribute('data-at'))),
  )
  return raw
}

test.describe('overview timeline', () => {
  test('merges runs and queries into one list with links that go somewhere', async ({ page }) => {
    const card = await open(page)
    const hrefs = await card.locator('a[data-at]').evaluateAll((rows) =>
      rows.map((row) => row.getAttribute('href') ?? ''),
    )
    expect(hrefs.length).toBeGreaterThan(0)
    for (const href of hrefs) {
      // The three deep links the card offers: a run's pipeline, a query's detail, the page that
      // started a load (Load & transfer; the Activity page is gone, stardelt-nova#948).
      const known = href.startsWith('/pipelines/') || href.startsWith('/queries?id=') || href === '/data'
      expect(known, href).toBe(true)
    }
    // A pipeline run and a query, side by side: the jobs-only card is gone (#761).
    expect(hrefs.some((href) => href.startsWith('/pipelines/'))).toBe(true)
    expect(hrefs.some((href) => href.startsWith('/queries?id='))).toBe(true)
    await expect(card.getByText('ingest_orders: running')).toBeVisible()
    await expect(card.getByText('select payment_type, count(*) from stardelt.taxi.trips group by 1')).toBeVisible()
  })

  test('orders the rows newest first', async ({ page }) => {
    const card = await open(page)
    const times = await rowTimes(card)
    expect(times.length).toBeGreaterThan(1)
    for (let i = 1; i < times.length; i++) {
      expect(times[i], `row ${i} is newer than row ${i - 1}`).toBeLessThanOrEqual(times[i - 1])
    }
    // The canned query (10:00) is newer than the `ingest_orders` run (09:00), which is newer
    // than the load (08:00), so their places in the list prove the three sources were sorted
    // together instead of one list being appended after another.
    const query = times.indexOf(AT('2026-11-08T10:00:00Z'))
    const run = times.indexOf(AT('2026-11-08T09:00:00Z'))
    expect(query).toBeGreaterThanOrEqual(0)
    expect(run).toBeGreaterThan(query)
  })

  test('shows no unreadable time', async ({ page }) => {
    const card = await open(page)
    const text = await card.innerText()
    expect(text).not.toMatch(/NaN|Invalid Date/)
    for (const row of await card.locator('a[data-at]').all()) {
      expect(Number.isFinite(Number(await row.getAttribute('data-at')))).toBe(true)
    }
  })
})
