import { test, expect, type Route } from '@playwright/test'

// The table page's verified queries (stardelt-nova#1021): a question a table's owners answered
// with SQL is read by anyone who may see the table, and an owner adds or deletes one in place.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const FIELDS = [
  { id: 1, name: 'id', required: true, type: 'long' },
  { id: 2, name: 'note', required: false, type: 'string' },
]

type Row = {
  id: number
  question: string
  sql: string
  verifiedBy: string
  verifiedAt: string
  lastCheckAt?: string | null
  lastCheckPassed?: boolean | null
  lastCheckMessage?: string | null
}

const REVENUE: Row = {
  id: 7,
  question: 'What did revenue last month come to?',
  sql: 'SELECT sum(revenue) FROM sales.orders',
  verifiedBy: 'alice',
  verifiedAt: '2026-11-10T09:00:00Z',
}
const REFUNDS: Row = {
  id: 8,
  question: 'How many orders were refunded?',
  sql: 'SELECT count(*) FROM sales.orders WHERE refunded',
  verifiedBy: 'bob',
  verifiedAt: '2026-11-11T10:30:00Z',
}

/** The rows the check test starts from: one failing, one passing (#1036). */
const FAILING: Row = {
  id: 11,
  question: 'How many rows does last month have?',
  sql: 'SELECT count(*) FROM sales.orders WHERE month = 11',
  verifiedBy: 'alice',
  verifiedAt: '2026-11-12T08:00:00Z',
  lastCheckAt: '2026-11-14T09:00:00Z',
  lastCheckPassed: false,
  lastCheckMessage: 'expected 3 rows, got 4',
}
const PASSING: Row = {
  id: 12,
  question: 'Which currency do we sell in?',
  sql: 'SELECT DISTINCT currency FROM sales.orders',
  verifiedBy: 'bob',
  verifiedAt: '2026-11-12T08:30:00Z',
  lastCheckAt: '2026-11-14T09:05:00Z',
  lastCheckPassed: true,
  lastCheckMessage: 'the result matches the expected result',
}

/**
 * The table page, stubbed. `rows` is what the verified-query list answers, and the stubs for
 * POST and DELETE change it, so a refetch after a write shows the new state.
 */
async function stub(
  page: import('@playwright/test').Page,
  opts: { canEdit: boolean; rows: Row[]; onDelete?: (id: number) => void },
) {
  const rows = [...opts.rows]

  await page.route('**/api/**', (r) => {
    const path = new URL(r.request().url()).pathname
    const method = r.request().method()

    if (path === '/api/me') {
      return json(r, {
        user: 'alice',
        auth_mode: 'oidc',
        demo: false,
        warehouse: 'warehouse',
        is_admin: opts.canEdit,
        groups: opts.canEdit ? ['sales'] : ['analytics'],
      })
    }
    if (path === '/api/catalog/catalogs') {
      return json(r, { catalogs: [{ name: 'warehouse', id: '1', canWrite: false, grants: [] }], default: 'warehouse' })
    }
    if (path === '/api/catalog/namespaces') return json(r, { namespaces: [['sales']] })
    if (path === '/api/catalog/namespaces/sales/tables') {
      return json(r, { identifiers: [{ namespace: ['sales'], name: 'orders' }] })
    }
    if (path === '/api/catalog/tables/sales/orders/verified-queries/check' && method === 'POST') {
      return json(r, { checked: rows.length, passed: 1, failed: rows.length - 1, items: rows })
    }
    if (path === '/api/catalog/tables/sales/orders/verified-queries') {
      if (method === 'POST') {
        const body = r.request().postDataJSON() as { question: string; sql: string }
        const row: Row = {
          id: 9,
          question: body.question,
          sql: body.sql,
          verifiedBy: 'alice',
          verifiedAt: '2026-11-15T12:00:00Z',
        }
        rows.unshift(row)
        return json(r, row, 201)
      }
      return json(r, rows)
    }
    if (path.startsWith('/api/verified-queries/') && path.endsWith('/check') && method === 'POST') {
      const id = Number(path.split('/')[3])
      const row = rows.find((candidate) => candidate.id === id)
      const answered: Row = {
        ...(row ?? FAILING),
        lastCheckAt: '2026-11-15T12:00:00Z',
        lastCheckPassed: true,
        lastCheckMessage: 'the result matches the expected result',
      }
      const at = rows.findIndex((candidate) => candidate.id === id)
      if (at >= 0) rows[at] = answered
      return json(r, answered)
    }
    if (path.startsWith('/api/verified-queries/') && method === 'DELETE') {
      const id = Number(path.split('/').pop())
      opts.onDelete?.(id)
      const at = rows.findIndex((row) => row.id === id)
      if (at >= 0) rows.splice(at, 1)
      return r.fulfill({ status: 204, body: '' })
    }
    if (path.startsWith('/api/catalog/tables/')) {
      if (path.endsWith('/documentation')) {
        return json(r, {
          description: null,
          tags: [],
          owner: opts.canEdit ? 'sales' : null,
          domain: null,
          guide: null,
          columns: [],
          canEdit: opts.canEdit,
          editRule: '',
          canRollback: false,
          rollbackRule: '',
        })
      }
      if (path.endsWith('/history')) return json(r, { snapshots: [], columns: [], rows: [] })
      return json(r, {
        'metadata-location': '',
        metadata: {
          'table-uuid': 'u',
          location: 's3://warehouse/sales/orders',
          'last-updated-ms': 0,
          'current-schema-id': 0,
          schemas: [{ 'schema-id': 0, type: 'struct', fields: FIELDS }],
          'partition-specs': [],
        },
      })
    }
    if (path === '/api/query') return json(r, { columns: [], rows: [], row_count: 0 })
    if (path === '/api/lineage') {
      return json(r, { nodes: [], edges: [], jobs: [], impact: { downstream: 0, upstream: 0, downstreamJobs: 0 } })
    }
    return json(r, {})
  })
}

test('a member reads the verified queries and gets no form', async ({ page }) => {
  await stub(page, { canEdit: false, rows: [REVENUE, REFUNDS] })
  await page.goto('/catalog?catalog=warehouse&ns=sales&tbl=orders')

  const section = page.getByRole('region', { name: 'Verified queries' })
  await expect(section).toBeVisible()

  // Newest first: both questions, each with the SQL that answers it.
  for (const row of [REVENUE, REFUNDS]) {
    await expect(section.getByText(row.question)).toBeVisible()
    await expect(section.getByText(row.sql)).toBeVisible()
    await expect(section.getByText(`Verified by ${row.verifiedBy}`)).toBeVisible()
  }

  // A reader gets neither a Delete button nor the add form.
  await expect(section.getByRole('button', { name: /^Delete/ })).toHaveCount(0)
  await expect(section.getByRole('textbox', { name: 'Question' })).toHaveCount(0)
  await expect(section.getByRole('textbox', { name: 'SQL' })).toHaveCount(0)
  await expect(section.getByRole('button', { name: 'Add' })).toHaveCount(0)
})

test('an owner adds a verified query and sees it once the list is refetched', async ({ page }) => {
  await stub(page, { canEdit: true, rows: [REVENUE] })
  await page.setViewportSize({ width: 1280, height: 800 })
  await page.emulateMedia({ colorScheme: 'dark' })
  await page.goto('/catalog?catalog=warehouse&ns=sales&tbl=orders')

  const section = page.getByRole('region', { name: 'Verified queries' })
  await expect(section).toBeVisible()
  await expect(section.getByRole('button', { name: `Delete the verified query: ${REVENUE.question}` })).toBeVisible()

  // The form is closed until the owner asks for it: no empty inputs under the list.
  await expect(section.getByRole('textbox', { name: 'Question' })).toHaveCount(0)
  await expect(section.getByRole('textbox', { name: 'SQL' })).toHaveCount(0)

  const question = 'Which customers spent the most?'
  const sql = 'SELECT customer_id, sum(total) FROM sales.orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10'

  const posted = page.waitForRequest(
    (r) => r.method() === 'POST' && new URL(r.url()).pathname === '/api/catalog/tables/sales/orders/verified-queries',
  )

  await section.getByRole('button', { name: '+ Add a verified query' }).click()
  await expect(section.getByRole('textbox', { name: 'Question' })).toBeVisible()

  // The owner's view, for the design record: one query and the form that adds the next.
  await page.screenshot({ path: '../docs/screenshots/1036/verified-queries-1440-dark.png' })

  await section.getByRole('textbox', { name: 'Question' }).fill(question)
  await section.getByRole('textbox', { name: 'SQL' }).fill(sql)
  await section.getByRole('button', { name: 'Add verified query' }).click()

  const request = await posted
  expect(request.postDataJSON()).toEqual({ question, sql })

  // The form closes with the add and the list shows the new question.
  await expect(section.getByText(question)).toBeVisible()
  await expect(section.getByRole('textbox', { name: 'Question' })).toHaveCount(0)
  await expect(section.getByRole('button', { name: '+ Add a verified query' })).toBeVisible()
})

test('an owner deletes a verified query and it leaves the list', async ({ page }) => {
  const deleted: number[] = []
  await stub(page, { canEdit: true, rows: [REVENUE, REFUNDS], onDelete: (id) => deleted.push(id) })
  await page.goto('/catalog?catalog=warehouse&ns=sales&tbl=orders')

  const section = page.getByRole('region', { name: 'Verified queries' })
  await expect(section.getByText(REVENUE.question)).toBeVisible()

  const sent = page.waitForRequest(
    (r) => r.method() === 'DELETE' && new URL(r.url()).pathname === '/api/verified-queries/7',
  )
  await section.getByRole('button', { name: `Delete the verified query: ${REVENUE.question}` }).click()
  await sent

  expect(deleted).toEqual([7])
  await expect(section.getByText(REVENUE.question)).toHaveCount(0)
  await expect(section.getByText(REFUNDS.question)).toBeVisible()
})

test('each verified query shows its last check, can be checked now and opens in the editor', async ({ page }) => {
  await stub(page, { canEdit: false, rows: [FAILING, PASSING] })
  await page.goto('/catalog?catalog=warehouse&ns=sales&tbl=orders')

  const section = page.getByRole('region', { name: 'Verified queries' })
  await expect(section).toBeVisible()

  // The failed check says what it found; the passed one says it passed.
  await expect(section.getByText('✕ expected 3 rows, got 4')).toBeVisible()
  await expect(section.getByText('✓ Checked')).toBeVisible()

  // A reader may open the statement in the SQL editor.
  const editor = section.getByRole('link', { name: 'Open in SQL editor' })
  await expect(editor).toHaveCount(2)
  await expect(editor.first()).toHaveAttribute('href', `/query?sql=${encodeURIComponent(FAILING.sql)}`)

  // A reader may run the check again: it only reads, and it runs as the caller.
  const checked = page.waitForRequest(
    (r) => r.method() === 'POST' && new URL(r.url()).pathname === '/api/verified-queries/11/check',
  )
  const failing = section.locator('li', { hasText: FAILING.question })
  await failing.getByRole('button', { name: 'Check now' }).click()
  await checked

  // The list is refetched with the outcome the check answered.
  await expect(section.getByText('✕ expected 3 rows, got 4')).toHaveCount(0)
  await expect(section.getByText('✓ Checked')).toHaveCount(2)
})

test('an owner checks every verified query in one call and sees the totals', async ({ page }) => {
  await stub(page, { canEdit: true, rows: [FAILING, PASSING] })
  await page.goto('/catalog?catalog=warehouse&ns=sales&tbl=orders')

  const section = page.getByRole('region', { name: 'Verified queries' })
  await expect(section).toBeVisible()

  // One request, not one per query.
  const calls: string[] = []
  page.on('request', (r) => {
    const url = new URL(r.url())
    if (r.method() === 'POST' && url.pathname.endsWith('/verified-queries/check')) calls.push(url.pathname)
  })

  await section.getByRole('button', { name: 'Check all' }).click()

  // The stub answers checked 2, passed 1, failed 1.
  await expect(section.getByText('1 of 2 passed · 1 failed')).toBeVisible()
  expect(calls).toEqual(['/api/catalog/tables/sales/orders/verified-queries/check'])
})
