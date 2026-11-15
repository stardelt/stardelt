import { test, expect, type Route } from '@playwright/test'

// The table page's verified queries (stardelt-nova#1021): a question a table's owners answered
// with SQL is read by anyone who may see the table, and an owner adds or deletes one in place.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const FIELDS = [
  { id: 1, name: 'id', required: true, type: 'long' },
  { id: 2, name: 'note', required: false, type: 'string' },
]

type Row = { id: number; question: string; sql: string; verifiedBy: string; verifiedAt: string }

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
      return json(r, { items: rows })
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

  // The owner's view, for the design record: one query and the form that adds the next.
  await page.screenshot({ path: '../docs/screenshots/1021/table-verified-queries-1440-dark.png' })

  const question = 'Which customers spent the most?'
  const sql = 'SELECT customer_id, sum(total) FROM sales.orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10'

  const posted = page.waitForRequest(
    (r) => r.method() === 'POST' && new URL(r.url()).pathname === '/api/catalog/tables/sales/orders/verified-queries',
  )

  await section.getByRole('textbox', { name: 'Question' }).fill(question)
  await section.getByRole('textbox', { name: 'SQL' }).fill(sql)
  await section.getByRole('button', { name: 'Add' }).click()

  const request = await posted
  expect(request.postDataJSON()).toEqual({ question, sql })

  // The form is cleared and the list shows the new question.
  await expect(section.getByText(question)).toBeVisible()
  await expect(section.getByRole('textbox', { name: 'Question' })).toHaveValue('')
  await expect(section.getByRole('textbox', { name: 'SQL' })).toHaveValue('')
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
