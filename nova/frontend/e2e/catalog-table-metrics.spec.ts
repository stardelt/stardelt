import { test, expect, type Route } from '@playwright/test'

// The table page's metric definitions (#124): a named number a table's owners declared over it
// is read by anyone who may see the table, and an owner adds or deletes one in place.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const FIELDS = [
  { id: 1, name: 'id', required: true, type: 'long' },
  { id: 2, name: 'note', required: false, type: 'string' },
]

type Row = {
  id: number
  name: string
  description: string
  expression: string
  timeColumn: string | null
  dimensions: string[]
  verifiedBy: string
  verifiedAt: string
}

const REVENUE: Row = {
  id: 7,
  name: 'revenue',
  description: 'Revenue in dollars, net of returns',
  expression: 'sum(total_cents) / 100.0',
  timeColumn: 'ordered_at',
  dimensions: ['country'],
  verifiedBy: 'alice',
  verifiedAt: '2026-11-10T09:00:00Z',
}

/**
 * The table page, stubbed. `rows` is what the metric list answers, and the stubs for POST and
 * DELETE change it, so a refetch after a write shows the new state.
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
    if (path === '/api/catalog/tables/sales/orders/metrics') {
      if (method === 'POST') {
        const body = r.request().postDataJSON() as {
          name: string
          description: string
          expression: string
          timeColumn?: string | null
          dimensions: string[]
        }
        const row: Row = {
          id: 9,
          name: body.name,
          description: body.description,
          expression: body.expression,
          timeColumn: body.timeColumn ?? null,
          dimensions: body.dimensions,
          verifiedBy: 'alice',
          verifiedAt: '2026-11-15T12:00:00Z',
        }
        rows.unshift(row)
        return json(r, row, 201)
      }
      return json(r, rows)
    }
    if (path.startsWith('/api/metrics/') && method === 'DELETE') {
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

test('a member reads the metric definitions and gets no form', async ({ page }) => {
  await stub(page, { canEdit: false, rows: [REVENUE] })
  await page.goto('/catalog?catalog=warehouse&ns=sales&tbl=orders')

  const section = page.getByRole('region', { name: 'Metrics' })
  await expect(section).toBeVisible()
  await expect(section.getByText(REVENUE.name, { exact: true })).toBeVisible()
  await expect(section.getByText(REVENUE.description)).toBeVisible()
  await expect(section.getByText(REVENUE.expression)).toBeVisible()
  await expect(section.getByText(`Counted over ${REVENUE.timeColumn}`)).toBeVisible()
  await expect(section.getByText(REVENUE.dimensions[0])).toBeVisible()
  await expect(section.getByText(`Verified by ${REVENUE.verifiedBy}`)).toBeVisible()

  // A reader gets neither a Delete button nor the add form.
  await expect(section.getByRole('button', { name: /^Delete/ })).toHaveCount(0)
  await expect(section.getByRole('button', { name: '+ Add a metric' })).toHaveCount(0)
  await expect(section.getByRole('textbox', { name: 'Name' })).toHaveCount(0)
})

test('a reader gets the list but no add form and no delete button', async ({ page }) => {
  await stub(page, { canEdit: false, rows: [REVENUE] })
  await page.goto('/catalog?catalog=warehouse&ns=sales&tbl=orders')

  const section = page.getByRole('region', { name: 'Metrics' })
  await expect(section.getByText(REVENUE.name, { exact: true })).toBeVisible()
  await expect(section.getByRole('button', { name: '+ Add a metric' })).toHaveCount(0)
  await expect(section.getByRole('button', { name: /^Delete/ })).toHaveCount(0)
  await expect(section.getByRole('textbox', { name: 'Name' })).toHaveCount(0)
})

test('an owner adds a metric and sees it once the list is refetched', async ({ page }) => {
  await stub(page, { canEdit: true, rows: [REVENUE] })
  await page.goto('/catalog?catalog=warehouse&ns=sales&tbl=orders')

  const section = page.getByRole('region', { name: 'Metrics' })
  await expect(section).toBeVisible()
  await expect(section.getByRole('button', { name: `Delete the metric ${REVENUE.name}` })).toBeVisible()

  const posted = page.waitForRequest(
    (r) => r.method() === 'POST' && new URL(r.url()).pathname === '/api/catalog/tables/sales/orders/metrics',
  )

  await section.getByRole('button', { name: '+ Add a metric' }).click()
  await section.getByRole('textbox', { name: 'Name' }).fill('orders_per_day')
  await section.getByRole('textbox', { name: 'Description' }).fill('Orders per day')
  await section.getByRole('textbox', { name: 'Expression' }).fill('count(*)')
  await section.getByRole('textbox', { name: 'Time column' }).fill('ordered_at')
  await section.getByRole('textbox', { name: 'Dimensions' }).fill('country, channel')
  await section.getByRole('button', { name: 'Add metric' }).click()

  const request = await posted
  expect(request.postDataJSON()).toEqual({
    name: 'orders_per_day',
    description: 'Orders per day',
    expression: 'count(*)',
    timeColumn: 'ordered_at',
    dimensions: ['country', 'channel'],
  })

  // The form closes with the add and the list shows the new metric.
  await expect(section.getByText('orders_per_day')).toBeVisible()
  await expect(section.getByRole('textbox', { name: 'Name' })).toHaveCount(0)
  await expect(section.getByRole('button', { name: '+ Add a metric' })).toBeVisible()
})
