import { test, expect, type Route } from '@playwright/test'

// The Columns tab's Class column (#491 slice 2): a table's owner sees the stored class and changes
// it through one select; the page sends the whole class map, because the route replaces it.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const FIELDS = [
  { id: 1, name: 'id', required: true, type: 'long' },
  { id: 2, name: 'amount', required: false, type: 'double' },
]

const DOCS = {
  description: null,
  tags: [],
  owner: null,
  domain: null,
  guide: null,
  lifecycle: null,
  replacedBy: null,
  columns: [
    { name: 'id', type: 'long', description: null, class: null },
    { name: 'amount', type: 'double', description: null, class: 'financial' },
  ],
  canEdit: true,
  editRule: 'You can edit.',
  canRollback: false,
  rollbackRule: '',
}

/** Every PUT body the page sent to the documentation route. */
const saved: unknown[] = []

async function stub(page: import('@playwright/test').Page) {
  saved.length = 0
  await page.route('**/api/**', (r) => {
    const path = new URL(r.request().url()).pathname

    if (path === '/api/me') {
      return json(r, { user: 'alice', auth_mode: 'oidc', demo: false, warehouse: 'warehouse', is_admin: false, groups: ['analytics'] })
    }
    if (path === '/api/catalog/catalogs') {
      return json(r, { catalogs: [{ name: 'warehouse', id: '1', canWrite: false, grants: [] }], default: 'warehouse' })
    }
    if (path === '/api/catalog/namespaces') return json(r, { namespaces: [['sales']] })
    if (path === '/api/catalog/namespaces/sales/tables') {
      return json(r, { identifiers: [{ namespace: ['sales'], name: 'orders' }] })
    }
    if (path.startsWith('/api/catalog/tables/')) {
      if (path.endsWith('/documentation')) {
        if (r.request().method() === 'PUT') saved.push(r.request().postDataJSON())
        return json(r, DOCS)
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
    if (path === '/api/lineage') return json(r, { nodes: [], edges: [], jobs: [], impact: { downstream: 0, upstream: 0, downstreamJobs: 0 } })
    return json(r, {})
  })
}

test('the Columns tab shows a column class and saves the whole map', async ({ page }) => {
  await stub(page)
  await page.goto('/catalog?catalog=warehouse&ns=sales&tbl=orders')
  await page.getByRole('tab', { name: 'Columns' }).click()

  await expect(page.getByLabel('Class of amount')).toHaveValue('financial')

  await page.getByLabel('Class of id').selectOption('personal')
  await expect(page.getByText('Saved.')).toBeVisible()

  expect(saved).toEqual([{ columnClasses: { amount: 'financial', id: 'personal' } }])
})
