import { test, expect, type Route } from '@playwright/test'

// The Streams page creates a stream itself (stardelt/stardelt#1977): the dialog validates the
// body with `POST /api/streams/validate` before it writes. Every `/api/**` call is stubbed,
// so the test needs no cluster.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const me = {
  user: 'e2e',
  auth_mode: 'static-dev',
  demo: false,
  warehouse: '',
  groups: ['shop'],
}

const principals = {
  groups: [
    { group: 'shop', principal: 'pipeline-shop', ready: true, message: null },
    { group: 'finance', principal: null, ready: false, message: 'the group has no ready principal' },
  ],
}

const catalogs = {
  catalogs: [
    { name: 'warehouse', id: 'warehouse' },
    { name: 'gold', id: 'gold' },
  ],
  default: 'warehouse',
}

const savedStream = {
  namespace: 'stardelt',
  name: 'orders',
  owner_group: 'shop',
  phase: 'Pending',
  reason: null,
  last_error: null,
  attempts: null,
  last_submitted_at: null,
  stopped: false,
  source_connection: '',
  source_topic: 'orders.created',
  sink_catalog: 'gold',
  sink_table: 'shop.orders',
  sink_mode: 'merge',
  run_as: 'pipeline-shop',
  viewers: [],
}

test('the dialog creates a stream after validation (#1977)', async ({ page }) => {
  let putBody: unknown = null

  await page.route('**/api/**', (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    if (path === '/api/me') return json(route, me)
    if (path === '/api/streams' && request.method() === 'GET') {
      return json(route, { namespace: 'stardelt', streams: [] })
    }
    if (path === '/api/pipelines/principals') return json(route, principals)
    if (path === '/api/catalog/catalogs') return json(route, catalogs)
    if (path === '/api/connections') return json(route, { connections: [], store: true })
    if (path === '/api/streams/validate' && request.method() === 'POST') {
      return json(route, { ok: true, errors: [] })
    }
    if (path === '/api/streams/stardelt/orders' && request.method() === 'PUT') {
      putBody = request.postDataJSON()
      return json(route, savedStream)
    }
    return json(route, {})
  })

  await page.goto('/streams')
  await expect(page.getByText('Create one to read a topic into a table.')).toBeVisible()

  await page.getByRole('button', { name: 'New stream' }).click()
  const dialog = page.getByRole('dialog')
  await expect(dialog.getByRole('heading', { name: 'New stream' })).toBeVisible()
  // The group without a ready principal is offered as a disabled row and its reason is shown.
  await expect(dialog.getByRole('option', { name: 'finance' })).toBeDisabled()
  await expect(dialog.getByText('the group has no ready principal')).toBeVisible()

  await dialog.getByLabel('Name', { exact: true }).fill('orders')
  await dialog.getByLabel('Owner group', { exact: true }).selectOption('shop')
  await dialog.getByLabel('Topic', { exact: true }).fill('orders.created')
  await dialog.getByLabel('Catalog', { exact: true }).selectOption('gold')
  await dialog.getByLabel('Table', { exact: true }).fill('shop.orders')
  await dialog.getByRole('button', { name: 'Create stream' }).click()

  await expect.poll(() => putBody).not.toBeNull()
  expect(putBody).toMatchObject({
    name: 'orders',
    ownerGroup: 'shop',
    source: { topic: 'orders.created' },
    sink: { catalog: 'gold', table: 'shop.orders', mode: 'merge' },
  })
  // No connection was chosen, so no reference is sent: the platform's own bus.
  expect((putBody as { source: { connectionRef?: string } }).source.connectionRef).toBeUndefined()

  await expect(page.getByRole('dialog')).toHaveCount(0)
})

test('a refusal sentence is shown and nothing is written (#1977)', async ({ page }) => {
  let puts = 0

  await page.route('**/api/**', (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    if (path === '/api/me') return json(route, me)
    if (path === '/api/streams' && request.method() === 'GET') {
      return json(route, { namespace: 'stardelt', streams: [] })
    }
    if (path === '/api/pipelines/principals') return json(route, principals)
    if (path === '/api/catalog/catalogs') return json(route, catalogs)
    if (path === '/api/connections') return json(route, { connections: [], store: true })
    if (path === '/api/streams/validate' && request.method() === 'POST') {
      return json(route, { ok: false, errors: [{ message: 'the group has no ready principal' }] })
    }
    if (request.method() === 'PUT') {
      puts += 1
      return json(route, savedStream)
    }
    return json(route, {})
  })

  await page.goto('/streams')
  await page.getByRole('button', { name: 'New stream' }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByLabel('Name', { exact: true }).fill('orders')
  await dialog.getByLabel('Owner group', { exact: true }).selectOption('shop')
  await dialog.getByLabel('Topic', { exact: true }).fill('orders.created')
  await dialog.getByLabel('Table', { exact: true }).fill('shop.orders')
  await dialog.getByRole('button', { name: 'Create stream' }).click()

  await expect(dialog.getByRole('alert').getByText('the group has no ready principal')).toBeVisible()
  await expect(dialog).toBeVisible()
  expect(puts).toBe(0)
})
