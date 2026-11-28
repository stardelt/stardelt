import { test, expect, type Route } from '@playwright/test'

// A stream's own page (#1989): the four facts a person needs about it — what it reads, where it
// writes, who may see it and whether it runs — and its Stop/Resume button. Every `/api/**` call
// that matters is stubbed, so no cluster is needed.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const STREAM = {
  namespace: 'stardelt',
  name: 'orders',
  owner_group: 'acme',
  phase: 'Running',
  reason: null,
  last_error: null,
  last_checkpoint_at: null,
  attempts: 2,
  last_submitted_at: '2026-11-26T10:00:00Z',
  stopped: false,
  deletion_policy: 'Retain',
  source_connection: 'kafka',
  source_topic: 'orders.created',
  sink_catalog: 'gold',
  sink_table: 'shop.orders',
  sink_mode: 'merge',
  run_as: null,
  viewers: ['visitors'],
}

test('the page shows the stream’s source, sink, viewers and Stop', async ({ page }) => {
  await page.route('**/api/streams', (route) => json(route, { streams: [STREAM] }))

  await page.goto('/streams/stardelt/orders')

  const main = page.locator('main')
  await expect(main.getByRole('heading', { name: 'orders', exact: true })).toBeVisible()
  await expect(main.getByText('acme')).toBeVisible()
  await expect(main.getByText('gold.shop.orders')).toBeVisible()
  await expect(main.getByText('visitors')).toBeVisible()
  await expect(main.getByRole('button', { name: 'Stop orders' })).toBeVisible()
})

test('Stop flips the button to Resume, and a reload keeps it', async ({ page }) => {
  let stopped = false
  await page.route('**/api/streams', (route) =>
    json(route, { streams: [{ ...STREAM, stopped, phase: stopped ? 'Stopped' : 'Running' }] }),
  )
  await page.route('**/api/streams/stardelt/orders', (route) => {
    stopped = true
    return json(route, { ...STREAM, stopped: true, phase: 'Stopped' })
  })

  await page.goto('/streams/stardelt/orders')
  await page.getByRole('button', { name: 'Stop orders' }).click()

  await expect(page.getByRole('button', { name: 'Resume orders' })).toBeVisible()

  await page.reload()
  await expect(page.getByRole('button', { name: 'Resume orders' })).toBeVisible()
})

test('Delete asks once, says what happens to the topic, and goes back to the list', async ({ page }) => {
  const deletes: string[] = []
  await page.route('**/api/streams/stardelt/orders', (route) => {
    deletes.push(route.request().method())
    return route.fulfill({ status: 204, body: '' })
  })
  await page.route('**/api/streams', (route) => json(route, { streams: [STREAM] }))

  await page.goto('/streams/stardelt/orders')
  await page.getByRole('button', { name: 'Delete orders' }).click()

  const dialog = page.getByRole('dialog')
  await expect(dialog.getByText('Its Kafka topic and every message in it stay')).toBeVisible()

  await dialog.getByRole('button', { name: 'Delete', exact: true }).click()

  await expect(page).toHaveURL(/\/streams$/)
  expect(deletes).toEqual(['DELETE'])
})

test('a stream whose topic is deleted with it says so', async ({ page }) => {
  await page.route('**/api/streams', (route) =>
    json(route, { streams: [{ ...STREAM, deletion_policy: 'Delete' }] }),
  )

  await page.goto('/streams/stardelt/orders')
  await page.getByRole('button', { name: 'Delete orders' }).click()

  const dialog = page.getByRole('dialog')
  await expect(dialog.getByText('its Kafka topic and every message in it are deleted too')).toBeVisible()
})

test('a refused Delete keeps the dialog open and shows the sentence', async ({ page }) => {
  await page.route('**/api/streams/stardelt/orders', (route) =>
    json(route, { error: 'this stream belongs to another group' }, 403),
  )
  await page.route('**/api/streams', (route) => json(route, { streams: [STREAM] }))

  await page.goto('/streams/stardelt/orders')
  await page.getByRole('button', { name: 'Delete orders' }).click()

  const dialog = page.getByRole('dialog')
  await dialog.getByRole('button', { name: 'Delete', exact: true }).click()

  await expect(dialog.getByText('this stream belongs to another group')).toBeVisible()
  await expect(dialog.getByText('Its Kafka topic and every message in it stay')).toBeVisible()
  await expect(page).toHaveURL(/\/streams\/stardelt\/orders$/)
})

test('a stream the caller cannot see says it does not exist', async ({ page }) => {
  await page.route('**/api/streams', (route) => json(route, { streams: [] }))

  await page.goto('/streams/stardelt/missing')

  const main = page.locator('main')
  await expect(main.getByText('Stream not found')).toBeVisible()
  await expect(main.getByRole('link', { name: 'Back to streams' })).toHaveAttribute('href', '/streams')
})

test('the sink facts show the newest row count and commit', async ({ page }) => {
  await page.route('**/api/streams', (route) => json(route, { streams: [STREAM] }))
  await page.route('**/api/catalog/tables/shop/orders/history*', (route) =>
    json(route, {
      columns: [
        { name: 'committed_at', type: 'timestamp(6) with time zone' },
        { name: 'summary', type: 'map(varchar, varchar)' },
      ],
      rows: [['2026-11-27 03:00:00.000 UTC', { 'total-records': '42', operation: 'append' }]],
    }),
  )

  await page.goto('/streams/stardelt/orders')

  const main = page.locator('main')
  await expect(main.getByText('42', { exact: true })).toBeVisible()
  await expect(main.getByText('Last commit')).toBeVisible()
})

test('a sink table that does not exist says so', async ({ page }) => {
  await page.route('**/api/streams', (route) => json(route, { streams: [STREAM] }))
  await page.route('**/api/catalog/tables/shop/orders/history*', (route) =>
    json(route, { error: 'no such table' }, 404),
  )

  await page.goto('/streams/stardelt/orders')

  const main = page.locator('main')
  await expect(main.getByText('The sink table does not exist.')).toBeVisible()
})
