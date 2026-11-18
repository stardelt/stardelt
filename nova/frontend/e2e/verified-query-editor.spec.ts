import { test, expect, type Route } from '@playwright/test'

// "Save as verified query" in the SQL editor (stardelt-nova#911 slice 2c): the person who
// just ran the statement they trust picks the table, keeps the question and, when the API
// can hold them, this run's rows as the expected answer. Every `/api/**` call is stubbed,
// so the test needs no cluster and no database.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const ME = {
  user: 'alice',
  auth_mode: 'oidc',
  demo: false,
  warehouse: 'warehouse',
  is_admin: false,
  groups: ['sales'],
}

const CATALOGS = { catalogs: [{ name: 'lakehouse' }], default: 'lakehouse' }

/** The two rows the stubbed engine answers; the dialog offers them as the expected answer. */
const ROWS = [
  [1, 'a'],
  [2, 'b'],
]

/**
 * The SQL editor, stubbed. `onCreate` sees the create request and returns the status the
 * verified-query route answers, so one helper serves the success and the refusal.
 */
async function stubApi(
  page: import('@playwright/test').Page,
  opts: { onCreate?: (body: unknown) => { status: number; body: unknown } } = {},
) {
  await page.route('**/api/**', (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname

    if (path === '/api/me') return json(route, ME)
    if (path === '/api/settings') return json(route, {})
    if (path.startsWith('/api/settings/')) return json(route, { key: path.split('/').pop(), value: null })
    if (path === '/api/saved-queries') return json(route, [])
    if (path === '/api/catalog/catalogs') return json(route, CATALOGS)
    if (path === '/api/catalog/namespaces') return json(route, { namespaces: [['sales']] })
    if (path === '/api/catalog/namespaces/sales/tables') {
      return json(route, { identifiers: [{ namespace: ['sales'], name: 'orders' }] })
    }
    if (path === '/api/catalog/tables/sales/orders/verified-queries' && request.method() === 'POST') {
      const answer = opts.onCreate?.(request.postDataJSON()) ?? {
        status: 201,
        body: {
          id: 1,
          question: 'q',
          sql: 'SELECT 1',
          verifiedBy: 'alice',
          verifiedAt: '2026-11-15T12:00:00Z',
          expected: null,
          lastCheckAt: null,
          lastCheckPassed: null,
          lastCheckMessage: null,
        },
      }
      return json(route, answer.body, answer.status)
    }
    if (path === '/api/query') {
      return json(route, {
        columns: [
          { name: 'id', type: 'integer' },
          { name: 'note', type: 'varchar' },
        ],
        rows: ROWS,
        row_count: ROWS.length,
      })
    }
    return json(route, {})
  })
}

test('the toolbar opens the dialog and Save keeps the question and the statement', async ({ page }) => {
  await stubApi(page)
  await page.goto('/query?catalog=lakehouse')

  await page.getByRole('button', { name: 'Save as verified query' }).click()

  const dialog = page.getByRole('dialog')
  await expect(dialog).toBeVisible()
  await expect(dialog.getByRole('heading', { name: 'Save as verified query' })).toBeVisible()

  // The statement at the cursor is prefilled; the table and the question are the person's
  // to name, because a statement does not say which table owns its answer.
  const statement = dialog.getByRole('textbox', { name: 'Statement' })
  await expect(statement).toHaveValue(/SELECT/)
  const sql = await statement.inputValue()

  await dialog.getByRole('combobox', { name: 'Table' }).selectOption('sales.orders')
  await dialog.getByRole('textbox', { name: 'Question' }).fill('What did revenue last month come to?')

  const posted = page.waitForRequest(
    (r) => r.method() === 'POST' && new URL(r.url()).pathname === '/api/catalog/tables/sales/orders/verified-queries',
  )
  await dialog.getByRole('button', { name: 'Save' }).click()

  const request = await posted
  expect(new URL(request.url()).searchParams.get('catalog')).toBe('lakehouse')
  expect(request.postDataJSON()).toEqual({ question: 'What did revenue last month come to?', sql })
  expect((request.postDataJSON() as { expected?: unknown }).expected).toBeUndefined()

  await expect(dialog).toHaveCount(0)
  await expect(page.getByRole('status')).toHaveText(
    'Saved — lakehouse.sales.orders lists it under Verified queries.',
  )
})

test("this run's result becomes the expected answer", async ({ page }) => {
  await stubApi(page)
  await page.goto('/query?catalog=lakehouse')

  // Run the sample, so the dialog has rows to offer.
  await page.getByRole('button', { name: /^Run/ }).click()
  await expect(page.locator('tbody tr')).toHaveCount(ROWS.length)

  await page.getByRole('button', { name: 'Save as verified query' }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByRole('combobox', { name: 'Table' }).selectOption('sales.orders')
  await dialog.getByRole('textbox', { name: 'Question' }).fill('Which two orders came first?')

  const option = dialog.getByRole('radio', { name: "Use this run's result (2 rows)" })
  await expect(option).toBeEnabled()
  await option.check()

  const posted = page.waitForRequest(
    (r) => r.method() === 'POST' && new URL(r.url()).pathname === '/api/catalog/tables/sales/orders/verified-queries',
  )
  await dialog.getByRole('button', { name: 'Save' }).click()

  expect((await posted).postDataJSON()).toMatchObject({ expected: ROWS })
})

test('a refusal keeps the dialog open and shows the API sentence', async ({ page }) => {
  const refusal = "Only the table's owner group and administrators may verify a query about it."
  await stubApi(page, { onCreate: () => ({ status: 403, body: { error: refusal } }) })
  await page.goto('/query?catalog=lakehouse')

  await page.getByRole('button', { name: 'Save as verified query' }).click()
  const dialog = page.getByRole('dialog')
  await dialog.getByRole('combobox', { name: 'Table' }).selectOption('sales.orders')
  await dialog.getByRole('textbox', { name: 'Question' }).fill('What did revenue last month come to?')
  await dialog.getByRole('button', { name: 'Save' }).click()

  await expect(dialog.getByRole('alert')).toHaveText(refusal)
  await expect(dialog).toBeVisible()
})
