import { test, expect, type Route } from '@playwright/test'

// The Data products page (#37 slice 4) and its freshness column (#37 slice 6).

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

async function stub(page: import('@playwright/test').Page, failHealth = false) {
  await page.route('**/api/**', (r) => {
    const path = new URL(r.request().url()).pathname
    if (path === '/api/me') {
      return json(r, { user: 'alice', auth_mode: 'oidc', demo: false, warehouse: 'warehouse', is_admin: false, groups: ['analytics'] })
    }
    if (path === '/api/catalog/catalogs') {
      return json(r, { catalogs: [{ name: 'warehouse', id: '1', canWrite: false, grants: [] }], default: 'warehouse' })
    }
    if (path === '/api/catalog/products') {
      return json(r, {
        catalog: 'warehouse',
        products: [
          { namespace: 'shop', table: 'daily_revenue', description: 'Revenue per day', owner: 'demo', domain: 'finance', lifecycle: 'published', replacedBy: null, readers: 5 },
          { namespace: 'shop', table: 'orders_raw', description: null, owner: null, domain: null, lifecycle: 'draft', replacedBy: null, readers: 0 },
        ],
      })
    }
    if (path === '/api/catalog/health') {
      if (failHealth) return json(r, { error: 'the catalog is not reachable' }, 502)
      return json(r, {
        checked: 1,
        tables: [{ catalog: 'warehouse', namespace: 'shop', name: 'daily_revenue', status: 'red', lastWrite: null, dueAt: null, nextDueAt: null, canEdit: false, detail: 'late: the promise was 07:00 Europe/Berlin' }],
      })
    }
    return json(r, {})
  })
}

test('a data product shows its freshness, and one without a promise shows a dash', async ({ page }) => {
  await stub(page)
  await page.goto('/products?catalog=warehouse')

  const revenue = page.getByRole('row', { name: /shop\.daily_revenue/ })
  await expect(revenue.getByText('Late', { exact: true })).toBeVisible()
  // Slice 7: the 5th cell is Readers (Table, Description, Owner, Freshness, Readers, State).
  await expect(revenue.getByRole('cell').nth(4)).toHaveText('5')
  const orders = page.getByRole('row', { name: /shop\.orders_raw/ })
  // The 4th cell is Freshness (Table, Description, Owner, Freshness, State); the Owner cell
  // of this row is a dash too, so the cell is named by its position, not by the word alone.
  await expect(orders.getByRole('cell').nth(3)).toHaveText('—')
})

test('a catalog whose health read fails still lists its products', async ({ page }) => {
  await stub(page, true)
  await page.goto('/products?catalog=warehouse')

  await expect(page.getByText('shop.daily_revenue')).toBeVisible()
  await expect(
    page.getByRole('row', { name: /shop\.daily_revenue/ }).getByRole('cell').nth(3),
  ).toHaveText('—')
})
