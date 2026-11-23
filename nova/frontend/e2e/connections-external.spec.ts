import { test, expect, type Route } from '@playwright/test'

// The Connections page in `spec.secrets.mode: external` (#1428, point 2c): the list says
// `store: false`, so the new-connection form names a path in the enterprise's store and
// sends no value. Every `/api/**` call is stubbed, so the test needs no cluster.

const ME = {
  user: 'e2e',
  auth_mode: 'static-dev',
  demo: false,
  warehouse: '',
  groups: ['shop'],
}

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

test('a new connection in external mode names a path in the store (#1428)', async ({ page }) => {
  await page.route('**/api/**', (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    if (path === '/api/me') return json(route, ME)
    if (path === '/api/connections' && request.method() === 'GET') {
      return json(route, { connections: [], store: false })
    }
    if (path === '/api/connections' && request.method() === 'POST') {
      return json(
        route,
        {
          name: 'crm-db',
          type: 'postgresql',
          scope: 'group',
          ownerGroup: 'shop',
          config: { host: 'db.shop.example.com' },
          secretKeys: ['user', 'password'],
          lastRotated: null,
          version: 1,
          sync: null,
          breakGlass: null,
          member: true,
        },
        201,
      )
    }
    return json(route, {})
  })

  await page.goto('/connections')
  // The page says which mode it is in before the form opens.
  await expect(
    page.getByText("This install reads connections from your enterprise's secret store.", { exact: false }),
  ).toBeVisible()
  await page.getByRole('button', { name: 'New connection' }).click()

  const dialog = page.getByRole('dialog')
  await expect(dialog.getByLabel('Path in the store', { exact: true })).toBeVisible()
  await expect(dialog.getByLabel('Password', { exact: true })).toHaveCount(0)

  await dialog.getByLabel('Name', { exact: true }).fill('crm-db')
  await dialog.getByLabel('Host', { exact: true }).fill('db.shop.example.com')
  await dialog.getByLabel('Path in the store', { exact: true }).fill('apps/shop/db')

  const posted = page.waitForRequest(
    (request) => request.method() === 'POST' && new URL(request.url()).pathname === '/api/connections',
  )
  await dialog.getByRole('button', { name: 'Create connection' }).click()
  const body = (await posted).postDataJSON() as Record<string, unknown>

  expect(body.remoteRef).toBe('apps/shop/db')
  expect(body.secretKeys).toEqual(['user', 'password'])
  expect(body.secret).toEqual({})
  expect(body.scope).toBe('group')
})
