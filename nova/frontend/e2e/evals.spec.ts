import { test, expect, type Route } from '@playwright/test'

// #2130: the Evals page stores a new eval in the catalog it is showing.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

test('a new eval is stored in the catalog the page shows', async ({ page }) => {
  const posts: string[] = []
  await page.route('**/api/**', (r) => {
    const req = r.request()
    const url = new URL(req.url())
    if (url.pathname === '/api/me') {
      return json(r, { user: 'alice', auth_mode: 'oidc', demo: false, warehouse: 'playground', is_admin: false, groups: ['demo'] })
    }
    if (url.pathname === '/api/catalog/catalogs') {
      return json(r, { catalogs: [{ name: 'playground', id: '1', canWrite: true, grants: [] }], default: 'playground' })
    }
    if (url.pathname === '/api/catalog/evals' && req.method() === 'POST') {
      posts.push(url.pathname + url.search)
      return json(r, { id: 1, ownerGroup: 'demo', question: 'How many customers?', expected: { kind: 'value', value: 200 }, createdBy: 'alice', createdAt: '2026-11-27T00:00:00Z' }, 201)
    }
    if (url.pathname === '/api/catalog/evals') return json(r, [])
    return json(r, {})
  })

  await page.goto('/evals?catalog=playground')
  await page.getByRole('button', { name: 'Add eval' }).click()
  await page.getByLabel('Question').fill('How many customers?')
  await page.getByLabel('Owner group').fill('demo')
  await page.getByRole('button', { name: 'Add eval' }).click()

  await expect.poll(() => posts).toEqual(['/api/catalog/evals?catalog=playground'])
})
