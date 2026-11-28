import { test, expect, type Route } from '@playwright/test'

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

test('the catalog page links its eval score to the Evals page', async ({ page }) => {
  await page.route('**/api/**', (r) => {
    const url = new URL(r.request().url())
    if (url.pathname === '/api/me') {
      return json(r, { user: 'alice', auth_mode: 'oidc', demo: false, warehouse: 'playground', is_admin: false, groups: ['analytics'] })
    }
    if (url.pathname === '/api/catalog/catalogs') {
      return json(r, { catalogs: [{ name: 'gold', id: '1', canWrite: true, grants: [] }], default: 'gold' })
    }
    if (url.pathname === '/api/catalog/namespaces') return json(r, { namespaces: [] })
    if (url.pathname === '/api/catalog/evals') {
      return json(r, [
        { id: 1, ownerGroup: 'analytics', question: 'How many orders?', expected: { kind: 'value', value: 1284 }, createdBy: 'alice', createdAt: '2026-11-27T00:00:00Z', lastRun: { id: 9, passed: true, createdAt: '2026-11-28T00:00:00Z' } },
        { id: 2, ownerGroup: 'analytics', question: 'Which region?', expected: { kind: 'value', value: 'north' }, createdBy: 'alice', createdAt: '2026-11-27T00:00:00Z', lastRun: { id: 10, passed: false, reason: 'the answer differs', createdAt: '2026-11-28T00:00:00Z' } },
        { id: 3, ownerGroup: 'analytics', question: 'Never run', expected: { kind: 'value', value: 1 }, createdBy: 'alice', createdAt: '2026-11-27T00:00:00Z', lastRun: null },
      ])
    }
    return json(r, {})
  })

  await page.goto('/catalog?catalog=gold')
  const score = page.getByRole('link', { name: '1 of 2 evals passed' })
  await expect(score).toBeVisible()
  await expect(score).toHaveAttribute('href', '/evals?catalog=gold')
})
