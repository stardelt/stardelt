import { test, expect, type Route } from '@playwright/test'

// The Evals page stores a new eval in the catalog it is showing (#2130).
// It also runs one: Ask, execute, post the answer (#125 point 2).

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const ME = { user: 'alice', auth_mode: 'oidc', demo: false, warehouse: 'playground', is_admin: false, groups: ['demo'] }
const CATALOGS = { catalogs: [{ name: 'playground', id: '1', canWrite: true, grants: [] }], default: 'playground' }

const row = (lastRun: unknown) => ({
  id: 1,
  ownerGroup: 'demo',
  question: 'How many customers?',
  expected: { kind: 'value', value: 200 },
  createdBy: 'alice',
  createdAt: '2026-11-27T00:00:00Z',
  lastRun,
})

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

test('a run asks, executes and records the verdict', async ({ page }) => {
  const calls: string[] = []
  let posted: unknown = null
  let ran = false
  await page.route('**/api/**', (r) => {
    const req = r.request()
    const url = new URL(req.url())
    if (url.pathname === '/api/me') return json(r, ME)
    if (url.pathname === '/api/catalog/catalogs') return json(r, CATALOGS)
    if (url.pathname === '/api/catalog/evals' && req.method() === 'GET') {
      return json(r, [row(ran ? { id: 1, passed: true, createdAt: '2026-11-28T00:00:00Z' } : null)])
    }
    if (url.pathname === '/api/ask') {
      calls.push(url.pathname + url.search)
      return json(r, { endpoint: 'e', model: 'm', sql: 'select count(*) from gold.shop.customers', sentence: '', tables: [], explain: 'passed' })
    }
    if (url.pathname === '/api/query') {
      calls.push(url.pathname)
      return json(r, { columns: ['count'], rows: [[200]], rowCount: 1, truncated: false })
    }
    if (url.pathname === '/api/evals/1/runs' && req.method() === 'POST') {
      calls.push(url.pathname)
      posted = req.postDataJSON()
      ran = true
      return json(r, { id: 1, evalId: 1, passed: true, createdBy: 'alice', createdAt: '2026-11-28T00:00:00Z' }, 201)
    }
    return json(r, {})
  })

  await page.goto('/evals?catalog=playground')
  await page.getByRole('button', { name: 'Run the eval: How many customers?' }).click()

  await expect.poll(() => calls).toEqual(['/api/ask?catalog=playground', '/api/query', '/api/evals/1/runs'])
  expect(posted).toEqual({
    model: 'm',
    endpoint: 'e',
    sql: 'select count(*) from gold.shop.customers',
    rows: [[200]],
  })
  await expect(page.getByRole('cell', { name: 'passed' })).toBeVisible()
})

test('a refused Ask is shown and no verdict is posted', async ({ page }) => {
  let runs = 0
  await page.route('**/api/**', (r) => {
    const req = r.request()
    const url = new URL(req.url())
    if (url.pathname === '/api/me') return json(r, ME)
    if (url.pathname === '/api/catalog/catalogs') return json(r, CATALOGS)
    if (url.pathname === '/api/catalog/evals') return json(r, [row(null)])
    if (url.pathname === '/api/ask') return json(r, { error: 'no model endpoint is connected' }, 503)
    if (url.pathname === '/api/evals/1/runs') {
      runs += 1
      return json(r, {}, 201)
    }
    return json(r, {})
  })

  await page.goto('/evals?catalog=playground')
  await page.getByRole('button', { name: 'Run the eval: How many customers?' }).click()

  await expect(page.getByText('no model endpoint is connected')).toBeVisible()
  expect(runs).toBe(0)
})
