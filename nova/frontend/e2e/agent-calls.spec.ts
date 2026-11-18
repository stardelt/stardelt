import { test, expect, type Route } from '@playwright/test'

// The Agent calls page (stardelt-nova#975): the administrator's read of the `/mcp` audit
// trail — when, who, which client, which tool, the status and the duration. The page
// renders the API's answer; it never asks for the arguments or the results.

const ME = {
  user: 'root',
  auth_mode: 'oidc',
  demo: false,
  warehouse: 'warehouse',
  is_admin: true,
  groups: ['admins'],
}

const CALLS = {
  calls: [
    {
      atMs: 1_700_000_000_000,
      sub: 'alice',
      client: 'claude-code',
      tool: 'platform_status',
      status: 'ok',
      durationMs: 42,
      argumentsSha256: 'a'.repeat(64),
    },
    {
      atMs: 1_699_999_000_000,
      sub: 'bob',
      tool: 'sql_read',
      status: 'error',
      durationMs: 1200,
      argumentsSha256: 'b'.repeat(64),
    },
  ],
}

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

test('the calls are listed, and a call without a client shows a dash', async ({ page }) => {
  await page.route('**/api/me', (route) => json(route, ME))
  await page.route('**/api/admin/mcp-audit**', (route) => json(route, CALLS))

  await page.goto('/admin/agent-calls')
  await expect(page.getByRole('heading', { name: 'Agent calls' })).toBeVisible()
  await expect(page.getByRole('cell', { name: 'platform_status', exact: true })).toBeVisible()
  await expect(page.getByRole('cell', { name: 'sql_read', exact: true })).toBeVisible()
  await expect(page.getByRole('cell', { name: 'alice', exact: true })).toBeVisible()
  await expect(page.getByRole('cell', { name: 'claude-code', exact: true })).toBeVisible()
  // bob's token carried no `azp`, so the client is absent and the cell says so.
  await expect(page.getByRole('cell', { name: '—', exact: true })).toBeVisible()
})

test('the filters ask the API for one person and one tool', async ({ page }) => {
  const asked: string[] = []
  await page.route('**/api/me', (route) => json(route, ME))
  await page.route('**/api/admin/mcp-audit**', (route) => {
    asked.push(route.request().url())
    return json(route, CALLS)
  })

  await page.goto('/admin/agent-calls')
  await expect(page.getByRole('cell', { name: 'platform_status', exact: true })).toBeVisible()

  await page.getByLabel('Person').fill('alice')
  await expect
    .poll(() => asked.map((url) => new URL(url).searchParams.get('sub')))
    .toContain('alice')

  await page.getByLabel('Tool').fill('sql_read')
  await expect
    .poll(() => asked.map((url) => new URL(url).searchParams.get('tool')))
    .toContain('sql_read')
})
