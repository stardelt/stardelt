import { test, expect, type Page, type Route } from '@playwright/test'

// The Models page (stardelt-nova#1001, this pass #1009): where prompts go, what the probe
// measured, this month's tokens against each group's quota, and the calls in words. The page
// arranges the API's two answers; both are stubbed here, because the no-cluster backend
// answers an empty endpoint list.

const ME = {
  user: 'root',
  auth_mode: 'oidc',
  demo: false,
  warehouse: 'warehouse',
  is_admin: true,
  groups: ['admins'],
}

// Two connected endpoints: a cloud one with a monthly quota and a passed probe, and an
// on-premise one without a quota and without a probe yet.
const ENDPOINTS = [
  {
    name: 'frontier',
    model: 'frontier-1',
    location: 'cloud',
    purposes: ['ask', 'agents'],
    sendRows: false,
    reachable: true,
    toolCalling: true,
    jsonOutput: true,
    contextTokens: 32768,
    checkedAt: '2026-11-15T08:00:00Z',
    message: 'ok',
    perGroupMonthlyTokens: 2_000_000,
    perPersonDailyTokens: null,
  },
  {
    name: 'local',
    model: 'qwen3-4b',
    location: 'on-premise',
    purposes: ['ask'],
    sendRows: false,
    reachable: null,
    toolCalling: null,
    jsonOutput: null,
    contextTokens: null,
    checkedAt: null,
    message: null,
    perGroupMonthlyTokens: null,
    perPersonDailyTokens: null,
  },
]

// 1,200,000 in + 844,775 out = 2,044,775 tokens, which is 102 % of the 2,000,000 quota.
const USAGE = {
  month: '2026-11',
  groups: [
    {
      group: 'shop',
      endpoint: 'frontier',
      tokensIn: 1_200_000,
      tokensOut: 844_775,
      calls: 41,
      estimatedCalls: 0,
    },
    {
      group: 'shop',
      endpoint: 'local',
      tokensIn: 900,
      tokensOut: 120,
      calls: 2,
      estimatedCalls: 0,
    },
  ],
  calls: [
    {
      atMs: 1_700_000_000_000,
      sub: 'alice',
      group: 'shop',
      feature: 'ask',
      endpoint: 'frontier',
      model: 'frontier-1',
      tokensIn: 1200,
      tokensOut: 180,
      tokensEstimated: false,
      latencyMs: 900,
      outcome: 'ok',
    },
  ],
}

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

async function stub(page: Page) {
  await page.route('**/api/me', (route) => json(route, ME))
  await page.route('**/api/model-endpoints', (route) => json(route, ENDPOINTS))
  await page.route('**/api/admin/model-usage**', (route) => json(route, USAGE))
}

async function dark(page: Page, width: number) {
  await page.setViewportSize({ width, height: 900 })
  await page.emulateMedia({ colorScheme: 'dark' })
  await page.addInitScript(() => {
    try {
      localStorage.setItem('stardelt-nova:theme', 'dark')
    } catch {
      /* ignore */
    }
  })
}

test('a cloud endpoint warns where its prompts go, and the probe lists its checks', async ({ page }) => {
  await dark(page, 1440)
  await stub(page)

  await page.goto('/admin/models')
  await expect(page.getByRole('heading', { name: 'Models' })).toBeVisible()

  // The one privacy fact on the page is not grey like `on-premise`.
  await expect(page.getByText('⚠ leaves your cluster')).toBeVisible()
  await expect(page.getByRole('cell', { name: 'on-premise', exact: true })).toBeVisible()

  // `Ready` carries the measurements behind it.
  await expect(page.getByText('✓ answers · ✓ tools · ✓ JSON · 32,768 tokens')).toBeVisible()
  await expect(page.getByText('✓ tools')).toBeVisible()

  await page.screenshot({ path: '../docs/screenshots/1009/models-1440-dark.png' })
})

test('each group row shows its share of the monthly quota, or says there is none', async ({ page }) => {
  await dark(page, 1440)
  await stub(page)

  await page.goto('/admin/models')

  // 2,044,775 of 2,000,000 is over the quota, so the line is red — and says the number,
  // never the colour alone.
  const quota = page.getByText('of 2,000,000 · 102 %')
  await expect(quota).toBeVisible()
  await expect(quota).toHaveClass(/text-bad/)

  // The endpoint without a quota says so instead of leaving the reader to guess.
  await expect(page.getByText('no limit')).toBeVisible()
})

test('the calls table says Ask, not ask', async ({ page }) => {
  await dark(page, 1440)
  await stub(page)

  await page.goto('/admin/models')
  const calls = page.getByRole('table', { name: 'The last model calls, newest first' })
  await expect(calls.getByRole('cell', { name: 'Ask', exact: true })).toBeVisible()
  await expect(calls.getByRole('cell', { name: 'ask', exact: true })).toHaveCount(0)
})

test('the page fits a phone', async ({ page }) => {
  await dark(page, 390)
  await stub(page)

  await page.goto('/admin/models')
  await expect(page.getByRole('heading', { name: 'Models' })).toBeVisible()
  await expect(page.getByText('⚠ leaves your cluster')).toBeVisible()
  await page.screenshot({ path: '../docs/screenshots/1009/models-390-dark.png' })
})
