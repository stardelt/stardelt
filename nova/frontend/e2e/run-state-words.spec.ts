import { test, expect, type Page, type Route } from '@playwright/test'

// The run state words are Nova's own (stardelt-nova#1110, #818 decision 2): the scheduler's
// `upstream_failed` is "Not run (an earlier task failed)" and its `up_for_retry` is
// "Retrying". The Runs page renders every state through `runStateWord`, so the rows and the
// State filter follow `RUN_STATES`; a state Nova has not met still prints as it came.
// Every `/api/**` call is stubbed.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const ME = {
  user: 'alice', auth_mode: 'oidc', demo: false, warehouse: 'warehouse',
  is_admin: false, groups: ['analytics', 'finance', 'ops'],
}

const UPSTREAM_FAILED = {
  namespace: 'stardelt', name: 'daily_revenue', ownerGroup: 'analytics', kind: 'sql',
  runId: 'scheduled__1', state: 'upstream_failed', type: 'scheduled', logicalDate: null,
  startedAt: '2026-11-17T09:00:00Z', endedAt: '2026-11-17T09:01:00Z', durationSeconds: 60,
  triggeredBy: 'scheduler', note: null,
}
const UP_FOR_RETRY = {
  namespace: 'stardelt', name: 'nightly_upload', ownerGroup: 'finance', kind: 'notebook',
  runId: 'scheduled__2', state: 'up_for_retry', type: 'scheduled', logicalDate: null,
  startedAt: '2026-11-17T09:05:00Z', endedAt: null, durationSeconds: null,
  triggeredBy: 'scheduler', note: null,
}

async function open(page: Page) {
  await page.route('**/api/**', (route) => {
    const url = new URL(route.request().url())
    if (url.pathname === '/api/me') return json(route, ME)
    if (url.pathname === '/api/runs')
      return json(route, {
        runs: [UPSTREAM_FAILED, UP_FOR_RETRY],
        groups: ['analytics', 'finance'],
      })
    return json(route, [])
  })
  await page.goto('/runs')
  return page
}

test('the run state words are Nova’s own, not the scheduler’s', async ({ page }) => {
  await open(page)
  const table = page.getByRole('table', { name: /Every run of the notebooks and pipelines/ })

  // The rows say what Nova says.
  await expect(table.getByRole('row', { name: /daily_revenue/ })).toContainText(
    'Not run (an earlier task failed)',
  )
  await expect(table.getByRole('row', { name: /nightly_upload/ })).toContainText('Retrying')

  // The State filter shows every option through `runStateWord`. `upstream_failed` is one of
  // the six filterable states, so its option carries the long word; `up_for_retry` is a badge
  // a row shows, not a filter of its own, so "Retrying" is not an option.
  const state = page.getByLabel('State', { exact: true })
  await expect(state.getByRole('option', { name: 'Not run (an earlier task failed)', exact: true }))
    .toHaveCount(1)
  await expect(state.getByRole('option', { name: 'Retrying', exact: true })).toHaveCount(0)

  // The scheduler's own words appear nowhere.
  await expect(page.getByText('Upstream failed', { exact: false })).toHaveCount(0)
  await expect(page.getByText('Up for retry', { exact: false })).toHaveCount(0)
})
