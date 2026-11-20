import { expect, test, type Route } from '@playwright/test'

// The report page (#120, #79 point 6): the executed notebook of the latest **successful** run of
// a notebook pipeline, reached with one link and no run id. Every `/api/**` call is stubbed, as in
// `pipeline-connection.spec.ts`. The browser is pinned to UTC so the printed time is fixed.

test.use({ timezoneId: 'UTC' })

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const ME = {
  user: 'alice', auth_mode: 'dev', demo: false, warehouse: 'warehouse',
  is_admin: false, groups: ['analytics'],
}

const run = (runId: string, state: string, startedAt: string, endedAt: string) => ({
  runId, state, type: 'scheduled', logicalDate: startedAt, startedAt, endedAt,
  durationSeconds: 60, triggeredBy: null, note: null,
})

// The list is newest-last on purpose: the page must order the successes itself.
const FAILED = run('failed-1', 'failed', '2026-11-19T03:00:00Z', '2026-11-19T03:05:00Z')
const OK_1 = run('ok-1', 'success', '2026-11-18T03:00:00Z', '2026-11-18T12:00:00Z')
const OK_0 = run('ok-0', 'success', '2026-11-17T03:00:00Z', '2026-11-17T04:00:00Z')

const pipeline = (tasks: unknown[], runs: unknown[]) => ({
  name: 'shop-report', namespace: 'stardelt', ownerGroup: 'analytics',
  description: null, canRun: true, canEdit: false,
  schedule: '0 3 * * *', paused: false, loaded: true,
  nextRun: '2026-11-20T03:00:00Z',
  state: 'idle', lastRun: runs[0] ?? null,
  tasks, runAs: 'pipeline-analytics', concurrency: 'Forbid', createdAt: null,
  runs, params: {}, retries: null,
})

const NOTEBOOK_TASK = {
  name: 'run', kind: 'notebook', notebook: 'shop-report.ipynb', objectVersion: 'v1',
  notebookParams: {},
}
const SQL_TASK = { name: 'load', kind: 'sql', sql: 'select 1' }

/** A minimal nbformat 4 document with one executed cell. */
const EXECUTED = {
  nbformat: 4,
  nbformat_minor: 5,
  metadata: {},
  cells: [{
    cell_type: 'code', metadata: {}, execution_count: 1,
    source: ["print('shop report')"],
    outputs: [{ output_type: 'stream', name: 'stdout', text: ['shop report\n'] }],
  }],
}

/** Stub every API call the page makes; `seen` collects the notebook requests. */
async function stub(page: import('@playwright/test').Page, body: unknown) {
  const seen: string[] = []
  await page.route('**/api/**', (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/me') return json(route, ME)
    if (path === '/api/pipelines/stardelt/shop-report') return json(route, body)
    // The executed notebook: `/runs/{runId}/tasks/{taskId}/notebook`.
    if (path.endsWith('/notebook')) {
      seen.push(path)
      return json(route, EXECUTED)
    }
    return json(route, [])
  })
  return seen
}

test('reports the latest successful run', async ({ page }) => {
  const seen = await stub(page, pipeline([NOTEBOOK_TASK], [FAILED, OK_1, OK_0]))
  await page.goto('/reports/stardelt/shop-report')

  await expect(page.getByRole('heading', { level: 1 })).toHaveText('shop-report')
  await expect(page.getByTestId('report-as-of')).toContainText('as of 2026-11-18 12:00:00 UTC')
  await expect(page.getByTestId('report-as-of')).toContainText('next run 2026-11-20 03:00:00 UTC')
  await expect(
    page.frameLocator('iframe[title="Executed notebook"]').locator('pre.out'),
  ).toHaveText(/shop report/)
  // The newest success was drawn, not the failed run the scheduler listed first.
  expect(seen).toEqual(['/api/pipelines/stardelt/shop-report/runs/ok-1/tasks/run/notebook'])
  // The earlier success is one click away, as a run notebook.
  await expect(page.getByRole('link', { name: '2026-11-17 04:00:00 UTC' })).toHaveAttribute(
    'href',
    '/pipelines/stardelt/shop-report/runs/ok-0/notebook/run',
  )
})

test('says so when the latest run failed', async ({ page }) => {
  const seen = await stub(page, pipeline([NOTEBOOK_TASK], [FAILED]))
  await page.goto('/reports/stardelt/shop-report')

  await expect(page.getByText('No report yet')).toBeVisible()
  await expect(page.getByText(/The latest run is failed\./)).toBeVisible()
  expect(seen).toEqual([])
})

test('a pipeline without a notebook task', async ({ page }) => {
  const seen = await stub(page, pipeline([SQL_TASK], [OK_1]))
  await page.goto('/reports/stardelt/shop-report')

  await expect(page.getByText('No notebook here')).toBeVisible()
  await expect(page.getByText('This pipeline does not run a notebook.')).toBeVisible()
  expect(seen).toEqual([])
})
