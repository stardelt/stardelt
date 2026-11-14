import { test, expect, type Page, type Route } from '@playwright/test'

// The run panel speaks Nova's words (stardelt-nova#959): the task row says the kind the
// pipeline declared, not the scheduler's operator class; the run says who started it in
// Nova's words, not the scheduler's trigger id. Every `/api/**` call is stubbed.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const ME = {
  user: 'alice', auth_mode: 'oidc', demo: false, warehouse: 'warehouse',
  is_admin: false, groups: ['analytics', 'finance', 'ops'],
}

const RUN = {
  runId: 'manual__2', state: 'succeeded', type: 'manual', logicalDate: null,
  startedAt: '2026-11-14T03:00:00Z', endedAt: '2026-11-14T03:05:00Z',
  durationSeconds: 300, triggeredBy: 'rest_api', note: null,
}

const PIPELINE = {
  name: 'nightly_upload', namespace: 'stardelt', ownerGroup: 'finance',
  canRun: true, canEdit: true,
  schedule: '0 2 * * *', paused: false, loaded: true, nextRun: null,
  state: 'succeeded',
  lastRun: null, runAs: 'svc-pipeline-finance', concurrency: 'Allow',
  createdAt: '2026-11-01T09:00:00Z', runtime: 'airflow', env: [],
  sparkConf: {}, idleStopMinutes: 0,
  tasks: [{ name: 'load', kind: 'sql' }],
  runs: [RUN], params: {}, retries: null,
}

const RUN_DETAIL = {
  run: RUN,
  tasks: [{
    taskId: 'load', state: 'success', tryNumber: 1,
    startedAt: '2026-11-14T03:00:10Z', endedAt: '2026-11-14T03:02:00Z',
    durationSeconds: 110, operator: 'PythonOperator',
  }],
}

const QUERIES = {
  queries: [{
    id: 'q1', state: 'FINISHED', taskId: 'load', startedAt: Date.UTC(2026, 10, 14, 3, 0, 0),
    wallMs: 6200, executionMs: 6000, inputBytes: 4096, inputRows: 10,
    sql: 'insert into gold select 1',
  }],
}

async function open(page: Page) {
  await page.route('**/api/**', (route) => {
    const url = new URL(route.request().url())
    if (url.pathname === '/api/me') return json(route, ME)
    if (url.pathname === '/api/pipelines/stardelt/nightly_upload') return json(route, PIPELINE)
    if (url.pathname === '/api/pipelines/stardelt/nightly_upload/runs/manual__2') return json(route, RUN_DETAIL)
    if (url.pathname === '/api/pipelines/stardelt/nightly_upload/runs/manual__2/queries') return json(route, QUERIES)
    return json(route, [])
  })
  await page.setViewportSize({ width: 1440, height: 900 })
  await page.emulateMedia({ colorScheme: 'dark' })
  await page.addInitScript(() => {
    try {
      localStorage.setItem('stardelt-nova:theme', 'dark')
    } catch {
      /* ignore */
    }
  })
  await page.goto('/pipelines/stardelt/nightly_upload')
  await page.getByRole('button', { name: 'Show tasks of run manual__2' }).click()
  return page
}

test('the run panel speaks Nova’s words, not the scheduler’s', async ({ page }) => {
  await open(page)

  // Triggered by: the scheduler's `rest_api` is Nova's Run now.
  const runRow = page.locator('tr', { has: page.getByRole('button', { name: /tasks of run manual__2/ }) })
  await expect(runRow.getByText('Nova', { exact: true })).toBeVisible()

  // The task row names the kind the pipeline declared, not the operator class behind it.
  const tasks = page.getByRole('table', { name: 'Task instances of this run.' })
  const taskRow = tasks.locator('tbody tr', { hasText: 'load' })
  await expect(taskRow.getByText('sql', { exact: true })).toBeVisible()

  // A finished statement reads like the task above it.
  const statements = page.getByRole('table', { name: "The run's queries, slowest first." })
  const statementRow = statements.locator('tbody tr', { hasText: 'insert into gold select 1' })
  await expect(statementRow.getByText('Succeeded')).toBeVisible()

  await expect(page.getByText('PythonOperator')).toHaveCount(0)
  await expect(page.getByText('rest_api')).toHaveCount(0)
  await expect(page.getByText('FINISHED')).toHaveCount(0)

  await page.screenshot({ path: '../docs/screenshots/959/run-panel-1440-dark.png' })
})
