import { test, expect, type Page, type Route } from '@playwright/test'

// The run panel speaks Nova's words (stardelt-nova#959): the task row says the kind the
// pipeline declared, not the scheduler's operator class; the run says who started it in
// Nova's words, not the scheduler's trigger id. Every `/api/**` call is stubbed.
// A run is titled by when it started, not by the scheduler's run id (stardelt-nova#966).

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

// The scheduler's run id for a manual run; the page shows the start time instead and keeps
// this in the tooltip and the accessible names.
const RUN_ID = 'manual__2026-11-14T03:00:00.123456+00:00'

const ME = {
  user: 'alice', auth_mode: 'oidc', demo: false, warehouse: 'warehouse',
  is_admin: false, groups: ['analytics', 'finance', 'ops'],
}

const RUN = {
  runId: RUN_ID, state: 'succeeded', type: 'manual', logicalDate: null,
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
  const runPath = `/api/pipelines/stardelt/nightly_upload/runs/${RUN_ID}`
  await page.route('**/api/**', (route) => {
    const url = new URL(route.request().url())
    // The run id is a path segment, so the client percent-encodes it; compare decoded.
    const path = decodeURIComponent(url.pathname)
    if (url.pathname === '/api/me') return json(route, ME)
    if (url.pathname === '/api/pipelines/stardelt/nightly_upload') return json(route, PIPELINE)
    if (path === runPath) return json(route, RUN_DETAIL)
    if (path === `${runPath}/queries`) return json(route, QUERIES)
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
  await page.getByRole('button', { name: `Show tasks of run ${RUN_ID}` }).click()
  return page
}

test('the run panel speaks Nova’s words, not the scheduler’s', async ({ page }) => {
  await open(page)

  // Triggered by: the scheduler's `rest_api` is Nova's Run now.
  const runRow = page.locator('tr', { has: page.getByRole('button', { name: new RegExp('tasks of run manual__') }) })
  await expect(runRow.getByText('Nova', { exact: true })).toBeVisible()

  // Type: the scheduler's `manual` is Nova's Manual.
  await expect(runRow.getByText('Manual', { exact: true })).toBeVisible()

  // A run is titled by when it started, in the runs table and in the panel heading.
  await expect(runRow).toContainText('2026-11-14')
  const heading = page.getByRole('heading', { name: /Tasks of run/ })
  await expect(heading).toContainText('2026-11-14')

  // The task row names the kind the pipeline declared, not the operator class behind it.
  const tasks = page.getByRole('table', { name: 'Task instances of this run.' })
  const taskRow = tasks.locator('tbody tr', { hasText: 'load' })
  await expect(taskRow.getByText('sql', { exact: true })).toBeVisible()

  // A finished statement reads like the task above it.
  const statements = page.getByRole('table', { name: "The run's queries, slowest first." })
  const statementRow = statements.locator('tbody tr', { hasText: 'insert into gold select 1' })
  await expect(statementRow.getByText('Succeeded')).toBeVisible()

  // The scheduler's run id is in the tooltip and the accessible names only.
  await expect(page.getByText('manual__', { exact: false })).toHaveCount(0)

  await expect(page.getByText('PythonOperator')).toHaveCount(0)
  await expect(page.getByText('rest_api')).toHaveCount(0)
  await expect(page.getByText('FINISHED')).toHaveCount(0)

  await page.screenshot({ path: '../docs/screenshots/966/run-title-1440-dark.png' })
})
