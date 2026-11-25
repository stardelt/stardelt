import { test, expect, type Page, type Route } from '@playwright/test'

// One run of a pipeline is a page of its own (stardelt-nova#1111): when it started, the
// timeline on the run's own span, the tasks as a graph laid out from `dependsOn`, and the
// task table. Every `/api/**` call is stubbed.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const RUN_ID = 'scheduled__2026-11-14T10:00:00+00:00'
const NS = 'stardelt'
const NAME = 'shop'

const ME = {
  user: 'alice', auth_mode: 'oidc', demo: false, warehouse: 'warehouse',
  is_admin: false, groups: ['analytics', 'finance', 'ops'],
}

const PIPELINE = {
  name: NAME, namespace: NS, ownerGroup: 'finance',
  canRun: true, canEdit: true,
  schedule: '0 2 * * *', paused: false, loaded: true, nextRun: null,
  state: 'failed',
  lastRun: null, runAs: 'svc-pipeline-finance', concurrency: 'Allow',
  createdAt: '2026-11-01T09:00:00Z', runtime: 'airflow', env: [],
  sparkConf: {}, idleStopMinutes: 0,
  tasks: [
    { name: 'load', kind: 'sql' },
    { name: 'transform', kind: 'sql', dependsOn: ['load'] },
    { name: 'publish', kind: 'notebook', dependsOn: ['load'] },
  ],
  runs: [],
  params: {}, retries: null,
}

const RUN = {
  runId: RUN_ID, state: 'failed', type: 'scheduled', logicalDate: null,
  startedAt: '2026-11-14T10:00:00Z', endedAt: '2026-11-14T10:05:00Z',
  durationSeconds: 300, triggeredBy: 'rest_api', note: null,
}

const RUN_DETAIL = {
  run: RUN,
  tasks: [
    {
      taskId: 'load', state: 'success', tryNumber: 1,
      startedAt: '2026-11-14T10:00:10Z', endedAt: '2026-11-14T10:02:00Z',
      durationSeconds: 110, operator: 'SQLExecuteQueryOperator',
    },
    {
      taskId: 'transform', state: 'failed', tryNumber: 1,
      startedAt: '2026-11-14T10:02:05Z', endedAt: '2026-11-14T10:03:05Z',
      durationSeconds: 60, operator: 'SQLExecuteQueryOperator',
    },
    {
      taskId: 'publish', state: 'success', tryNumber: 2,
      startedAt: '2026-11-14T10:03:10Z', endedAt: '2026-11-14T10:04:50Z',
      durationSeconds: 100, operator: 'PythonOperator',
    },
  ],
}

async function open(page: Page, width = 1440, run: Record<string, unknown> = RUN) {
  const runPath = `/api/pipelines/${NS}/${NAME}/runs/${RUN_ID}`
  await page.route('**/api/**', (route) => {
    const url = new URL(route.request().url())
    // The run id is a path segment, so the client percent-encodes it; compare decoded.
    const path = decodeURIComponent(url.pathname)
    if (url.pathname === '/api/me') return json(route, ME)
    if (url.pathname === `/api/pipelines/${NS}/${NAME}`) return json(route, PIPELINE)
    if (path === runPath) return json(route, { ...RUN_DETAIL, run })
    if (path === `${runPath}/queries`) return json(route, { queries: [] })
    return json(route, [])
  })
  await page.setViewportSize({ width, height: 900 })
  await page.emulateMedia({ colorScheme: 'dark' })
  await page.addInitScript(() => {
    try {
      localStorage.setItem('stardelt-nova:theme', 'dark')
    } catch {
      /* ignore */
    }
  })
  await page.goto(`/pipelines/${NS}/${NAME}/runs/${encodeURIComponent(RUN_ID)}`)
  return page
}

test('the run names the deploy it came from', async ({ page }) => {
  await open(page, 1440, { ...RUN, bundle: 'shop-etl', target: 'dev', revision: '9f3c2a1' })

  const deploy = page.getByText('Deployed from')
  await expect(deploy).toBeVisible()
  await expect(page.getByText('shop-etl · dev @ 9f3c2a1')).toBeVisible()
  const link = page.getByRole('link', { name: /shop-etl · dev/ })
  await expect(link).toHaveAttribute('href', '/deployments?bundle=shop-etl&target=dev')
})

test('one run is a page: header, timeline, graph and the task table', async ({ page }) => {
  await open(page)

  // The window's own title bar names the page, not "Not found".
  await expect(page.locator('body')).toContainText('shop · Run')

  // The header: the start time, the state through the badge, the duration in Nova's words.
  const header = page.locator('header')
  await expect(header.getByRole('heading', { name: /Run 2026-11-14/ })).toBeVisible()
  await expect(header.getByText('Failed', { exact: true })).toBeVisible()
  await expect(header.getByText('5 m 00 s', { exact: true })).toBeVisible()
  // A hand-made run carries no deploy stamp, so the header says nothing about one.
  await expect(page.getByText('Deployed from')).toHaveCount(0)

  // The timeline: one bar per task, each at its own offset, the failed one with a width.
  const bars = page.getByTestId('run-timeline-bar')
  await expect(bars).toHaveCount(3)
  const boxes = await bars.evaluateAll((els) =>
    els.map((el) => {
      const rect = el.getBoundingClientRect()
      return { task: el.getAttribute('data-task'), x: Math.round(rect.x), width: Math.round(rect.width) }
    }),
  )
  const xs = boxes.map((box) => box.x)
  expect(new Set(xs).size, `distinct offsets: ${JSON.stringify(boxes)}`).toBe(3)
  const failed = boxes.find((box) => box.task === 'transform')
  expect(failed?.width ?? 0).toBeGreaterThan(0)

  // The graph: three boxes, two lines for the two `dependsOn` edges.
  await expect(page.getByTestId('run-graph-node')).toHaveCount(3)
  await expect(page.getByTestId('run-graph-edge')).toHaveCount(2)

  // The table: the retried task at try 2, the failed one's state.
  const tasks = page.getByRole('table', { name: 'Task instances of this run.' })
  const publish = tasks.locator('tbody tr', { hasText: 'publish' })
  await expect(publish.getByText('2', { exact: true })).toBeVisible()
  const transform = tasks.locator('tbody tr', { hasText: 'transform' })
  await expect(transform.getByText('Failed', { exact: true })).toBeVisible()

  await page.screenshot({ path: '../docs/screenshots/1111/run-detail-1440-dark.png' })

  // 390 px: the graph is dropped, the table answers.
  await page.setViewportSize({ width: 390, height: 844 })
  // The navigation rail slides away on a transform; the screenshot waits for it.
  await page.waitForTimeout(300)
  await expect(page.getByTestId('run-graph-node')).toHaveCount(0)
  await expect(page.getByRole('table', { name: 'Task instances of this run.' })).toBeVisible()
  await page.screenshot({ path: '../docs/screenshots/1111/run-detail-390-dark.png' })
})
