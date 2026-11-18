import { test, expect, type Page, type Route } from '@playwright/test'

// Run now with values (stardelt-nova#817 slice 1c): the pipeline page's Run now opens a
// form for a run date and the declared parameter values, and posts them as the body of
// `POST /api/pipelines/{ns}/{name}/runs`; the list's row button stays the fast path and
// posts `{}`. Every `/api/**` call is stubbed.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const NS = 'stardelt'
const NAME = 'shop'

const ME = {
  user: 'alice', auth_mode: 'oidc', demo: false, warehouse: 'warehouse',
  is_admin: true, groups: ['analytics', 'finance', 'ops'],
}

const RUN = {
  runId: 'manual__2026-11-14T10:00:00+00:00', state: 'queued', type: 'manual',
  logicalDate: null, startedAt: null, endedAt: null, durationSeconds: null,
  triggeredBy: 'alice', note: null,
}

const PIPELINE = {
  name: NAME, namespace: NS, ownerGroup: 'finance',
  canRun: true, canEdit: true,
  schedule: '0 2 * * *', paused: false, loaded: true, nextRun: null,
  state: 'success',
  lastRun: null, runAs: 'svc-pipeline-finance', concurrency: 'Allow',
  createdAt: '2026-11-01T09:00:00Z',
  tasks: [{ name: 'load', kind: 'sql' }],
  runs: [RUN],
  params: { days: '7' },
  retries: null,
}

/** The person's own today, built the way the form builds it: no `toISOString()`. */
function localToday(): string {
  const now = new Date()
  const pad = (value: number) => String(value).padStart(2, '0')
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`
}

async function open(page: Page) {
  await page.route('**/api/**', (route) => {
    const url = new URL(route.request().url())
    const runsPath = `/api/pipelines/${NS}/${NAME}/runs`
    if (url.pathname === '/api/me') return json(route, ME)
    if (route.request().method() === 'POST' && url.pathname === runsPath) return json(route, RUN)
    if (url.pathname === `/api/pipelines/${NS}/${NAME}`) return json(route, PIPELINE)
    if (url.pathname === '/api/pipelines') return json(route, { pipelines: [PIPELINE] })
    return json(route, [])
  })
  return page
}

test.describe('run now with values', () => {
  test('the pipeline page asks for a run date and the declared parameters', async ({ page }) => {
    await open(page)
    await page.goto(`/pipelines/${NS}/${NAME}`)

    await page.getByRole('button', { name: `Run now ${NAME}` }).click()
    const dialog = page.getByRole('dialog')
    await expect(dialog).toBeVisible()

    // The declared value is prefilled, and the date defaults to the person's today.
    const days = dialog.getByLabel('days', { exact: true })
    await expect(days).toHaveValue('7')
    await expect(dialog.locator('input[type="date"]')).toHaveValue(localToday())
    await expect(dialog).toContainText('Values apply to this run only. The schedule does not change.')

    // A changed value travels as the body, with the date; the page then says which date ran.
    const posted = page.waitForRequest(
      (request) =>
        request.method() === 'POST' &&
        new URL(request.url()).pathname === `/api/pipelines/${NS}/${NAME}/runs`,
    )
    await days.fill('30')
    await dialog.getByRole('button', { name: 'Run now', exact: true }).click()
    const body = JSON.parse((await posted).postData() ?? '{}')
    expect(body).toEqual({ runDate: localToday(), params: { days: '30' } })
    await expect(dialog).toHaveCount(0)
    await expect(page.getByRole('status')).toContainText(`Started a run of ${NAME} for ${localToday()}.`)
  })

  test('the list keeps the one-press Run now: no dialog, an empty body', async ({ page }) => {
    await open(page)
    await page.goto('/pipelines')

    const posted = page.waitForRequest(
      (request) =>
        request.method() === 'POST' &&
        new URL(request.url()).pathname === `/api/pipelines/${NS}/${NAME}/runs`,
    )
    await page.getByRole('button', { name: `Run now ${NAME}` }).click()
    expect(JSON.parse((await posted).postData() ?? '{}')).toEqual({})
    await expect(page.getByRole('dialog')).toHaveCount(0)
  })
})
