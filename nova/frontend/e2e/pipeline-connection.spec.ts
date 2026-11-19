import { expect, test, type Route } from '@playwright/test'

// The Schedule dialog of a stored notebook pipeline (#382): a save must not drop the task's
// Connection. Every `/api/**` call is stubbed, as in `published-notebooks.spec.ts`.

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const ME = {
  user: 'alice', auth_mode: 'dev', demo: false, warehouse: 'warehouse',
  is_admin: false, groups: ['analytics'],
}
const PIPELINE = {
  name: 'p-notebook', namespace: 'stardelt', ownerGroup: 'analytics',
  description: null, canRun: true, canEdit: true,
  schedule: '0 3 * * *', paused: false, loaded: true, nextRun: null,
  state: 'idle', lastRun: null,
  tasks: [{
    name: 'run', kind: 'notebook', notebook: 'smoke.ipynb', objectVersion: 'v3',
    notebookParams: { days: '1' }, connection: 'crm-db',
  }],
  runAs: 'pipeline-analytics', concurrency: 'Forbid', createdAt: null,
  runs: [], params: {}, retries: null,
  options: { retries: 0, backoffMinutes: 5, timeoutMinutes: 60, concurrency: 'Forbid' },
  notify: null,
}
const NOTEBOOKS = [{
  group: 'analytics', name: 'smoke.ipynb', objectVersion: 'v3', size: 2048,
  uploadedAt: '2026-11-19T08:00:00Z', uploadedBy: 'alice',
}]
const VERSIONS = [{
  objectVersion: 'v3', size: 2048, uploadedAt: '2026-11-19T08:00:00Z',
  uploadedBy: 'alice', sha256: 'aaaaaaaaaaaabbbb',
}]
const PARAMS = {
  parameters: [{ name: 'days', default: '1' }],
  objectVersion: 'v3',
}

test('saving the schedule keeps the notebook task’s Connection', async ({ page }) => {
  let put: { notebook?: { name?: string; connection?: string | null } } | null = null
  await page.route('**/api/**', (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/me') return json(route, ME)
    if (path === '/api/pipelines/stardelt/p-notebook') {
      if (route.request().method() === 'PUT') {
        put = JSON.parse(route.request().postData() ?? '{}')
        return json(route, PIPELINE)
      }
      return json(route, PIPELINE)
    }
    if (path === '/api/notebooks') return json(route, NOTEBOOKS)
    if (path === '/api/notebooks/analytics/smoke.ipynb/versions') return json(route, VERSIONS)
    if (path === '/api/notebooks/analytics/smoke.ipynb/parameters') return json(route, PARAMS)
    return json(route, [])
  })
  await page.goto('/pipelines/stardelt/p-notebook')
  await page.getByRole('button', { name: 'Edit schedule' }).click()
  await page.getByRole('button', { name: 'Save schedule' }).click()
  await expect.poll(() => put?.notebook?.connection ?? null).toBe('crm-db')
  expect(put?.notebook?.name).toBe('smoke.ipynb')
})
