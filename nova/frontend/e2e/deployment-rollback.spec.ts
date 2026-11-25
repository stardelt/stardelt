import { expect, test, type Route } from '@playwright/test'

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

const ME = {
  user: 'bob', auth_mode: 'static-dev', demo: false, warehouse: 'warehouse',
  is_admin: true, groups: ['admins'],
}

const APPLIED = {
  id: 7, bundle: 'shop', ownerGroup: 'analytics', target: 'prod', revision: 'abc1234',
  repository: null, status: 'applied',
  pipelines: [{ namespace: 'stardelt', name: 'shop-3-gold', action: 'updated' }],
  resources: [{ kind: 'Pipeline', metadata: { namespace: 'stardelt', name: 'shop-3-gold' } }],
  files: [{ path: 'pipeline.yaml', sha256: 'a'.repeat(64) }],
  previousDeploymentId: null, actor: 'sub', actorName: 'Bob',
  createdAt: '2026-11-20T10:00:00Z',
}

test('an applied deployment offers Roll back and the replay opens as a new deployment', async ({ page }) => {
  let posted = 0
  await page.route('**/api/**', (route) => {
    const url = new URL(route.request().url())
    if (url.pathname === '/api/me') return json(route, ME)
    if (url.pathname === '/api/deployments') return json(route, [APPLIED])
    if (url.pathname === '/api/deployments/7') return json(route, APPLIED)
    if (url.pathname === '/api/deployments/8') return json(route, { ...APPLIED, id: 8, previousDeploymentId: 7 })
    if (url.pathname === '/api/deployments/7/rollback' && route.request().method() === 'POST') {
      posted += 1
      return json(route, { ...APPLIED, id: 8, previousDeploymentId: 7 }, 201)
    }
    return json(route, {})
  })

  await page.goto('/deployments/7')
  await expect(page.getByRole('heading', { name: 'shop · prod' })).toBeVisible()
  await page.getByRole('button', { name: 'Roll back', exact: true }).click()
  const dialog = page.getByRole('dialog', { name: 'Confirm rollback' })
  await expect(dialog).toBeVisible()
  await dialog.getByRole('button', { name: 'Confirm roll back' }).click()
  await expect(page).toHaveURL(/\/deployments\/8$/)
  expect(posted).toBe(1)
})

test('a failed deployment has nothing to replay', async ({ page }) => {
  await page.route('**/api/**', (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/me') return json(route, ME)
    if (path === '/api/deployments/9') return json(route, { ...APPLIED, id: 9, status: 'failed', resources: [] })
    return json(route, {})
  })
  await page.goto('/deployments/9')
  await expect(page.getByText('A failed deploy has nothing to replay.')).toBeVisible()
  await expect(page.getByRole('button', { name: 'Roll back', exact: true })).toHaveCount(0)
})

test('a caller outside the owner group is told why', async ({ page }) => {
  await page.route('**/api/**', (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/me') return json(route, { ...ME, is_admin: false, groups: ['other'] })
    if (path === '/api/deployments/7') return json(route, APPLIED)
    return json(route, {})
  })
  await page.goto('/deployments/7')
  await expect(
    page.getByText("Only the owner group's members (analytics) and administrators may roll back."),
  ).toBeVisible()
  await expect(page.getByRole('button', { name: 'Roll back', exact: true })).toHaveCount(0)
})

test('a stage link narrows the Deployments page to one bundle and stage', async ({ page }) => {
  const stage = (name: string, bundle: string, target: string) => ({
    name, namespace: 'stardelt', ownerGroup: 'analytics', canRun: true, canEdit: true,
    state: 'idle', loaded: true, lastRun: null, tasks: [],
    deployment: { bundle, target, revision: 'abc1234' },
  })
  await page.route('**/api/**', (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/me') return json(route, ME)
    if (path === '/api/pipelines') {
      return json(route, { pipelines: [stage('gold', 'shop', 'prod'), stage('dev-load', 'other', 'dev')] })
    }
    if (path === '/api/deployments') return json(route, [])
    return json(route, {})
  })

  await page.goto('/deployments?bundle=shop&target=prod')
  await expect(page.getByRole('heading', { name: 'shop · prod' })).toBeVisible()
  await expect(page.getByRole('heading', { name: 'other · dev' })).toHaveCount(0)
  await expect(page.getByText('Showing shop · prod.')).toBeVisible()
  const every = page.getByRole('link', { name: 'Show every stage' })
  await expect(every).toHaveAttribute('href', '/deployments')
  await every.click()
  await expect(page).toHaveURL(/\/deployments$/)
  await expect(page.getByRole('heading', { name: 'other · dev' })).toBeVisible()
})
