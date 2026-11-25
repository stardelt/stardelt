import { defineConfig } from '@playwright/test'

// UI conformance suite (`npm run e2e`). It starts the real backend in its no-login dev mode
// with no cluster behind it, so every page also shows how it behaves when its upstreams are
// unreachable. Needs `npm run build` (the backend serves `dist/`) and a built backend binary.
// `NOVA_E2E_PORT` lets parallel checkouts run at the same time; it defaults to the CI port.
// Specs that need data stub the API with `page.route`.
const port = Number(process.env.NOVA_E2E_PORT ?? 18080)

const server = (p: number) => ({
  command: process.env.NOVA_E2E_BIN ?? '../target/debug/stardelt-nova-backend',
  url: `http://127.0.0.1:${p}/api/me`,
  reuseExistingServer: !process.env.CI,
  env: {
    NOVA_BIND_ADDR: `127.0.0.1:${p}`,
    NOVA_STATIC_DIR: 'dist',
    NOVA_AUTH_MODE: 'dev',
    NOVA_DEV_USER: 'e2e',
    // The server has no cluster behind it, on any machine: without this,
    // `kube::Config::infer()` (`backend/src/main.rs:852`) picks up the
    // developer's ~/.kube/config and `GET /api/compute` lists real
    // SparkClusters, so e2e/compute.spec.ts fails on every cluster-connected
    // checkout but passes in CI (stardelt-nova#744).
    KUBECONFIG: '/dev/null',
  },
})

export default defineConfig({
  testDir: 'e2e',
  workers: 1,
  fullyParallel: false,
  timeout: 120_000,
  reporter: [['list']],
  use: {
    // Locally you may point at an installed browser: PW_CHROMIUM=/usr/bin/chromium
    launchOptions: process.env.PW_CHROMIUM
      ? { executablePath: process.env.PW_CHROMIUM, args: ['--no-sandbox'] }
      : {},
  },
  webServer: server(port),
  projects: [
    { name: 'main', use: { baseURL: `http://127.0.0.1:${port}` } },
  ],
})
