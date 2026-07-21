import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: './demo',
  timeout: 90_000,
  workers: 1,
  reporter: 'line',
  outputDir: '../docs/demo/test-output',
  use: {
    baseURL: 'http://localhost:3000',
    video: { mode: 'on', size: { width: 1280, height: 720 } },
    viewport: { width: 1280, height: 720 },
    ...devices['Desktop Chrome'],
  },
})
