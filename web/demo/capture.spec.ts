import http from 'node:http'
import { expect, test } from '@playwright/test'
import { apiURL, authorization, createRun, createWorkflow, login, token, waitForRun } from '../e2e/helpers'

type Container = { Id: string; Names: string[] }

function dockerRequest(method: string, path: string): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const request = http.request({ socketPath: '/var/run/docker.sock', method, path }, response => {
      const chunks: Buffer[] = []
      response.on('data', chunk => chunks.push(Buffer.from(chunk)))
      response.on('end', () => response.statusCode && response.statusCode < 300
        ? resolve(Buffer.concat(chunks))
        : reject(new Error(`Docker API ${method} ${path} returned ${response.statusCode}: ${Buffer.concat(chunks)}`)))
    })
    request.on('error', reject)
    request.end()
  })
}

async function terminateWorkerProcesses(): Promise<Container[]> {
  const raw = await dockerRequest('GET', '/containers/json')
  const containers = (JSON.parse(raw.toString()) as Container[]).filter(container =>
    container.Names.some(name => name.includes('runmesh-worker-go') || name.includes('runmesh-worker-python')))
  await Promise.all(containers.map(container => dockerRequest('POST', `/containers/${container.Id}/kill`)))
  return containers
}

async function restartWorkerProcesses(containers: Container[]): Promise<void> {
  await Promise.all(containers.map(container => dockerRequest('POST', `/containers/${container.Id}/start`)))
}

test('capture the real dashboard and worker lease recovery', async ({ page, request }) => {
  await login(page)
  await page.waitForTimeout(4_000)
  await page.screenshot({ path: '../docs/demo/dashboard-overview.png', fullPage: true })

  const created = page.waitForResponse(response => response.url().endsWith('/v1/workflows') && response.request().method() === 'POST')
  await page.getByRole('button', { name: 'New workflow' }).click()
  const workflowName = ((await created).json() as Promise<{ name: string }>).then(workflow => workflow.name)
  await page.locator('.workflow-card').filter({ hasText: await workflowName }).getByRole('button', { name: 'Launch' }).click()
  await expect(page.locator('.drawer .run-meta .status')).toHaveText('SUCCEEDED', { timeout: 20_000 })
  await page.screenshot({ path: '../docs/demo/run-detail.png' })
  await page.getByRole('button', { name: 'Close run detail' }).click()
  await expect(page.getByRole('dialog')).toBeHidden()

  const bearer = await token(request)
  const workflow = await createWorkflow(request, bearer, 'examples.slow', 3)
  const runID = await createRun(request, bearer, workflow, { delay_ms: 5_000 })
  let running = false
  for (let attempt = 0; attempt < 40; attempt++) {
    const response = await request.get(`${apiURL}/v1/runs/${runID}`, { headers: authorization(bearer) })
    const run = await response.json() as { tasks?: { status: string }[] }
    if (run.tasks?.some(task => task.status === 'RUNNING')) { running = true; break }
    await page.waitForTimeout(250)
  }
  expect(running).toBeTruthy()

  await page.getByRole('button', { name: 'Overview' }).click()
  await page.getByLabel('Search runs').fill(runID)
  await page.getByText(runID.slice(0, 8), { exact: true }).click()
  await expect(page.locator('.drawer').getByText('RUNNING').first()).toBeVisible()
  const terminatedWorkers = await terminateWorkerProcesses()
  expect(terminatedWorkers.length).toBeGreaterThanOrEqual(2)
  await page.waitForTimeout(8_000)
  await restartWorkerProcesses(terminatedWorkers)

  await waitForRun(request, bearer, runID, 'SUCCEEDED', 60_000)
  await expect(page.locator('.drawer .run-meta .status')).toHaveText('SUCCEEDED', { timeout: 5_000 })
  await expect(page.getByText('Attempt 2')).toBeVisible()
  await page.screenshot({ path: '../docs/demo/lease-recovery.png', fullPage: true })
  await page.waitForTimeout(15_000)
})
