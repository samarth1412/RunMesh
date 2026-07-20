import { createHash } from 'node:crypto'
import { expect, test } from '@playwright/test'
import { apiURL, authorization, createRun, createWorkflow, login, token, waitForRun } from './helpers'

test('Keycloak login, workflow execution, live updates, cancellation, and dead-letter replay', async ({ page, request }) => {
  await login(page)
  const createdWorkflow = page.waitForResponse(response => response.url().endsWith('/v1/workflows') && response.request().method() === 'POST')
  await page.getByRole('button', { name: 'New workflow' }).click()
  const workflowName = ((await createdWorkflow).json() as Promise<{ name: string }>).then(workflow => workflow.name)
  await expect(page.getByRole('heading', { name: 'Workflows' })).toBeVisible()
  await page.locator('.workflow-card').filter({ hasText: await workflowName }).getByRole('button', { name: 'Launch' }).click()
  await expect(page.getByText('RUN DETAIL')).toBeVisible()
  await expect(page.locator('.drawer .run-meta .status')).toHaveText('SUCCEEDED', { timeout: 15_000 })
  await page.locator('.drawer .icon-button').click()

  const bearer = await token(request)
  const liveWorkflow = await createWorkflow(request, bearer)
  const liveRun = await createRun(request, bearer, liveWorkflow)
  await page.getByRole('button', { name: 'Overview' }).click()
  // The polling fallback is 15 seconds, so this five-second assertion proves
  // that the tenant WebSocket caused an immediate durable-state refresh.
  await expect(page.getByText(liveRun.slice(0, 8), { exact: true })).toBeVisible({ timeout: 5_000 })

  const cancellableWorkflow = await createWorkflow(request, bearer, 'validation.never', 100)
  const cancellableRun = await createRun(request, bearer, cancellableWorkflow)
  await expect(page.getByText(cancellableRun.slice(0, 8), { exact: true })).toBeVisible({ timeout: 5_000 })
  await page.getByText(cancellableRun.slice(0, 8), { exact: true }).click()
  await page.getByRole('button', { name: 'Cancel execution' }).click()
  await expect(page.locator('.drawer').getByText('CANCELLED').first()).toBeVisible()
  await page.locator('.drawer .icon-button').click()

  const failingWorkflow = await createWorkflow(request, bearer, 'validation.dead', 1)
  const failingRun = await createRun(request, bearer, failingWorkflow)
  await waitForRun(request, bearer, failingRun, 'FAILED')
  await page.getByRole('button', { name: /Dead letter/ }).click()
  await expect(page.getByText('validation.dead').first()).toBeVisible()
  const replay = page.waitForResponse(response => response.url().includes('/replay') && response.request().method() === 'POST')
  await page.getByRole('button', { name: 'Replay' }).first().click()
  expect((await replay).status()).toBe(202)
})

test('artifact upload, signed download, artifact-backed run, and tenant isolation', async ({ request }) => {
  const admin = await token(request)
  const otherTenant = await token(request, 'tenant-admin')
  const bytes = Buffer.from(JSON.stringify({ name: 'Artifact E2E' }))
  const checksum = createHash('sha256').update(bytes).digest('hex')
  const created = await request.post(`${apiURL}/v1/artifacts/uploads`, {
    headers: authorization(admin),
    data: { kind: 'input', content_type: 'application/json', size_bytes: bytes.length, checksum_sha256: checksum },
  })
  expect(created.status(), await created.text()).toBe(201)
  const upload = await created.json()
  const signedHeaders = Object.fromEntries(Object.entries(upload.headers as Record<string, string>).filter(([key]) => key.toLowerCase() !== 'host'))
  const put = await request.put(upload.upload_url as string, { headers: signedHeaders, data: bytes })
  expect(put.ok(), await put.text()).toBeTruthy()
  const completed = await request.post(`${apiURL}/v1/artifacts/${upload.artifact.id as string}/complete`, { headers: authorization(admin), data: {} })
  expect(completed.status(), await completed.text()).toBe(200)
  const signed = await request.get(`${apiURL}/v1/artifacts/${upload.artifact.id as string}/download`, { headers: authorization(admin) })
  expect(signed.status(), await signed.text()).toBe(200)
  const download = await request.get((await signed.json()).download_url as string)
  expect(await download.body()).toEqual(bytes)

  const workflow = await createWorkflow(request, admin)
  const runID = await createRun(request, admin, workflow, {}, upload.artifact.id as string)
  const run = await waitForRun(request, admin, runID, 'SUCCEEDED')
  expect(run.input_artifact_download_url).toContain('X-Amz-Signature')

  const isolatedArtifact = await request.get(`${apiURL}/v1/artifacts/${upload.artifact.id as string}/download`, { headers: authorization(otherTenant) })
  expect(isolatedArtifact.status()).toBe(404)
  const isolatedRun = await request.get(`${apiURL}/v1/runs/${runID}`, { headers: authorization(otherTenant) })
  expect(isolatedRun.status()).toBe(404)
})
