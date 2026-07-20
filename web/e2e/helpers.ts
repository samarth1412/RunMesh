import { expect, type APIRequestContext, type Page } from '@playwright/test'

export const apiURL = process.env.RUNMESH_API_URL ?? 'http://localhost:8080'
const identityURL = process.env.RUNMESH_IDENTITY_URL ?? 'http://localhost:8180/realms/runmesh/protocol/openid-connect/token'

export async function token(request: APIRequestContext, username = 'admin', clientID = 'runmesh-web'): Promise<string> {
  const response = await request.post(identityURL, { form: { grant_type: 'password', client_id: clientID, username, password: 'runmesh' } })
  expect(response.ok(), await response.text()).toBeTruthy()
  return (await response.json()).access_token as string
}

export function authorization(value: string): Record<string, string> {
  return { Authorization: `Bearer ${value}`, 'Content-Type': 'application/json' }
}

export async function login(page: Page, username = 'admin'): Promise<void> {
  await page.goto('/')
  await page.locator('#username').fill(username)
  await page.locator('#password').fill('runmesh')
  await page.locator('#kc-login').click()
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible()
}

export async function createWorkflow(request: APIRequestContext, bearer: string, handler = 'examples.greet', maximumAttempts = 3): Promise<string> {
  const response = await request.post(`${apiURL}/v1/workflows`, {
    headers: authorization(bearer),
    data: { name: `e2e-${Date.now()}-${Math.random().toString(36).slice(2)}`, tasks: { task: { handler, maximum_attempts: maximumAttempts } } },
  })
  expect(response.status(), await response.text()).toBe(201)
  return (await response.json()).id as string
}

export async function createRun(request: APIRequestContext, bearer: string, workflowID: string, input: Record<string, unknown> = { name: 'E2E' }, inputArtifactID?: string): Promise<string> {
  const response = await request.post(`${apiURL}/v1/workflows/${workflowID}/runs`, {
    headers: { ...authorization(bearer), 'Idempotency-Key': crypto.randomUUID() },
    data: { input, ...(inputArtifactID ? { input_artifact_id: inputArtifactID } : {}) },
  })
  expect(response.status(), await response.text()).toBe(201)
  return (await response.json()).id as string
}

export async function waitForRun(request: APIRequestContext, bearer: string, runID: string, expected: string, timeout = 30_000): Promise<Record<string, unknown>> {
  const deadline = Date.now() + timeout
  let value: Record<string, unknown> = {}
  while (Date.now() < deadline) {
    const response = await request.get(`${apiURL}/v1/runs/${runID}`, { headers: authorization(bearer) })
    expect(response.ok(), await response.text()).toBeTruthy()
    value = await response.json() as Record<string, unknown>
    if (value.status === expected) return value
    await new Promise(resolve => setTimeout(resolve, 250))
  }
  throw new Error(`run ${runID} did not reach ${expected}; last status=${String(value.status)}`)
}
