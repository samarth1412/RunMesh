import { expect, test } from '@playwright/test'
import { apiURL, authorization, createWorkflow, token } from './helpers'

test('rejects invalid, forged, expired, underprivileged, and revoked credentials', async ({ request }) => {
  const admin = await token(request)
  expect((await request.get(`${apiURL}/v1/workflows`, { headers: authorization('not-a-jwt') })).status()).toBe(401)

  const pieces = admin.split('.')
  const claims = JSON.parse(Buffer.from(pieces[1], 'base64url').toString()) as Record<string, unknown>
  claims.runmesh_tenant_id = '00000000-0000-0000-0000-000000000010'
  pieces[1] = Buffer.from(JSON.stringify(claims)).toString('base64url')
  expect((await request.get(`${apiURL}/v1/workflows`, { headers: authorization(pieces.join('.')) })).status()).toBe(401)

  const expiring = await token(request, 'admin', 'runmesh-expiring-test')
  await new Promise(resolve => setTimeout(resolve, 2_000))
  expect((await request.get(`${apiURL}/v1/workflows`, { headers: authorization(expiring) })).status()).toBe(401)

  const viewer = await token(request, 'viewer')
  const escalation = await request.post(`${apiURL}/v1/workflows`, { headers: authorization(viewer), data: { name: 'forbidden', tasks: { task: { handler: 'examples.greet' } } } })
  expect(escalation.status()).toBe(403)

  const keyResponse = await request.post(`${apiURL}/v1/api-keys`, { headers: authorization(admin), data: { name: `scope-test-${Date.now()}`, scopes: ['workflows:read'] } })
  expect(keyResponse.status(), await keyResponse.text()).toBe(201)
  const key = await keyResponse.json()
  const missingScope = await request.post(`${apiURL}/v1/workflows`, { headers: authorization(key.token as string), data: { name: 'forbidden-key', tasks: { task: { handler: 'examples.greet' } } } })
  expect(missingScope.status()).toBe(403)
  expect((await request.delete(`${apiURL}/v1/api-keys/${key.api_key.id as string}`, { headers: authorization(admin) })).status()).toBe(204)
  expect((await request.get(`${apiURL}/v1/workflows`, { headers: authorization(key.token as string) })).status()).toBe(401)

  const expiringKeyResponse = await request.post(`${apiURL}/v1/api-keys`, {
    headers: authorization(admin),
    data: { name: `expiry-test-${Date.now()}`, scopes: ['workflows:read'], expires_at: new Date(Date.now() + 1_000).toISOString() },
  })
  expect(expiringKeyResponse.status(), await expiringKeyResponse.text()).toBe(201)
  const expiringKey = (await expiringKeyResponse.json()).token as string
  await new Promise(resolve => setTimeout(resolve, 1_500))
  expect((await request.get(`${apiURL}/v1/workflows`, { headers: authorization(expiringKey) })).status()).toBe(401)
})

test('rejects oversized inputs and spoofed-header rate-limit bypass attempts', async ({ request }) => {
  const otherTenant = await token(request, 'tenant-admin')
  const otherWorkflow = await createWorkflow(request, otherTenant)
  const oversized = await request.post(`${apiURL}/v1/workflows`, {
    headers: authorization(otherTenant),
    data: { name: 'x'.repeat(1_100_000), tasks: { task: { handler: 'examples.greet' } } },
  })
  expect([400, 413, 422]).toContain(oversized.status())
  const oversizedArtifact = await request.post(`${apiURL}/v1/artifacts/uploads`, {
    headers: authorization(otherTenant),
    data: { kind: 'input', content_type: 'application/octet-stream', size_bytes: 100 * 1024 * 1024 + 1 },
  })
  expect(oversizedArtifact.status()).toBe(422)

  // Changing untrusted proxy-era identity headers must not create fresh buckets.
  const attempts = await Promise.all(Array.from({ length: 350 }, (_, index) =>
    request.get(`${apiURL}/v1/workflows`, {
      headers: { ...authorization(otherTenant), 'X-Runmesh-Tenant-ID': crypto.randomUUID(), 'X-Runmesh-Role': 'admin' },
    }).then(response => ({ index, status: response.status() }))))
  const limited = attempts.filter(result => result.status === 429).length
  expect(limited).toBeGreaterThan(0)

  // A real other-tenant resource remains invisible regardless of path ID.
  const admin = await token(request)
  expect((await request.get(`${apiURL}/v1/workflows/${otherWorkflow}`, { headers: authorization(admin) })).status()).toBe(404)
})
