import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import App from './App'

const now = new Date().toISOString()
const run = {
  id: 'run-12345678',
  workflow_definition_id: 'workflow-1',
  workflow_version: 3,
  status: 'FAILED',
  idempotency_key: 'tenant-order-42',
  created_at: now,
}

function response(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    statusText: status === 401 ? 'Unauthorized' : 'OK',
    headers: { 'Content-Type': 'application/json' },
  })
}

describe('operations dashboard', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const path = String(input)
      if (path.endsWith('/v1/runs')) return response({ items: [run] })
      return response({ items: [] })
    }))
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('moves from the loading state to the live operations overview', async () => {
    render(<App />)
    expect(screen.getByText('Loading dashboard')).toBeTruthy()
    expect(await screen.findByText('Your workflows, moving.')).toBeTruthy()
    expect(screen.getByText('run-1234')).toBeTruthy()
    expect(screen.getByText('Live updates reconnecting')).toBeTruthy()
  })

  it('filters executions by a tenant-scoped idempotency key', async () => {
    render(<App />)
    await screen.findByText('run-1234')
    fireEvent.change(screen.getByLabelText('Search runs'), { target: { value: 'missing' } })
    expect(screen.getByText('No matching executions')).toBeTruthy()
    fireEvent.change(screen.getByLabelText('Search runs'), { target: { value: 'order-42' } })
    expect(screen.getByText('run-1234')).toBeTruthy()
  })

  it('shows task dependencies, worker identity, and failed attempt history', async () => {
    vi.mocked(fetch).mockImplementation(async (input: string | URL | Request) => {
      const path = String(input)
      if (path.endsWith('/v1/runs/run-12345678')) return response({
        ...run,
        tasks: [{
          id: 'task-2',
          task_key: 'notify',
          handler: 'examples.notify',
          status: 'DEAD',
          attempt_count: 2,
          maximum_attempts: 2,
          available_at: now,
          depends_on: ['extract'],
          attempts: [{
            attempt_number: 2,
            worker_id: 'python-worker-2',
            scheduled_at: now,
            started_at: now,
            ended_at: now,
            exit_status: 'FAILED',
            error_type: 'TimeoutError',
            error_message: 'handler exceeded deadline',
            log_artifact_download_url: 'http://artifacts.example/log',
          }],
        }],
      })
      if (path.endsWith('/v1/runs')) return response({ items: [run] })
      return response({ items: [] })
    })

    render(<App />)
    fireEvent.click(await screen.findByText('run-1234'))
    expect(await screen.findByRole('dialog', { name: /run run-12345678/i })).toBeTruthy()
    expect(screen.getByText('depends on extract')).toBeTruthy()
    expect(screen.getByText('python-worker-2')).toBeTruthy()
    expect(screen.getByText('TimeoutError: handler exceeded deadline')).toBeTruthy()
    expect(screen.getByRole('link', { name: 'Log' }).getAttribute('href')).toBe('http://artifacts.example/log')
  })

  it('does not reopen a closed drawer when an older refresh finishes', async () => {
    let detailRequests = 0
    let resolveRefresh!: (value: Response) => void
    const pendingRefresh = new Promise<Response>((resolve) => { resolveRefresh = resolve })
    vi.mocked(fetch).mockImplementation(async (input: string | URL | Request) => {
      const path = String(input)
      if (path.endsWith('/v1/runs/run-12345678')) {
        detailRequests += 1
        if (detailRequests >= 2) return pendingRefresh
        return response({ ...run, tasks: [] })
      }
      if (path.endsWith('/v1/runs')) return response({ items: [run] })
      return response({ items: [] })
    })

    render(<App />)
    fireEvent.click(await screen.findByText('run-1234'))
    await screen.findByRole('dialog')
    fireEvent.click(screen.getByRole('button', { name: 'Refresh dashboard' }))
    await waitFor(() => expect(detailRequests).toBeGreaterThanOrEqual(2))
    fireEvent.click(screen.getByRole('button', { name: 'Close run detail' }))
    resolveRefresh(response({ ...run, tasks: [] }))
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
  })

  it('renders an actionable unauthorized state', async () => {
    vi.mocked(fetch).mockResolvedValue(response({ error: 'invalid token' }, 401))
    render(<App />)
    await waitFor(() => expect(screen.getByRole('alert').textContent).toContain('session is no longer authorized'))
    expect(screen.getByRole('button', { name: 'Sign in' })).toBeTruthy()
  })
})
