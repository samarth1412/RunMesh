import type { Run, TaskRun, Worker, Workflow } from './types'
import { bearerToken } from './auth'

const base = import.meta.env.VITE_API_URL ?? ''
async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const token = await bearerToken()
  const response = await fetch(base + path, { ...init, headers: { 'Content-Type': 'application/json', ...(token ? { Authorization: `Bearer ${token}` } : {}), ...init?.headers } })
  if (!response.ok) throw new Error((await response.json().catch(() => null))?.error ?? `${response.status} ${response.statusText}`)
  return response.status === 204 ? (undefined as T) : response.json()
}
export const api = {
  workflows: () => request<{ items: Workflow[] }>('/v1/workflows'),
  runs: () => request<{ items: Run[] }>('/v1/runs'),
  run: (id: string) => request<Run>(`/v1/runs/${id}`),
  workers: () => request<{ items: Worker[] }>('/v1/workers'),
  dead: () => request<{ items: TaskRun[] }>('/v1/dead-letter'),
  createSample: () => request<Workflow>('/v1/workflows', { method: 'POST', body: JSON.stringify({ name: `document-processing-${Date.now().toString(36)}`, tasks: { download: { handler: 'examples.greet' }, extract: { handler: 'examples.upper', depends_on: ['download'] }, notify: { handler: 'examples.notify', depends_on: ['extract'] } } }) }),
  launch: (workflowId: string) => request<Run>(`/v1/workflows/${workflowId}/runs`, { method: 'POST', headers: { 'Idempotency-Key': crypto.randomUUID() }, body: JSON.stringify({ input: { name: 'Ada', text: 'reliable systems' } }) }),
  cancel: (runId: string) => request<void>(`/v1/runs/${runId}/cancel`, { method: 'POST', body: '{}' }),
  replay: (taskId: string) => request<void>(`/v1/dead-letter/${taskId}/replay`, { method: 'POST', body: '{}' }),
}

export function subscribeToEvents(onEvent: () => void, onConnection: (connected: boolean) => void): () => void {
  if (typeof WebSocket === 'undefined') return () => undefined
  let cancelled = false
  let socket: WebSocket | undefined
  let retry: number | undefined
  let attempts = 0
  const connect = async () => {
    const token = await bearerToken()
    if (cancelled) return
    const endpoint = new URL(base + '/v1/stream', window.location.href)
    endpoint.protocol = endpoint.protocol === 'https:' ? 'wss:' : 'ws:'
    socket = new WebSocket(endpoint, token ? ['runmesh', `bearer.${token}`] : ['runmesh'])
    socket.onopen = () => { attempts = 0; onConnection(true) }
    socket.onmessage = () => onEvent()
    socket.onerror = () => socket?.close()
    socket.onclose = () => {
      onConnection(false)
      if (!cancelled) {
        const delay = Math.min(30_000, 1_000 * 2 ** attempts++)
        retry = window.setTimeout(() => void connect(), delay)
      }
    }
  }
  void connect()
  return () => {
    cancelled = true
    if (retry !== undefined) window.clearTimeout(retry)
    socket?.close()
  }
}
