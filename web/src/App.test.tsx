import { act } from 'react'
import { createRoot } from 'react-dom/client'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import App from './App'

describe('operations dashboard', () => {
  let container: HTMLDivElement
  beforeEach(() => {
    container = document.createElement('div')
    document.body.appendChild(container)
    vi.stubGlobal('fetch', vi.fn(async () => ({ ok: true, status: 200, json: async () => ({ items: [] }) })))
  })
  afterEach(() => { container.remove(); vi.unstubAllGlobals() })
  it('renders the live operations overview', async () => {
    await act(async () => { createRoot(container).render(<App />) })
    expect(container.textContent).toContain('Your workflows, moving.')
    expect(container.textContent).toContain('No executions yet')
  })
})
