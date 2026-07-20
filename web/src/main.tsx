import { StrictMode, useEffect, useState } from 'react'
import { createRoot } from 'react-dom/client'
import App from './App'
import './styles.css'
import './dashboard.css'

import { authenticate } from './auth'

export function AuthenticatedApp() {
  const [ready, setReady] = useState(false)
  const [error, setError] = useState('')
  useEffect(() => { void authenticate().then(() => setReady(true)).catch(value => setError(String(value))) }, [])
  if (error) return <main className="auth-state" role="alert"><h1>Authentication failed</h1><p>{error}</p><button onClick={() => window.location.reload()}>Try again</button></main>
  if (!ready) return <main className="auth-state" aria-live="polite"><p>Signing in…</p></main>
  return <App />
}

createRoot(document.getElementById('root')!).render(<StrictMode><AuthenticatedApp /></StrictMode>)
