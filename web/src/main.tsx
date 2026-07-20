import { StrictMode, useEffect, useState } from 'react'
import { createRoot } from 'react-dom/client'
import App from './App'
import './styles.css'

import { authenticate } from './auth'

function AuthenticatedApp() {
  const [ready, setReady] = useState(false)
  const [error, setError] = useState('')
  useEffect(() => { void authenticate().then(() => setReady(true)).catch(value => setError(String(value))) }, [])
  if (error) return <main><h1>Authentication failed</h1><p>{error}</p></main>
  if (!ready) return <main><p>Signing in…</p></main>
  return <App />
}

createRoot(document.getElementById('root')!).render(<StrictMode><AuthenticatedApp /></StrictMode>)
