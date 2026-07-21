import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  Activity,
  AlertTriangle,
  Boxes,
  Braces,
  ChevronRight,
  CirclePlay,
  Clock3,
  GitBranch,
  LayoutDashboard,
  Plus,
  Radio,
  RefreshCw,
  Search,
  Server,
  Workflow as WorkflowIcon,
  X,
} from 'lucide-react'
import { APIError, api, subscribeToEvents } from './api'
import type { Run, TaskAttempt, TaskRun, Worker, Workflow } from './types'

type Page = 'Overview' | 'Workflows' | 'Workers' | 'Dead letter'
type DashboardError = { message: string; status?: number }

const nav: [Page, typeof Activity][] = [
  ['Overview', LayoutDashboard],
  ['Workflows', WorkflowIcon],
  ['Workers', Server],
  ['Dead letter', AlertTriangle],
]
const terminal = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED'])

function dashboardError(value: unknown): DashboardError {
  if (value instanceof APIError) return { message: value.message, status: value.status }
  return { message: value instanceof Error ? value.message : 'Unable to reach the control plane' }
}

export default function App() {
  const [page, setPage] = useState<Page>('Overview')
  const [workflows, setWorkflows] = useState<Workflow[]>([])
  const [runs, setRuns] = useState<Run[]>([])
  const [workers, setWorkers] = useState<Worker[]>([])
  const [dead, setDead] = useState<TaskRun[]>([])
  const [selected, setSelected] = useState<Run | null>(null)
  const [error, setError] = useState<DashboardError | null>(null)
  const [busy, setBusy] = useState(false)
  const [loading, setLoading] = useState(true)
  const [query, setQuery] = useState('')
  const [streamConnected, setStreamConnected] = useState(false)

  const selectedId = selected?.id
  const refresh = useCallback(async () => {
    try {
      const [workflowResult, runResult, workerResult, deadResult] = await Promise.all([
        api.workflows(),
        api.runs(),
        api.workers(),
        api.dead(),
      ])
      setWorkflows(workflowResult.items)
      setRuns(runResult.items)
      setWorkers(workerResult.items.filter((worker) => Date.now() - Date.parse(worker.last_seen_at) < 30_000))
      setDead(deadResult.items)
      if (selectedId) {
        const detail = await api.run(selectedId)
        // A refresh that started before the drawer closed must not reopen it,
        // and an older response must not replace a newly selected run.
        setSelected((current) => current?.id === selectedId ? detail : current)
      }
      setError(null)
    } catch (value) {
      setError(dashboardError(value))
    } finally {
      setLoading(false)
    }
  }, [selectedId])

  useEffect(() => {
    const initial = window.setTimeout(() => void refresh(), 0)
    const fallback = window.setInterval(() => void refresh(), 15_000)
    let refreshTimer: number | undefined
    const unsubscribe = subscribeToEvents(() => {
      if (refreshTimer !== undefined) window.clearTimeout(refreshTimer)
      refreshTimer = window.setTimeout(() => void refresh(), 150)
    }, setStreamConnected)
    return () => {
      window.clearTimeout(initial)
      window.clearInterval(fallback)
      unsubscribe()
      if (refreshTimer !== undefined) window.clearTimeout(refreshTimer)
    }
  }, [refresh])

  const perform = async (operation: () => Promise<void>) => {
    setBusy(true)
    try {
      await operation()
      setError(null)
    } catch (value) {
      setError(dashboardError(value))
    } finally {
      setBusy(false)
    }
  }
  const create = () => perform(async () => {
    await api.createSample()
    await refresh()
    setPage('Workflows')
  })
  const launch = (id: string) => perform(async () => {
    const run = await api.launch(id)
    setSelected(await api.run(run.id))
    await refresh()
  })
  const openRun = (run: Run) => perform(async () => setSelected(await api.run(run.id)))
  const replay = (id: string) => perform(async () => {
    await api.replay(id)
    await refresh()
  })

  const stats = useMemo(() => ({
    active: runs.filter((run) => !terminal.has(run.status)).length,
    succeeded: runs.filter((run) => run.status === 'SUCCEEDED').length,
    failed: runs.filter((run) => run.status === 'FAILED').length,
  }), [runs])
  const visibleRuns = useMemo(() => {
    const needle = query.trim().toLowerCase()
    if (!needle) return runs
    return runs.filter((run) => `${run.id} ${run.idempotency_key} ${run.status}`.toLowerCase().includes(needle))
  }, [query, runs])

  return <div className="shell">
    <aside>
      <div className="brand"><div className="mark"><GitBranch size={19}/></div><span>RunMesh</span></div>
      <div className="workspace"><div className="workspace-avatar">LD</div><div><small>WORKSPACE</small><strong>Local Development</strong></div><ChevronRight size={15}/></div>
      <nav aria-label="Primary navigation">{nav.map(([label, Icon]) => <button type="button" key={label} className={page === label ? 'active' : ''} onClick={() => setPage(label)}><Icon size={17}/><span>{label}</span>{label === 'Dead letter' && dead.length > 0 && <em>{dead.length}</em>}</button>)}</nav>
      <div className="aside-bottom" aria-live="polite"><span className={`system-dot ${streamConnected ? '' : 'reconnecting'}`}/> {streamConnected ? 'Live updates connected' : 'Live updates reconnecting'}<small>{streamConnected ? 'Tenant event stream active' : '15s polling fallback active'}</small></div>
    </aside>
    <main>
      <header><div><p>OPERATIONS / {page.toUpperCase()}</p><h1>{page}</h1></div><div className="header-actions">{page === 'Overview' && <label className="search"><Search size={16}/><span className="sr-only">Search runs</span><input aria-label="Search runs" placeholder="Search runs..." value={query} onChange={(event) => setQuery(event.target.value)}/></label>}<button type="button" className="icon-button" aria-label="Refresh dashboard" title="Refresh dashboard" onClick={() => void refresh()}><RefreshCw size={17}/></button><button type="button" className="primary" onClick={() => void create()} disabled={busy}><Plus size={17}/> New workflow</button></div></header>
      {error && <div className={`error ${error.status === 401 ? 'unauthorized' : ''}`} role="alert"><AlertTriangle size={17}/><span>{error.status === 401 ? 'Your session is no longer authorized. Sign in again to continue.' : error.message}</span>{error.status === 401 && <button type="button" className="quiet" onClick={() => window.location.reload()}>Sign in</button>}<button type="button" aria-label="Dismiss error" onClick={() => setError(null)}><X size={16}/></button></div>}
      <section className="content" aria-busy={loading}>
        {loading ? <LoadingState/> : <>
          {page === 'Overview' && <Overview stats={stats} runs={visibleRuns} workers={workers} workflows={workflows} onRun={(run) => void openRun(run)}/>}
          {page === 'Workflows' && <Workflows workflows={workflows} onLaunch={(id) => void launch(id)}/>}
          {page === 'Workers' && <Workers workers={workers}/>}
          {page === 'Dead letter' && <DeadLetter tasks={dead} replay={(id) => void replay(id)}/>}
        </>}
      </section>
    </main>
    {selected && <RunDrawer run={selected} onClose={() => setSelected(null)} onCancel={() => void perform(async () => { await api.cancel(selected.id); setSelected(await api.run(selected.id)); await refresh() })}/>}
  </div>
}

function LoadingState() {
  return <div className="loading-grid" aria-live="polite"><span className="sr-only">Loading dashboard</span>{Array.from({ length: 5 }, (_, index) => <i key={index}/>)}</div>
}

function Overview({ stats, runs, workers, workflows, onRun }: { stats: {active:number;succeeded:number;failed:number}; runs:Run[]; workers:Worker[]; workflows:Workflow[]; onRun:(run:Run)=>void }) {
  return <><div className="hero"><div><span className="eyebrow"><Radio size={13}/> LIVE CONTROL PLANE</span><h2>Your workflows, moving.</h2><p>At-least-once delivery with leases, retries, and complete attempt history.</p></div><div className="mesh" aria-hidden="true"><i/><i/><i/><i/><i/><i/></div></div>
    <div className="stats"><Stat icon={Activity} label="Active runs" value={stats.active} detail="right now"/><Stat icon={Boxes} label="Total runs" value={runs.length} detail={`${stats.succeeded} succeeded`}/><Stat icon={AlertTriangle} label="Failed runs" value={stats.failed} detail="needs attention" tone="warn"/><Stat icon={Server} label="Live workers" value={workers.length} detail={`${workflows.length} workflows`}/></div>
    <div className="panel"><PanelTitle title="Recent executions" subtitle="Latest workflow activity across this tenant"/><RunTable runs={runs} onRun={onRun}/></div></>
}
function Stat({icon:Icon,label,value,detail,tone}:{icon:typeof Activity;label:string;value:number;detail:string;tone?:string}) { return <div className={`stat ${tone ?? ''}`}><div className="stat-top"><span>{label}</span><Icon size={17}/></div><strong>{value.toLocaleString()}</strong><small>{detail}</small></div> }
function PanelTitle({title,subtitle}:{title:string;subtitle:string}) { return <div className="panel-title"><div><h3>{title}</h3><p>{subtitle}</p></div></div> }
function RunTable({runs,onRun}:{runs:Run[];onRun:(run:Run)=>void}) { if (!runs.length) return <Empty icon={CirclePlay} title="No matching executions" text="Create a workflow or adjust the run search."/>; return <div className="table"><div className="tr th"><span>RUN ID</span><span>STATUS</span><span>VERSION</span><span>STARTED</span><span/></div>{runs.slice(0, 8).map((run) => <button type="button" className="tr" key={run.id} onClick={() => onRun(run)}><span className="mono">{run.id.slice(0, 8)}</span><span><Status value={run.status}/></span><span>v{run.workflow_version}</span><span>{ago(run.created_at)}</span><ChevronRight size={16}/></button>)}</div> }
function Workflows({workflows,onLaunch}:{workflows:Workflow[];onLaunch:(id:string)=>void}) { return <div className="cards">{workflows.map((workflow) => <article className="workflow-card" key={workflow.id}><div className="card-head"><div className="workflow-icon"><Braces size={19}/></div><Status value="ACTIVE"/></div><h3>{workflow.name}</h3><p>Version {workflow.version} · {Object.keys(workflow.dag.tasks).length} tasks</p><div className="dag-mini" aria-label={`${workflow.name} task graph`}>{Object.entries(workflow.dag.tasks).slice(0, 4).map(([key, task]) => <span key={key} title={task.depends_on?.length ? `Depends on ${task.depends_on.join(', ')}` : 'Root task'}><b>{key.slice(0,1).toUpperCase()}</b>{task.depends_on?.length ? <small>{task.depends_on.length}</small> : null}</span>)}</div><div className="card-foot"><span>Updated {ago(workflow.created_at)}</span><button type="button" className="launch" onClick={() => onLaunch(workflow.id)}><CirclePlay size={15}/> Launch</button></div></article>)}{!workflows.length && <div className="panel wide"><Empty icon={WorkflowIcon} title="No workflows" text="Use “New workflow” to create a three-step example DAG."/></div>}</div> }
function Workers({workers}:{workers:Worker[]}) { return <div className="panel"><PanelTitle title="Worker fleet" subtitle="Heartbeats and registered capabilities"/><div className="worker-grid">{workers.map((worker) => <div className="worker" key={worker.worker_id}><div className="worker-state"><span/><Server size={20}/></div><div><h3>{worker.worker_id}</h3><p>{worker.handlers.join(' · ') || 'No handlers'}</p></div><strong>{worker.active_tasks}<small> active</small></strong><time>{ago(worker.last_seen_at)}</time></div>)}{!workers.length && <Empty icon={Server} title="No workers connected" text="Start the bundled Python or Go worker to register capabilities."/>}</div></div> }
function DeadLetter({tasks,replay}:{tasks:TaskRun[];replay:(id:string)=>void}) { return <div className="panel"><PanelTitle title="Dead-letter queue" subtitle="Tasks that exhausted retries or failed permanently"/>{tasks.length ? <div className="table"><div className="tr th"><span>TASK</span><span>HANDLER</span><span>ATTEMPTS</span><span>AVAILABLE</span><span/></div>{tasks.map((task) => <div className="tr" key={task.id}><span>{task.task_key}</span><span className="mono">{task.handler}</span><span>{task.attempt_count}/{task.maximum_attempts}</span><span>{ago(task.available_at)}</span><button type="button" className="quiet" onClick={() => replay(task.id)}>Replay</button></div>)}</div> : <Empty icon={AlertTriangle} title="Dead letter is empty" text="Permanent failures and exhausted tasks will appear here."/>}</div> }

function RunDrawer({run,onClose,onCancel}:{run:Run;onClose:()=>void;onCancel:()=>void}) {
  useEffect(() => {
    const closeOnEscape = (event: KeyboardEvent) => { if (event.key === 'Escape') onClose() }
    window.addEventListener('keydown', closeOnEscape)
    return () => window.removeEventListener('keydown', closeOnEscape)
  }, [onClose])
  return <div className="overlay" onMouseDown={onClose}><aside className="drawer" role="dialog" aria-modal="true" aria-label={`Run ${run.id}`} onMouseDown={(event) => event.stopPropagation()}><div className="drawer-head"><div><span className="eyebrow">RUN DETAIL</span><h2>{run.id.slice(0,8)}</h2></div><button type="button" className="icon-button" aria-label="Close run detail" onClick={onClose}><X size={18}/></button></div><div className="run-meta"><Status value={run.status}/><span><Clock3 size={14}/>{ago(run.created_at)}</span><span>version {run.workflow_version}</span></div><h3 className="section-label">TASK DEPENDENCY GRAPH</h3><div className="task-list">{run.tasks?.map((task, index) => <div className="task-block" key={task.id}><div className="task-row"><div className="timeline"><i className={task.status.toLowerCase()}/>{index < (run.tasks?.length ?? 0) - 1 && <b/>}</div><div><strong>{task.task_key}</strong><small>{task.handler}</small>{task.depends_on?.length ? <span className="dependencies">depends on {task.depends_on.join(', ')}</span> : <span className="dependencies root">root task</span>}</div><Status value={task.status}/><span className="attempt">{task.attempt_count}/{task.maximum_attempts}</span></div>{task.attempts?.length ? <div className="attempt-history">{task.attempts.map((attempt) => <AttemptRow key={attempt.attempt_number} attempt={attempt}/>)}</div> : null}</div>)}</div>{!terminal.has(run.status) && <button type="button" className="danger" onClick={onCancel}>Cancel execution</button>}</aside></div>
}

function AttemptRow({attempt}:{attempt:TaskAttempt}) {
  const failure = [attempt.error_type, attempt.error_message].filter(Boolean).join(': ')
  return <div className="attempt-row"><span>Attempt {attempt.attempt_number}</span><strong>{attempt.worker_id}</strong><Status value={attempt.exit_status || (attempt.executing_at ? 'RUNNING' : 'LEASED')}/><div className="attempt-detail">{failure && <p title={failure}>{failure}</p>}{attempt.artifact_download_url && <a href={attempt.artifact_download_url} target="_blank" rel="noreferrer">Output</a>}{attempt.log_artifact_download_url && <a href={attempt.log_artifact_download_url} target="_blank" rel="noreferrer">Log</a>}</div><time>{attempt.ended_at ? duration(attempt.started_at, attempt.ended_at) : ago(attempt.started_at)}</time></div>
}

function Status({value}:{value:string}) { return <span className={`status ${value.toLowerCase()}`}><i/>{value.replace('_',' ')}</span> }
function Empty({icon:Icon,title,text}:{icon:typeof Activity;title:string;text:string}) { return <div className="empty"><Icon size={25}/><h3>{title}</h3><p>{text}</p></div> }
function ago(date:string) { const seconds = Math.floor((Date.now() - Date.parse(date)) / 1000); if (seconds < 5) return 'just now'; if (seconds < 60) return `${seconds}s ago`; if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`; if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`; return `${Math.floor(seconds / 86400)}d ago` }
function duration(start:string,end:string) { const milliseconds = Math.max(0, Date.parse(end) - Date.parse(start)); return milliseconds < 1000 ? `${milliseconds}ms` : `${(milliseconds / 1000).toFixed(1)}s` }
