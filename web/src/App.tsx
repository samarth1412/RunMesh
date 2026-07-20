import { useCallback, useEffect, useMemo, useState } from 'react'
import { Activity, AlertTriangle, Boxes, Braces, ChevronRight, CirclePlay, Clock3, GitBranch, LayoutDashboard, Plus, Radio, RefreshCw, Search, Server, Workflow as WorkflowIcon, X } from 'lucide-react'
import { api, subscribeToEvents } from './api'
import type { Run, TaskRun, Worker, Workflow } from './types'

type Page = 'Overview' | 'Workflows' | 'Workers' | 'Dead letter'
const nav: [Page, typeof Activity][] = [['Overview', LayoutDashboard], ['Workflows', WorkflowIcon], ['Workers', Server], ['Dead letter', AlertTriangle]]
const terminal = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED'])

export default function App() {
  const [page, setPage] = useState<Page>('Overview')
  const [workflows, setWorkflows] = useState<Workflow[]>([])
  const [runs, setRuns] = useState<Run[]>([])
  const [workers, setWorkers] = useState<Worker[]>([])
  const [dead, setDead] = useState<TaskRun[]>([])
  const [selected, setSelected] = useState<Run | null>(null)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [streamConnected, setStreamConnected] = useState(false)

  const selectedId = selected?.id
  const refresh = useCallback(async () => {
    try {
      const [wf, rs, wk, dl] = await Promise.all([api.workflows(), api.runs(), api.workers(), api.dead()])
      setWorkflows(wf.items); setRuns(rs.items); setWorkers(wk.items.filter(worker => Date.now() - Date.parse(worker.last_seen_at) < 30_000)); setDead(dl.items); setError('')
      if (selectedId) setSelected(await api.run(selectedId))
    } catch (e) { setError(e instanceof Error ? e.message : 'Unable to reach control plane') }
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
      clearTimeout(initial); clearInterval(fallback); unsubscribe()
      if (refreshTimer !== undefined) clearTimeout(refreshTimer)
    }
  }, [refresh])
  const create = async () => { setBusy(true); try { await api.createSample(); await refresh(); setPage('Workflows') } catch (e) { setError(String(e)) } finally { setBusy(false) } }
  const launch = async (id: string) => { setBusy(true); try { const run = await api.launch(id); setSelected(await api.run(run.id)); await refresh() } catch (e) { setError(String(e)) } finally { setBusy(false) } }
  const stats = useMemo(() => ({ active: runs.filter(r => !terminal.has(r.status)).length, succeeded: runs.filter(r => r.status === 'SUCCEEDED').length, failed: runs.filter(r => r.status === 'FAILED').length }), [runs])

  return <div className="shell">
    <aside>
      <div className="brand"><div className="mark"><GitBranch size={19}/></div><span>RunMesh</span></div>
      <div className="workspace"><div className="workspace-avatar">LD</div><div><small>WORKSPACE</small><strong>Local Development</strong></div><ChevronRight size={15}/></div>
      <nav>{nav.map(([label, Icon]) => <button key={label} className={page === label ? 'active' : ''} onClick={() => setPage(label)}><Icon size={17}/><span>{label}</span>{label === 'Dead letter' && dead.length > 0 && <em>{dead.length}</em>}</button>)}</nav>
      <div className="aside-bottom"><span className="system-dot"/> {streamConnected ? 'Live updates connected' : 'Live updates reconnecting'}<small>{streamConnected ? 'Tenant event stream active' : '15s polling fallback active'}</small></div>
    </aside>
    <main>
      <header><div><p>OPERATIONS / {page.toUpperCase()}</p><h1>{page}</h1></div><div className="header-actions"><label className="search"><Search size={16}/><input placeholder="Search runs..."/></label><button className="icon-button" onClick={() => void refresh()}><RefreshCw size={17}/></button><button className="primary" onClick={create} disabled={busy}><Plus size={17}/> New workflow</button></div></header>
      {error && <div className="error"><AlertTriangle size={17}/><span>{error}</span><button onClick={() => setError('')}><X size={16}/></button></div>}
      <section className="content">
        {page === 'Overview' && <Overview stats={stats} runs={runs} workers={workers} workflows={workflows} onRun={async r => setSelected(await api.run(r.id))}/>}
        {page === 'Workflows' && <Workflows workflows={workflows} onLaunch={launch}/>}
        {page === 'Workers' && <Workers workers={workers}/>}
        {page === 'Dead letter' && <DeadLetter tasks={dead} replay={async id => { await api.replay(id); await refresh() }}/>}
      </section>
    </main>
    {selected && <RunDrawer run={selected} onClose={() => setSelected(null)} onCancel={async () => { await api.cancel(selected.id); setSelected(await api.run(selected.id)); await refresh() }}/>}
  </div>
}

function Overview({ stats, runs, workers, workflows, onRun }: { stats: {active:number;succeeded:number;failed:number}; runs:Run[]; workers:Worker[]; workflows:Workflow[]; onRun:(r:Run)=>void }) {
  return <><div className="hero"><div><span className="eyebrow"><Radio size={13}/> LIVE CONTROL PLANE</span><h2>Your workflows, moving.</h2><p>At-least-once delivery with leases, retries, and complete attempt history.</p></div><div className="mesh"><i/><i/><i/><i/><i/><i/></div></div>
    <div className="stats"><Stat icon={Activity} label="Active runs" value={stats.active} detail="right now"/><Stat icon={Boxes} label="Total runs" value={runs.length} detail={`${stats.succeeded} succeeded`}/><Stat icon={AlertTriangle} label="Failed runs" value={stats.failed} detail="needs attention" tone="warn"/><Stat icon={Server} label="Live workers" value={workers.length} detail={`${workflows.length} workflows`}/></div>
    <div className="panel"><PanelTitle title="Recent executions" subtitle="Latest workflow activity across this tenant"/><RunTable runs={runs} onRun={onRun}/></div></>
}
function Stat({icon:Icon,label,value,detail,tone}:{icon:typeof Activity;label:string;value:number;detail:string;tone?:string}){return <div className={`stat ${tone??''}`}><div className="stat-top"><span>{label}</span><Icon size={17}/></div><strong>{value.toLocaleString()}</strong><small>{detail}</small></div>}
function PanelTitle({title,subtitle}:{title:string;subtitle:string}){return <div className="panel-title"><div><h3>{title}</h3><p>{subtitle}</p></div><button className="quiet">View all <ChevronRight size={15}/></button></div>}
function RunTable({runs,onRun}:{runs:Run[];onRun:(r:Run)=>void}){if(!runs.length)return <Empty icon={CirclePlay} title="No executions yet" text="Create a workflow, then launch its first run."/>;return <div className="table"><div className="tr th"><span>RUN ID</span><span>STATUS</span><span>VERSION</span><span>STARTED</span><span/></div>{runs.slice(0,8).map(r=><button className="tr" key={r.id} onClick={()=>onRun(r)}><span className="mono">{r.id.slice(0,8)}</span><span><Status value={r.status}/></span><span>v{r.workflow_version}</span><span>{ago(r.created_at)}</span><ChevronRight size={16}/></button>)}</div>}
function Workflows({workflows,onLaunch}:{workflows:Workflow[];onLaunch:(id:string)=>void}){return <div className="cards">{workflows.map(w=><article className="workflow-card" key={w.id}><div className="card-head"><div className="workflow-icon"><Braces size={19}/></div><Status value="ACTIVE"/></div><h3>{w.name}</h3><p>Version {w.version} · {Object.keys(w.dag.tasks).length} tasks</p><div className="dag-mini">{Object.keys(w.dag.tasks).slice(0,4).map((key,i)=><span key={key}>{i>0&&<i/>}<b>{key.slice(0,1).toUpperCase()}</b></span>)}</div><div className="card-foot"><span>Updated {ago(w.created_at)}</span><button className="launch" onClick={()=>onLaunch(w.id)}><CirclePlay size={15}/> Launch</button></div></article>)}{!workflows.length&&<div className="panel wide"><Empty icon={WorkflowIcon} title="No workflows" text="Use “New workflow” to create a three-step example DAG."/></div>}</div>}
function Workers({workers}:{workers:Worker[]}){return <div className="panel"><PanelTitle title="Worker fleet" subtitle="Heartbeats and registered capabilities"/><div className="worker-grid">{workers.map(w=><div className="worker" key={w.worker_id}><div className="worker-state"><span/><Server size={20}/></div><div><h3>{w.worker_id}</h3><p>{w.handlers.join(' · ') || 'No handlers'}</p></div><strong>{w.active_tasks}<small> active</small></strong><time>{ago(w.last_seen_at)}</time></div>)}{!workers.length&&<Empty icon={Server} title="No workers connected" text="Start the bundled Python or Go worker to register capabilities."/>}</div></div>}
function DeadLetter({tasks,replay}:{tasks:TaskRun[];replay:(id:string)=>void}){return <div className="panel"><PanelTitle title="Dead-letter queue" subtitle="Tasks that exhausted retries or failed permanently"/>{tasks.length?<div className="table"><div className="tr th"><span>TASK</span><span>HANDLER</span><span>ATTEMPTS</span><span>AVAILABLE</span><span/></div>{tasks.map(t=><div className="tr" key={t.id}><span>{t.task_key}</span><span className="mono">{t.handler}</span><span>{t.attempt_count}/{t.maximum_attempts}</span><span>{ago(t.available_at)}</span><button className="quiet" onClick={()=>replay(t.id)}>Replay</button></div>)}</div>:<Empty icon={AlertTriangle} title="Dead letter is empty" text="Permanent failures and exhausted tasks will appear here."/>}</div>}
function RunDrawer({run,onClose,onCancel}:{run:Run;onClose:()=>void;onCancel:()=>void}){return <div className="overlay" onMouseDown={onClose}><aside className="drawer" onMouseDown={e=>e.stopPropagation()}><div className="drawer-head"><div><span className="eyebrow">RUN DETAIL</span><h2>{run.id.slice(0,8)}</h2></div><button className="icon-button" onClick={onClose}><X size={18}/></button></div><div className="run-meta"><Status value={run.status}/><span><Clock3 size={14}/>{ago(run.created_at)}</span><span>version {run.workflow_version}</span></div><h3 className="section-label">TASK GRAPH</h3><div className="task-list">{run.tasks?.map((task,i)=><div className="task-row" key={task.id}><div className="timeline"><i className={task.status.toLowerCase()}/>{i<(run.tasks?.length??0)-1&&<b/>}</div><div><strong>{task.task_key}</strong><small>{task.handler}</small></div><Status value={task.status}/><span className="attempt">{task.attempt_count}/{task.maximum_attempts}</span></div>)}</div>{!terminal.has(run.status)&&<button className="danger" onClick={onCancel}>Cancel execution</button>}</aside></div>}
function Status({value}:{value:string}){return <span className={`status ${value.toLowerCase()}`}><i/>{value.replace('_',' ')}</span>}
function Empty({icon:Icon,title,text}:{icon:typeof Activity;title:string;text:string}){return <div className="empty"><Icon size={25}/><h3>{title}</h3><p>{text}</p></div>}
function ago(date:string){const seconds=Math.floor((Date.now()-Date.parse(date))/1000);if(seconds<5)return 'just now';if(seconds<60)return `${seconds}s ago`;if(seconds<3600)return `${Math.floor(seconds/60)}m ago`;if(seconds<86400)return `${Math.floor(seconds/3600)}h ago`;return `${Math.floor(seconds/86400)}d ago`}
