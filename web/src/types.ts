export type TaskSpec = { handler: string; depends_on?: string[]; maximum_attempts?: number; timeout_seconds?: number }
export type Workflow = { id: string; name: string; version: number; dag: { tasks: Record<string, TaskSpec> }; created_at: string }
export type TaskRun = { id: string; task_key: string; handler: string; status: string; attempt_count: number; maximum_attempts: number; available_at: string }
export type Run = { id: string; workflow_definition_id: string; workflow_version: number; status: string; idempotency_key: string; created_at: string; started_at?: string; completed_at?: string; tasks?: TaskRun[] }
export type Worker = { worker_id: string; handlers: string[]; active_tasks: number; metadata: Record<string, string>; last_seen_at: string }
