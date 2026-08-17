export type Stage = "discover" | "formalize" | "review" | "prove";
export type TaskStatus = "pending" | "running" | "succeeded" | "failed" | "blocked" | "interrupted";
export type TaskPhase = "idle" | "agent" | "postprocess";

export interface Task {
  work_unit_id: string;
  document_id: string;
  ordinal: number;
  unit_title: string;
  /** Legacy state aliases, retained while old snapshots remain readable. */
  chapter_id?: string;
  book_id?: string;
  chapter_number?: number;
  chapter_title?: string;
  stage: Stage;
  status: TaskStatus;
  queued?: boolean;
  repairing?: boolean;
  repair_work_unit_id?: string;
  phase?: TaskPhase;
  detail: string;
  rounds: number;
  updated_at: string;
  latest_run_id?: string | null;
}

export interface Usage {
  input_tokens: number;
  cached_input_tokens: number;
  output_tokens: number;
  reasoning_output_tokens: number;
  total_tokens: number;
}

export interface CoordinatorBuild {
  active: boolean;
  mode: string;
  stage: string;
  completed: number;
  total: number;
  iteration: number;
  maximum_iterations: number;
  target_work_unit_ids?: string[];
  error_count: number;
  warning_count: number;
  current_work_unit_id: string | null;
  target_chapter_ids?: string[];
  current_chapter_id?: string | null;
  output_tail: string[];
}

export interface Shepherd {
  enabled: boolean;
  status: "idle" | "planning" | "repairing" | "error" | string;
  model: string;
  worker_model: string;
  interval_seconds: number;
  failure_threshold: number;
  current_sweep_id: string;
  current_run_id: string;
  last_started_at?: string | null;
  last_finished_at?: string | null;
  next_run_at?: string | null;
  last_summary: string;
  last_error: string;
  pending_failures: number;
  planned_units: number;
  running_units: number;
  succeeded_units: number;
  failed_units: number;
  agents?: ShepherdAgent[];
}

export interface ShepherdAgent {
  run_id: string;
  role: "shepherd" | "repair_worker" | string;
  work_unit_id: string;
  stage: Stage | string;
  status: string;
  label: string;
  repair_work_unit_id: string;
  objective: string;
}

export interface SwarmState {
  swarm_id?: string;
  revision?: number;
  source: string;
  updated_at: string;
  usage: Usage;
  invocation_usage: Usage;
  cost: { estimated_usd: number };
  invocation_cost: { estimated_usd: number };
  agents: {
    active: number;
    maximum: number;
    queued: number;
    postprocessing?: number;
    by_stage?: Partial<Record<Stage, number>>;
    postprocessing_by_stage?: Partial<Record<Stage, number>>;
  };
  scheduling?: {
    statements?: { critical_path?: string[] };
  };
  isolation?: { backend?: string };
  coordinator_build: CoordinatorBuild;
  shepherd?: Shepherd;
  tasks: Record<string, Task>;
  activities?: Record<string, AgentActivity>;
}

export interface DashboardDelta {
  revision: number;
  resync_required: boolean;
  changes: Array<{
    revision: number;
    entity_type: string;
    entity_id: string;
  }>;
  tasks: Record<string, Task>;
  removed_task_ids: string[];
  globals: Partial<Omit<SwarmState, "tasks" | "activities">>;
  run_ids: string[];
  active_run_ids: string[];
  activities: Record<string, AgentActivity>;
}

export interface SwarmSummary {
  id: string;
  revision: number;
  active: boolean;
  updated_at: string;
  active_agents: number;
  maximum_agents: number;
  queued_agents: number;
  running_tasks: number;
  task_count: number;
  document_count: number;
  book_count?: number;
}

export interface SystemLoad {
  cpu_percent: number | null;
  memory_used_bytes: number;
  memory_total_bytes: number;
  memory_percent: number;
}

export interface SwarmListResponse {
  swarms: SwarmSummary[];
}

export interface AgentActivity {
  run_id?: string;
  current?: string;
  updated_at?: string;
  commands?: number;
  mcp_calls?: number;
  file_changes?: number;
  failures?: number;
  todo_completed?: number;
  todo_total?: number;
  todos?: AgentTodo[];
  latest_summary?: string;
  recent?: ActivityEvent[];
}

export interface AgentTodo {
  completed: boolean;
  text: string;
}

export interface ActivityEvent {
  sequence?: number;
  at: string;
  kind: string;
  status: string;
  title: string;
  detail?: string;
}

export type DeclarationKind =
  | "theorem"
  | "lemma"
  | "def"
  | "abbrev"
  | "structure"
  | "class"
  | "instance";

export interface LeanStatement {
  id: string;
  name: string;
  kind: DeclarationKind;
  signature: string;
  excerpt: string;
  doc: string;
  path: string;
  line: number;
  endLine: number;
  book: string;
  bookNumber: number;
  chapter: number;
  section: string;
  status: "proved" | "sorry";
}

export interface StatementResponse {
  source: "repository" | "demo";
  total: number;
  declarations: LeanStatement[];
  facets: {
    books: Array<{ id: string; number: number; label: string; count: number }>;
    kinds: Partial<Record<DeclarationKind, number>>;
    statuses: { proved: number; sorry: number };
  };
}
