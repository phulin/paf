export type Stage = "formalize" | "fixup" | "review" | "prove";
export type TaskStatus = "pending" | "running" | "succeeded" | "failed" | "blocked";

export interface Task {
  chapter_id: string;
  book_id: string;
  chapter_number: number;
  chapter_title: string;
  stage: Stage;
  status: TaskStatus;
  phase: string;
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
  error_count: number;
  warning_count: number;
  current_chapter_id: string | null;
  output_tail: string[];
}

export interface SwarmState {
  swarm_id?: string;
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
    by_stage?: Partial<Record<Stage, number>>;
  };
  scheduling?: {
    statements?: { critical_path?: string[] };
  };
  isolation?: { backend?: string };
  coordinator_build: CoordinatorBuild;
  tasks: Record<string, Task>;
  activities?: Record<string, AgentActivity>;
}

export interface SwarmSummary {
  id: string;
  active: boolean;
  updated_at: string;
  active_agents: number;
  maximum_agents: number;
  queued_agents: number;
  running_tasks: number;
  task_count: number;
  book_count: number;
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
  latest_summary?: string;
  recent?: ActivityEvent[];
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
