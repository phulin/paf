export type Stage = "discover" | "formalize" | "review" | "prove";
export type TaskStatus = "pending" | "running" | "succeeded" | "failed" | "blocked" | "interrupted";
export type TaskPhase = "idle" | "agent" | "postprocess";
export type SchedulingStatus = "waiting" | "queued" | "blocked" | "executing" | "complete";

export interface Requirement {
  kind: string;
  owner_task_key?: string | null;
  request_id?: string | null;
  detail: string;
}

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
  waiting_on?: Requirement[];
  scheduling_status?: SchedulingStatus;
  blocked_by?: string[];
  phase?: TaskPhase;
  detail: string;
  rounds: number;
  updated_at: string;
  latest_run_id?: string | null;
  proof_complete?: boolean;
  interface_current?: boolean;
  dependencies_current?: boolean;
  head_build_status?: "clean" | "pending" | "failed" | "unknown";
  sorry_count?: number | null;
  fully_certified?: boolean;
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

export interface CapabilityPackage {
  id: string;
  capability_key: string;
  title: string;
  mathematical_objective: string;
  status: string;
  disposition?: string | null;
  aliases: string[];
  textbook_refs: string[];
  write_scope: string[];
  expansion_scope: string[];
  base_revision: string;
  plan_revision: number;
  revision: number;
  branch: string;
  worktree: string;
  parent_package_id?: string | null;
  integrated_revision?: string | null;
  created_at: string;
  updated_at: string;
}

export interface PackageConsumer {
  id: string;
  package_id: string;
  work_unit_id: string;
  path: string;
  declaration: string;
  stage: string;
  residual_goal: string;
  source_digest?: string | null;
  blocker_ids: string[];
  attempted_routes: string[];
  acceptance_contract: Record<string, unknown>;
  status: string;
  accepted_revision?: string | null;
  detached_package_id?: string | null;
}

export interface PackageStep {
  id: string;
  package_id: string;
  kind: string;
  objective: string;
  status: string;
  assigned_worker_id?: string | null;
  intended_declarations: string[];
  intended_paths: string[];
  depends_on_step_ids: string[];
  commit_ids: string[];
  validation_contract: Record<string, unknown>;
  remaining_gap: string;
  plan_revision: number;
}

export interface PackageEvidence {
  id: string;
  package_id: string;
  producer: string;
  kind: string;
  paths: string[];
  declarations: string[];
  payload: Record<string, unknown>;
  created_at: string;
}

export interface StewardLease {
  package_id: string;
  agent_id: string;
  generation: number;
  acquired_at: string;
  heartbeat_at: string;
  expires_at: string;
}

export interface PathReservation {
  normalized_path: string;
  package_id: string;
  mode: string;
  lease_generation: number;
}

export interface PackageDependency {
  package_id: string;
  depends_on_package_id: string;
  required_revision?: string | null;
}

export interface RelevantReadInterface {
  package_id: string;
  interface_id: string;
  digest: string;
  source_revision: string;
}

export interface IntegrationJournal {
  id: string;
  package_id: string;
  lease_generation: number;
  base_revision: string;
  phase: string;
  candidate_revision: string;
  canonical_revision_before: string;
  canonical_revision_after?: string | null;
  validation_digest: string;
  provisional_consumer_ids: string[];
  updated_at: string;
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
  capability_packages?: Record<string, CapabilityPackage>;
  package_consumers?: Record<string, PackageConsumer>;
  package_steps?: Record<string, PackageStep>;
  package_evidence?: Record<string, PackageEvidence>;
  steward_leases?: Record<string, StewardLease>;
  path_reservations?: Record<string, PathReservation>;
  package_dependencies?: PackageDependency[];
  relevant_read_interfaces?: RelevantReadInterface[];
  integration_journal?: Record<string, IntegrationJournal>;
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
