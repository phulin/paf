use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

pub const STAGES: [&str; 4] = ["discover", "formalize", "review", "prove"];
pub const PROTOCOL_VERSION: u64 = 4;

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Usage {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_output_tokens: u64,
    pub total_tokens: u64,
    pub measured: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Cost {
    pub estimated_usd: f64,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Agents {
    pub active: usize,
    pub maximum: usize,
    pub queued: usize,
    pub maximum_by_pool: HashMap<String, usize>,
    pub by_stage: HashMap<String, usize>,
    pub postprocessing_by_stage: HashMap<String, usize>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct CoordinatorBuild {
    pub active: bool,
    pub mode: String,
    pub stage: String,
    pub completed: usize,
    pub total: usize,
    pub iteration: usize,
    pub maximum_iterations: usize,
    pub target_work_unit_ids: HashSet<String>,
    pub error_count: usize,
    pub warning_count: usize,
    pub current_work_unit_id: Option<String>,
    pub output_tail: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct CapabilityPackage {
    pub id: String,
    pub capability_key: String,
    pub title: String,
    pub mathematical_objective: String,
    pub status: String,
    pub disposition: Option<String>,
    pub aliases: Vec<String>,
    pub textbook_refs: Vec<String>,
    pub write_scope: Vec<String>,
    pub expansion_scope: Vec<String>,
    pub plan_revision: usize,
    pub revision: usize,
    pub parent_package_id: Option<String>,
    pub integrated_revision: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct UpstreamRequest {
    pub id: String,
    pub status: String,
    pub title: String,
    pub consumer_chapter_id: String,
    pub consumer_path: String,
    pub blocked_declaration: String,
    pub residual_goal: String,
    pub obstruction: String,
    pub needed_result: String,
    pub capability_key: String,
    pub owner_chapter_id: String,
    pub owner_paths: Vec<String>,
    pub attempted_alternatives: Vec<String>,
    pub blocker_ids: Vec<String>,
    pub evaluation_run_id: String,
    pub decision: Value,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct StewardCase {
    pub id: String,
    pub status: String,
    pub title: String,
    pub disposition: String,
    pub needed_result: String,
    pub request_ids: Vec<String>,
    pub context_work_unit_ids: Vec<String>,
    pub acceptance_tests: Vec<String>,
    pub rationale: String,
    pub steward_run_id: String,
    pub implementation_run_ids: Vec<String>,
    pub decision: Value,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct PackageConsumer {
    pub id: String,
    pub package_id: String,
    pub work_unit_id: String,
    pub path: String,
    pub declaration: String,
    pub stage: String,
    pub residual_goal: String,
    pub source_digest: Option<String>,
    pub blocker_ids: Vec<String>,
    pub attempted_routes: Vec<String>,
    pub acceptance_contract: Value,
    pub status: String,
    pub accepted_revision: Option<String>,
    pub detached_package_id: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct PackageStep {
    pub id: String,
    pub package_id: String,
    pub kind: String,
    pub objective: String,
    pub status: String,
    pub assigned_worker_id: Option<String>,
    pub intended_declarations: Vec<String>,
    pub intended_paths: Vec<String>,
    pub depends_on_step_ids: Vec<String>,
    pub commit_ids: Vec<String>,
    pub validation_contract: Value,
    pub remaining_gap: String,
    pub plan_revision: usize,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct PackageEvidence {
    pub id: String,
    pub package_id: String,
    pub producer: String,
    pub kind: String,
    pub paths: Vec<String>,
    pub declarations: Vec<String>,
    pub payload: Value,
    pub created_at: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct StewardLease {
    pub package_id: String,
    pub agent_id: String,
    pub generation: usize,
    pub acquired_at: String,
    pub heartbeat_at: String,
    pub expires_at: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct PathReservation {
    pub normalized_path: String,
    pub package_id: String,
    pub mode: String,
    pub lease_generation: usize,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct PackageDependency {
    pub package_id: String,
    pub depends_on_package_id: String,
    pub required_revision: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct RelevantReadInterface {
    pub package_id: String,
    pub interface_id: String,
    pub digest: String,
    pub source_revision: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Task {
    pub work_unit_id: String,
    pub document_id: String,
    pub ordinal: usize,
    pub unit_title: String,
    pub stage: String,
    pub status: String,
    pub active_auxiliary_role: String,
    pub phase: String,
    pub detail: String,
    pub queued: bool,
    pub scheduling_status: String,
    pub head_build_status: String,
    pub sorry_count: Option<usize>,
    pub blocked_by: Vec<String>,
    pub rounds: usize,
    pub updated_at: String,
    pub latest_run_id: Option<String>,
    pub work_unit_usage: Usage,
    pub work_unit_cost: Cost,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ActivityEntry {
    pub sequence: u64,
    pub at: String,
    pub kind: String,
    pub status: String,
    pub title: String,
    pub detail: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Todo {
    pub text: String,
    pub completed: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Activity {
    pub run_id: String,
    pub sequence: u64,
    pub updated_at: String,
    pub current: String,
    pub commands: usize,
    pub mcp_calls: usize,
    pub file_changes: usize,
    pub failures: usize,
    pub latest_summary: String,
    pub latest_error: String,
    pub todo_completed: usize,
    pub todo_total: usize,
    pub todos: Vec<Todo>,
    pub files: Vec<String>,
    pub recent: Vec<ActivityEntry>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct HistoricalRun {
    pub id: String,
    pub stage: String,
    pub role: String,
    pub round: usize,
    pub status: String,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ChapterRuns {
    pub work_unit_id: String,
    pub runs: Vec<HistoricalRun>,
    pub selected_run_id: Option<String>,
    pub activity: Option<Activity>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct WorkUnit {
    pub id: String,
    pub document_id: String,
    pub title: String,
    pub ordinal: usize,
    pub source_start_line: usize,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct SchedulePhase {
    pub critical_path: Vec<String>,
    pub rank: HashMap<String, f64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Scheduling {
    pub statements: SchedulePhase,
    pub proofs: SchedulePhase,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct SourceDependencyTree {
    pub dependencies: HashMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Isolation {
    pub backend: String,
    pub codex_access: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct SwarmState {
    pub revision: u64,
    pub updated_at: String,
    pub source: String,
    pub invocation_usage: Usage,
    pub usage: Usage,
    pub invocation_cost: Cost,
    pub cost: Cost,
    pub agents: Agents,
    pub coordinator_build: CoordinatorBuild,
    pub upstream_requests: HashMap<String, UpstreamRequest>,
    pub steward_cases: HashMap<String, StewardCase>,
    pub capability_packages: HashMap<String, CapabilityPackage>,
    pub package_consumers: HashMap<String, PackageConsumer>,
    pub package_steps: HashMap<String, PackageStep>,
    pub package_evidence: HashMap<String, PackageEvidence>,
    pub steward_leases: HashMap<String, StewardLease>,
    pub path_reservations: HashMap<String, PathReservation>,
    pub package_dependencies: Vec<PackageDependency>,
    pub relevant_read_interfaces: Vec<RelevantReadInterface>,
    pub scheduling: Scheduling,
    pub source_dependency_tree: SourceDependencyTree,
    pub isolation: Isolation,
    pub work_units: Vec<WorkUnit>,
    pub tasks: HashMap<String, Task>,
    pub activities: HashMap<String, Activity>,
    pub formalize_graph: Value,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WireEvent {
    pub protocol_version: u64,
    pub event: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub result: Option<bool>,
    #[serde(default)]
    pub snapshot: Option<Value>,
    #[serde(default)]
    pub delta: Option<DashboardDelta>,
    #[serde(default)]
    pub preparation: Option<Preparation>,
    #[serde(default)]
    pub message: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Preparation {
    pub phase: String,
    pub completed: usize,
    pub total: usize,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct DashboardDelta {
    pub revision: u64,
    pub resync_required: bool,
    pub tasks: HashMap<String, Task>,
    pub removed_task_ids: Vec<String>,
    pub globals: GlobalDelta,
    pub activities: HashMap<String, Activity>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct GlobalDelta {
    pub updated_at: Option<String>,
    pub source: Option<String>,
    pub invocation_usage: Option<Usage>,
    pub usage: Option<Usage>,
    pub invocation_cost: Option<Cost>,
    pub cost: Option<Cost>,
    pub agents: Option<Agents>,
    pub coordinator_build: Option<CoordinatorBuild>,
    pub upstream_requests: Option<HashMap<String, UpstreamRequest>>,
    pub steward_cases: Option<HashMap<String, StewardCase>>,
    pub capability_packages: Option<HashMap<String, CapabilityPackage>>,
    pub package_consumers: Option<HashMap<String, PackageConsumer>>,
    pub package_steps: Option<HashMap<String, PackageStep>>,
    pub package_evidence: Option<HashMap<String, PackageEvidence>>,
    pub steward_leases: Option<HashMap<String, StewardLease>>,
    pub path_reservations: Option<HashMap<String, PathReservation>>,
    pub package_dependencies: Option<Vec<PackageDependency>>,
    pub relevant_read_interfaces: Option<Vec<RelevantReadInterface>>,
    pub scheduling: Option<Scheduling>,
    pub source_dependency_tree: Option<SourceDependencyTree>,
    pub isolation: Option<Isolation>,
    pub formalize_graph: Option<Value>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DetailTab {
    #[default]
    Timeline,
    Prompt,
    Summary,
    Plan,
    Files,
}

impl DetailTab {
    pub const ALL: [Self; 5] = [
        Self::Timeline,
        Self::Prompt,
        Self::Summary,
        Self::Plan,
        Self::Files,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Timeline => "Timeline",
            Self::Prompt => "Prompt",
            Self::Summary => "Update",
            Self::Plan => "Plan",
            Self::Files => "Files",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Timeline => "timeline",
            Self::Prompt => "prompt",
            Self::Summary => "summary",
            Self::Plan => "plan",
            Self::Files => "files",
        }
    }

    fn from_name(name: &str) -> Self {
        match name {
            "summary" => Self::Summary,
            "prompt" => Self::Prompt,
            "plan" => Self::Plan,
            "files" => Self::Files,
            _ => Self::Timeline,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DashboardModel {
    pub state: SwarmState,
    pub daemon_status: String,
    pub result: Option<bool>,
    pub selected: usize,
    pub detail: bool,
    pub steward_detail: bool,
    pub steward_selected: usize,
    pub detail_case_id: Option<String>,
    pub detail_tab: DetailTab,
    pub scroll: u16,
    pub detail_max_scroll: u16,
    pub detail_follow_tail: bool,
    pub detail_runs: Vec<HistoricalRun>,
    pub selected_run: usize,
    pub run_prompts: HashMap<String, String>,
    loading_chapter_runs: Option<(String, Option<String>)>,
    loading_case_runs: Option<(String, Option<String>)>,
    loading_prompt_runs: HashSet<String>,
    pub full_timeline_runs: HashSet<String>,
    pub loading_timeline_runs: HashSet<String>,
    pub timeline_errors: HashMap<String, String>,
    pub(crate) timeline_render_cache: crate::viewport::TimelineRenderCache,
    pub label: String,
    pub startup_warning: String,
    pub stopping: bool,
    pub preparation: Option<Preparation>,
    pub reload_requested: bool,
    pub detach_requested: bool,
    pub search_query: Option<String>,
    pub search_error: String,
    search_origin: Option<usize>,
    restore_agent_view: Option<String>,
}

impl DashboardModel {
    #[cfg(test)]
    pub fn loading(label: String, startup_warning: String) -> Self {
        Self::loading_with_agent_view(label, startup_warning, None, None)
    }

    pub fn loading_with_agent_view(
        label: String,
        startup_warning: String,
        agent_view: Option<String>,
        detail_tab: Option<&str>,
    ) -> Self {
        Self {
            state: SwarmState::default(),
            daemon_status: "connecting".into(),
            result: None,
            selected: 0,
            detail: false,
            steward_detail: false,
            steward_selected: 0,
            detail_case_id: None,
            detail_tab: detail_tab.map(DetailTab::from_name).unwrap_or_default(),
            scroll: 0,
            detail_max_scroll: 0,
            detail_follow_tail: true,
            detail_runs: Vec::new(),
            selected_run: 0,
            run_prompts: HashMap::new(),
            loading_chapter_runs: None,
            loading_case_runs: None,
            loading_prompt_runs: HashSet::new(),
            full_timeline_runs: HashSet::new(),
            loading_timeline_runs: HashSet::new(),
            timeline_errors: HashMap::new(),
            timeline_render_cache: crate::viewport::TimelineRenderCache::default(),
            label,
            startup_warning,
            stopping: false,
            preparation: Some(Preparation {
                phase: "Connecting to the orchestrator".into(),
                completed: 0,
                total: 1,
            }),
            reload_requested: false,
            detach_requested: false,
            search_query: None,
            search_error: String::new(),
            search_origin: None,
            restore_agent_view: agent_view,
        }
    }

    pub fn apply(&mut self, event: WireEvent) -> Result<()> {
        if event.protocol_version != PROTOCOL_VERSION {
            bail!(
                "unsupported dashboard protocol {}, expected {}",
                event.protocol_version,
                PROTOCOL_VERSION
            );
        }
        if !event.status.is_empty() {
            self.daemon_status = event.status;
        }
        match event.event.as_str() {
            "snapshot" => {
                self.state = serde_json::from_value(
                    event.snapshot.context("snapshot event has no snapshot")?,
                )
                .context("invalid dashboard model")?;
                self.preparation = None;
                self.restore_agent_view();
            }
            "delta" => self.apply_delta(event.delta.context("delta event has no delta")?)?,
            "preparation" => {
                self.preparation = Some(
                    event
                        .preparation
                        .context("preparation event has no progress")?,
                );
            }
            "complete" => self.result = event.result,
            "error" => bail!("{}", event.message),
            other => bail!("unknown dashboard event {other:?}"),
        }
        self.clamp_selection();
        Ok(())
    }

    fn restore_agent_view(&mut self) {
        let Some(work_unit_id) = self.restore_agent_view.take() else {
            return;
        };
        if let Some(selected) = self
            .rows()
            .iter()
            .position(|row| row.unit.id == work_unit_id)
        {
            self.selected = selected;
            self.enter_detail();
        }
    }

    fn apply_delta(&mut self, delta: DashboardDelta) -> Result<()> {
        if delta.resync_required {
            bail!("server requested a full dashboard resynchronization")
        }
        self.state.revision = delta.revision;
        self.state.tasks.extend(delta.tasks);
        for task_id in delta.removed_task_ids {
            self.state.tasks.remove(&task_id);
        }
        for (run_id, activity) in delta.activities {
            self.merge_activity(run_id, activity);
        }
        apply_optional(&mut self.state.updated_at, delta.globals.updated_at);
        apply_optional(&mut self.state.source, delta.globals.source);
        apply_optional(
            &mut self.state.invocation_usage,
            delta.globals.invocation_usage,
        );
        apply_optional(&mut self.state.usage, delta.globals.usage);
        apply_optional(
            &mut self.state.invocation_cost,
            delta.globals.invocation_cost,
        );
        apply_optional(&mut self.state.cost, delta.globals.cost);
        apply_optional(&mut self.state.agents, delta.globals.agents);
        apply_optional(
            &mut self.state.coordinator_build,
            delta.globals.coordinator_build,
        );
        apply_optional(
            &mut self.state.upstream_requests,
            delta.globals.upstream_requests,
        );
        apply_optional(&mut self.state.steward_cases, delta.globals.steward_cases);
        apply_optional(
            &mut self.state.capability_packages,
            delta.globals.capability_packages,
        );
        apply_optional(
            &mut self.state.package_consumers,
            delta.globals.package_consumers,
        );
        apply_optional(&mut self.state.package_steps, delta.globals.package_steps);
        apply_optional(
            &mut self.state.package_evidence,
            delta.globals.package_evidence,
        );
        apply_optional(&mut self.state.steward_leases, delta.globals.steward_leases);
        apply_optional(
            &mut self.state.path_reservations,
            delta.globals.path_reservations,
        );
        apply_optional(
            &mut self.state.package_dependencies,
            delta.globals.package_dependencies,
        );
        apply_optional(
            &mut self.state.relevant_read_interfaces,
            delta.globals.relevant_read_interfaces,
        );
        apply_optional(&mut self.state.scheduling, delta.globals.scheduling);
        apply_optional(
            &mut self.state.source_dependency_tree,
            delta.globals.source_dependency_tree,
        );
        apply_optional(&mut self.state.isolation, delta.globals.isolation);
        apply_optional(
            &mut self.state.formalize_graph,
            delta.globals.formalize_graph,
        );
        Ok(())
    }

    pub fn rows(&self) -> Vec<RowModel<'_>> {
        let mut task_groups: HashMap<&str, HashMap<&str, &Task>> = HashMap::new();
        for task in self.state.tasks.values() {
            task_groups
                .entry(task.work_unit_id.as_str())
                .or_default()
                .insert(task.stage.as_str(), task);
        }
        self.state
            .work_units
            .iter()
            .filter_map(|unit| {
                task_groups
                    .remove(unit.id.as_str())
                    .map(|tasks| RowModel { unit, tasks })
            })
            .collect()
    }

    pub fn selected_row(&self) -> Option<RowModel<'_>> {
        self.rows().into_iter().nth(self.selected)
    }

    pub fn selected_activity(&self) -> Option<&Activity> {
        if self.detail
            && let Some(run) = self.detail_runs.get(self.selected_run)
        {
            return self.state.activities.get(&run.id);
        }
        let row = self.selected_row()?;
        row.tasks
            .values()
            .filter_map(|task| {
                let run_id = task.latest_run_id.as_ref()?;
                let activity = self.state.activities.get(run_id)?;
                Some((
                    task.status == "running" || !task.active_auxiliary_role.is_empty(),
                    &activity.updated_at,
                    activity,
                ))
            })
            .max_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(right.1)))
            .map(|value| value.2)
    }

    pub fn move_selection(&mut self, delta: isize) {
        let length = self.rows().len();
        if length == 0 {
            self.selected = 0;
            return;
        }
        self.selected = self.selected.saturating_add_signed(delta).min(length - 1);
        self.scroll = 0;
    }

    pub fn begin_search(&mut self) {
        self.search_query = Some(String::new());
        self.search_error.clear();
        self.search_origin = Some(self.selected);
    }

    pub fn cancel_search(&mut self) {
        if let Some(origin) = self.search_origin {
            self.selected = origin.min(self.rows().len().saturating_sub(1));
            self.scroll = 0;
        }
        self.search_query = None;
        self.search_error.clear();
        self.search_origin = None;
    }

    pub fn accept_search(&mut self) {
        self.search_query = None;
        self.search_error.clear();
        self.search_origin = None;
        if self.detail {
            self.leave_detail();
        }
        self.steward_detail = false;
    }

    pub fn push_search_character(&mut self, character: char) {
        if let Some(query) = &mut self.search_query {
            query.push(character);
        }
        self.update_search_selection();
    }

    pub fn pop_search_character(&mut self) {
        if let Some(query) = &mut self.search_query {
            query.pop();
        }
        self.update_search_selection();
    }

    pub fn clear_search_query(&mut self) {
        if let Some(query) = &mut self.search_query {
            query.clear();
        }
        self.update_search_selection();
    }

    /// Select the first work unit matching the current book search.
    ///
    /// A numeric suffix after the final dot is an exact `document_id.ordinal` target. All other
    /// queries are case-insensitive substrings of `document_id` and select the first displayed row
    /// in the matching document.
    fn update_search_selection(&mut self) -> bool {
        let query = self.search_query.as_deref().unwrap_or_default().trim();
        if query.is_empty() {
            if let Some(origin) = self.search_origin {
                self.selected = origin.min(self.rows().len().saturating_sub(1));
                self.scroll = 0;
            }
            self.search_error.clear();
            return true;
        }

        let rows = self.rows();
        let exact_target = query.rsplit_once('.').and_then(|(document_id, ordinal)| {
            (!document_id.is_empty())
                .then(|| {
                    ordinal
                        .parse::<usize>()
                        .ok()
                        .map(|ordinal| (document_id, ordinal))
                })
                .flatten()
        });
        let selected = if let Some((document_id, ordinal)) = exact_target {
            rows.iter().position(|row| {
                exact_document_id_matches(&row.unit.document_id, document_id)
                    && row.unit.ordinal == ordinal
            })
        } else {
            let query = query.to_lowercase();
            rows.iter()
                .position(|row| row.unit.document_id.to_lowercase().contains(&query))
        };
        let Some(selected) = selected else {
            self.search_error = format!("No book or unit found for {query:?}");
            return false;
        };
        drop(rows);
        self.selected = selected;
        self.scroll = 0;
        self.search_error.clear();
        true
    }

    fn clamp_selection(&mut self) {
        self.selected = self.selected.min(self.rows().len().saturating_sub(1));
    }

    pub fn cycle_tab(&mut self, backwards: bool) {
        let current = DetailTab::ALL
            .iter()
            .position(|tab| *tab == self.detail_tab)
            .unwrap_or_default();
        let next = if backwards {
            current.checked_sub(1).unwrap_or(DetailTab::ALL.len() - 1)
        } else {
            (current + 1) % DetailTab::ALL.len()
        };
        self.detail_tab = DetailTab::ALL[next];
        self.scroll = 0;
        self.detail_max_scroll = 0;
        self.detail_follow_tail = true;
    }

    pub fn enter_detail(&mut self) {
        self.detail = true;
        self.steward_detail = false;
        self.detail_case_id = None;
        self.detail_runs.clear();
        self.loading_chapter_runs = None;
        self.loading_case_runs = None;
        self.selected_run = 0;
        self.scroll = 0;
        self.detail_max_scroll = 0;
        self.detail_follow_tail = true;
    }

    pub fn leave_detail(&mut self) {
        self.detail = false;
        self.steward_detail = self.detail_case_id.take().is_some();
        self.detail_runs.clear();
        self.loading_chapter_runs = None;
        self.loading_case_runs = None;
        self.selected_run = 0;
        self.run_prompts.clear();
        self.scroll = 0;
        self.detail_max_scroll = 0;
        self.detail_follow_tail = true;
    }

    pub fn enter_steward_detail(&mut self) {
        self.steward_detail = true;
        self.detail = false;
        self.detail_case_id = None;
        self.loading_case_runs = None;
        self.steward_selected = self
            .steward_selected
            .min(self.steward_cases().len().saturating_sub(1));
        self.scroll = 0;
        self.detail_max_scroll = 0;
        self.detail_follow_tail = true;
    }

    pub fn leave_steward_detail(&mut self) {
        self.steward_detail = false;
        self.scroll = 0;
        self.detail_max_scroll = 0;
        self.detail_follow_tail = true;
    }

    pub fn enter_case_run_detail(&mut self, case_id: String) {
        self.detail = true;
        self.steward_detail = false;
        self.detail_case_id = Some(case_id);
        self.detail_runs.clear();
        self.loading_case_runs = None;
        self.selected_run = 0;
        self.scroll = 0;
        self.detail_max_scroll = 0;
        self.detail_follow_tail = true;
    }

    pub fn steward_cases(&self) -> Vec<&StewardCase> {
        let mut cases = self.state.steward_cases.values().collect::<Vec<_>>();
        cases.sort_by(|left, right| {
            steward_case_status_rank(&left.status)
                .cmp(&steward_case_status_rank(&right.status))
                .then(right.updated_at.cmp(&left.updated_at))
                .then(left.id.cmp(&right.id))
        });
        cases
    }

    pub fn selected_steward_case(&self) -> Option<&StewardCase> {
        self.steward_cases().get(self.steward_selected).copied()
    }

    pub fn move_upstream_request_selection(&mut self, delta: isize) {
        let length = self.steward_cases().len();
        if length == 0 {
            self.steward_selected = 0;
            return;
        }
        self.steward_selected = self
            .steward_selected
            .saturating_add_signed(delta)
            .min(length - 1);
        self.scroll = 0;
        self.detail_max_scroll = 0;
        self.detail_follow_tail = true;
    }

    pub fn trace_run_id(&self) -> Option<&str> {
        self.selected_run_id()
    }

    pub fn selected_run_id(&self) -> Option<&str> {
        self.detail_runs
            .get(self.selected_run)
            .map(|run| run.id.as_str())
    }

    pub fn selected_prompt(&self) -> Option<&str> {
        self.selected_run_id()
            .and_then(|run_id| self.run_prompts.get(run_id).map(String::as_str))
    }

    pub fn begin_chapter_runs_load(
        &mut self,
        chapter: &str,
        selected_run_id: Option<&str>,
    ) -> bool {
        let request = (chapter.to_owned(), selected_run_id.map(str::to_owned));
        if self.loading_chapter_runs.as_ref() == Some(&request) {
            return false;
        }
        self.loading_chapter_runs = Some(request);
        true
    }

    pub fn apply_loaded_chapter_runs(
        &mut self,
        chapter: &str,
        selected_run_id: Option<&str>,
        details: ChapterRuns,
    ) -> bool {
        let request = (chapter.to_owned(), selected_run_id.map(str::to_owned));
        if self.loading_chapter_runs.as_ref() != Some(&request)
            || !self.detail
            || self.selected_row().map(|row| row.unit.id.as_str()) != Some(chapter)
        {
            return false;
        }
        self.loading_chapter_runs = None;
        self.apply_chapter_runs(details);
        true
    }

    pub fn begin_case_runs_load(&mut self, case_id: &str, selected_run_id: Option<&str>) -> bool {
        let request = (case_id.to_owned(), selected_run_id.map(str::to_owned));
        if self.loading_case_runs.as_ref() == Some(&request) {
            return false;
        }
        self.loading_case_runs = Some(request);
        true
    }

    pub fn apply_loaded_case_runs(
        &mut self,
        case_id: &str,
        selected_run_id: Option<&str>,
        details: ChapterRuns,
    ) -> bool {
        let request = (case_id.to_owned(), selected_run_id.map(str::to_owned));
        if self.loading_case_runs.as_ref() != Some(&request)
            || !self.detail
            || self.detail_case_id.as_deref() != Some(case_id)
        {
            return false;
        }
        self.loading_case_runs = None;
        self.apply_chapter_runs(details);
        true
    }

    pub fn begin_prompt_load(&mut self, run_id: &str) -> bool {
        if self.run_prompts.contains_key(run_id) || self.loading_prompt_runs.contains(run_id) {
            return false;
        }
        self.loading_prompt_runs.insert(run_id.to_owned());
        true
    }

    pub fn apply_loaded_prompt(&mut self, run_id: String, prompt: String) {
        self.loading_prompt_runs.remove(&run_id);
        self.run_prompts.insert(run_id, prompt);
    }

    pub fn fail_prompt_load(&mut self, run_id: String, error: String) {
        self.loading_prompt_runs.remove(&run_id);
        self.run_prompts
            .insert(run_id, format!("Prompt unavailable: {error}"));
    }

    pub fn selected_timeline_status(&self) -> Option<&str> {
        let run_id = self.selected_run_id()?;
        if let Some(error) = self.timeline_errors.get(run_id) {
            Some(error)
        } else if self.loading_timeline_runs.contains(run_id) {
            Some("Loading earlier transcript events…")
        } else {
            None
        }
    }

    pub fn begin_timeline_load(&mut self, run_id: &str) -> bool {
        if self.full_timeline_runs.contains(run_id) || self.loading_timeline_runs.contains(run_id) {
            return false;
        }
        self.timeline_errors.remove(run_id);
        self.loading_timeline_runs.insert(run_id.to_owned());
        true
    }

    pub fn apply_full_timeline(&mut self, run_id: String, activity: Activity) {
        self.loading_timeline_runs.remove(&run_id);
        self.timeline_errors.remove(&run_id);
        self.full_timeline_runs.insert(run_id.clone());
        self.merge_activity(run_id, activity);
    }

    pub fn fail_timeline_load(&mut self, run_id: String, error: String) {
        self.loading_timeline_runs.remove(&run_id);
        self.timeline_errors.insert(run_id, error);
    }

    fn merge_activity(&mut self, run_id: String, mut incoming: Activity) {
        if self.full_timeline_runs.contains(&run_id)
            && let Some(existing) = self.state.activities.get(&run_id)
        {
            let mut recent = incoming.recent;
            recent.extend(existing.recent.iter().cloned());
            recent.sort_by_key(|entry| entry.sequence);
            recent.dedup_by_key(|entry| entry.sequence);
            if existing.sequence > incoming.sequence {
                incoming = existing.clone();
            }
            incoming.recent = recent;
        }
        self.state.activities.insert(run_id, incoming);
    }

    pub fn apply_chapter_runs(&mut self, details: ChapterRuns) {
        let selected = details
            .selected_run_id
            .as_deref()
            .and_then(|id| details.runs.iter().position(|run| run.id == id))
            .unwrap_or_else(|| details.runs.len().saturating_sub(1));
        if let Some(activity) = details.activity {
            self.merge_activity(activity.run_id.clone(), activity);
        }
        self.detail_runs = details.runs;
        self.selected_run = selected;
        self.scroll = 0;
        self.detail_max_scroll = 0;
        self.detail_follow_tail = true;
    }

    pub fn cycle_run(&mut self, backwards: bool) {
        if self.detail_runs.len() < 2 {
            return;
        }
        self.selected_run = if backwards {
            self.selected_run
                .checked_sub(1)
                .unwrap_or(self.detail_runs.len() - 1)
        } else {
            (self.selected_run + 1) % self.detail_runs.len()
        };
        self.scroll = 0;
        self.detail_max_scroll = 0;
        self.detail_follow_tail = true;
    }

    pub fn sync_detail_viewport(&mut self, maximum: u16) {
        self.detail_max_scroll = maximum;
        if self.detail_follow_tail {
            self.scroll = maximum;
        } else {
            self.scroll = self.scroll.min(maximum);
        }
    }

    pub fn scroll_detail(&mut self, delta: i16) {
        self.scroll = self
            .scroll
            .saturating_add_signed(delta)
            .min(self.detail_max_scroll);
        if self.scroll < self.detail_max_scroll {
            self.detail_follow_tail = false;
        } else if delta > 0 {
            self.detail_follow_tail = true;
        }
    }

    pub fn scroll_detail_home(&mut self) {
        self.scroll = 0;
        self.detail_follow_tail = self.detail_max_scroll == 0;
    }

    pub fn scroll_detail_end(&mut self) {
        self.scroll = self.detail_max_scroll;
        self.detail_follow_tail = true;
    }

    pub fn is_build_target(&self, work_unit_id: &str) -> bool {
        let build = &self.state.coordinator_build;
        build.target_work_unit_ids.contains(work_unit_id)
            || build.current_work_unit_id.as_deref() == Some(work_unit_id)
    }

    pub fn is_building(&self, work_unit_id: &str, stage: &str) -> bool {
        let build = &self.state.coordinator_build;
        build.active
            && build.completed < build.total
            && build.stage == stage
            && self.is_build_target(work_unit_id)
    }

    /// Explain the first pending stage that is actually able to make progress.
    ///
    /// Dependency state is derived from the pushed snapshot instead of being persisted as
    /// presentation text. That keeps this reason current as prerequisites complete without
    /// generating extra state revisions solely for the dashboard.
    pub fn pending_reason(&self, row: &RowModel<'_>) -> Option<String> {
        for (stage, own_prerequisite) in [("formalize", "discover"), ("review", "formalize")] {
            let Some(task) = row.tasks.get(stage) else {
                continue;
            };
            if task.status != "pending" || task.queued || !task.active_auxiliary_role.is_empty() {
                continue;
            }
            if row
                .tasks
                .get(own_prerequisite)
                .is_none_or(|prerequisite| prerequisite.status != "succeeded")
            {
                continue;
            }
            let Some(dependencies) = self
                .state
                .source_dependency_tree
                .dependencies
                .get(&row.unit.id)
            else {
                continue;
            };
            let mut waiting = dependencies
                .iter()
                .filter(|dependency| {
                    self.state
                        .tasks
                        .get(&format!("{dependency}:{stage}"))
                        .is_none_or(|required| required.status != "succeeded")
                })
                .collect::<Vec<_>>();
            waiting.sort_by_key(|dependency| {
                self.state
                    .work_units
                    .iter()
                    .position(|unit| unit.id == dependency.as_str())
                    .unwrap_or(usize::MAX)
            });
            if !waiting.is_empty() {
                return Some(format!(
                    "waiting: {}",
                    waiting
                        .into_iter()
                        .map(|dependency| self.work_unit_label(dependency))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            return Some("waiting: scheduler".into());
        }

        let pending = STAGES
            .iter()
            .filter_map(|stage| row.tasks.get(stage))
            .find(|task| {
                task.status == "pending"
                    && task.active_auxiliary_role.is_empty()
                    && (task.queued || !task.detail.is_empty())
            })?;
        if pending.queued && pending.detail.is_empty() {
            Some("waiting: agent capacity".into())
        } else {
            Some(compact_task_detail(&pending.detail))
        }
    }

    fn work_unit_label(&self, work_unit_id: &str) -> String {
        self.state
            .work_units
            .iter()
            .find(|unit| unit.id == work_unit_id)
            .map_or_else(
                || work_unit_id.to_owned(),
                |unit| format!("{}.{}", unit.document_id, unit.ordinal),
            )
    }
}

fn steward_case_status_rank(status: &str) -> u8 {
    match status {
        "implementing" => 0,
        "ready" | "needs_scope" => 1,
        "needs_human" => 2,
        "verified" => 3,
        "rejected" | "resolved" => 4,
        _ => 5,
    }
}

fn exact_document_id_matches(actual: &str, query: &str) -> bool {
    strip_books_prefix(actual).eq_ignore_ascii_case(strip_books_prefix(query))
}

fn strip_books_prefix(document_id: &str) -> &str {
    if document_id
        .get(.."books/".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("books/"))
    {
        &document_id["books/".len()..]
    } else {
        document_id
    }
}

pub fn compact_task_detail(detail: &str) -> String {
    const LEGACY_PREFIX: &str = "waiting for prerequisite reviews:";
    if detail
        .get(..LEGACY_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(LEGACY_PREFIX))
    {
        format!("waiting:{}", &detail[LEGACY_PREFIX.len()..])
    } else {
        detail.to_owned()
    }
}

fn apply_optional<T>(target: &mut T, value: Option<T>) {
    if let Some(value) = value {
        *target = value;
    }
}

#[derive(Clone, Debug)]
pub struct RowModel<'a> {
    pub unit: &'a WorkUnit,
    pub tasks: HashMap<&'a str, &'a Task>,
}

impl RowModel<'_> {
    pub fn activity<'a>(&self, state: &'a SwarmState) -> Option<&'a Activity> {
        self.tasks
            .values()
            .filter_map(|task| {
                let run_id = task.latest_run_id.as_ref()?;
                let activity = state.activities.get(run_id)?;
                Some((task.status == "running", &activity.updated_at, activity))
            })
            .max_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(right.1)))
            .map(|value| value.2)
    }
}

pub fn elapsed_seconds(timestamp: &str) -> i64 {
    DateTime::parse_from_rfc3339(timestamp)
        .map(|then| (Utc::now() - then.with_timezone(&Utc)).num_seconds().max(0))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> Value {
        serde_json::json!({
            "revision": 4,
            "work_units": [{
                "id": "book/chapter-01", "document_id": "book", "title": "Opening",
                "ordinal": 1, "source_start_line": 3
            }],
            "tasks": {
                "book/chapter-01:review": {
                    "work_unit_id": "book/chapter-01", "document_id": "book",
                    "ordinal": 1, "unit_title": "Opening", "stage": "review",
                    "status": "running", "latest_run_id": "run-1"
                }
            },
            "activities": {"run-1": {"run_id": "run-1", "current": "editing proof"}}
        })
    }

    #[test]
    fn applies_activity_only_delta_without_a_revision_change() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model
            .apply(WireEvent {
                protocol_version: PROTOCOL_VERSION,
                event: "snapshot".into(),
                status: "running".into(),
                result: None,
                snapshot: Some(snapshot()),
                delta: None,
                preparation: None,
                message: String::new(),
            })
            .unwrap();
        model
            .apply(WireEvent {
                protocol_version: PROTOCOL_VERSION,
                event: "delta".into(),
                status: "running".into(),
                result: None,
                snapshot: None,
                delta: Some(DashboardDelta {
                    revision: 4,
                    activities: HashMap::from([(
                        "run-1".into(),
                        Activity {
                            run_id: "run-1".into(),
                            current: "checking goals".into(),
                            ..Activity::default()
                        },
                    )]),
                    ..DashboardDelta::default()
                }),
                preparation: None,
                message: String::new(),
            })
            .unwrap();

        assert_eq!(model.selected_activity().unwrap().current, "checking goals");
        assert_eq!(model.state.revision, 4);
    }

    #[test]
    fn restores_agent_view_by_work_unit_after_snapshot() {
        let mut model = DashboardModel::loading_with_agent_view(
            "test".into(),
            String::new(),
            Some("book/chapter-01".into()),
            Some("plan"),
        );

        model
            .apply(WireEvent {
                protocol_version: PROTOCOL_VERSION,
                event: "snapshot".into(),
                status: "running".into(),
                result: None,
                snapshot: Some(snapshot()),
                delta: None,
                preparation: None,
                message: String::new(),
            })
            .unwrap();

        assert!(model.detail);
        assert_eq!(model.detail_tab, DetailTab::Plan);
        assert_eq!(model.selected_row().unwrap().unit.id, "book/chapter-01");
    }

    #[test]
    fn search_selects_the_first_unit_in_a_matching_book() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        for (id, document_id, ordinal) in [
            ("intro/unit-1", "intro", 1),
            ("more-algebra/unit-7", "more-algebra", 7),
            ("more-algebra/unit-8", "more-algebra", 8),
        ] {
            model.state.work_units.push(WorkUnit {
                id: id.into(),
                document_id: document_id.into(),
                ordinal,
                ..WorkUnit::default()
            });
            model.state.tasks.insert(
                format!("{id}:review"),
                Task {
                    work_unit_id: id.into(),
                    stage: "review".into(),
                    ..Task::default()
                },
            );
        }

        model.begin_search();
        model.push_search_character('A');
        assert_eq!(model.selected_row().unwrap().unit.id, "more-algebra/unit-7");
        for character in "LGEBRA".chars() {
            model.push_search_character(character);
        }
        assert_eq!(model.selected_row().unwrap().unit.id, "more-algebra/unit-7");
        model.accept_search();
        assert!(model.search_query.is_none());
    }

    #[test]
    fn search_selects_an_exact_book_and_unit_number() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        for ordinal in [7, 8] {
            let id = format!("more-algebra/unit-{ordinal}");
            model.state.work_units.push(WorkUnit {
                id: id.clone(),
                document_id: "books/more-algebra".into(),
                ordinal,
                ..WorkUnit::default()
            });
            model.state.tasks.insert(
                format!("{id}:review"),
                Task {
                    work_unit_id: id,
                    stage: "review".into(),
                    ..Task::default()
                },
            );
        }

        model.begin_search();
        for character in "more-algebra.8".chars() {
            model.push_search_character(character);
        }
        assert_eq!(model.selected_row().unwrap().unit.ordinal, 8);
        model.accept_search();

        model.begin_search();
        for character in "books/more-algebra.7".chars() {
            model.push_search_character(character);
        }
        assert_eq!(model.selected_row().unwrap().unit.ordinal, 7);
        model.accept_search();
    }

    #[test]
    fn unsuccessful_search_stays_at_the_last_match_for_correction() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model.begin_search();
        for character in "missing.3".chars() {
            model.push_search_character(character);
        }

        assert_eq!(model.search_query.as_deref(), Some("missing.3"));
        assert!(model.search_error.contains("missing.3"));
    }

    #[test]
    fn cancelling_search_restores_the_original_selection() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        for document_id in ["intro", "algebra"] {
            let id = format!("{document_id}/unit-1");
            model.state.work_units.push(WorkUnit {
                id: id.clone(),
                document_id: document_id.into(),
                ordinal: 1,
                ..WorkUnit::default()
            });
            model.state.tasks.insert(
                format!("{id}:review"),
                Task {
                    work_unit_id: id,
                    stage: "review".into(),
                    ..Task::default()
                },
            );
        }

        model.begin_search();
        for character in "algebra".chars() {
            model.push_search_character(character);
        }
        assert_eq!(model.selected, 1);
        model.cancel_search();
        assert_eq!(model.selected, 0);
    }

    #[test]
    fn stale_chapter_history_does_not_replace_a_newer_run_request() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model
            .apply(WireEvent {
                protocol_version: PROTOCOL_VERSION,
                event: "snapshot".into(),
                status: "running".into(),
                result: None,
                snapshot: Some(snapshot()),
                delta: None,
                preparation: None,
                message: String::new(),
            })
            .unwrap();
        model.enter_detail();
        assert!(model.begin_chapter_runs_load("book/chapter-01", None));
        assert!(model.begin_chapter_runs_load("book/chapter-01", Some("run-2")));

        assert!(!model.apply_loaded_chapter_runs(
            "book/chapter-01",
            None,
            ChapterRuns {
                selected_run_id: Some("run-1".into()),
                runs: vec![HistoricalRun {
                    id: "run-1".into(),
                    ..HistoricalRun::default()
                }],
                ..ChapterRuns::default()
            },
        ));
        assert!(model.detail_runs.is_empty());

        assert!(model.apply_loaded_chapter_runs(
            "book/chapter-01",
            Some("run-2"),
            ChapterRuns {
                selected_run_id: Some("run-2".into()),
                runs: vec![HistoricalRun {
                    id: "run-2".into(),
                    ..HistoricalRun::default()
                }],
                ..ChapterRuns::default()
            },
        ));
        assert_eq!(model.selected_run_id(), Some("run-2"));
    }

    #[test]
    fn steward_case_run_detail_loads_history_and_returns_to_case_view() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model.enter_steward_detail();
        model.enter_case_run_detail("case-1".into());
        assert!(model.begin_case_runs_load("case-1", None));
        assert!(model.apply_loaded_case_runs(
            "case-1",
            None,
            ChapterRuns {
                work_unit_id: "package-1".into(),
                selected_run_id: Some("steward-run".into()),
                runs: vec![HistoricalRun {
                    id: "steward-run".into(),
                    role: "upstream_steward".into(),
                    round: 3,
                    ..HistoricalRun::default()
                }],
                ..ChapterRuns::default()
            },
        ));
        assert_eq!(model.selected_run_id(), Some("steward-run"));

        model.leave_detail();

        assert!(!model.detail);
        assert!(model.steward_detail);
        assert!(model.detail_case_id.is_none());
    }

    #[test]
    fn chapter_run_tabs_select_history_and_switch_activity() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model.detail = true;
        model.apply_chapter_runs(ChapterRuns {
            runs: vec![
                HistoricalRun {
                    id: "formalize-3".into(),
                    stage: "formalize".into(),
                    round: 3,
                    ..HistoricalRun::default()
                },
                HistoricalRun {
                    id: "review-2".into(),
                    stage: "review".into(),
                    round: 2,
                    ..HistoricalRun::default()
                },
            ],
            selected_run_id: Some("review-2".into()),
            activity: Some(Activity {
                run_id: "review-2".into(),
                current: "reviewing".into(),
                ..Activity::default()
            }),
            ..ChapterRuns::default()
        });

        assert_eq!(model.selected_run, 1);
        assert_eq!(model.selected_activity().unwrap().current, "reviewing");
        model.cycle_run(true);
        assert_eq!(model.selected_run, 0);
    }

    #[test]
    fn prompt_tab_content_is_kept_per_run() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model.detail = true;
        model.detail_tab = DetailTab::Prompt;
        model.detail_runs = vec![HistoricalRun {
            id: "review-2".into(),
            ..HistoricalRun::default()
        }];

        assert_eq!(model.selected_prompt(), None);
        model
            .run_prompts
            .insert("review-2".into(), "Review carefully.".into());
        assert_eq!(model.selected_prompt(), Some("Review carefully."));
    }

    #[test]
    fn full_timeline_is_merged_with_new_live_events() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model.state.activities.insert(
            "run".into(),
            Activity {
                run_id: "run".into(),
                sequence: 3,
                recent: vec![ActivityEntry {
                    sequence: 3,
                    title: "recent".into(),
                    ..ActivityEntry::default()
                }],
                ..Activity::default()
            },
        );
        assert!(model.begin_timeline_load("run"));
        model.apply_full_timeline(
            "run".into(),
            Activity {
                run_id: "run".into(),
                sequence: 2,
                recent: (1..=2)
                    .map(|sequence| ActivityEntry {
                        sequence,
                        title: format!("event {sequence}"),
                        ..ActivityEntry::default()
                    })
                    .collect(),
                ..Activity::default()
            },
        );

        assert_eq!(
            model.state.activities["run"]
                .recent
                .iter()
                .map(|entry| entry.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!(model.full_timeline_runs.contains("run"));
        assert!(!model.loading_timeline_runs.contains("run"));
    }

    #[test]
    fn completed_coordinator_build_is_not_still_building_during_finalization() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model.state.coordinator_build = CoordinatorBuild {
            active: true,
            stage: "formalize".into(),
            completed: 3,
            total: 3,
            target_work_unit_ids: ["book/chapter-01".into()].into_iter().collect(),
            ..CoordinatorBuild::default()
        };

        assert!(!model.is_building("book/chapter-01", "formalize"));
        model.state.coordinator_build.completed = 2;
        assert!(model.is_building("book/chapter-01", "formalize"));
    }

    #[test]
    fn pending_reason_tracks_pushed_prerequisite_status_in_source_order() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        for (id, document_id, ordinal) in [
            ("introductions/unit-05", "introductions", 5),
            ("cohomology/unit-07", "cohomology", 7),
            ("results/unit-02", "results", 2),
        ] {
            model.state.work_units.push(WorkUnit {
                id: id.into(),
                document_id: document_id.into(),
                ordinal,
                ..WorkUnit::default()
            });
        }
        model.state.source_dependency_tree.dependencies.insert(
            "results/unit-02".into(),
            vec!["cohomology/unit-07".into(), "introductions/unit-05".into()],
        );
        for id in ["introductions/unit-05", "cohomology/unit-07"] {
            model.state.tasks.insert(
                format!("{id}:review"),
                Task {
                    work_unit_id: id.into(),
                    stage: "review".into(),
                    status: "pending".into(),
                    ..Task::default()
                },
            );
        }
        for (stage, status) in [
            ("discover", "succeeded"),
            ("formalize", "succeeded"),
            ("review", "pending"),
        ] {
            model.state.tasks.insert(
                format!("results/unit-02:{stage}"),
                Task {
                    work_unit_id: "results/unit-02".into(),
                    stage: stage.into(),
                    status: status.into(),
                    detail: "waiting: stale prerequisite".into(),
                    ..Task::default()
                },
            );
        }

        assert_eq!(
            model.pending_reason(&model.rows()[2]),
            Some("waiting: introductions.5, cohomology.7".into())
        );

        model
            .state
            .tasks
            .get_mut("introductions/unit-05:review")
            .unwrap()
            .status = "succeeded".into();
        assert_eq!(
            model.pending_reason(&model.rows()[2]),
            Some("waiting: cohomology.7".into())
        );

        model
            .state
            .tasks
            .get_mut("cohomology/unit-07:review")
            .unwrap()
            .status = "succeeded".into();
        assert_eq!(
            model.pending_reason(&model.rows()[2]),
            Some("waiting: scheduler".into())
        );
    }
}
