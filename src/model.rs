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
    pub target_work_unit_ids: Vec<String>,
    pub error_count: usize,
    pub warning_count: usize,
    pub current_work_unit_id: Option<String>,
    pub output_tail: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct Shepherd {
    pub enabled: bool,
    pub status: String,
    pub model: String,
    pub worker_model: String,
    pub interval_seconds: f64,
    pub failure_threshold: usize,
    pub current_sweep_id: String,
    pub current_run_id: String,
    pub last_started_at: Option<String>,
    pub last_finished_at: Option<String>,
    pub next_run_at: Option<String>,
    pub last_summary: String,
    pub last_error: String,
    pub pending_failures: usize,
    pub planned_units: usize,
    pub running_units: usize,
    pub succeeded_units: usize,
    pub failed_units: usize,
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
    pub phase: String,
    pub detail: String,
    pub queued: bool,
    pub repairing: bool,
    pub repair_work_unit_id: String,
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
    pub shepherd: Shepherd,
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
    pub shepherd: Option<Shepherd>,
    pub scheduling: Option<Scheduling>,
    pub source_dependency_tree: Option<SourceDependencyTree>,
    pub isolation: Option<Isolation>,
    pub formalize_graph: Option<Value>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DetailTab {
    #[default]
    Timeline,
    Summary,
    Plan,
    Files,
}

impl DetailTab {
    pub const ALL: [Self; 4] = [Self::Timeline, Self::Summary, Self::Plan, Self::Files];

    pub fn label(self) -> &'static str {
        match self {
            Self::Timeline => "Timeline",
            Self::Summary => "Update",
            Self::Plan => "Plan",
            Self::Files => "Files",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Timeline => "timeline",
            Self::Summary => "summary",
            Self::Plan => "plan",
            Self::Files => "files",
        }
    }

    fn from_name(name: &str) -> Self {
        match name {
            "summary" => Self::Summary,
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
    pub detail_tab: DetailTab,
    pub scroll: u16,
    pub detail_max_scroll: u16,
    pub detail_follow_tail: bool,
    pub detail_runs: Vec<HistoricalRun>,
    pub selected_run: usize,
    pub label: String,
    pub startup_warning: String,
    pub stopping: bool,
    pub preparation: Option<Preparation>,
    pub reload_requested: bool,
    pub detach_requested: bool,
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
            detail_tab: detail_tab.map(DetailTab::from_name).unwrap_or_default(),
            scroll: 0,
            detail_max_scroll: 0,
            detail_follow_tail: true,
            detail_runs: Vec::new(),
            selected_run: 0,
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
        self.state.activities.extend(delta.activities);
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
        apply_optional(&mut self.state.shepherd, delta.globals.shepherd);
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
        if self.detail {
            if let Some(run) = self.detail_runs.get(self.selected_run) {
                return self.state.activities.get(&run.id);
            }
        }
        let row = self.selected_row()?;
        row.tasks
            .values()
            .filter_map(|task| {
                let run_id = task.latest_run_id.as_ref()?;
                let activity = self.state.activities.get(run_id)?;
                Some((task.status == "running", &activity.updated_at, activity))
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
        self.detail_runs.clear();
        self.selected_run = 0;
        self.scroll = 0;
        self.detail_max_scroll = 0;
        self.detail_follow_tail = true;
    }

    pub fn leave_detail(&mut self) {
        self.detail = false;
        self.detail_runs.clear();
        self.selected_run = 0;
        self.scroll = 0;
        self.detail_max_scroll = 0;
        self.detail_follow_tail = true;
    }

    pub fn apply_chapter_runs(&mut self, details: ChapterRuns) {
        let selected = details
            .selected_run_id
            .as_deref()
            .and_then(|id| details.runs.iter().position(|run| run.id == id))
            .unwrap_or_else(|| details.runs.len().saturating_sub(1));
        if let Some(activity) = details.activity {
            self.state
                .activities
                .insert(activity.run_id.clone(), activity);
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

    pub fn build_targets(&self) -> HashSet<&str> {
        let mut targets: HashSet<&str> = self
            .state
            .coordinator_build
            .target_work_unit_ids
            .iter()
            .map(String::as_str)
            .collect();
        if let Some(current) = self.state.coordinator_build.current_work_unit_id.as_deref() {
            targets.insert(current);
        }
        targets
    }

    pub fn is_building(&self, work_unit_id: &str, stage: &str) -> bool {
        let build = &self.state.coordinator_build;
        build.active
            && build.completed < build.total
            && build.stage == stage
            && self.build_targets().contains(work_unit_id)
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
            if task.status != "pending" || task.queued {
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
            .find(|task| task.status == "pending" && (task.queued || !task.detail.is_empty()))?;
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
    fn completed_coordinator_build_is_not_still_building_during_finalization() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model.state.coordinator_build = CoordinatorBuild {
            active: true,
            stage: "formalize".into(),
            completed: 3,
            total: 3,
            target_work_unit_ids: vec!["book/chapter-01".into()],
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
