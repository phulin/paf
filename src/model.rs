use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

pub const STAGES: [&str; 4] = ["discover", "formalize", "review", "prove"];

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
    pub scheduling: Scheduling,
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
    pub scheduling: Option<Scheduling>,
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
    pub label: String,
    pub startup_warning: String,
    pub stopping: bool,
}

impl DashboardModel {
    pub fn loading(label: String, startup_warning: String) -> Self {
        Self {
            state: SwarmState::default(),
            daemon_status: "connecting".into(),
            result: None,
            selected: 0,
            detail: false,
            detail_tab: DetailTab::default(),
            scroll: 0,
            label,
            startup_warning,
            stopping: false,
        }
    }

    pub fn apply(&mut self, event: WireEvent) -> Result<()> {
        if event.protocol_version != 3 {
            bail!(
                "unsupported dashboard protocol {}, expected 3",
                event.protocol_version
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
            }
            "delta" => self.apply_delta(event.delta.context("delta event has no delta")?)?,
            "complete" => self.result = event.result,
            other => bail!("unknown dashboard event {other:?}"),
        }
        self.clamp_selection();
        Ok(())
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
        apply_optional(&mut self.state.scheduling, delta.globals.scheduling);
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
                protocol_version: 3,
                event: "snapshot".into(),
                status: "running".into(),
                result: None,
                snapshot: Some(snapshot()),
                delta: None,
            })
            .unwrap();
        model
            .apply(WireEvent {
                protocol_version: 3,
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
            })
            .unwrap();

        assert_eq!(model.selected_activity().unwrap().current, "checking goals");
        assert_eq!(model.state.revision, 4);
    }
}
