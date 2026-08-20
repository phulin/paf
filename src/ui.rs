use std::collections::HashMap;

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, Gauge, Paragraph, Row, Scrollbar, ScrollbarOrientation,
    ScrollbarState, Table, TableState, Tabs, Wrap,
};

use crate::model::{
    Activity, DashboardModel, DetailTab, RowModel, STAGES, Task, compact_task_detail,
    elapsed_seconds,
};
use crate::viewport::TimelineRenderCache;

const BLUE: Color = Color::Rgb(122, 162, 247);
const CYAN: Color = Color::Rgb(125, 207, 255);
const GREEN: Color = Color::Rgb(158, 206, 106);
const YELLOW: Color = Color::Rgb(224, 175, 104);
const RED: Color = Color::Rgb(247, 118, 142);
const PURPLE: Color = Color::Rgb(187, 154, 247);
const MUTED: Color = Color::Rgb(169, 177, 214);
const SURFACE: Color = Color::Rgb(36, 40, 59);

pub fn draw(frame: &mut Frame<'_>, model: &mut DashboardModel) {
    if model.detail {
        draw_detail(frame, model);
    } else if model.shepherd_detail {
        draw_shepherd_detail(frame, model);
    } else {
        draw_dashboard(frame, model);
    }
    if model.preparation.is_some() {
        draw_preparation_modal(frame, model);
    }
    if model.search_query.is_some() {
        draw_search_modal(frame, model);
    }
}

fn draw_search_modal(frame: &mut Frame<'_>, model: &DashboardModel) {
    let Some(query) = &model.search_query else {
        return;
    };
    let area = centered(frame.area(), 72, 7);
    frame.render_widget(Clear, area);
    let block = Block::default()
        .title(" Search books ")
        .title_alignment(Alignment::Center)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(CYAN))
        .style(Style::default().bg(SURFACE));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(1),
        ])
        .split(inner);
    frame.render_widget(
        Paragraph::new(format!("> {query}")).style(Style::default().fg(Color::White).bg(SURFACE)),
        layout[0],
    );
    frame.render_widget(
        Paragraph::new("Enter keeps selection · Esc restores · exact: [books/]book-name.8")
            .style(Style::default().fg(MUTED).bg(SURFACE)),
        layout[1],
    );
    if !model.search_error.is_empty() {
        frame.render_widget(
            Paragraph::new(model.search_error.as_str())
                .style(Style::default().fg(YELLOW).bg(SURFACE)),
            layout[2],
        );
    }
    let cursor_offset = u16::try_from(query.chars().count())
        .unwrap_or(u16::MAX)
        .min(layout[0].width.saturating_sub(3));
    frame.set_cursor_position((layout[0].x + 2 + cursor_offset, layout[0].y));
}

fn draw_preparation_modal(frame: &mut Frame<'_>, model: &DashboardModel) {
    let Some(preparation) = &model.preparation else {
        return;
    };
    let area = centered(frame.area(), 74, 9);
    frame.render_widget(Clear, area);
    let block = Block::default()
        .title(" Preparing PAF ")
        .title_alignment(Alignment::Center)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(CYAN))
        .style(Style::default().bg(SURFACE));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Min(1),
        ])
        .split(inner);
    frame.render_widget(
        Paragraph::new(preparation.phase.clone())
            .style(Style::default().fg(MUTED).bg(SURFACE))
            .alignment(Alignment::Center)
            .wrap(Wrap { trim: true }),
        layout[0],
    );
    let ratio = if preparation.total == 0 {
        0.0
    } else {
        preparation.completed as f64 / preparation.total as f64
    };
    frame.render_widget(
        Gauge::default()
            .block(Block::default().borders(Borders::ALL))
            .gauge_style(Style::default().fg(BLUE).bg(SURFACE))
            .ratio(ratio.clamp(0.0, 1.0))
            .label(format!("{} / {}", preparation.completed, preparation.total)),
        layout[1],
    );
    frame.render_widget(
        Paragraph::new(if model.stopping {
            "Stopping after preparation finishes…"
        } else {
            "d detaches  q stops before workers are launched"
        })
        .style(Style::default().fg(MUTED).bg(SURFACE))
        .alignment(Alignment::Center),
        layout[2],
    );
}

fn centered(area: Rect, preferred_width: u16, preferred_height: u16) -> Rect {
    let width = preferred_width.min(area.width);
    let height = preferred_height.min(area.height);
    Rect::new(
        area.x + area.width.saturating_sub(width) / 2,
        area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    )
}

fn draw_dashboard(frame: &mut Frame<'_>, model: &DashboardModel) {
    let warning_height = u16::from(!model.startup_warning.is_empty()) * 3;
    let footer_height = if model.state.coordinator_build.active {
        6
    } else {
        3
    };
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Length(warning_height),
            Constraint::Length(5),
            Constraint::Min(5),
            Constraint::Length(footer_height),
            Constraint::Length(1),
        ])
        .split(frame.area());

    frame.render_widget(summary(model), layout[0]);
    if warning_height > 0 {
        frame.render_widget(
            Paragraph::new(format!("⚠ {}", model.startup_warning))
                .style(Style::default().fg(YELLOW))
                .block(Block::default().borders(Borders::BOTTOM)),
            layout[1],
        );
    }
    draw_stage_cards(frame, model, layout[2]);
    draw_task_table(frame, model, layout[3]);
    draw_status(frame, model, layout[4]);
    frame.render_widget(
        Paragraph::new(
            "↑↓ select  Enter/i inspect  / search  s shepherd  p pause/resume  r reload TUI  d detach  q stop",
        )
        .style(Style::default().fg(MUTED))
        .alignment(Alignment::Center),
        layout[5],
    );
}

fn draw_shepherd_detail(frame: &mut Frame<'_>, model: &mut DashboardModel) {
    let shepherd = &model.state.shepherd;
    let agents = model.selected_shepherd_agents();
    let selected_run = model.selected_shepherd_run();
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Length(3),
            Constraint::Length((agents.len().clamp(1, 8) + 3) as u16),
            Constraint::Min(6),
            Constraint::Length(1),
        ])
        .split(frame.area());
    let summary = selected_run.map_or_else(
        || {
            if shepherd.last_error.is_empty() {
                shepherd.last_summary.as_str()
            } else {
                shepherd.last_error.as_str()
            }
        },
        |run| {
            if run.error.is_empty() {
                run.summary.as_str()
            } else {
                run.error.as_str()
            }
        },
    );
    frame.render_widget(
        Paragraph::new(vec![
            Line::styled(
                format!("Shepherd · {}", shepherd.status),
                Style::default().fg(YELLOW).add_modifier(Modifier::BOLD),
            ),
            Line::from(format!(
                "failures {} · repairs {}/{} · succeeded {} · failed {} · cost ${:.2}    {}",
                shepherd.pending_failures,
                shepherd.running_units,
                shepherd.planned_units,
                shepherd.succeeded_units,
                shepherd.failed_units,
                shepherd.cost.estimated_usd,
                summary,
            )),
        ])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Shepherd trace "),
        ),
        layout[0],
    );

    let tab_titles = if shepherd.runs.is_empty() {
        vec![Line::from("Latest")]
    } else {
        shepherd
            .runs
            .iter()
            .enumerate()
            .map(|(index, run)| Line::from(format!("Run {} · {}", index + 1, title(&run.status))))
            .collect()
    };
    frame.render_widget(
        Tabs::new(tab_titles)
            .select(model.shepherd_run_selected)
            .highlight_style(Style::default().fg(YELLOW).add_modifier(Modifier::BOLD))
            .divider(" │ ")
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Shepherd runs "),
            ),
        layout[1],
    );

    let rows = if agents.is_empty() {
        vec![Row::new(["—", "No Shepherd sweep has run yet", "", "", ""])]
    } else {
        agents
            .iter()
            .map(|agent| {
                let book = if !agent.document_title.is_empty() {
                    agent.document_title.clone()
                } else if !agent.document_id.is_empty() {
                    agent.document_id.clone()
                } else {
                    "Sweep".into()
                };
                let chapter = if !agent.location.is_empty() {
                    agent.location.clone()
                } else if let Some(ordinal) = agent.ordinal {
                    format!("Chapter {ordinal} · {}", agent.unit_title)
                } else if !agent.unit_title.is_empty() {
                    agent.unit_title.clone()
                } else {
                    agent.work_unit_id.clone()
                };
                Row::new(vec![
                    if agent.role == "shepherd" {
                        Cell::from("planner")
                    } else {
                        Cell::from("worker")
                    },
                    Cell::from(book),
                    Cell::from(chapter),
                    Cell::from(agent.objective.clone()),
                    Cell::from(agent.status.clone()),
                ])
            })
            .collect()
    };
    let table = Table::new(
        rows,
        [
            Constraint::Length(9),
            Constraint::Length(20),
            Constraint::Min(24),
            Constraint::Min(28),
            Constraint::Length(13),
        ],
    )
    .header(
        Row::new(["Role", "Book", "Chapter", "Objective", "Status"])
            .style(Style::default().fg(CYAN).add_modifier(Modifier::BOLD)),
    )
    .row_highlight_style(Style::default().bg(SURFACE).add_modifier(Modifier::BOLD))
    .highlight_symbol("▸ ")
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Relevant agents "),
    );
    let mut table_state = TableState::default()
        .with_selected((!agents.is_empty()).then_some(model.shepherd_selected));
    frame.render_stateful_widget(table, layout[2], &mut table_state);

    let text = detail_text(
        DetailTab::Timeline,
        model.selected_shepherd_activity(),
        None,
        None,
    );
    draw_detail_content(frame, model, text, layout[3]);
    frame.render_widget(
        Paragraph::new("←→ switch run  ↑↓ select agent  Enter open full agent view  s/Esc/q back")
            .style(Style::default().fg(MUTED))
            .alignment(Alignment::Center),
        layout[4],
    );
}

fn summary(model: &DashboardModel) -> Paragraph<'static> {
    let state = &model.state;
    let invocation = format_usage(&state.invocation_usage);
    let lifetime = format_count(state.usage.total_tokens);
    let stage_agents = STAGES
        .iter()
        .filter_map(|stage| {
            let count = state
                .agents
                .by_stage
                .get(*stage)
                .copied()
                .unwrap_or_default();
            (count > 0).then(|| format!("{stage} {count}"))
        })
        .collect::<Vec<_>>()
        .join(" · ");
    let agent_detail = if stage_agents.is_empty() {
        "none".to_owned()
    } else {
        stage_agents
    };
    let build = if state.coordinator_build.active {
        let phase = if state.coordinator_build.completed < state.coordinator_build.total {
            "build"
        } else {
            "finalize"
        };
        format!(
            "coordinator {phase} {}/{} · err {} · warn {}",
            state.coordinator_build.completed,
            state.coordinator_build.total,
            state.coordinator_build.error_count,
            state.coordinator_build.warning_count
        )
    } else {
        "build idle".into()
    };
    let isolation = if state.isolation.backend.is_empty() {
        "—"
    } else {
        &state.isolation.backend
    };
    Paragraph::new(vec![
        Line::from(vec![
            Span::styled(
                format!("PAF · {}", model.label),
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!("    {invocation}")),
            Span::styled(
                format!(
                    "    API-equivalent ${:.2}",
                    state.invocation_cost.estimated_usd
                ),
                Style::default().fg(PURPLE),
            ),
            Span::raw(format!(
                "    lifetime {lifetime} · ${:.2}",
                state.cost.estimated_usd
            )),
        ]),
        Line::from(format!(
            "Agents {}/{} · {} · queued {}    {}    Shepherd {} · ${:.2} · failures {} · repair {}/{}",
            state.agents.active,
            state.agents.maximum,
            agent_detail,
            state.agents.queued,
            build,
            if state.shepherd.enabled {
                state.shepherd.status.as_str()
            } else {
                "off"
            },
            state.shepherd.cost.estimated_usd,
            state.shepherd.pending_failures,
            state.shepherd.running_units,
            state.shepherd.planned_units,
        )),
        Line::from(format!(
            "revision {} · isolation {} · Lean MCP on · stream {}",
            state.revision, isolation, model.daemon_status
        )),
    ])
    .block(Block::default().borders(Borders::BOTTOM))
}

fn draw_stage_cards(frame: &mut Frame<'_>, model: &DashboardModel, area: Rect) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(STAGES.map(|_| Constraint::Ratio(1, 4)))
        .split(area);
    let mut statistics = [StageStatistics::default(); 4];
    for task in model.state.tasks.values() {
        let Some(index) = STAGES.iter().position(|stage| *stage == task.stage) else {
            continue;
        };
        let current = &mut statistics[index];
        match task_display_status(task) {
            "succeeded" => current.succeeded += 1,
            "failed" => current.failed += 1,
            "blocked" => current.blocked += 1,
            "interrupted" => current.interrupted += 1,
            "pending" if task.queued => current.queued += 1,
            "pending" => current.pending += 1,
            _ => {}
        }
        if task.phase == "postprocess"
            && !model.is_building(task.work_unit_id.as_str(), task.stage.as_str())
        {
            current.postprocess += 1;
        }
        if task.repairing {
            current.repairing += 1;
        }
        if model.is_building(task.work_unit_id.as_str(), task.stage.as_str()) {
            current.building += 1;
        }
    }
    for (index, stage) in STAGES.iter().enumerate() {
        let statistics = statistics[index];
        let agents = model
            .state
            .agents
            .by_stage
            .get(*stage)
            .copied()
            .unwrap_or_default();
        let content = vec![
            Line::from(format!(
                "agent {agents} · repair {} · post {} · build {}",
                statistics.repairing, statistics.postprocess, statistics.building
            )),
            Line::from(vec![
                Span::styled(
                    format!("✓ {}", statistics.succeeded),
                    Style::default().fg(GREEN),
                ),
                Span::styled(
                    format!("  ✗ {}", statistics.failed),
                    Style::default().fg(RED),
                ),
                Span::raw(format!("  · {}", statistics.pending)),
                Span::styled(
                    format!("  ! {}", statistics.blocked),
                    Style::default().fg(YELLOW),
                ),
            ]),
            Line::from(format!(
                "queued {} · Ⅱ {}",
                statistics.queued, statistics.interrupted
            )),
        ];
        frame.render_widget(
            Paragraph::new(content).block(
                Block::default()
                    .title(format!(" {} ", title(stage)))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(BLUE)),
            ),
            columns[index],
        );
    }
}

fn draw_task_table(frame: &mut Frame<'_>, model: &DashboardModel, area: Rect) {
    let rows = model.rows();
    let viewport = usize::from(area.height.saturating_sub(3).max(1));
    let start = model
        .selected
        .saturating_sub(viewport / 2)
        .min(rows.len().saturating_sub(viewport));
    let end = (start + viewport).min(rows.len());
    let critical: std::collections::HashSet<&str> = model
        .state
        .scheduling
        .statements
        .critical_path
        .iter()
        .map(String::as_str)
        .collect();
    let rendered = rows[start..end].iter().map(|row| {
        let activity = row.activity(&model.state);
        let book = if critical.contains(row.unit.document_id.as_str()) {
            format!("★ {}", row.unit.document_id)
        } else {
            row.unit.document_id.clone()
        };
        let ranks = format!(
            "{:.0}/{:.0}",
            model
                .state
                .scheduling
                .statements
                .rank
                .get(&row.unit.document_id)
                .copied()
                .unwrap_or_default(),
            model
                .state
                .scheduling
                .proofs
                .rank
                .get(&row.unit.document_id)
                .copied()
                .unwrap_or_default()
        );
        let mut cells = vec![
            Cell::from(book),
            Cell::from(ranks),
            Cell::from(format!("{:02} {}", row.unit.ordinal, row.unit.title)),
        ];
        for stage in STAGES {
            let building = model.is_building(row.unit.id.as_str(), stage);
            let cell = row.tasks.get(stage).map_or_else(
                || Cell::from("· pending"),
                |task| Cell::from(task_mark(task, building)).style(task_mark_style(task, building)),
            );
            cells.push(cell);
        }
        let fresh = row
            .tasks
            .values()
            .any(|task| task.head_build_status == "clean");
        let sorry_count = row.tasks.values().find_map(|task| task.sorry_count);
        let formalized = row.tasks.get("formalize").is_some_and(|task| {
            task_mark_style(task, model.is_building(row.unit.id.as_str(), "formalize")).fg
                == Some(GREEN)
        });
        cells.push(Cell::from(build_mark(fresh, sorry_count, formalized)));
        cells.push(Cell::from(current_activity(model, row, activity)));
        cells.push(Cell::from(row_spend(row)));
        Row::new(cells).height(1)
    });

    let widths = [
        Constraint::Length(18),
        Constraint::Length(8),
        Constraint::Min(22),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(14),
        Constraint::Length(20),
        Constraint::Min(24),
        Constraint::Length(14),
    ];
    let table = Table::new(rendered, widths)
        .header(
            Row::new([
                "Document",
                "S/P rank",
                "Work unit",
                "Discover",
                "Formalize",
                "Review",
                "Prove",
                "Build",
                "Current activity",
                "Tokens · $",
            ])
            .style(Style::default().fg(CYAN).add_modifier(Modifier::BOLD))
            .bottom_margin(1),
        )
        .row_highlight_style(Style::default().bg(SURFACE).add_modifier(Modifier::BOLD))
        .highlight_symbol("▸ ")
        .block(Block::default().borders(Borders::TOP).title(format!(
            " Work units · {}-{} of {} ",
            if rows.is_empty() { 0 } else { start + 1 },
            end,
            rows.len()
        )));
    let mut state = TableState::default()
        .with_selected((!rows.is_empty()).then_some(model.selected.saturating_sub(start)));
    frame.render_stateful_widget(table, area, &mut state);
}

#[derive(Clone, Copy, Default)]
struct StageStatistics {
    succeeded: usize,
    failed: usize,
    pending: usize,
    queued: usize,
    blocked: usize,
    interrupted: usize,
    postprocess: usize,
    building: usize,
    repairing: usize,
}

fn draw_status(frame: &mut Frame<'_>, model: &DashboardModel, area: Rect) {
    let build = &model.state.coordinator_build;
    if build.active {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(1)])
            .split(area);
        let ratio = if build.total == 0 {
            0.0
        } else {
            build.completed as f64 / build.total as f64
        };
        let phase = if build.completed < build.total {
            "BUILD"
        } else {
            "FINALIZE"
        };
        let label = format!(
            "COORDINATOR {phase} {}/{} · iter {}/{} · err {} · warn {}{}",
            build.completed,
            build.total,
            build.iteration,
            build.maximum_iterations,
            build.error_count,
            build.warning_count,
            build
                .current_work_unit_id
                .as_ref()
                .map(|id| format!(" · {id}"))
                .unwrap_or_default()
        );
        frame.render_widget(
            Gauge::default()
                .block(Block::default().borders(Borders::ALL))
                .gauge_style(Style::default().fg(CYAN).bg(SURFACE))
                .ratio(ratio.clamp(0.0, 1.0))
                .label(label),
            chunks[0],
        );
        let tail = build
            .output_tail
            .iter()
            .rev()
            .take(chunks[1].height as usize)
            .rev()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        frame.render_widget(
            Paragraph::new(tail).style(Style::default().fg(MUTED)),
            chunks[1],
        );
    } else {
        let pending_reason = model
            .selected_row()
            .and_then(|row| model.pending_reason(&row));
        let message = if model.stopping {
            "Stopping workers and integrating workspace changes…".to_owned()
        } else if model.result == Some(true) {
            "Pipeline completed successfully".to_owned()
        } else if model.result == Some(false) {
            "Pipeline finished with failures".to_owned()
        } else if let Some(reason) = &pending_reason {
            format!("Pending · {reason}")
        } else {
            format!("{} · {}", title(&model.daemon_status), model.label)
        };
        frame.render_widget(
            Paragraph::new(message)
                .block(Block::default().borders(Borders::TOP))
                .style(Style::default().fg(if model.result == Some(false) {
                    RED
                } else if !model.stopping && model.result.is_none() && pending_reason.is_some() {
                    YELLOW
                } else {
                    GREEN
                })),
            area,
        );
    }
}

fn draw_detail(frame: &mut Frame<'_>, model: &mut DashboardModel) {
    let Some(row) = model.selected_row() else {
        frame.render_widget(Paragraph::new("No work unit selected"), frame.area());
        return;
    };
    let activity = model.selected_activity();
    let update_height = if activity.is_some_and(|value| !value.latest_summary.is_empty()) {
        (frame.area().height / 5).clamp(4, 10)
    } else {
        3
    };
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(5),
            Constraint::Length(update_height),
            Constraint::Length(3),
            Constraint::Min(4),
            Constraint::Length(1),
        ])
        .split(frame.area());
    let active_task = latest_task(&row.tasks);
    frame.render_widget(
        Paragraph::new(format!(
            "{} · {:02} {} · {}",
            row.unit.document_id,
            row.unit.ordinal,
            row.unit.title,
            active_task
                .map(|task| format!("{} {}", title(&task.stage), task_status_label(task)))
                .unwrap_or_else(|| "no run".into())
        ))
        .style(Style::default().fg(CYAN).add_modifier(Modifier::BOLD))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Agent detail "),
        ),
        layout[0],
    );
    let (run_titles, selected_run) = if model.detail_runs.is_empty() {
        (vec![Line::from("No runs")], 0)
    } else {
        let labels = model
            .detail_runs
            .iter()
            .map(|run| match run.role.as_str() {
                "shepherd" => "Shepherd planner".into(),
                "repair_worker" => {
                    format!("Repair {} round {}", title(&run.stage), run.round)
                }
                _ => format!("{} round {}", title(&run.stage), run.round),
            })
            .collect::<Vec<_>>();
        visible_run_tabs(&labels, model.selected_run, layout[1].width as usize)
    };
    frame.render_widget(
        Tabs::new(run_titles)
            .select(selected_run)
            .highlight_style(Style::default().fg(YELLOW).add_modifier(Modifier::BOLD))
            .divider(" │ ")
            .block(Block::default().borders(Borders::BOTTOM).title(" Runs ")),
        layout[1],
    );
    let pending_reason = model.pending_reason(&row);
    let has_running_task = row
        .tasks
        .values()
        .any(|task| task_is_running(task) || task.repairing);
    let metrics = activity.filter(|_| has_running_task).map_or_else(
        || {
            pending_reason
                .as_ref()
                .map_or_else(|| "Awaiting compact agent activity".into(), |reason| {
                    format!("CURRENT\n{reason}")
                })
        },
        |activity| {
            format!(
                "CURRENT\n{}\n\nWORK\n{} shell · {} MCP · {} edits · {} failures · plan {}/{} · last event {} ago",
                activity.current,
                activity.commands,
                activity.mcp_calls,
                activity.file_changes,
                activity.failures,
                activity.todo_completed,
                activity.todo_total,
                format_duration(elapsed_seconds(&activity.updated_at))
            )
        },
    );
    frame.render_widget(
        Paragraph::new(metrics)
            .wrap(Wrap { trim: false })
            .block(Block::default().borders(Borders::ALL)),
        layout[2],
    );
    draw_latest_update(frame, activity, layout[3]);
    let tab_titles = DetailTab::ALL.map(|tab| Line::from(tab.label()));
    let selected_tab = DetailTab::ALL
        .iter()
        .position(|tab| *tab == model.detail_tab)
        .unwrap_or_default();
    frame.render_widget(
        Tabs::new(tab_titles)
            .select(selected_tab)
            .highlight_style(Style::default().fg(CYAN).add_modifier(Modifier::BOLD))
            .divider(" │ ")
            .block(Block::default().borders(Borders::BOTTOM)),
        layout[4],
    );
    if model.detail_tab == DetailTab::Timeline {
        draw_timeline_content(frame, model, layout[5]);
    } else {
        let text = detail_text(
            model.detail_tab,
            activity,
            model.selected_prompt(),
            model.selected_timeline_status(),
        );
        draw_detail_content(frame, model, text, layout[5]);
    }
    frame.render_widget(
        Paragraph::new(
            "←→ runs  Tab/Shift-Tab views  ↑↓ scroll  r reload TUI  d detach  Esc/q back",
        )
        .style(Style::default().fg(MUTED))
        .alignment(Alignment::Center),
        layout[6],
    );
}

fn draw_latest_update(frame: &mut Frame<'_>, activity: Option<&Activity>, area: Rect) {
    let lines = activity
        .filter(|value| !value.latest_summary.is_empty())
        .map_or_else(
            || {
                vec![Line::styled(
                    "No agent update yet.",
                    Style::default().fg(MUTED),
                )]
            },
            |value| format_agent_update(&value.latest_summary),
        );
    frame.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: false }).block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Latest agent update "),
        ),
        area,
    );
}

fn visible_run_tabs(
    labels: &[String],
    selected: usize,
    available_width: usize,
) -> (Vec<Line<'static>>, usize) {
    const DIVIDER_WIDTH: usize = 3;
    const TAB_PADDING_WIDTH: usize = 2;

    if labels.is_empty() {
        return (Vec::new(), 0);
    }
    let selected = selected.min(labels.len() - 1);
    let width = |start: usize, end: usize| {
        let left_hidden = usize::from(start > 0);
        let right_hidden = usize::from(end < labels.len());
        let count = end - start + left_hidden + right_hidden;
        labels[start..end]
            .iter()
            .map(|label| label.chars().count())
            .sum::<usize>()
            + left_hidden
            + right_hidden
            + TAB_PADDING_WIDTH * count
            + DIVIDER_WIDTH * count.saturating_sub(1)
    };

    let mut start = selected;
    let mut end = selected + 1;
    loop {
        let mut changed = false;
        if start > 0 && width(start - 1, end) <= available_width {
            start -= 1;
            changed = true;
        }
        if end < labels.len() && width(start, end + 1) <= available_width {
            end += 1;
            changed = true;
        }
        if !changed {
            break;
        }
    }

    let mut left_hidden = start > 0;
    let mut right_hidden = end < labels.len();
    let rendered_width = |show_left: bool, show_right: bool| {
        let count = end - start + usize::from(show_left) + usize::from(show_right);
        labels[start..end]
            .iter()
            .map(|label| label.chars().count())
            .sum::<usize>()
            + usize::from(show_left)
            + usize::from(show_right)
            + TAB_PADDING_WIDTH * count
            + DIVIDER_WIDTH * count.saturating_sub(1)
    };
    if rendered_width(left_hidden, right_hidden) > available_width {
        left_hidden = false;
    }
    if rendered_width(left_hidden, right_hidden) > available_width {
        right_hidden = false;
    }
    let mut titles = Vec::with_capacity(end - start + 2);
    if left_hidden {
        titles.push(Line::from("‹"));
    }
    titles.extend(labels[start..end].iter().cloned().map(Line::from));
    if right_hidden {
        titles.push(Line::from("›"));
    }
    (titles, selected - start + usize::from(left_hidden))
}

fn draw_detail_content(
    frame: &mut Frame<'_>,
    model: &mut DashboardModel,
    text: Text<'static>,
    area: Rect,
) {
    let block = Block::default().borders(Borders::ALL);
    let inner = block.inner(area);
    let paragraph = Paragraph::new(text).wrap(Wrap { trim: false });
    let line_count = paragraph.line_count(inner.width);
    let maximum = line_count
        .saturating_sub(inner.height as usize)
        .min(u16::MAX as usize) as u16;
    model.sync_detail_viewport(maximum);
    frame.render_widget(paragraph.scroll((model.scroll, 0)).block(block), area);
    if maximum > 0 {
        // Ratatui models `content_length` as the number of valid scrollbar
        // positions once an explicit viewport length is supplied. The paragraph's
        // valid offsets are 0..=maximum, not 0..line_count.
        let mut scrollbar = ScrollbarState::new(maximum as usize + 1)
            .position(model.scroll as usize)
            .viewport_content_length(inner.height as usize);
        frame.render_stateful_widget(
            Scrollbar::new(ScrollbarOrientation::VerticalRight),
            area,
            &mut scrollbar,
        );
    }
}

fn draw_timeline_content(frame: &mut Frame<'_>, model: &mut DashboardModel, area: Rect) {
    let block = Block::default().borders(Borders::ALL);
    let inner = block.inner(area);
    let activity = model.selected_activity();
    let run_id = activity.map_or_else(String::new, |value| value.run_id.clone());
    let recent_len = activity.map_or(0, |value| value.recent.len());
    let last_entry_sequence = activity
        .and_then(|value| value.recent.last())
        .map(|entry| entry.sequence);
    let status = model.selected_timeline_status().map(str::to_owned);
    let cache_base_matches = {
        let cache = &model.timeline_render_cache;
        cache.run_id == run_id && cache.status == status && cache.width == inner.width
    };
    let is_append = cache_base_matches
        && recent_len >= model.timeline_render_cache.recent_len
        && (model.timeline_render_cache.recent_len == 0
            || activity
                .and_then(|value| value.recent.get(model.timeline_render_cache.recent_len - 1))
                .map(|entry| entry.sequence)
                == model.timeline_render_cache.last_entry_sequence);
    if is_append && recent_len > model.timeline_render_cache.recent_len {
        let append_from = model.timeline_render_cache.recent_len;
        let lines = timeline_entry_lines(
            &activity.expect("a non-empty timeline has activity").recent[append_from..],
        );
        append_wrapped_lines(&mut model.timeline_render_cache, lines, inner.width);
        model.timeline_render_cache.recent_len = recent_len;
        model.timeline_render_cache.last_entry_sequence = last_entry_sequence;
    } else if !is_append {
        model.timeline_render_cache =
            build_timeline_render_cache(activity, status.as_deref(), inner.width);
    }

    let maximum = model
        .timeline_render_cache
        .rendered_lines
        .saturating_sub(inner.height as usize)
        .min(u16::MAX as usize) as u16;
    model.sync_detail_viewport(maximum);

    let cache = &model.timeline_render_cache;
    let scroll = model.scroll as usize;
    let start = cache
        .offsets
        .partition_point(|offset| *offset <= scroll)
        .saturating_sub(1)
        .min(cache.lines.len().saturating_sub(1));
    let target = scroll.saturating_add(inner.height as usize);
    let end = cache
        .offsets
        .partition_point(|offset| *offset < target)
        .max(start + usize::from(!cache.lines.is_empty()))
        .min(cache.lines.len());
    let local_scroll = scroll.saturating_sub(cache.offsets.get(start).copied().unwrap_or(0));
    let visible = cache.lines.get(start..end).unwrap_or_default().to_vec();

    frame.render_widget(block, area);
    frame.render_widget(
        Paragraph::new(visible)
            .wrap(Wrap { trim: false })
            .scroll((local_scroll.min(u16::MAX as usize) as u16, 0)),
        inner,
    );
    draw_detail_scrollbar(frame, model.scroll, maximum, inner.height, area);
}

pub(crate) fn build_timeline_render_cache(
    activity: Option<&Activity>,
    status: Option<&str>,
    width: u16,
) -> TimelineRenderCache {
    let text = detail_text(DetailTab::Timeline, activity, None, status);
    let mut cache = TimelineRenderCache {
        run_id: activity.map_or_else(String::new, |value| value.run_id.clone()),
        recent_len: activity.map_or(0, |value| value.recent.len()),
        last_entry_sequence: activity
            .and_then(|value| value.recent.last())
            .map(|entry| entry.sequence),
        status: status.map(str::to_owned),
        width,
        offsets: vec![0],
        ..TimelineRenderCache::default()
    };
    append_wrapped_lines(&mut cache, text.lines, width);
    cache
}

fn append_wrapped_lines(cache: &mut TimelineRenderCache, lines: Vec<Line<'static>>, width: u16) {
    cache.lines.reserve(lines.len());
    cache.offsets.reserve(lines.len());
    for line in lines {
        cache.rendered_lines += Paragraph::new(line.clone())
            .wrap(Wrap { trim: false })
            .line_count(width);
        cache.lines.push(line);
        cache.offsets.push(cache.rendered_lines);
    }
}

fn draw_detail_scrollbar(
    frame: &mut Frame<'_>,
    scroll: u16,
    maximum: u16,
    viewport_height: u16,
    area: Rect,
) {
    if maximum == 0 {
        return;
    }
    let mut scrollbar = ScrollbarState::new(maximum as usize + 1)
        .position(scroll as usize)
        .viewport_content_length(viewport_height as usize);
    frame.render_stateful_widget(
        Scrollbar::new(ScrollbarOrientation::VerticalRight),
        area,
        &mut scrollbar,
    );
}

fn detail_text(
    tab: DetailTab,
    activity: Option<&Activity>,
    prompt: Option<&str>,
    timeline_status: Option<&str>,
) -> Text<'static> {
    if tab == DetailTab::Prompt {
        return Text::from(match prompt {
            Some("") => "No prompt was recorded for this run.".to_owned(),
            Some(value) => value.to_owned(),
            None => "Loading prompt…".to_owned(),
        });
    }
    match (tab, activity) {
        (DetailTab::Timeline, None) if timeline_status.is_some() => {
            Text::from(timeline_status.unwrap_or_default().to_owned())
        }
        (_, None) => Text::from("No activity recorded for the latest run."),
        (DetailTab::Timeline, Some(activity)) => {
            let mut lines = timeline_status
                .map(|status| {
                    vec![
                        Line::styled(status.to_owned(), Style::default().fg(MUTED)),
                        Line::from(""),
                    ]
                })
                .unwrap_or_default();
            lines.extend(timeline_entry_lines(&activity.recent));
            Text::from(lines)
        }
        (DetailTab::Summary, Some(activity)) => {
            let mut lines = vec![Line::styled(
                "LATEST AGENT UPDATE",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            )];
            lines.extend(format_agent_update(&activity.latest_summary));
            if !activity.latest_error.is_empty() {
                lines.push(Line::from(""));
                lines.push(Line::styled("LATEST ERROR", Style::default().fg(RED)));
                lines.extend(
                    activity
                        .latest_error
                        .lines()
                        .map(|line| Line::from(line.to_owned())),
                );
            }
            Text::from(lines)
        }
        (DetailTab::Plan, Some(activity)) => Text::from(
            activity
                .todos
                .iter()
                .map(|todo| {
                    Line::from(format!(
                        "{} {}",
                        if todo.completed { "✓" } else { "·" },
                        todo.text
                    ))
                })
                .collect::<Vec<_>>(),
        ),
        (DetailTab::Files, Some(activity)) => Text::from(
            activity
                .files
                .iter()
                .map(|path| Line::from(format!("• {path}")))
                .collect::<Vec<_>>(),
        ),
        (DetailTab::Prompt, _) => unreachable!("prompt handled above"),
    }
}

fn timeline_entry_lines(entries: &[crate::model::ActivityEntry]) -> Vec<Line<'static>> {
    entries
        .iter()
        .flat_map(|entry| {
            let clock = entry.at.get(11..19).unwrap_or(&entry.at);
            let mark = match entry.status.as_str() {
                "started" => "▶",
                "completed" => "✓",
                "failed" => "✗",
                _ => "•",
            };
            let mut lines = vec![Line::from(vec![
                Span::raw(format!("{clock} {mark} ")),
                Span::styled(
                    format!("[{}]", activity_kind(&entry.kind)),
                    Style::default()
                        .fg(kind_color(&entry.kind))
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!(" {}", entry.title)),
            ])];
            let detail_lines = if entry.kind == "message" {
                format_agent_update(&entry.detail)
            } else {
                entry
                    .detail
                    .lines()
                    .map(|detail| Line::from(detail.to_owned()))
                    .collect()
            };
            lines.extend(detail_lines.into_iter().map(|mut line| {
                line.spans.insert(0, Span::raw("    "));
                line
            }));
            lines
        })
        .collect()
}

fn format_agent_update(raw: &str) -> Vec<Line<'static>> {
    let Ok(serde_json::Value::Object(object)) = serde_json::from_str(raw) else {
        return raw
            .lines()
            .map(|line| Line::from(line.to_owned()))
            .collect();
    };
    let mut lines = Vec::new();

    if let Some(summary) = object.get("summary").and_then(serde_json::Value::as_str) {
        push_labeled_text(&mut lines, "Summary", summary, CYAN);
    }
    if let Some(complete) = object.get("complete").and_then(serde_json::Value::as_bool) {
        lines.push(Line::from(vec![
            Span::styled("Status: ", Style::default().fg(MUTED)),
            Span::styled(
                if complete { "complete" } else { "in progress" },
                Style::default()
                    .fg(if complete { GREEN } else { YELLOW })
                    .add_modifier(Modifier::BOLD),
            ),
        ]));
    }
    if let Some(changed) = object.get("changed").and_then(serde_json::Value::as_bool) {
        lines.push(Line::from(vec![
            Span::styled("Changes: ", Style::default().fg(MUTED)),
            Span::raw(if changed { "made" } else { "none" }),
        ]));
    }
    if let Some(issues) = object.get("issues") {
        push_json_collection(&mut lines, "Issues", issues);
    }
    if let Some(source_issues) = object.get("source_issues") {
        push_json_collection(&mut lines, "Source issues", source_issues);
    }

    for (key, value) in &object {
        if matches!(
            key.as_str(),
            "summary" | "complete" | "changed" | "issues" | "source_issues"
        ) {
            continue;
        }
        push_json_value(&mut lines, &json_label(key), value, 0);
    }

    if lines.is_empty() {
        vec![Line::from("No details reported.")]
    } else {
        lines
    }
}

fn push_json_collection(lines: &mut Vec<Line<'static>>, label: &str, value: &serde_json::Value) {
    match value {
        serde_json::Value::Array(items) if items.is_empty() => {}
        serde_json::Value::Array(items) => {
            lines.push(Line::styled(
                format!("{label}:"),
                Style::default().fg(PURPLE).add_modifier(Modifier::BOLD),
            ));
            for item in items {
                match item {
                    serde_json::Value::String(text) => {
                        lines.push(Line::from(format!("• {text}")));
                    }
                    serde_json::Value::Object(fields) => {
                        let heading = fields
                            .get("location")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("detail");
                        lines.push(Line::styled(
                            format!("• {heading}"),
                            Style::default().fg(YELLOW).add_modifier(Modifier::BOLD),
                        ));
                        for (key, child) in fields {
                            if key != "location" {
                                push_json_value(lines, &json_label(key), child, 2);
                            }
                        }
                    }
                    other => push_json_value(lines, "•", other, 0),
                }
            }
        }
        other => push_json_value(lines, label, other, 0),
    }
}

fn push_json_value(
    lines: &mut Vec<Line<'static>>,
    label: &str,
    value: &serde_json::Value,
    indent: usize,
) {
    let prefix = " ".repeat(indent);
    match value {
        serde_json::Value::String(text) => {
            push_labeled_text(lines, &format!("{prefix}{label}"), text, MUTED);
        }
        serde_json::Value::Bool(value) => lines.push(Line::from(vec![
            Span::styled(format!("{prefix}{label}: "), Style::default().fg(MUTED)),
            Span::raw(if *value { "yes" } else { "no" }),
        ])),
        serde_json::Value::Number(value) => lines.push(Line::from(vec![
            Span::styled(format!("{prefix}{label}: "), Style::default().fg(MUTED)),
            Span::raw(value.to_string()),
        ])),
        serde_json::Value::Null => lines.push(Line::from(vec![
            Span::styled(format!("{prefix}{label}: "), Style::default().fg(MUTED)),
            Span::raw("—"),
        ])),
        serde_json::Value::Array(_) => {
            push_json_collection(lines, &format!("{prefix}{label}"), value)
        }
        serde_json::Value::Object(fields) => {
            lines.push(Line::styled(
                format!("{prefix}{label}:"),
                Style::default().fg(MUTED),
            ));
            for (key, child) in fields {
                push_json_value(lines, &json_label(key), child, indent + 2);
            }
        }
    }
}

fn push_labeled_text(lines: &mut Vec<Line<'static>>, label: &str, text: &str, color: Color) {
    let mut parts = text.lines();
    let first = parts.next().unwrap_or_default();
    lines.push(Line::from(vec![
        Span::styled(
            format!("{label}: "),
            Style::default().fg(color).add_modifier(Modifier::BOLD),
        ),
        Span::raw(first.to_owned()),
    ]));
    lines.extend(parts.map(|part| Line::from(format!("  {part}"))));
}

fn json_label(key: &str) -> String {
    let label = key.replace(['_', '-'], " ");
    let mut characters = label.chars();
    match characters.next() {
        Some(first) => first.to_uppercase().collect::<String>() + characters.as_str(),
        None => label,
    }
}

fn latest_task<'a>(tasks: &'a HashMap<&str, &'a Task>) -> Option<&'a Task> {
    tasks.values().copied().max_by_key(|task| &task.updated_at)
}

fn current_activity(
    model: &DashboardModel,
    row: &RowModel<'_>,
    activity: Option<&Activity>,
) -> String {
    let build = &model.state.coordinator_build;
    if model.is_building(row.unit.id.as_str(), build.stage.as_str()) {
        return "coordinator build".into();
    }
    if build.active && model.is_build_target(row.unit.id.as_str()) {
        return "coordinator finalize".into();
    }
    let has_running_task = row
        .tasks
        .values()
        .any(|task| task_is_running(task) || task.repairing);
    if let Some(activity) = activity.filter(|_| has_running_task) {
        let idle = elapsed_seconds(&activity.updated_at);
        return if idle >= 60 {
            format!("{} · idle {}m", activity.current, idle / 60)
        } else {
            activity.current.clone()
        };
    }
    if let Some(reason) = model.pending_reason(row) {
        return reason;
    }
    if let Some(activity) = activity {
        let idle = elapsed_seconds(&activity.updated_at);
        return if idle >= 60 {
            format!("{} · idle {}m", activity.current, idle / 60)
        } else {
            activity.current.clone()
        };
    }
    latest_task(&row.tasks)
        .filter(|task| !task.detail.is_empty())
        .map(|task| compact_task_detail(&task.detail))
        .unwrap_or_else(|| "—".into())
}

fn row_spend(row: &RowModel<'_>) -> String {
    let Some(task) = latest_task(&row.tasks) else {
        return "—".into();
    };
    if !task.work_unit_usage.measured {
        return "—".into();
    }
    format!(
        "{} · ${:.2}",
        format_count(task.work_unit_usage.total_tokens),
        task.work_unit_cost.estimated_usd
    )
}

fn task_mark(task: &Task, building: bool) -> String {
    let mark = if building {
        "◆ building".into()
    } else if task.repairing {
        "↻ repairing".into()
    } else if let Some(letter) = auxiliary_role_letter(&task.active_auxiliary_role) {
        format!("▶ running ({letter})")
    } else if task.queued {
        "· queued".into()
    } else if task.status == "running" && task.phase == "postprocess" {
        "◇ postprocess".into()
    } else {
        match task_display_status(task) {
            "running" => "▶ running".into(),
            "succeeded" => "✓ done".into(),
            "failed" => "✗ failed".into(),
            "blocked" => "! blocked".into(),
            "interrupted" => "Ⅱ interrupted".into(),
            _ => "· pending".into(),
        }
    };
    if task.rounds > 0 {
        format!("{mark} ({})", task.rounds)
    } else {
        mark.into()
    }
}

fn build_mark(fresh: bool, sorry_count: Option<usize>, formalized: bool) -> String {
    let freshness = if fresh { "✓ fresh" } else { "○ stale" };
    if formalized {
        format!(
            "{freshness} · {} sorry",
            sorry_count.map_or_else(|| "?".into(), |count| count.to_string())
        )
    } else {
        freshness.into()
    }
}

fn task_mark_style(task: &Task, building: bool) -> Style {
    if building {
        return Style::default().fg(PURPLE);
    }
    if task.repairing || task.queued || (task.status == "running" && task.phase == "postprocess") {
        return Style::default();
    }
    match task_display_status(task) {
        "running" => Style::default().fg(BLUE),
        "succeeded" => Style::default().fg(GREEN),
        "failed" => Style::default().fg(RED),
        "blocked" => Style::default().fg(YELLOW),
        _ => Style::default(),
    }
}

fn task_display_status(task: &Task) -> &str {
    if !task.active_auxiliary_role.is_empty() {
        "running"
    } else if task.status == "pending" && task.scheduling_status == "blocked" {
        "blocked"
    } else {
        task.status.as_str()
    }
}

fn task_status_label(task: &Task) -> String {
    auxiliary_role_letter(&task.active_auxiliary_role).map_or_else(
        || task_display_status(task).into(),
        |letter| format!("running ({letter})"),
    )
}

fn task_is_running(task: &Task) -> bool {
    task.status == "running" || !task.active_auxiliary_role.is_empty()
}

fn auxiliary_role_letter(role: &str) -> Option<&'static str> {
    match role {
        "upstream_repair" => Some("u"),
        "warning_cleanup" => Some("w"),
        "repair_worker" => Some("r"),
        "shepherd" => Some("s"),
        "" => None,
        _ => Some("a"),
    }
}

fn format_count(value: u64) -> String {
    match value {
        0..=999 => value.to_string(),
        1_000..=999_999 => format!("{:.1}k", value as f64 / 1_000.0),
        1_000_000..=999_999_999 => format!("{:.2}m", value as f64 / 1_000_000.0),
        _ => format!("{:.2}b", value as f64 / 1_000_000_000.0),
    }
}

fn format_usage(usage: &crate::model::Usage) -> String {
    if !usage.measured {
        return "tokens awaiting measured usage".into();
    }
    format!(
        "tokens {} · in {} (cached {}) · out {} · reasoning {}",
        format_count(usage.total_tokens),
        format_count(usage.input_tokens),
        format_count(usage.cached_input_tokens),
        format_count(usage.output_tokens),
        format_count(usage.reasoning_output_tokens)
    )
}

fn format_duration(seconds: i64) -> String {
    if seconds >= 3600 {
        format!("{}h {:02}m", seconds / 3600, seconds % 3600 / 60)
    } else if seconds >= 60 {
        format!("{}m {:02}s", seconds / 60, seconds % 60)
    } else {
        format!("{seconds}s")
    }
}

fn title(value: &str) -> String {
    let mut characters = value.chars();
    characters
        .next()
        .map(|first| first.to_uppercase().collect::<String>() + characters.as_str())
        .unwrap_or_default()
}

fn activity_kind(kind: &str) -> &str {
    match kind {
        "usage" => "tokens",
        "todo" => "plan",
        "message" => "msg",
        "reasoning" => "think",
        "command_execution" => "bash",
        "file_change" => "edit",
        "mcp_tool_call" => "mcp",
        "collab_tool_call" | "collab_agent_tool_call" => "swarm",
        "web_search" => "web",
        "context_compaction" | "compaction" => "compact",
        other => other,
    }
}

fn kind_color(kind: &str) -> Color {
    match kind {
        "command_execution" => CYAN,
        "file_change" => GREEN,
        "mcp_tool_call" | "error" => RED,
        "todo" => YELLOW,
        "reasoning" | "usage" => PURPLE,
        _ => BLUE,
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use super::*;
    use crate::model::{
        ActivityEntry, DashboardModel, PROTOCOL_VERSION, Preparation, Task, WireEvent, WorkUnit,
    };

    fn plain_tabs(lines: &[Line<'_>]) -> Vec<String> {
        lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn run_tabs_scroll_to_keep_the_selection_visible() {
        let labels = (1..=10)
            .map(|round| format!("Formalize round {round}"))
            .collect::<Vec<_>>();

        let (first, first_selected) = visible_run_tabs(&labels, 0, 48);
        assert_eq!(
            plain_tabs(&first),
            ["Formalize round 1", "Formalize round 2", "›"]
        );
        assert_eq!(first_selected, 0);

        let (middle, middle_selected) = visible_run_tabs(&labels, 7, 60);
        assert_eq!(
            plain_tabs(&middle),
            ["‹", "Formalize round 7", "Formalize round 8", "›"]
        );
        assert_eq!(plain_tabs(&middle)[middle_selected], "Formalize round 8");

        let (last, last_selected) = visible_run_tabs(&labels, 9, 48);
        assert_eq!(
            plain_tabs(&last),
            ["‹", "Formalize round 9", "Formalize round 10"]
        );
        assert_eq!(plain_tabs(&last)[last_selected], "Formalize round 10");

        let (narrow, narrow_selected) = visible_run_tabs(&labels, 7, 20);
        assert_eq!(plain_tabs(&narrow), ["Formalize round 8"]);
        assert_eq!(narrow_selected, 0);
    }

    #[test]
    fn renders_preparation_progress_as_a_modal() {
        let mut model = DashboardModel::loading("review stage".into(), String::new());
        model
            .apply(WireEvent {
                protocol_version: PROTOCOL_VERSION,
                event: "preparation".into(),
                status: "preparing".into(),
                result: None,
                snapshot: None,
                delta: None,
                preparation: Some(Preparation {
                    phase: "Preparing isolated workspaces and Lean caches".into(),
                    completed: 7,
                    total: 9,
                }),
                message: String::new(),
            })
            .unwrap();
        let backend = TestBackend::new(100, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, &mut model)).unwrap();
        let rendered = terminal.backend().to_string();
        assert!(rendered.contains("Preparing PAF"));
        assert!(rendered.contains("Preparing isolated workspaces and Lean caches"));
        assert!(rendered.contains("7 / 9"));
        assert!(rendered.contains("d detaches  q stops before workers are launched"));
    }

    #[test]
    fn renders_book_search_as_a_modal() {
        let mut model = DashboardModel::loading("review stage".into(), String::new());
        model.preparation = None;
        model.begin_search();
        model.search_query = Some("more-algebra.8".into());
        let backend = TestBackend::new(100, 24);
        let mut terminal = Terminal::new(backend).unwrap();

        terminal.draw(|frame| draw(frame, &mut model)).unwrap();

        let rendered = terminal.backend().to_string();
        assert!(rendered.contains("Search books"));
        assert!(rendered.contains("> more-algebra.8"));
        assert!(rendered.contains("Enter keeps selection · Esc restores"));
    }

    #[test]
    fn renders_dashboard_and_agent_detail() {
        let mut model = DashboardModel::loading("review stage".into(), "install rg".into());
        model
            .apply(WireEvent {
                protocol_version: PROTOCOL_VERSION,
                event: "snapshot".into(),
                status: "running".into(),
                result: None,
                snapshot: Some(serde_json::json!({
                    "revision": 8,
                    "invocation_usage": {"measured": true, "total_tokens": 1200},
                    "agents": {"active": 1, "maximum": 4, "by_stage": {"review": 1}},
                    "work_units": [{"id": "book/chapter-01", "document_id": "book", "title": "Opening", "ordinal": 1}],
                    "tasks": {"book/chapter-01:review": {
                        "work_unit_id": "book/chapter-01", "document_id": "book", "ordinal": 1,
                        "unit_title": "Opening", "stage": "review", "status": "running",
                        "latest_run_id": "run-1"
                    }},
                    "activities": {"run-1": {
                        "run_id": "run-1", "current": "editing theorem", "updated_at": "2026-08-16T00:00:00+00:00",
                        "latest_summary": "{\"complete\":false,\"changed\":true,\"summary\":\"The theorem now has a proof.\",\"issues\":[\"One follow-up remains.\"],\"source_issues\":[]}",
                        "recent": [
                            {"sequence": 1, "at": "2026-08-16T00:00:00+00:00", "kind": "file_change", "status": "completed", "title": "success"},
                            {"sequence": 2, "at": "2026-08-16T00:00:01+00:00", "kind": "message", "status": "completed", "title": "agent update", "detail": "{\"complete\":false,\"changed\":true,\"summary\":\"The theorem now has a proof.\",\"issues\":[\"One follow-up remains.\"],\"source_issues\":[]}"}
                        ]
                    }}
                })),
                delta: None,
                preparation: None,
                message: String::new(),
            })
            .unwrap();
        let backend = TestBackend::new(180, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, &mut model)).unwrap();
        let dashboard = terminal.backend().to_string();
        assert!(dashboard.contains("PAF · review stage"));
        assert!(dashboard.contains("editing theorem"));
        assert!(dashboard.contains("install rg"));
        assert!(dashboard.contains("reload TUI"));

        model.enter_detail();
        terminal.draw(|frame| draw(frame, &mut model)).unwrap();
        let detail = terminal.backend().to_string();
        assert!(detail.contains("Agent detail"));
        assert!(detail.contains("Latest agent update"));
        assert!(detail.contains("Summary: The theorem now has a proof."));
        assert!(detail.contains("Status: in progress"));
        assert!(detail.contains("Issues:"));
        assert!(detail.contains("One follow-up remains."));
        assert!(!detail.contains("{\"complete\""));
        assert!(detail.contains("[edit] success"));
        assert!(detail.contains("[msg] agent update"));
        assert!(detail.contains("reload TUI"));
    }

    #[test]
    fn formats_structured_agent_updates_and_preserves_plain_text() {
        let structured = format_agent_update(
            r#"{"complete":true,"summary":"Finished the proof.","source_issues":[{"location":"book.tex:12","description":"Direction is reversed.","suggested_correction":"Reverse the arrow."}],"extra_context":{"attempt":2}}"#,
        );
        let rendered = plain_tabs(&structured).join("\n");

        assert!(rendered.contains("Summary: Finished the proof."));
        assert!(rendered.contains("Status: complete"));
        assert!(rendered.contains("Source issues:"));
        assert!(rendered.contains("• book.tex:12"));
        assert!(rendered.contains("Description: Direction is reversed."));
        assert!(rendered.contains("Suggested correction: Reverse the arrow."));
        assert!(rendered.contains("Extra context:"));
        assert!(rendered.contains("Attempt: 2"));

        assert_eq!(
            plain_tabs(&format_agent_update("First line\nSecond line")),
            ["First line", "Second line"]
        );
    }

    #[test]
    fn renders_shepherd_trace_and_relevant_agent_navigation() {
        let mut model = DashboardModel::loading("pipeline".into(), String::new());
        model
            .apply(WireEvent {
                protocol_version: PROTOCOL_VERSION,
                event: "snapshot".into(),
                status: "running".into(),
                result: None,
                snapshot: Some(serde_json::json!({
                    "shepherd": {
                        "enabled": true,
                        "status": "repairing",
                        "pending_failures": 2,
                        "planned_units": 1,
                        "running_units": 1,
                        "current_sweep_id": "sweep-2",
                        "cost": {"estimated_usd": 3.25},
                        "last_summary": "repair the shared blocker",
                        "runs": [{
                            "id": "sweep-1",
                            "status": "completed",
                            "summary": "older repair",
                            "agents": []
                        }, {
                            "id": "sweep-2",
                            "status": "repairing",
                            "failure_count": 2,
                            "summary": "repair the shared blocker",
                            "agents": [{
                            "run_id": "plan-run",
                            "role": "shepherd",
                            "work_unit_id": "book/chapter-01",
                            "stage": "discover",
                            "status": "running",
                            "label": "Shepherd planner",
                            "objective": "rank repair candidates",
                            "location": "2 chapters in 1 book"
                        }, {
                            "run_id": "worker-run",
                            "role": "repair_worker",
                            "work_unit_id": "book/chapter-01",
                            "stage": "review",
                            "status": "running",
                            "label": "Repair review",
                            "repair_work_unit_id": "repair-1",
                            "objective": "repair the failed declaration",
                            "document_title": "Algebra",
                            "ordinal": 3,
                            "unit_title": "Ideals"
                        }]
                        }]
                    },
                    "activities": {
                        "plan-run": {
                            "run_id": "plan-run",
                            "current": "ranking repair candidates",
                            "recent": [{
                                "sequence": 1,
                                "at": "2026-08-16T00:00:00+00:00",
                                "kind": "reasoning",
                                "status": "started",
                                "title": "Inspect failures"
                            }]
                        }
                    }
                })),
                delta: None,
                preparation: None,
                message: String::new(),
            })
            .unwrap();
        assert_eq!(model.state.shepherd.cost.estimated_usd, 3.25);
        model.enter_shepherd_detail();
        assert_eq!(model.shepherd_run_selected, 1);

        let backend = TestBackend::new(140, 32);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, &mut model)).unwrap();
        let rendered = terminal.backend().to_string();
        assert!(rendered.contains("Shepherd trace"));
        assert!(rendered.contains("Run 1 · Completed"));
        assert!(rendered.contains("Run 2 · Repairing"));
        assert!(rendered.contains("2 chapters in 1 book"));
        assert!(rendered.contains("Algebra"));
        assert!(rendered.contains("Chapter 3 · Ideals"));
        assert!(rendered.contains("cost $3.25"));
        assert!(rendered.contains("Inspect failures"));
        assert!(rendered.contains("Enter open full agent view"));

        model.move_shepherd_run_selection(-1);
        assert_eq!(model.shepherd_run_selected, 0);
        assert!(model.selected_shepherd_agent().is_none());
    }

    #[test]
    fn renders_live_pending_dependencies_instead_of_stale_activity() {
        let mut model = DashboardModel::loading("review stage".into(), String::new());
        model.preparation = None;
        for (id, document_id, ordinal) in [
            ("introductions/unit-05", "introductions", 5),
            ("cohomology/unit-07", "cohomology", 7),
            ("results/unit-02", "results", 2),
        ] {
            model.state.work_units.push(WorkUnit {
                id: id.into(),
                document_id: document_id.into(),
                title: id.into(),
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
                    latest_run_id: (stage == "formalize").then(|| "old-run".into()),
                    ..Task::default()
                },
            );
        }
        model.state.activities.insert(
            "old-run".into(),
            Activity {
                current: "finished an earlier stage".into(),
                ..Activity::default()
            },
        );
        model.selected = 2;

        let row = model.selected_row().unwrap();
        assert_eq!(
            current_activity(&model, &row, model.selected_activity()),
            "waiting: introductions.5, cohomology.7"
        );

        let backend = TestBackend::new(220, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, &mut model)).unwrap();
        let rendered = terminal.backend().to_string();
        assert!(rendered.contains("waiting: introductions.5, cohomology.7"));
    }

    #[test]
    fn completed_build_renders_as_finalizing_instead_of_building() {
        let mut model = DashboardModel::loading("formalize stage".into(), String::new());
        model.preparation = None;
        model.state.coordinator_build = crate::model::CoordinatorBuild {
            active: true,
            mode: "targeted".into(),
            stage: "formalize".into(),
            completed: 3,
            total: 3,
            target_work_unit_ids: ["book/chapter-01".into()].into_iter().collect(),
            ..crate::model::CoordinatorBuild::default()
        };
        model.state.work_units.push(WorkUnit {
            id: "book/chapter-01".into(),
            document_id: "book".into(),
            title: "Opening".into(),
            ordinal: 1,
            ..WorkUnit::default()
        });
        model.state.tasks.insert(
            "book/chapter-01:formalize".into(),
            Task {
                work_unit_id: "book/chapter-01".into(),
                stage: "formalize".into(),
                status: "running".into(),
                phase: "postprocess".into(),
                ..Task::default()
            },
        );

        let backend = TestBackend::new(180, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, &mut model)).unwrap();
        let rendered = terminal.backend().to_string();

        assert!(rendered.contains("coordinator finalize 3/3"));
        assert!(rendered.contains("◇ postprocess"));
        assert!(rendered.contains("coordinator finalize"));
        assert!(!rendered.contains("targeted"));
        assert!(!rendered.contains("◆ building"));
    }

    #[test]
    fn build_freshness_renders_the_sorry_count() {
        let mut model = DashboardModel::loading("proof stage".into(), String::new());
        model.preparation = None;
        model.state.work_units.push(WorkUnit {
            id: "book/chapter-01".into(),
            document_id: "book".into(),
            title: "Opening".into(),
            ordinal: 1,
            ..WorkUnit::default()
        });
        model.state.tasks.insert(
            "book/chapter-01:formalize".into(),
            Task {
                work_unit_id: "book/chapter-01".into(),
                stage: "formalize".into(),
                status: "succeeded".into(),
                ..Task::default()
            },
        );
        model.state.tasks.insert(
            "book/chapter-01:prove".into(),
            Task {
                work_unit_id: "book/chapter-01".into(),
                stage: "prove".into(),
                head_build_status: "clean".into(),
                sorry_count: Some(3),
                ..Task::default()
            },
        );

        let backend = TestBackend::new(180, 40);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|frame| draw(frame, &mut model)).unwrap();

        assert!(terminal.backend().to_string().contains("✓ fresh · 3 sorry"));
    }

    #[test]
    fn build_freshness_omits_sorry_count_before_formalization_succeeds() {
        assert_eq!(build_mark(true, Some(3), false), "✓ fresh");
        assert_eq!(build_mark(false, Some(3), false), "○ stale");
    }

    #[test]
    fn coordinator_build_wins_over_repairing_overlay() {
        let task = Task {
            status: "failed".into(),
            repairing: true,
            rounds: 2,
            ..Task::default()
        };

        assert_eq!(task_mark(&task, false), "↻ repairing (2)");
        assert_eq!(task_mark(&task, true), "◆ building (2)");
    }

    #[test]
    fn task_marks_color_running_done_failed_blocked_and_building() {
        let task = |status: &str| Task {
            status: status.into(),
            ..Task::default()
        };

        assert_eq!(task_mark_style(&task("running"), false).fg, Some(BLUE));
        assert_eq!(task_mark_style(&task("succeeded"), false).fg, Some(GREEN));
        assert_eq!(task_mark_style(&task("failed"), false).fg, Some(RED));
        assert_eq!(task_mark_style(&task("blocked"), false).fg, Some(YELLOW));
        assert_eq!(task_mark_style(&task("pending"), false).fg, None);
        assert_eq!(task_mark_style(&task("pending"), true).fg, Some(PURPLE));
    }

    #[test]
    fn task_marks_distinguish_auxiliary_run_roles() {
        for (role, letter) in [
            ("upstream_repair", "u"),
            ("warning_cleanup", "w"),
            ("repair_worker", "r"),
            ("shepherd", "s"),
            ("future_auxiliary_role", "a"),
        ] {
            let task = Task {
                status: "pending".into(),
                active_auxiliary_role: role.into(),
                ..Task::default()
            };

            assert_eq!(task_mark(&task, false), format!("▶ running ({letter})"));
            assert_eq!(task_status_label(&task), format!("running ({letter})"));
            assert_eq!(task_mark_style(&task, false).fg, Some(BLUE));
        }
    }

    #[test]
    fn agent_detail_enters_at_the_tail_and_uses_wrapped_viewport_bounds() {
        let mut model = DashboardModel::loading("review stage".into(), String::new());
        model.preparation = None;
        model.state.work_units.push(WorkUnit {
            id: "book/chapter-01".into(),
            document_id: "book".into(),
            title: "Opening".into(),
            ordinal: 1,
            ..WorkUnit::default()
        });
        model.state.tasks.insert(
            "book/chapter-01:review".into(),
            Task {
                work_unit_id: "book/chapter-01".into(),
                stage: "review".into(),
                latest_run_id: Some("run-1".into()),
                ..Task::default()
            },
        );
        model.state.activities.insert(
            "run-1".into(),
            Activity {
                run_id: "run-1".into(),
                latest_summary: r#"{"complete":false,"summary":"Still working."}"#.into(),
                recent: (0..20)
                    .map(|sequence| ActivityEntry {
                        sequence,
                        at: "2026-08-16T00:00:00+00:00".into(),
                        kind: "message".into(),
                        status: "completed".into(),
                        title: format!("event-{sequence:02}"),
                        detail: String::new(),
                    })
                    .collect(),
                ..Activity::default()
            },
        );
        model.enter_detail();
        let backend = TestBackend::new(80, 20);
        let mut terminal = Terminal::new(backend).unwrap();

        terminal.draw(|frame| draw(frame, &mut model)).unwrap();

        let rendered = terminal.backend().to_string();
        assert!(model.detail_max_scroll > 0);
        assert_eq!(model.scroll, model.detail_max_scroll);
        assert!(rendered.contains("event-19"));
        assert!(!rendered.contains("event-00"));

        let previous_tail = model.scroll;
        model
            .state
            .activities
            .get_mut("run-1")
            .unwrap()
            .recent
            .push(ActivityEntry {
                sequence: 20,
                at: "2026-08-16T00:00:01+00:00".into(),
                kind: "message".into(),
                status: "completed".into(),
                title: "event-20".into(),
                detail: String::new(),
            });
        terminal.draw(|frame| draw(frame, &mut model)).unwrap();
        assert!(model.scroll > previous_tail);
        assert!(terminal.backend().to_string().contains("event-20"));

        model.scroll_detail(-3);
        let scrollback = model.scroll;
        model
            .state
            .activities
            .get_mut("run-1")
            .unwrap()
            .recent
            .push(ActivityEntry {
                sequence: 21,
                at: "2026-08-16T00:00:02+00:00".into(),
                kind: "message".into(),
                status: "completed".into(),
                title: "event-21".into(),
                detail: String::new(),
            });
        terminal.draw(|frame| draw(frame, &mut model)).unwrap();
        assert_eq!(model.scroll, scrollback);
        assert!(!model.detail_follow_tail);
    }

    #[test]
    fn agent_detail_scrollbar_reaches_the_bottom_at_the_tail() {
        let mut model = DashboardModel::loading("scrollbar test".into(), String::new());
        model.enter_detail();
        let text = Text::from(
            (0..40)
                .map(|line| Line::from(format!("event-{line:02}")))
                .collect::<Vec<_>>(),
        );
        let backend = TestBackend::new(20, 10);
        let mut terminal = Terminal::new(backend).unwrap();

        terminal
            .draw(|frame| draw_detail_content(frame, &mut model, text.clone(), frame.area()))
            .unwrap();

        assert_eq!(model.scroll, model.detail_max_scroll);
        let buffer = terminal.backend().buffer();
        assert_eq!(buffer[(19, 8)].symbol(), "█");
        assert_eq!(buffer[(19, 9)].symbol(), "▼");
    }

    #[test]
    fn long_timeline_scrolling_reuses_the_wrapped_layout() {
        let mut model = DashboardModel::loading("timeline cache test".into(), String::new());
        model.preparation = None;
        model.state.work_units.push(WorkUnit {
            id: "book/chapter-01".into(),
            document_id: "book".into(),
            title: "Opening".into(),
            ordinal: 1,
            ..WorkUnit::default()
        });
        model.state.tasks.insert(
            "book/chapter-01:review".into(),
            Task {
                work_unit_id: "book/chapter-01".into(),
                stage: "review".into(),
                latest_run_id: Some("long-run".into()),
                ..Task::default()
            },
        );
        model.state.activities.insert(
            "long-run".into(),
            Activity {
                run_id: "long-run".into(),
                sequence: 10_000,
                recent: (0..10_000)
                    .map(|sequence| ActivityEntry {
                        sequence,
                        at: "2026-08-16T00:00:00+00:00".into(),
                        kind: "message".into(),
                        status: "completed".into(),
                        title: format!(
                            "event-{sequence:05} with enough content to wrap in a narrow terminal"
                        ),
                        detail: format!("detail-{sequence:05}"),
                    })
                    .collect(),
                ..Activity::default()
            },
        );
        model.enter_detail();
        let backend = TestBackend::new(64, 20);
        let mut terminal = Terminal::new(backend).unwrap();

        terminal.draw(|frame| draw(frame, &mut model)).unwrap();
        let cached_lines = model.timeline_render_cache.lines.as_ptr();
        model.scroll_detail(-3);
        let started = Instant::now();
        terminal.draw(|frame| draw(frame, &mut model)).unwrap();
        let elapsed = started.elapsed();

        assert_eq!(model.timeline_render_cache.lines.as_ptr(), cached_lines);
        assert!(
            elapsed < Duration::from_millis(250),
            "cached timeline scroll took {elapsed:?}"
        );

        let first_line_text = model.timeline_render_cache.lines[0].spans[0]
            .content
            .as_ptr();
        let prior_lines = model.timeline_render_cache.lines.len();
        let prior_scroll = model.scroll;
        let activity = model.state.activities.get_mut("long-run").unwrap();
        activity.sequence += 1;
        activity.recent.push(ActivityEntry {
            sequence: 10_000,
            at: "2026-08-16T00:00:01+00:00".into(),
            kind: "message".into(),
            status: "completed".into(),
            title: "appended-event".into(),
            detail: "appended-detail".into(),
        });
        let started = Instant::now();
        terminal.draw(|frame| draw(frame, &mut model)).unwrap();
        let elapsed = started.elapsed();

        assert_eq!(
            model.timeline_render_cache.lines[0].spans[0]
                .content
                .as_ptr(),
            first_line_text,
            "an append must retain the previously laid-out lines"
        );
        assert_eq!(model.timeline_render_cache.lines.len(), prior_lines + 2);
        assert_eq!(
            model.scroll, prior_scroll,
            "an append must preserve scrollback"
        );
        assert!(
            elapsed < Duration::from_millis(250),
            "incremental timeline append took {elapsed:?}"
        );
    }

    #[test]
    fn renders_a_ten_thousand_unit_active_build_without_materializing_table_rows() {
        let mut model = DashboardModel::loading("scale test".into(), String::new());
        for ordinal in 0..10_000 {
            let id = format!("book/chapter-{ordinal:05}");
            model.state.work_units.push(WorkUnit {
                id: id.clone(),
                document_id: "book".into(),
                title: format!("Unit {ordinal}"),
                ordinal,
                source_start_line: ordinal,
            });
            for stage in STAGES {
                model.state.tasks.insert(
                    format!("{id}:{stage}"),
                    Task {
                        work_unit_id: id.clone(),
                        document_id: "book".into(),
                        ordinal,
                        unit_title: format!("Unit {ordinal}"),
                        stage: stage.into(),
                        ..Task::default()
                    },
                );
            }
        }
        model.state.coordinator_build = crate::model::CoordinatorBuild {
            active: true,
            mode: "review".into(),
            stage: "review".into(),
            total: model.state.work_units.len(),
            target_work_unit_ids: model
                .state
                .work_units
                .iter()
                .map(|unit| unit.id.clone())
                .collect(),
            ..crate::model::CoordinatorBuild::default()
        };
        model.selected = 9_999;
        let backend = TestBackend::new(180, 40);
        let mut terminal = Terminal::new(backend).unwrap();

        let started = Instant::now();
        terminal.draw(|frame| draw(frame, &mut model)).unwrap();
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_secs(2),
            "large draw took {elapsed:?}"
        );
        assert!(terminal.backend().to_string().contains("of 10000"));
        eprintln!("10,000-unit dashboard draw: {elapsed:?}");
    }
}
