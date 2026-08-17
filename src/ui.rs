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
    } else {
        draw_dashboard(frame, model);
    }
    if model.preparation.is_some() {
        draw_preparation_modal(frame, model);
    }
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
            "↑↓ select  Enter/i inspect  p pause/resume  r reload TUI  d detach  q stop",
        )
        .style(Style::default().fg(MUTED))
        .alignment(Alignment::Center),
        layout[5],
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
            "{} {phase} {}/{} · err {} · warn {}",
            state.coordinator_build.mode,
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
            "Agents {}/{} · {} · queued {}    {}",
            state.agents.active, state.agents.maximum, agent_detail, state.agents.queued, build
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
        match task.status.as_str() {
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
                "agent {agents} · post {} · build {}",
                statistics.postprocess, statistics.building
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
            let value = row.tasks.get(stage).map_or_else(
                || "· pending".into(),
                |task| task_mark(task, model.is_building(row.unit.id.as_str(), stage)),
            );
            cells.push(Cell::from(value));
        }
        let fresh = model
            .state
            .formalize_graph
            .get("clean")
            .and_then(serde_json::Value::as_object)
            .is_some_and(|clean| clean.contains_key(&row.unit.id));
        cells.push(Cell::from(if fresh { "✓ fresh" } else { "○ stale" }));
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
        Constraint::Length(9),
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
            "{} {phase} {}/{} · iter {}/{} · err {} · warn {}{}",
            build.mode.to_uppercase(),
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
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(5),
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
                .map(|task| format!("{} {}", title(&task.stage), task.status))
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
    let pending_reason = model.pending_reason(&row);
    let has_running_task = row.tasks.values().any(|task| task.status == "running");
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
        layout[1],
    );
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
        layout[2],
    );
    let text = detail_text(model.detail_tab, activity);
    draw_detail_content(frame, model, text, layout[3]);
    frame.render_widget(
        Paragraph::new("Tab/Shift-Tab switch  ↑↓ scroll  r reload TUI  d detach  Esc/q back")
            .style(Style::default().fg(MUTED))
            .alignment(Alignment::Center),
        layout[4],
    );
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

fn detail_text(tab: DetailTab, activity: Option<&Activity>) -> Text<'static> {
    match (tab, activity) {
        (_, None) => Text::from("No activity recorded for the latest run."),
        (DetailTab::Timeline, Some(activity)) => Text::from(
            activity
                .recent
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
                    lines.extend(
                        entry
                            .detail
                            .lines()
                            .map(|detail| Line::from(format!("    {detail}"))),
                    );
                    lines
                })
                .collect::<Vec<_>>(),
        ),
        (DetailTab::Summary, Some(activity)) => {
            let mut lines = vec![Line::styled(
                "LATEST AGENT UPDATE",
                Style::default().fg(CYAN).add_modifier(Modifier::BOLD),
            )];
            lines.extend(
                activity
                    .latest_summary
                    .lines()
                    .map(|line| Line::from(line.to_owned())),
            );
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
        return format!("{} coordinator build", build.mode);
    }
    if build.active && model.build_targets().contains(row.unit.id.as_str()) {
        return format!("{} coordinator finalize", build.mode);
    }
    let has_running_task = row.tasks.values().any(|task| task.status == "running");
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
        "◆ building"
    } else if task.queued {
        "· queued"
    } else if task.status == "running" && task.phase == "postprocess" {
        "◇ postprocess"
    } else {
        match task.status.as_str() {
            "running" => "▶ running",
            "succeeded" => "✓ done",
            "failed" => "✗ failed",
            "blocked" => "! blocked",
            "interrupted" => "Ⅱ interrupted",
            _ => "· pending",
        }
    };
    if task.rounds > 0 {
        format!("{mark} ({})", task.rounds)
    } else {
        mark.into()
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
                        "recent": [{"sequence": 1, "at": "2026-08-16T00:00:00+00:00", "kind": "file_change", "status": "completed", "title": "success"}]
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
        assert!(detail.contains("[edit] success"));
        assert!(detail.contains("reload TUI"));
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
            target_work_unit_ids: vec!["book/chapter-01".into()],
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

        assert!(rendered.contains("targeted finalize 3/3"));
        assert!(rendered.contains("◇ postprocess"));
        assert!(rendered.contains("targeted coordinator finalize"));
        assert!(!rendered.contains("◆ building"));
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
    fn renders_a_ten_thousand_unit_viewport_without_materializing_table_rows() {
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
