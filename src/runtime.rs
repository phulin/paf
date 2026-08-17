use std::fmt;
use std::io::{BufRead, BufReader, Write, stdout};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind};
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use crossterm::{Command, execute};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use serde_json::{Value, json};

use crate::model::{ChapterRuns, DashboardModel, WireEvent};
use crate::ui;

enum RuntimeEvent {
    Terminal(Event),
    Wire(Box<Result<WireEvent, String>>),
}

pub enum TuiExit {
    Complete(bool),
    Detach,
    Reload(Option<AgentView>),
}

pub struct AgentView {
    pub work_unit_id: String,
    pub detail_tab: &'static str,
}

struct TerminalGuard;

struct DashboardStreamGuard(UnixStream);

const MOUSE_SCROLL_ROWS: isize = 3;

/// Ask the terminal for button and wheel events using SGR coordinates.
///
/// Crossterm's general mouse-capture command also enables all-motion reporting, which sends a
/// packet for every pointer movement. Normal tracking includes wheel events and avoids flooding
/// slower SSH/tmux links with events this TUI does not use.
struct EnableMouseWheelCapture;

impl Command for EnableMouseWheelCapture {
    fn write_ansi(&self, output: &mut impl fmt::Write) -> fmt::Result {
        output.write_str("\x1b[?1000h\x1b[?1006h")
    }
}

struct DisableMouseWheelCapture;

impl Command for DisableMouseWheelCapture {
    fn write_ansi(&self, output: &mut impl fmt::Write) -> fmt::Result {
        output.write_str("\x1b[?1006l\x1b[?1000l")
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(
            stdout(),
            DisableMouseWheelCapture,
            LeaveAlternateScreen,
            crossterm::cursor::Show
        );
    }
}

impl Drop for DashboardStreamGuard {
    fn drop(&mut self) {
        let _ = self.0.shutdown(Shutdown::Both);
    }
}

pub fn run(
    socket_path: &str,
    label: &str,
    startup_warning: &str,
    initial_agent_view: Option<&str>,
    initial_detail_tab: Option<&str>,
) -> Result<TuiExit> {
    // Establish the push stream before taking over the terminal so startup errors remain plain
    // shell diagnostics.
    let stream = subscribe(socket_path)?;
    let _stream_guard = DashboardStreamGuard(
        stream
            .try_clone()
            .context("could not guard the dashboard stream")?,
    );
    let (sender, receiver) = mpsc::channel();
    spawn_stream_reader(stream, sender.clone());
    spawn_terminal_reader(sender);

    enable_raw_mode().context("could not enable terminal raw mode")?;
    let _guard = TerminalGuard;
    execute!(
        stdout(),
        EnterAlternateScreen,
        EnableMouseWheelCapture,
        crossterm::cursor::Hide
    )
    .context("could not enter the alternate screen")?;
    let backend = CrosstermBackend::new(stdout());
    let mut terminal = Terminal::new(backend).context("could not initialize terminal")?;

    let mut model = DashboardModel::loading_with_agent_view(
        label.to_owned(),
        startup_warning.to_owned(),
        initial_agent_view.map(str::to_owned),
        initial_detail_tab,
    );
    let mut dirty = true;
    loop {
        if dirty {
            terminal
                .draw(|frame| ui::draw(frame, &mut model))
                .context("terminal draw failed")?;
        }
        match receiver.recv_timeout(Duration::from_secs(1)) {
            Ok(RuntimeEvent::Wire(event)) if event.is_ok() => {
                let event = event.expect("guarded as successful wire event");
                let complete = event.event == "complete";
                model.apply(event)?;
                if model.detail && model.detail_runs.is_empty() {
                    load_chapter_runs(&mut model, socket_path, None)?;
                }
                dirty = true;
                if complete {
                    terminal.draw(|frame| ui::draw(frame, &mut model))?;
                    return Ok(TuiExit::Complete(model.result.unwrap_or(false)));
                }
            }
            Ok(RuntimeEvent::Wire(event)) => {
                bail!(event.expect_err("guarded as failed wire event"))
            }
            Ok(RuntimeEvent::Terminal(event)) => {
                dirty = handle_terminal_event(event, &mut model, socket_path)?;
                if model.detach_requested {
                    return Ok(TuiExit::Detach);
                }
                if model.reload_requested {
                    let agent_view = model.detail.then(|| {
                        model.selected_row().map(|row| AgentView {
                            work_unit_id: row.unit.id.clone(),
                            detail_tab: model.detail_tab.name(),
                        })
                    });
                    return Ok(TuiExit::Reload(agent_view.flatten()));
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                // This is a presentation clock for idle/elapsed labels, never a state poll.
                dirty = model.detail || model.state.agents.active > 0;
            }
            Err(RecvTimeoutError::Disconnected) => {
                bail!("dashboard event sources disconnected unexpectedly")
            }
        }
    }
}

fn subscribe(socket_path: &str) -> Result<UnixStream> {
    let mut stream = UnixStream::connect(socket_path)
        .with_context(|| format!("could not connect to dashboard socket {socket_path}"))?;
    stream
        .write_all(b"{\"command\":\"subscribe\",\"view\":\"dashboard\"}\n")
        .context("could not subscribe to dashboard stream")?;
    Ok(stream)
}

fn spawn_stream_reader(stream: UnixStream, sender: Sender<RuntimeEvent>) {
    thread::spawn(move || {
        let mut reader = BufReader::new(stream);
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = sender.send(RuntimeEvent::Wire(Box::new(Err(
                        "dashboard stream closed before a completion event".into(),
                    ))));
                    break;
                }
                Ok(_) => {
                    let value = serde_json::from_str::<WireEvent>(&line)
                        .map_err(|error| format!("invalid dashboard event: {error}"));
                    let complete = value
                        .as_ref()
                        .is_ok_and(|event| event.event.as_str() == "complete");
                    if sender.send(RuntimeEvent::Wire(Box::new(value))).is_err() || complete {
                        break;
                    }
                }
                Err(error) => {
                    let _ = sender.send(RuntimeEvent::Wire(Box::new(Err(format!(
                        "dashboard stream read failed: {error}"
                    )))));
                    break;
                }
            }
        }
    });
}

fn spawn_terminal_reader(sender: Sender<RuntimeEvent>) {
    thread::spawn(move || {
        while let Ok(event) = event::read() {
            if sender.send(RuntimeEvent::Terminal(event)).is_err() {
                break;
            }
        }
    });
}

fn handle_terminal_event(
    event: Event,
    model: &mut DashboardModel,
    socket_path: &str,
) -> Result<bool> {
    let key = match event {
        Event::Key(key) => key,
        Event::Mouse(mouse) => return Ok(handle_mouse_event(mouse, model)),
        Event::Resize(_, _) => return Ok(true),
        _ => return Ok(false),
    };
    if !key.is_press() {
        return Ok(false);
    }
    let stop_key = key.code == KeyCode::Char('q')
        || (key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL));
    if key.code == KeyCode::Char('d') {
        model.detach_requested = true;
        return Ok(true);
    }
    if model.preparation.is_some() && !stop_key {
        return Ok(false);
    }
    if key.code == KeyCode::Char('r') {
        model.reload_requested = true;
        return Ok(true);
    }
    if model.detail {
        let previous_run = model
            .detail_runs
            .get(model.selected_run)
            .map(|run| run.id.clone());
        let dirty = handle_detail_key(key, model)?;
        let selected_run = model
            .detail_runs
            .get(model.selected_run)
            .map(|run| run.id.clone());
        if selected_run != previous_run {
            load_chapter_runs(model, socket_path, selected_run.as_deref())?;
        }
        return Ok(dirty);
    }
    match key.code {
        KeyCode::Char('q') | KeyCode::Char('c')
            if key.code == KeyCode::Char('q') || key.modifiers.contains(KeyModifiers::CONTROL) =>
        {
            if !model.stopping {
                let response = send_control(socket_path, "stop")?;
                if response.get("error").is_some() {
                    bail!("orchestrator rejected stop request: {response}")
                }
                model.stopping = true;
                model.daemon_status = "stopping".into();
            }
            Ok(true)
        }
        KeyCode::Char('i') | KeyCode::Enter => {
            if model.selected_row().is_some() {
                model.enter_detail();
                load_chapter_runs(model, socket_path, None)?;
            }
            Ok(true)
        }
        KeyCode::Up | KeyCode::Char('k') => {
            model.move_selection(-1);
            Ok(true)
        }
        KeyCode::Down | KeyCode::Char('j') => {
            model.move_selection(1);
            Ok(true)
        }
        KeyCode::PageUp => {
            model.move_selection(-10);
            Ok(true)
        }
        KeyCode::PageDown => {
            model.move_selection(10);
            Ok(true)
        }
        KeyCode::Home => {
            model.move_selection(-(model.selected as isize));
            Ok(true)
        }
        KeyCode::End => {
            model.move_selection(isize::MAX);
            Ok(true)
        }
        KeyCode::Char('p') => {
            let command = if model.daemon_status == "paused" {
                "resume"
            } else {
                "pause"
            };
            let response = send_control(socket_path, command)?;
            if let Some(status) = response.get("status").and_then(Value::as_str) {
                model.daemon_status = status.into();
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn handle_mouse_event(mouse: MouseEvent, model: &mut DashboardModel) -> bool {
    if model.preparation.is_some() {
        return false;
    }
    let delta = match mouse.kind {
        MouseEventKind::ScrollUp => -MOUSE_SCROLL_ROWS,
        MouseEventKind::ScrollDown => MOUSE_SCROLL_ROWS,
        _ => return false,
    };
    if model.detail {
        model.scroll_detail(delta as i16);
    } else {
        model.move_selection(delta);
    }
    true
}

fn handle_detail_key(key: KeyEvent, model: &mut DashboardModel) -> Result<bool> {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => {
            model.leave_detail();
        }
        KeyCode::Tab => model.cycle_tab(key.modifiers.contains(KeyModifiers::SHIFT)),
        KeyCode::BackTab => model.cycle_tab(true),
        KeyCode::Left | KeyCode::Char('h') => model.cycle_run(true),
        KeyCode::Right | KeyCode::Char('l') => model.cycle_run(false),
        KeyCode::Up | KeyCode::Char('k') => model.scroll_detail(-1),
        KeyCode::Down | KeyCode::Char('j') => model.scroll_detail(1),
        KeyCode::PageUp => model.scroll_detail(-10),
        KeyCode::PageDown => model.scroll_detail(10),
        KeyCode::Home => model.scroll_detail_home(),
        KeyCode::End => model.scroll_detail_end(),
        _ => return Ok(false),
    }
    Ok(true)
}

fn load_chapter_runs(
    model: &mut DashboardModel,
    socket_path: &str,
    selected_run_id: Option<&str>,
) -> Result<()> {
    let Some(chapter) = model.selected_row().map(|row| row.unit.id.clone()) else {
        return Ok(());
    };
    let mut request = json!({"command": "chapter_runs", "chapter": chapter});
    if let Some(run_id) = selected_run_id {
        request["run_id"] = Value::String(run_id.to_owned());
    }
    let response = send_control_request(socket_path, &request)?;
    if let Some(error) = response.get("error") {
        bail!("orchestrator rejected chapter history request: {error}")
    }
    let details: ChapterRuns = serde_json::from_value(
        response
            .get("chapter_runs")
            .cloned()
            .context("chapter history response omitted chapter_runs")?,
    )
    .context("invalid chapter history response")?;
    model.apply_chapter_runs(details);
    Ok(())
}

fn send_control(socket_path: &str, command: &str) -> Result<Value> {
    send_control_request(socket_path, &json!({"command": command}))
}

fn send_control_request(socket_path: &str, request: &Value) -> Result<Value> {
    let mut stream =
        UnixStream::connect(socket_path).context("could not connect for control command")?;
    serde_json::to_writer(&mut stream, request)?;
    stream.write_all(b"\n")?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    serde_json::from_str(&line).context("invalid control command response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Task, WorkUnit};

    #[test]
    fn mouse_capture_uses_sgr_without_motion_tracking() {
        let mut enabled = String::new();
        EnableMouseWheelCapture.write_ansi(&mut enabled).unwrap();
        assert!(enabled.contains("\x1b[?1000h"));
        assert!(enabled.contains("\x1b[?1006h"));
        assert!(!enabled.contains("?1002h"));
        assert!(!enabled.contains("?1003h"));
    }

    #[test]
    fn mouse_wheel_scrolls_dashboard_rows_and_detail_content() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model.preparation = None;
        for ordinal in 0..8 {
            let id = format!("book/chapter-{ordinal:02}");
            model.state.work_units.push(WorkUnit {
                id: id.clone(),
                document_id: "book".into(),
                title: format!("Chapter {ordinal}"),
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
        let wheel = |kind| {
            Event::Mouse(MouseEvent {
                kind,
                column: 0,
                row: 0,
                modifiers: KeyModifiers::NONE,
            })
        };

        assert!(
            handle_terminal_event(wheel(MouseEventKind::ScrollDown), &mut model, "/unused")
                .unwrap()
        );
        assert_eq!(model.selected, 3);
        handle_terminal_event(wheel(MouseEventKind::ScrollUp), &mut model, "/unused").unwrap();
        assert_eq!(model.selected, 0);

        model.detail = true;
        model.detail_max_scroll = 20;
        model.scroll = 9;
        model.detail_follow_tail = false;
        handle_terminal_event(wheel(MouseEventKind::ScrollUp), &mut model, "/unused").unwrap();
        assert_eq!(model.scroll, 6);
        handle_terminal_event(wheel(MouseEventKind::ScrollDown), &mut model, "/unused").unwrap();
        assert_eq!(model.scroll, 9);

        model.scroll = model.detail_max_scroll;
        handle_terminal_event(wheel(MouseEventKind::ScrollDown), &mut model, "/unused").unwrap();
        assert_eq!(model.scroll, model.detail_max_scroll);
        assert!(model.detail_follow_tail);
    }

    #[test]
    fn detail_scrollback_pauses_and_resumes_tail_following() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model.enter_detail();

        model.sync_detail_viewport(20);
        assert_eq!(model.scroll, 20);
        assert!(model.detail_follow_tail);

        handle_detail_key(
            KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE),
            &mut model,
        )
        .unwrap();
        assert_eq!(model.scroll, 10);
        assert!(!model.detail_follow_tail);

        model.sync_detail_viewport(25);
        assert_eq!(model.scroll, 10, "new messages must not move scrollback");

        handle_detail_key(KeyEvent::new(KeyCode::End, KeyModifiers::NONE), &mut model).unwrap();
        assert_eq!(model.scroll, 25);
        assert!(model.detail_follow_tail);

        model.sync_detail_viewport(30);
        assert_eq!(model.scroll, 30, "a pinned view follows new messages");

        handle_detail_key(
            KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE),
            &mut model,
        )
        .unwrap();
        assert_eq!(
            model.scroll, 30,
            "scrolling cannot expose space below the log"
        );
    }

    #[test]
    fn reload_key_requests_a_fresh_tui_without_stopping() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model.preparation = None;
        model.detail = true;
        let changed = handle_terminal_event(
            Event::Key(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE)),
            &mut model,
            "/unused",
        )
        .unwrap();
        assert!(changed);
        assert!(model.reload_requested);
        assert!(!model.stopping);
    }

    #[test]
    fn detach_key_exits_from_any_view_without_stopping() {
        for detail in [false, true] {
            let mut model = DashboardModel::loading("test".into(), String::new());
            model.detail = detail;
            let changed = handle_terminal_event(
                Event::Key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::NONE)),
                &mut model,
                "/unused",
            )
            .unwrap();
            assert!(changed);
            assert!(model.detach_requested);
            assert!(!model.stopping);
        }
    }

    #[test]
    fn detail_q_returns_to_dashboard_instead_of_stopping() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model.detail = true;
        let changed = handle_detail_key(
            KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE),
            &mut model,
        )
        .unwrap();
        assert!(changed);
        assert!(!model.detail);
        assert!(!model.stopping);
    }
}
