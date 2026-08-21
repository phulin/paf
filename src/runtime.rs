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
    ChapterRuns {
        chapter: String,
        selected_run_id: Option<String>,
        result: Box<Result<ChapterRuns, String>>,
    },
    PackageRuns {
        package_id: String,
        selected_run_id: Option<String>,
        result: Box<Result<ChapterRuns, String>>,
    },
    Prompt {
        run_id: String,
        result: Result<String, String>,
    },
    Timeline {
        run_id: String,
        result: Box<
            Result<
                (
                    crate::model::Activity,
                    Option<crate::viewport::TimelineRenderCache>,
                ),
                String,
            >,
        >,
    },
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
    spawn_terminal_reader(sender.clone());

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
                    if model.detail_package_id.is_some() {
                        request_package_runs(&mut model, socket_path, None, sender.clone());
                    } else {
                        request_chapter_runs(&mut model, socket_path, None, sender.clone());
                    }
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
                let previous_run = model.trace_run_id().map(str::to_owned);
                dirty = handle_terminal_event_with_sender(
                    event,
                    &mut model,
                    socket_path,
                    sender.clone(),
                )?;
                let selected_run = model.trace_run_id().map(str::to_owned);
                if selected_run != previous_run && selected_run.is_some() {
                    request_timeline_if_needed(&mut model, socket_path, sender.clone());
                }
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
            Ok(RuntimeEvent::ChapterRuns {
                chapter,
                selected_run_id,
                result,
            }) => {
                let details = result.map_err(anyhow::Error::msg)?;
                if model.apply_loaded_chapter_runs(&chapter, selected_run_id.as_deref(), details) {
                    request_prompt_if_needed(&mut model, socket_path, sender.clone());
                    request_timeline_if_needed(&mut model, socket_path, sender.clone());
                }
                dirty = true;
            }
            Ok(RuntimeEvent::PackageRuns {
                package_id,
                selected_run_id,
                result,
            }) => {
                let details = result.map_err(anyhow::Error::msg)?;
                if model.apply_loaded_package_runs(&package_id, selected_run_id.as_deref(), details)
                {
                    request_prompt_if_needed(&mut model, socket_path, sender.clone());
                    request_timeline_if_needed(&mut model, socket_path, sender.clone());
                }
                dirty = true;
            }
            Ok(RuntimeEvent::Prompt { run_id, result }) => {
                match result {
                    Ok(prompt) => model.apply_loaded_prompt(run_id, prompt),
                    Err(error) => model.fail_prompt_load(run_id, error),
                }
                dirty = true;
            }
            Ok(RuntimeEvent::Timeline { run_id, result }) => {
                match *result {
                    Ok((activity, cache)) => {
                        model.apply_full_timeline(run_id.clone(), activity);
                        if model.detail
                            && model.selected_run_id() == Some(run_id.as_str())
                            && let Some(cache) = cache
                        {
                            model.timeline_render_cache = cache;
                        }
                    }
                    Err(error) => model.fail_timeline_load(run_id, error),
                }
                dirty = true;
            }
            Err(RecvTimeoutError::Timeout) => {
                // This is a presentation clock for idle/elapsed labels, never a state poll.
                dirty = model.detail || model.package_detail || model.state.agents.active > 0;
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

fn handle_terminal_event_with_sender(
    event: Event,
    model: &mut DashboardModel,
    socket_path: &str,
    sender: Sender<RuntimeEvent>,
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
    if model.search_query.is_some() {
        return Ok(handle_search_key(key, model));
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
    if key.code == KeyCode::Char('/') {
        model.begin_search();
        return Ok(true);
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
        let previous_tab = model.detail_tab;
        let dirty = handle_detail_key(key, model)?;
        let selected_run = model
            .detail_runs
            .get(model.selected_run)
            .map(|run| run.id.clone());
        if selected_run != previous_run {
            if model.detail_package_id.is_some() {
                request_package_runs(model, socket_path, selected_run.as_deref(), sender.clone());
            } else {
                request_chapter_runs(model, socket_path, selected_run.as_deref(), sender.clone());
            }
        }
        if model.detail_tab != previous_tab || selected_run != previous_run {
            request_prompt_if_needed(model, socket_path, sender);
        }
        return Ok(dirty);
    }
    if model.package_detail {
        if key.code == KeyCode::Enter
            && let Some(package_id) = model.selected_package().map(|value| value.id.clone())
        {
            model.enter_package_run_detail(package_id);
            request_package_runs(model, socket_path, None, sender);
            return Ok(true);
        }
        return Ok(handle_package_key(key, model));
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
                request_chapter_runs(model, socket_path, None, sender);
            }
            Ok(true)
        }
        KeyCode::Char('k') => {
            model.enter_package_detail();
            Ok(true)
        }
        KeyCode::Up => {
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

fn handle_search_key(key: KeyEvent, model: &mut DashboardModel) -> bool {
    match key.code {
        KeyCode::Esc => model.cancel_search(),
        KeyCode::Enter => {
            model.accept_search();
        }
        KeyCode::Backspace => model.pop_search_character(),
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            model.clear_search_query();
        }
        KeyCode::Char(character)
            if !key
                .modifiers
                .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
        {
            model.push_search_character(character);
        }
        _ => return false,
    }
    true
}

#[cfg(test)]
fn handle_terminal_event(
    event: Event,
    model: &mut DashboardModel,
    socket_path: &str,
) -> Result<bool> {
    let (sender, _receiver) = mpsc::channel();
    handle_terminal_event_with_sender(event, model, socket_path, sender)
}

fn request_prompt_if_needed(
    model: &mut DashboardModel,
    socket_path: &str,
    sender: Sender<RuntimeEvent>,
) {
    if model.detail_tab != crate::model::DetailTab::Prompt {
        return;
    }
    let Some(run_id) = model.selected_run_id().map(str::to_owned) else {
        return;
    };
    if !model.begin_prompt_load(&run_id) {
        return;
    }
    let socket_path = socket_path.to_owned();
    thread::spawn(move || {
        let result = send_control_request(
            &socket_path,
            &json!({"command": "run_prompt", "run_id": run_id}),
        )
        .and_then(|response| {
            if let Some(error) = response.get("error") {
                bail!("orchestrator rejected run prompt request: {error}")
            }
            response
                .get("prompt")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .context("run prompt response omitted prompt")
        })
        .map_err(|error| error.to_string());
        let _ = sender.send(RuntimeEvent::Prompt { run_id, result });
    });
}

fn request_timeline_if_needed(
    model: &mut DashboardModel,
    socket_path: &str,
    sender: Sender<RuntimeEvent>,
) {
    let Some(run_id) = model.trace_run_id().map(str::to_owned) else {
        return;
    };
    if !model.begin_timeline_load(&run_id) {
        return;
    }
    let layout_width = model.timeline_render_cache.width;
    let socket_path = socket_path.to_owned();
    thread::spawn(move || {
        let result = send_control_request(
            &socket_path,
            &json!({"command": "run_timeline", "run_id": run_id}),
        )
        .and_then(|response| {
            if let Some(error) = response.get("error") {
                bail!("{error}")
            }
            let activity = serde_json::from_value(
                response
                    .get("activity")
                    .cloned()
                    .context("timeline response omitted activity")?,
            )
            .context("invalid timeline activity")?;
            let cache = (layout_width > 0)
                .then(|| ui::build_timeline_render_cache(Some(&activity), None, layout_width));
            Ok((activity, cache))
        })
        .map_err(|error| error.to_string());
        let _ = sender.send(RuntimeEvent::Timeline {
            run_id,
            result: Box::new(result),
        });
    });
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
    if model.detail || model.package_detail {
        model.scroll_detail(delta as i16);
    } else {
        model.move_selection(delta);
    }
    true
}

fn handle_package_key(key: KeyEvent, model: &mut DashboardModel) -> bool {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('k') => {
            model.leave_package_detail();
        }
        KeyCode::Up | KeyCode::Char('j') => model.move_package_selection(-1),
        KeyCode::Down => model.move_package_selection(1),
        KeyCode::PageUp => model.move_package_selection(-10),
        KeyCode::PageDown => model.move_package_selection(10),
        KeyCode::Home => model.move_package_selection(-(model.package_selected as isize)),
        KeyCode::End => model.move_package_selection(isize::MAX),
        _ => return false,
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

fn request_chapter_runs(
    model: &mut DashboardModel,
    socket_path: &str,
    selected_run_id: Option<&str>,
    sender: Sender<RuntimeEvent>,
) {
    let Some(chapter) = model.selected_row().map(|row| row.unit.id.clone()) else {
        return;
    };
    if !model.begin_chapter_runs_load(&chapter, selected_run_id) {
        return;
    }
    let selected_run_id = selected_run_id.map(str::to_owned);
    let mut request = json!({"command": "chapter_runs", "chapter": chapter});
    if let Some(run_id) = &selected_run_id {
        request["run_id"] = Value::String(run_id.clone());
    }
    let socket_path = socket_path.to_owned();
    thread::spawn(move || {
        let result = send_control_request(&socket_path, &request)
            .and_then(|response| {
                if let Some(error) = response.get("error") {
                    bail!("orchestrator rejected chapter history request: {error}")
                }
                serde_json::from_value(
                    response
                        .get("chapter_runs")
                        .cloned()
                        .context("chapter history response omitted chapter_runs")?,
                )
                .context("invalid chapter history response")
            })
            .map_err(|error| error.to_string());
        let _ = sender.send(RuntimeEvent::ChapterRuns {
            chapter,
            selected_run_id,
            result: Box::new(result),
        });
    });
}

fn request_package_runs(
    model: &mut DashboardModel,
    socket_path: &str,
    selected_run_id: Option<&str>,
    sender: Sender<RuntimeEvent>,
) {
    let Some(package_id) = model.detail_package_id.clone() else {
        return;
    };
    if !model.begin_package_runs_load(&package_id, selected_run_id) {
        return;
    }
    let selected_run_id = selected_run_id.map(str::to_owned);
    let mut request = json!({"command": "package_runs", "package_id": package_id});
    if let Some(run_id) = &selected_run_id {
        request["run_id"] = Value::String(run_id.clone());
    }
    let socket_path = socket_path.to_owned();
    thread::spawn(move || {
        let result = send_control_request(&socket_path, &request)
            .and_then(|response| {
                if let Some(error) = response.get("error") {
                    bail!("orchestrator rejected package history request: {error}")
                }
                serde_json::from_value(
                    response
                        .get("package_runs")
                        .cloned()
                        .context("package history response omitted package_runs")?,
                )
                .context("invalid package history response")
            })
            .map_err(|error| error.to_string());
        let _ = sender.send(RuntimeEvent::PackageRuns {
            package_id,
            selected_run_id,
            result: Box::new(result),
        });
    });
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
    use std::os::unix::net::UnixListener;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
    fn opening_agent_detail_does_not_wait_for_chapter_history() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let socket_path = std::env::temp_dir().join(format!(
            "paf-tui-open-agent-{}-{nonce}.sock",
            std::process::id()
        ));
        let listener = UnixListener::bind(&socket_path).unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = String::new();
            BufReader::new(stream.try_clone().unwrap())
                .read_line(&mut request)
                .unwrap();
            assert!(request.contains("\"command\":\"chapter_runs\""));
            thread::sleep(Duration::from_millis(500));
            stream
                .write_all(b"{\"chapter_runs\":{\"work_unit_id\":\"book/chapter-01\",\"runs\":[],\"selected_run_id\":null,\"activity\":null}}\n")
                .unwrap();
        });

        let mut model = DashboardModel::loading("test".into(), String::new());
        model.preparation = None;
        model.state.work_units.push(WorkUnit {
            id: "book/chapter-01".into(),
            ..WorkUnit::default()
        });
        model.state.tasks.insert(
            "book/chapter-01:review".into(),
            Task {
                work_unit_id: "book/chapter-01".into(),
                stage: "review".into(),
                ..Task::default()
            },
        );
        let (sender, receiver) = mpsc::channel();

        let started = Instant::now();
        let changed = handle_terminal_event_with_sender(
            Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)),
            &mut model,
            socket_path.to_str().unwrap(),
            sender,
        )
        .unwrap();
        let elapsed = started.elapsed();

        assert!(changed);
        assert!(model.detail);
        assert!(
            elapsed < Duration::from_millis(250),
            "opening the agent view waited for history I/O: {elapsed:?}"
        );
        assert!(matches!(
            receiver.recv_timeout(Duration::from_secs(2)).unwrap(),
            RuntimeEvent::ChapterRuns { .. }
        ));
        server.join().unwrap();
        std::fs::remove_file(socket_path).unwrap();
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
    fn slash_opens_search_and_typing_captures_dashboard_shortcuts() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model.preparation = None;

        assert!(
            handle_terminal_event(
                Event::Key(KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE)),
                &mut model,
                "/unused",
            )
            .unwrap()
        );
        for character in ['d', 'o', 'c'] {
            handle_terminal_event(
                Event::Key(KeyEvent::new(KeyCode::Char(character), KeyModifiers::NONE)),
                &mut model,
                "/unused",
            )
            .unwrap();
        }

        assert_eq!(model.search_query.as_deref(), Some("doc"));
        assert!(!model.detach_requested);
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
    fn package_key_opens_and_closes_the_package_view() {
        let mut model = DashboardModel::loading("test".into(), String::new());
        model.preparation = None;
        let package_key = Event::Key(KeyEvent::new(KeyCode::Char('k'), KeyModifiers::NONE));

        assert!(handle_terminal_event(package_key.clone(), &mut model, "/unused").unwrap());
        assert!(model.package_detail);
        assert!(!model.detail);
        assert!(handle_terminal_event(package_key, &mut model, "/unused").unwrap());
        assert!(!model.package_detail);
        assert!(!model.stopping);
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
