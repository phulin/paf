use std::fmt;
use std::io::{BufRead, BufReader, Write, stdout};
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

use crate::model::{DashboardModel, WireEvent};
use crate::ui;

enum RuntimeEvent {
    Terminal(Event),
    Wire(Box<Result<WireEvent, String>>),
}

struct TerminalGuard;

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

pub fn run(socket_path: &str, label: &str, startup_warning: &str) -> Result<bool> {
    // Establish the push stream before taking over the terminal so startup errors remain plain
    // shell diagnostics.
    let stream = subscribe(socket_path)?;
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

    let mut model = DashboardModel::loading(label.to_owned(), startup_warning.to_owned());
    let mut dirty = true;
    loop {
        if dirty {
            terminal
                .draw(|frame| ui::draw(frame, &model))
                .context("terminal draw failed")?;
        }
        match receiver.recv_timeout(Duration::from_secs(1)) {
            Ok(RuntimeEvent::Wire(event)) if event.is_ok() => {
                let event = event.expect("guarded as successful wire event");
                let complete = event.event == "complete";
                model.apply(event)?;
                dirty = true;
                if complete {
                    terminal.draw(|frame| ui::draw(frame, &model))?;
                    return Ok(model.result.unwrap_or(false));
                }
            }
            Ok(RuntimeEvent::Wire(event)) => {
                bail!(event.expect_err("guarded as failed wire event"))
            }
            Ok(RuntimeEvent::Terminal(event)) => {
                dirty = handle_terminal_event(event, &mut model, socket_path)?;
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
    if model.preparation.is_some() && !stop_key {
        return Ok(false);
    }
    if model.detail {
        return handle_detail_key(key, model);
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
                model.detail = true;
                model.scroll = 0;
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
        model.scroll = model.scroll.saturating_add_signed(delta as i16);
    } else {
        model.move_selection(delta);
    }
    true
}

fn handle_detail_key(key: KeyEvent, model: &mut DashboardModel) -> Result<bool> {
    match key.code {
        KeyCode::Esc | KeyCode::Char('q') => {
            model.detail = false;
            model.scroll = 0;
        }
        KeyCode::Tab => model.cycle_tab(key.modifiers.contains(KeyModifiers::SHIFT)),
        KeyCode::BackTab => model.cycle_tab(true),
        KeyCode::Up | KeyCode::Char('k') => model.scroll = model.scroll.saturating_sub(1),
        KeyCode::Down | KeyCode::Char('j') => model.scroll = model.scroll.saturating_add(1),
        KeyCode::PageUp => model.scroll = model.scroll.saturating_sub(10),
        KeyCode::PageDown => model.scroll = model.scroll.saturating_add(10),
        KeyCode::Home => model.scroll = 0,
        _ => return Ok(false),
    }
    Ok(true)
}

fn send_control(socket_path: &str, command: &str) -> Result<Value> {
    let mut stream = UnixStream::connect(socket_path)
        .with_context(|| format!("could not connect for {command} command"))?;
    serde_json::to_writer(&mut stream, &json!({"command": command}))?;
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
        model.scroll = 9;
        handle_terminal_event(wheel(MouseEventKind::ScrollUp), &mut model, "/unused").unwrap();
        assert_eq!(model.scroll, 6);
        handle_terminal_event(wheel(MouseEventKind::ScrollDown), &mut model, "/unused").unwrap();
        assert_eq!(model.scroll, 9);
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
