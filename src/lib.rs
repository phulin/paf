mod model;
mod runtime;
mod ui;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

#[pyfunction]
#[pyo3(signature = (
    socket_path,
    label,
    startup_warning="",
    initial_agent_view=None,
    initial_detail_tab=None,
))]
fn run(
    socket_path: &str,
    label: &str,
    startup_warning: &str,
    initial_agent_view: Option<&str>,
    initial_detail_tab: Option<&str>,
) -> PyResult<String> {
    runtime::run(
        socket_path,
        label,
        startup_warning,
        initial_agent_view,
        initial_detail_tab,
    )
    .map(|result| match result {
        runtime::TuiExit::Complete(true) => "success".into(),
        runtime::TuiExit::Complete(false) => "failure".into(),
        runtime::TuiExit::Detach => "detach".into(),
        runtime::TuiExit::Reload(agent_view) => serde_json::json!({
            "action": "reload",
            "agent_view": agent_view.as_ref().map(|view| &view.work_unit_id),
            "detail_tab": agent_view.as_ref().map(|view| view.detail_tab),
        })
        .to_string(),
    })
    .map_err(|error| PyRuntimeError::new_err(format!("native TUI failed: {error:#}")))
}

#[pymodule]
fn _rust_tui(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(run, module)?)?;
    Ok(())
}
