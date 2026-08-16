mod model;
mod runtime;
mod ui;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

#[pyfunction]
#[pyo3(signature = (socket_path, label, startup_warning=""))]
fn run(socket_path: &str, label: &str, startup_warning: &str) -> PyResult<&'static str> {
    runtime::run(socket_path, label, startup_warning)
        .map(|result| match result {
            runtime::TuiExit::Complete(true) => "success",
            runtime::TuiExit::Complete(false) => "failure",
            runtime::TuiExit::Reload => "reload",
        })
        .map_err(|error| PyRuntimeError::new_err(format!("native TUI failed: {error:#}")))
}

#[pymodule]
fn _rust_tui(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(run, module)?)?;
    Ok(())
}
