//! Python bindings for PySail.
//!
//! This module allows Python to interact with the Sail computation engine
//! by binding the Rust functions and types to Python.
mod catalog;
mod celeborn;
mod cli;
mod flight;
mod globals;
mod spark;

use pyo3::prelude::*;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: sail_mimalloc::MiMalloc = sail_mimalloc::MiMalloc;

/// Creates the `_native` Python module.
/// Registers the version constant, the `main` function,
/// and various submodules.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Worker processes in the `local-process-cluster` mode must run the Sail CLI via
    // the Python interpreter, since the current executable is the Python interpreter.
    let executable: String = m.py().import("sys")?.getattr("executable")?.extract()?;
    if !executable.is_empty() {
        sail_common::config::set_cli_command(vec![
            executable,
            "-m".to_string(),
            "pysail".to_string(),
        ]);
    }
    celeborn::register_module(m)?;
    catalog::register_module(m)?;
    flight::register_module(m)?;
    spark::register_module(m)?;
    m.add_function(wrap_pyfunction!(cli::main, m)?)?;
    m.add("_SAIL_VERSION", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
