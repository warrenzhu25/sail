use figment::providers::{Env, Serialized};
use figment::Figment;
use serde::{Deserialize, Serialize};

use crate::error::{CommonError, CommonResult};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CliConfig {
    pub run_python: bool,
}

impl CliConfig {
    pub fn load() -> CommonResult<Self> {
        Figment::from(Serialized::defaults(CliConfig::default()))
            .merge(Env::prefixed("SAIL_INTERNAL__").map(|p| p.as_str().replace("__", ".").into()))
            .extract()
            .map_err(|e| CommonError::InvalidArgument(e.to_string()))
    }
}

/// Environment variables for CLI configuration.
pub struct CliConfigEnv;

impl CliConfigEnv {
    /// Turn Sail CLI into a Python interpreter.
    /// This allows the embedded Python interpreter to fork child Python processes.
    pub const RUN_PYTHON: &'static str = "SAIL_INTERNAL__RUN_PYTHON";
}

static CLI_COMMAND: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();

/// Sets the command (program and leading arguments) that runs the Sail CLI in a new
/// process, when the current executable is not the Sail CLI itself. For example,
/// when Sail runs inside a Python interpreter via PySail, the command is
/// `[<python>, "-m", "pysail"]`. The command can only be set once.
pub fn set_cli_command(command: Vec<String>) {
    let _ = CLI_COMMAND.set(command);
}

/// Returns the command that runs the Sail CLI in a new process, if it has been set.
pub fn cli_command() -> Option<&'static [String]> {
    CLI_COMMAND.get().map(|x| x.as_slice())
}
