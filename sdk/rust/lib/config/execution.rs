//! Global execution buffering configuration.

use microsandbox_protocol::exec::MIN_EXEC_OUTPUT_LIMIT;
use microsandbox_types::ConfigPatch;
use serde::{Deserialize, Serialize};

use crate::{MicrosandboxError, MicrosandboxResult};

//--------------------------------------------------------------------------------------------------
// Constants
//--------------------------------------------------------------------------------------------------

const DEFAULT_EXEC_OUTPUT_LIMIT: u32 = 1024 * 1024;
const DEFAULT_EXEC_INPUT_LIMIT: u32 = 64 * 1024;

//--------------------------------------------------------------------------------------------------
// Types
//--------------------------------------------------------------------------------------------------

/// Host-side defaults for command input and output, shared by the CLI and SDKs.
#[derive(Debug, Clone, Serialize, Deserialize, ConfigPatch)]
#[serde(default)]
#[config_patch(serde)]
pub struct ExecConfig {
    /// Maximum unread output buffered per command, in bytes, including queue overhead.
    ///
    /// Defaults to 1 MiB; `None` disables the limit. A limit must be at least 64 KiB. Stdout and
    /// stderr share it; output retained by callers and transport buffers is excluded.
    pub output_limit: Option<u32>,

    /// Maximum stdin payload bytes queued in the SDK per execution.
    /// Defaults to 64 KiB; `None` disables this buffer limit. A finite limit must
    /// be positive. Transport flow control remains bounded independently.
    pub input_limit: Option<u32>,
}

//--------------------------------------------------------------------------------------------------
// Methods
//--------------------------------------------------------------------------------------------------

impl ExecConfig {
    pub(crate) fn validate(&self) -> MicrosandboxResult<()> {
        if self
            .output_limit
            .is_some_and(|bytes| bytes < MIN_EXEC_OUTPUT_LIMIT)
        {
            return Err(MicrosandboxError::InvalidConfig(format!(
                "exec.output_limit must be at least {MIN_EXEC_OUTPUT_LIMIT} bytes"
            )));
        }
        if self.input_limit == Some(0) {
            return Err(MicrosandboxError::InvalidConfig(
                "exec.input_limit must be positive or null".into(),
            ));
        }
        Ok(())
    }
}

//--------------------------------------------------------------------------------------------------
// Trait Implementations
//--------------------------------------------------------------------------------------------------

impl Default for ExecConfig {
    fn default() -> Self {
        Self {
            output_limit: Some(DEFAULT_EXEC_OUTPUT_LIMIT),
            input_limit: Some(DEFAULT_EXEC_INPUT_LIMIT),
        }
    }
}
