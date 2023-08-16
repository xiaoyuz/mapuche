use crate::rocks::Result as RocksResult;
use crate::Frame;
use serde::{Deserialize, Serialize};

/// Represents an "unknown" command. This is not a real `Redis` command.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    /// Create a new `Unknown` command which responds to unknown commands
    /// issued by clients
    pub(crate) fn new(key: impl ToString) -> Unknown {
        Unknown {
            command_name: key.to_string(),
        }
    }

    /// Responds to the client, indicating the command is not recognized.
    ///
    /// This usually means the command is not yet implemented by `mapuche`.
    pub(crate) async fn apply(&self) -> RocksResult<Frame> {
        let response = Frame::Error(format!("ERR unknown command '{}'", self.command_name));
        Ok(response)
    }
}
