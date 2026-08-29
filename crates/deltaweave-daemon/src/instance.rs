//! Identity of one running daemon process.

/// Unique live daemon process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonInstance {
    /// Opaque instance identifier returned by Hello.
    pub instance_id: String,
}

impl Default for DaemonInstance {
    fn default() -> Self {
        Self::new()
    }
}

impl DaemonInstance {
    /// Creates a new instance id for this process.
    #[must_use]
    pub fn new() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        Self {
            instance_id: format!("{nanos:x}-{}", std::process::id()),
        }
    }
}
