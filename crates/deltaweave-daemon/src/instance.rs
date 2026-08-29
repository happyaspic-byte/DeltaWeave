//! Identity and lifecycle of one running daemon process.

/// Unique live daemon process.
#[derive(Clone, Debug)]
pub struct DaemonInstance {
    /// Opaque instance identifier returned by Hello.
    pub instance_id: String,
    stop: tokio::sync::watch::Sender<bool>,
}

impl PartialEq for DaemonInstance {
    fn eq(&self, other: &Self) -> bool {
        self.instance_id == other.instance_id
    }
}

impl Eq for DaemonInstance {}

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
        let (stop, _) = tokio::sync::watch::channel(false);
        Self {
            instance_id: format!("{nanos:x}-{}", std::process::id()),
            stop,
        }
    }

    pub(crate) fn request_stop(&self) {
        self.stop.send_replace(true);
    }

    pub(crate) fn subscribe_stop(&self) -> tokio::sync::watch::Receiver<bool> {
        self.stop.subscribe()
    }
}
