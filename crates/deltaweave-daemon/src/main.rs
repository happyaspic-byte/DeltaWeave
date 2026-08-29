//! Foreground `deltaweave-daemon` process.

#![forbid(unsafe_code)]

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    deltaweave_daemon::run().await
}
