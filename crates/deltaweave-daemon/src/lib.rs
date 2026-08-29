//! Supervisory daemon with authenticated, bounded local IPC.
//!
//! This crate owns configuration, lifecycle, a reusable synchronization loop,
//! and the local control plane. It does not change the iroh/QUIC wire protocol.

#![forbid(unsafe_code)]

mod auth;
mod config;
mod daemon;
mod frame;
mod ipc;
mod lock;
mod platform;
mod state;
mod sync_loop;

pub use auth::AuthToken;
pub use config::{ControlConfig, DaemonConfig, SyncLoopConfig};
pub use daemon::{Daemon, RunningDaemon};
pub use frame::{MAX_FRAME_BYTES, read_frame, write_frame};
pub use ipc::{IpcClient, IpcRequest, IpcServer};
pub use lock::DaemonLock;
pub use state::{Command, CommandResponse, LifecycleState, Snapshot, WatchState};
pub use sync_loop::{SyncLoop, SyncLoopEvent, SyncTask};
