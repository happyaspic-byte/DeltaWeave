//! Length-delimited JSON IPC over a Unix socket.

use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use deltaweave_daemon_api::{
    Command, CommandResult, ErrorBody, ErrorCode, PROTOCOL_VERSION_MAJOR, PROTOCOL_VERSION_MINOR,
    ProtocolVersion, Request, Response,
};
use serde::Serialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};

use crate::DaemonInstance;

const MAX_FRAME: usize = 1024 * 1024;

/// Successful Hello payload used by clients and tests.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HelloReply {
    /// Daemon instance id.
    pub instance_id: String,
    /// Negotiated protocol version.
    pub protocol_version: ProtocolVersion,
}

/// Binds a Unix socket or fails if another daemon already owns the path.
pub fn try_bind_unix(path: impl AsRef<Path>) -> Result<std::os::unix::net::UnixListener> {
    let path = path.as_ref();
    match std::os::unix::net::UnixListener::bind(path) {
        Ok(listener) => Ok(listener),
        Err(err) => {
            if path.exists() {
                bail!("already running");
            }
            Err(err).with_context(|| format!("failed to bind {}", path.display()))
        }
    }
}

/// Accepts IPC clients on `path` until Stop is accepted.
pub async fn serve_unix(instance: DaemonInstance, path: PathBuf) -> Result<()> {
    let listener = match UnixListener::bind(&path) {
        Ok(listener) => listener,
        Err(_) if path.exists() => bail!("already running"),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to bind {}", path.display()));
        }
    };
    let mut stop = instance.subscribe_stop();
    loop {
        tokio::select! {
            changed = stop.changed() => {
                changed.context("daemon stop channel closed")?;
                if *stop.borrow() {
                    return Ok(());
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let instance = instance.clone();
                tokio::spawn(async move {
                    let _ = handle_connection(instance, stream).await;
                });
            }
        }
    }
}

/// Waits until `path` exists or panics after five seconds.
pub async fn wait_until_exists(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "socket {} did not appear",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Connects and completes Hello against a live daemon socket.
pub async fn connect_and_hello(path: &Path) -> Result<HelloReply> {
    match send_command(path, Command::Hello).await? {
        CommandResult::Hello {
            instance_id,
            protocol_version,
        } => Ok(HelloReply {
            instance_id,
            protocol_version,
        }),
        _ => bail!("unexpected hello reply"),
    }
}

/// Sends one command and returns its successful payload.
pub async fn send_command(path: &Path, command: Command) -> Result<CommandResult> {
    let mut stream = UnixStream::connect(path)
        .await
        .with_context(|| format!("failed to connect to {}", path.display()))?;
    let request = Request {
        protocol_version: ProtocolVersion {
            major: PROTOCOL_VERSION_MAJOR,
            minor: PROTOCOL_VERSION_MINOR,
        },
        request_id: "client".into(),
        command,
    };
    write_frame(&mut stream, &request).await?;
    let response: Response = read_frame(&mut stream).await?;
    response
        .result
        .map_err(|error| anyhow::anyhow!(error.message))
}

async fn handle_connection(instance: DaemonInstance, mut stream: UnixStream) -> Result<()> {
    loop {
        let request: Request = match read_frame(&mut stream).await {
            Ok(request) => request,
            Err(_) => return Ok(()),
        };
        let response = dispatch(&instance, request);
        let stopping = matches!(
            response.result,
            Ok(CommandResult::Accepted { ref id }) if id == "daemon"
        );
        write_frame(&mut stream, &response).await?;
        if stopping {
            instance.request_stop();
            return Ok(());
        }
    }
}

fn command_response(request_id: String, result: anyhow::Result<CommandResult>) -> Response {
    Response {
        request_id,
        result: result.map_err(|error| ErrorBody {
            code: ErrorCode::InvalidRequest,
            message: error.to_string(),
        }),
    }
}

fn dispatch(instance: &DaemonInstance, request: Request) -> Response {
    let negotiated = request
        .protocol_version
        .negotiate(PROTOCOL_VERSION_MAJOR, PROTOCOL_VERSION_MINOR);
    match (negotiated, request.command) {
        (Err(error), _) => Response {
            request_id: request.request_id,
            result: Err(error),
        },
        (Ok(protocol_version), Command::Hello) => Response {
            request_id: request.request_id,
            result: Ok(CommandResult::Hello {
                protocol_version,
                instance_id: instance.instance_id.clone(),
            }),
        },
        (
            Ok(_),
            Command::CreateJob {
                preview_confirmed: false,
                ..
            },
        ) => Response {
            request_id: request.request_id,
            result: Err(ErrorBody {
                code: ErrorCode::InvalidRequest,
                message: "preview confirmation required".into(),
            }),
        },
        (Ok(_), Command::ListJobs) => command_response(request.request_id, instance.list_jobs()),
        (
            Ok(_),
            Command::CreateJob {
                name,
                local_root,
                peer_endpoint_id,
                direction,
                preview_confirmed: true,
            },
        ) => command_response(
            request.request_id,
            instance.create_job(name, local_root, peer_endpoint_id, direction),
        ),
        (Ok(_), Command::PauseJob { id }) => {
            command_response(request.request_id, instance.set_job_paused(&id, true))
        }
        (Ok(_), Command::ResumeJob { id }) => {
            command_response(request.request_id, instance.set_job_paused(&id, false))
        }
        (Ok(_), Command::CancelJob { id }) => {
            command_response(request.request_id, instance.cancel_job(&id))
        }
        (Ok(_), Command::ListConflicts { id }) => {
            command_response(request.request_id, instance.list_job_conflicts(&id))
        }
        (Ok(_), Command::Stop) => Response {
            request_id: request.request_id,
            result: Ok(CommandResult::Accepted {
                id: "daemon".into(),
            }),
        },
        (Ok(_), Command::ResolveConflict { id, path, action }) => Response {
            request_id: request.request_id,
            result: instance
                .resolve_job_conflict(&id, &path, action)
                .map_err(|error| ErrorBody {
                    code: ErrorCode::InvalidRequest,
                    message: error.to_string(),
                }),
        },
        (Ok(_), _) => Response {
            request_id: request.request_id,
            result: Err(ErrorBody {
                code: ErrorCode::InvalidRequest,
                message: "command not implemented".into(),
            }),
        },
    }
}

async fn write_frame<T: Serialize>(stream: &mut UnixStream, value: &T) -> Result<()> {
    let json = serde_json::to_vec(value)?;
    ensure!(json.len() <= MAX_FRAME, "frame too large");
    let len = u32::try_from(json.len()).context("frame length overflows u32")?;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&json).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_frame<T: serde::de::DeserializeOwned>(stream: &mut UnixStream) -> Result<T> {
    let mut len_buf = [0_u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = usize::try_from(u32::from_le_bytes(len_buf))?;
    ensure!(len > 0 && len <= MAX_FRAME, "invalid frame length {len}");
    let mut buf = vec![0_u8; len];
    stream.read_exact(&mut buf).await?;
    serde_json::from_slice(&buf).context("invalid IPC JSON")
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltaweave_daemon_api::Direction;

    #[test]
    fn create_job_requires_preview_confirmation() {
        let response = dispatch(
            &DaemonInstance::new(),
            Request {
                protocol_version: ProtocolVersion {
                    major: PROTOCOL_VERSION_MAJOR,
                    minor: PROTOCOL_VERSION_MINOR,
                },
                request_id: "create".into(),
                command: Command::CreateJob {
                    name: "ISOs".into(),
                    local_root: "/tmp/x".into(),
                    peer_endpoint_id: "aa".repeat(32),
                    direction: Direction::Bidirectional,
                    preview_confirmed: false,
                },
            },
        );

        let error = response.result.unwrap_err();
        assert_eq!(error.code, ErrorCode::InvalidRequest);
        assert_eq!(error.message, "preview confirmation required");
    }

    #[test]
    fn resolve_conflict_keep_remote_replaces_canonical() {
        use std::sync::Arc;

        use crate::{ConfigStore, JobConfig};
        use deltaweave_daemon_api::ConflictAction;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("sync");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("file.txt"), b"local").unwrap();
        std::fs::write(root.join("file.conflict-abcdef123456.txt"), b"remote").unwrap();

        let store = ConfigStore::open(dir.path().join("config.redb")).unwrap();
        store
            .insert_job(&JobConfig {
                id: "job-a".into(),
                name: "ISOs".into(),
                local_root: root.clone(),
                state_root: dir.path().join("state"),
                peer_endpoint_id: "aa".repeat(32),
                direction: crate::Direction::Bidirectional,
                continuous: true,
                paused: false,
            })
            .unwrap();

        let response = dispatch(
            &DaemonInstance::with_config(Some(Arc::new(store))),
            Request {
                protocol_version: ProtocolVersion {
                    major: PROTOCOL_VERSION_MAJOR,
                    minor: PROTOCOL_VERSION_MINOR,
                },
                request_id: "resolve".into(),
                command: Command::ResolveConflict {
                    id: "job-a".into(),
                    path: "file.txt".into(),
                    action: ConflictAction::KeepRemote,
                },
            },
        );

        assert_eq!(
            response.result.unwrap(),
            CommandResult::Accepted {
                id: "file.txt".into()
            }
        );
        assert_eq!(std::fs::read(root.join("file.txt")).unwrap(), b"remote");
    }

    fn request(command: Command) -> Request {
        Request {
            protocol_version: ProtocolVersion {
                major: PROTOCOL_VERSION_MAJOR,
                minor: PROTOCOL_VERSION_MINOR,
            },
            request_id: "cmd".into(),
            command,
        }
    }

    #[test]
    fn create_job_persists_when_preview_confirmed() {
        use std::sync::Arc;

        use crate::{ConfigStore, JobSupervisor};

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("sync");
        std::fs::create_dir_all(&root).unwrap();
        let store = Arc::new(ConfigStore::open(dir.path().join("config.redb")).unwrap());
        let instance = DaemonInstance::with_runtime(Some(store.clone()), JobSupervisor::new());

        let created = dispatch(
            &instance,
            request(Command::CreateJob {
                name: "ISOs".into(),
                local_root: root.to_string_lossy().into(),
                peer_endpoint_id: "aa".repeat(32),
                direction: Direction::Bidirectional,
                preview_confirmed: true,
            }),
        )
        .result
        .unwrap();
        let CommandResult::Accepted { id } = created else {
            panic!("expected Accepted, got {created:?}");
        };

        let listed = dispatch(&instance, request(Command::ListJobs))
            .result
            .unwrap();
        let CommandResult::Jobs { jobs } = listed else {
            panic!("expected Jobs, got {listed:?}");
        };
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, id);
        assert_eq!(jobs[0].name, "ISOs");
        assert_eq!(jobs[0].peer_endpoint_id, "aa".repeat(32));
        assert!(!jobs[0].paused);
    }

    #[test]
    fn pause_and_resume_job_update_supervisor() {
        use std::sync::Arc;

        use crate::{ConfigStore, JobSupervisor};

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("sync");
        std::fs::create_dir_all(&root).unwrap();
        let store = Arc::new(ConfigStore::open(dir.path().join("config.redb")).unwrap());
        let supervisor = JobSupervisor::new();
        let instance = DaemonInstance::with_runtime(Some(store), supervisor);

        let created = dispatch(
            &instance,
            request(Command::CreateJob {
                name: "ISOs".into(),
                local_root: root.to_string_lossy().into(),
                peer_endpoint_id: "aa".repeat(32),
                direction: Direction::Bidirectional,
                preview_confirmed: true,
            }),
        )
        .result
        .unwrap();
        let CommandResult::Accepted { id } = created else {
            panic!("expected Accepted, got {created:?}");
        };

        let paused = dispatch(&instance, request(Command::PauseJob { id: id.clone() }))
            .result
            .unwrap();
        assert_eq!(paused, CommandResult::Accepted { id: id.clone() });
        let listed = dispatch(&instance, request(Command::ListJobs))
            .result
            .unwrap();
        let CommandResult::Jobs { jobs } = listed else {
            panic!("expected Jobs, got {listed:?}");
        };
        assert!(jobs[0].paused);

        let resumed = dispatch(&instance, request(Command::ResumeJob { id: id.clone() }))
            .result
            .unwrap();
        assert_eq!(resumed, CommandResult::Accepted { id });
        let listed = dispatch(&instance, request(Command::ListJobs))
            .result
            .unwrap();
        let CommandResult::Jobs { jobs } = listed else {
            panic!("expected Jobs, got {listed:?}");
        };
        assert!(!jobs[0].paused);
    }
}
