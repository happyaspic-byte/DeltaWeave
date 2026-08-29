//! Length-delimited JSON IPC over a Unix socket or Windows named pipe.

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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

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

/// Connects and completes Hello against a live daemon endpoint.
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
    #[cfg(unix)]
    {
        let mut stream = tokio::net::UnixStream::connect(path)
            .await
            .with_context(|| format!("failed to connect to {}", path.display()))?;
        exchange(&mut stream, command).await
    }
    #[cfg(windows)]
    {
        let pipe_name = windows_impl::pipe_name_from(path);
        let mut client = windows_impl::wait_for_pipe(&pipe_name).await?;
        exchange(&mut client, command).await
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (path, command);
        bail!("daemon IPC is not implemented on this platform")
    }
}

async fn exchange<S>(stream: &mut S, command: Command) -> Result<CommandResult>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = Request {
        protocol_version: ProtocolVersion {
            major: PROTOCOL_VERSION_MAJOR,
            minor: PROTOCOL_VERSION_MINOR,
        },
        request_id: "client".into(),
        command,
    };
    write_frame(stream, &request).await?;
    let response: Response = read_frame(stream).await?;
    response
        .result
        .map_err(|error| anyhow::anyhow!(error.message))
}

async fn handle_connection<S>(instance: DaemonInstance, mut stream: S) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
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

fn command_response(request_id: String, result: Result<CommandResult>) -> Response {
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

async fn write_frame<S, T>(stream: &mut S, value: &T) -> Result<()>
where
    S: AsyncWrite + Unpin,
    T: Serialize,
{
    let json = serde_json::to_vec(value)?;
    ensure!(json.len() <= MAX_FRAME, "frame too large");
    let len = u32::try_from(json.len()).context("frame length overflows u32")?;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&json).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_frame<S, T>(stream: &mut S) -> Result<T>
where
    S: AsyncRead + Unpin,
    T: serde::de::DeserializeOwned,
{
    let mut len_buf = [0_u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = usize::try_from(u32::from_le_bytes(len_buf))?;
    ensure!(len > 0 && len <= MAX_FRAME, "invalid frame length {len}");
    let mut buf = vec![0_u8; len];
    stream.read_exact(&mut buf).await?;
    serde_json::from_slice(&buf).context("invalid IPC JSON")
}

#[cfg(unix)]
mod unix_impl {
    use super::*;

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
        let listener = match tokio::net::UnixListener::bind(&path) {
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
}

#[cfg(unix)]
pub use unix_impl::{serve_unix, try_bind_unix, wait_until_exists};

#[cfg(windows)]
mod windows_impl {
    use super::*;
    use tokio::net::windows::named_pipe::ServerOptions;

    pub(super) fn pipe_name_from(path: &Path) -> String {
        let text = path.to_string_lossy();
        if text.starts_with(r"\\.\pipe\") {
            text.into_owned()
        } else {
            format!(r"\\.\pipe\{text}")
        }
    }

    pub(super) async fn wait_for_pipe(
        name: &str,
    ) -> Result<tokio::net::windows::named_pipe::NamedPipeClient> {
        use tokio::net::windows::named_pipe::ClientOptions;

        const ERROR_PIPE_BUSY: i32 = 231;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match ClientOptions::new().open(name) {
                Ok(client) => return Ok(client),
                Err(err)
                    if err.kind() == std::io::ErrorKind::NotFound
                        || err.raw_os_error() == Some(ERROR_PIPE_BUSY) => {}
                Err(err) => return Err(err).with_context(|| format!("failed to open {name}")),
            }
            ensure!(
                Instant::now() < deadline,
                "named pipe {name} did not appear"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Serves IPC on a named pipe until Stop is accepted.
    ///
    /// `first_pipe_instance(true)` makes a second daemon fail with
    /// PermissionDenied instead of silently attaching, remote clients are
    /// rejected, and the pipe DACL is rewritten to allow only the creating
    /// user; the daemon never impersonates a client.
    fn create_pipe(
        name: &str,
        first: bool,
    ) -> Result<tokio::net::windows::named_pipe::NamedPipeServer> {
        let server = ServerOptions::new()
            .first_pipe_instance(first)
            .reject_remote_clients(true)
            .create(name)?;
        restrict_to_current_user(&server)?;
        Ok(server)
    }

    fn restrict_to_current_user(
        server: &tokio::net::windows::named_pipe::NamedPipeServer,
    ) -> Result<()> {
        use std::os::windows::io::AsRawHandle;

        use windows_acl::acl::ACL;
        use windows_acl::helper::{current_user, name_to_sid, string_to_sid};

        let mut acl = ACL::from_object_handle(server.as_raw_handle(), false)
            .map_err(|code| anyhow::anyhow!("failed to read pipe ACL ({code})"))?;
        for well_known in ["S-1-1-0", "S-1-5-11", "S-1-5-32-545"] {
            if let Ok(sid) = string_to_sid(well_known) {
                let _ = acl.remove(sid.as_ptr().cast(), None, None);
            }
        }
        let username = current_user().context("failed to resolve current Windows user")?;
        let user_sid = name_to_sid(&username, None)
            .map_err(|code| anyhow::anyhow!("failed to resolve current user SID ({code})"))?;
        acl.allow(user_sid.as_ptr().cast(), false, 0x001F_01FF)
            .map_err(|code| anyhow::anyhow!("failed to restrict pipe to current user ({code})"))?;
        Ok(())
    }

    pub async fn serve_windows(instance: DaemonInstance, path: PathBuf) -> Result<()> {
        let name = pipe_name_from(&path);
        let mut server = create_pipe(&name, true)?;
        let mut stop = instance.subscribe_stop();
        loop {
            tokio::select! {
                changed = stop.changed() => {
                    changed.context("daemon stop channel closed")?;
                    if *stop.borrow() {
                        return Ok(());
                    }
                }
                connected = server.connect() => {
                    connected?;
                    let connected_server = std::mem::replace(&mut server, create_pipe(&name, false)?);
                    let instance = instance.clone();
                    tokio::spawn(async move {
                        let _ = handle_connection(instance, connected_server).await;
                    });
                }
            }
        }
    }
}

#[cfg(windows)]
pub use windows_impl::serve_windows;

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

    #[cfg(windows)]
    #[test]
    fn windows_pipe_name_uses_local_namespace() {
        assert_eq!(
            super::windows_impl::pipe_name_from(Path::new(r"\\.\pipe\deltaweave")),
            r"\\.\pipe\deltaweave"
        );
        assert_eq!(
            super::windows_impl::pipe_name_from(Path::new("deltaweave")),
            r"\\.\pipe\deltaweave"
        );
    }
}
