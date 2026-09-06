//! Local-only authenticated browser management of real DeltaWeave engines.
#![forbid(unsafe_code)]

mod assets;
mod auth;
mod operations;
mod routes;

use anyhow::{Context, Result, ensure};
use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use deltaweave_control::Manager;
use iroh::EndpointId;
use operations::{Command, Prepared};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, mpsc},
    task::JoinHandle,
};

#[derive(Clone)]
pub struct Config {
    pub root: PathBuf,
    pub state: PathBuf,
    pub identity: Option<PathBuf>,
    pub bind: SocketAddr,
    pub peer_bind: SocketAddr,
}

/// HTTP listener and private management configuration for the multi-folder console.
#[derive(Clone, Debug)]
pub struct WebConfig {
    /// Socket to bind. Unspecified addresses require explicit allowed hosts.
    pub bind: SocketAddr,
    /// Private persistent management state and administrator access key directory.
    pub data_dir: PathBuf,
    /// Additional exact HTTP host names or authorities; wildcard values are rejected.
    pub allowed_hosts: Vec<String>,
}

/// Runs the embedded multi-folder management server until shutdown.
pub async fn run(config: WebConfig) -> Result<()> {
    ensure!(
        assets::available(),
        "browser assets are missing; run npm --prefix web run build and rebuild DeltaWeave"
    );
    let hosts = routes::HostPolicy::new(config.bind, &config.allowed_hosts)?;
    let listener = tokio::net::TcpListener::bind(config.bind)
        .await
        .context("bind management HTTP listener")?;
    let manager = Manager::open(config.data_dir.clone()).await?;
    let (auth, _bootstrap) = match auth::Auth::open(&config.data_dir) {
        Ok(auth) => auth,
        Err(error) => {
            manager.shutdown().await?;
            return Err(error);
        }
    };
    let state = Arc::new(routes::AppState {
        manager: Arc::clone(&manager),
        auth: Arc::new(auth),
        hosts,
    });
    let app = routes::router(Arc::clone(&state));
    println!(
        "DeltaWeave web listening on http://{}",
        listener.local_addr()?
    );
    println!(
        "Administrator access key file: {}",
        config.data_dir.join("admin-token").display()
    );
    let served = axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            state.auth.revoke_all();
        })
        .await;
    let shutdown = manager.shutdown().await;
    served.context("management HTTP server failed")?;
    shutdown
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}
#[derive(Clone, Serialize)]
pub struct Activity {
    pub id: u64,
    pub kind: String,
    pub status: String,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub error: Option<String>,
}
#[derive(Clone, Serialize)]
pub struct Snapshot {
    pub version: String,
    pub root: String,
    pub endpoint_id: String,
    pub phase: String,
    pub receiver: Option<Value>,
    pub scan: Option<Value>,
    pub sync: Option<Value>,
    pub activity: Vec<Activity>,
}
struct Inner {
    state: Snapshot,
    busy: bool,
    closing: bool,
    next_id: u64,
}
struct Shared {
    inner: Mutex<Inner>,
}
#[derive(Clone)]
pub struct WebApp {
    shared: Arc<Shared>,
    token: Arc<String>,
    commands: mpsc::UnboundedSender<Command>,
    worker: Arc<Mutex<Option<JoinHandle<Result<()>>>>>,
}
impl WebApp {
    pub async fn new(config: Config) -> Result<Self> {
        let config = Arc::new(tokio::task::spawn_blocking(move || Prepared::new(config)).await??);
        let shared = Arc::new(Shared {
            inner: Mutex::new(Inner {
                state: Snapshot {
                    version: env!("CARGO_PKG_VERSION").into(),
                    root: config.root.display().to_string(),
                    endpoint_id: config.secret.public().to_string(),
                    phase: "idle".into(),
                    receiver: None,
                    scan: None,
                    sync: None,
                    activity: Vec::new(),
                },
                busy: false,
                closing: false,
                next_id: 1,
            }),
        });
        // A fresh OS-random secret independent of the persistent endpoint key.
        let token = Arc::new(hex::encode(iroh::SecretKey::generate().to_bytes()));
        let (commands, rx) = mpsc::unbounded_channel();
        let worker = tokio::spawn(operations::worker(config, Arc::clone(&shared), rx));
        Ok(Self {
            shared,
            token,
            commands,
            worker: Arc::new(Mutex::new(Some(worker))),
        })
    }
    pub fn token(&self) -> &str {
        &self.token
    }
    pub async fn snapshot(&self) -> Snapshot {
        self.shared.inner.lock().await.state.clone()
    }
    /// `authority` must be the actual HTTP listener's SocketAddr display value.
    pub fn router(&self, authority: &str) -> Router {
        let security = Security {
            authority: authority.into(),
            token: Arc::clone(&self.token),
        };
        Router::new()
            .route(
                "/",
                get(|| async { Html(include_str!("../ui/index.html")) }),
            )
            .route(
                "/styles.css",
                get(|| async {
                    (
                        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
                        include_str!("../ui/styles.css"),
                    )
                }),
            )
            .route(
                "/app.js",
                get(|| async {
                    (
                        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                        include_str!("../ui/app.js"),
                    )
                }),
            )
            .route(
                "/model.js",
                get(|| async {
                    (
                        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                        include_str!("../ui/model.js"),
                    )
                }),
            )
            .route("/api/state", get(state))
            .route("/api/scan", post(scan))
            .route("/api/sync", post(sync))
            .route("/api/receiver/start", post(start))
            .route("/api/receiver/stop", post(stop))
            .fallback(|| async { ApiError::new(StatusCode::NOT_FOUND, "route not found") })
            .method_not_allowed_fallback(|| async {
                ApiError::new(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
            })
            .with_state(self.clone())
            .layer(middleware::from_fn_with_state(security, secure))
    }
    /// Reject new operations, join accepted work, then close and drain receiver.
    pub async fn shutdown(&self) -> Result<()> {
        let mut worker = self.worker.lock().await;
        if let Some(handle) = worker.take() {
            self.shared.inner.lock().await.closing = true;
            let _ = self.commands.send(Command::Shutdown);
            handle
                .await
                .context("operation worker exited unexpectedly")??;
        }
        Ok(())
    }
    async fn accept(&self, command: Command) -> std::result::Result<Response, ApiError> {
        let mut inner = self.shared.inner.lock().await;
        if inner.closing {
            return Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "server is shutting down",
            ));
        }
        let allowed = match &command {
            Command::Stop => matches!(
                inner.state.phase.as_str(),
                "receiving" | "stopping_receiver"
            ),
            _ => inner.state.phase == "idle",
        };
        if inner.busy || !allowed {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "another operation is active; stop the receiver before scanning or synchronizing",
            ));
        }
        let activity = Activity {
            id: inner.next_id,
            kind: command.kind().into(),
            status: "running".into(),
            started_at_ms: now_ms(),
            finished_at_ms: None,
            error: None,
        };
        // Enqueue while holding the snapshot lock: worker cannot complete before
        // this response's accepted snapshot has been cloned.
        let phase = command.phase();
        self.commands.send(command).map_err(|_| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "operation worker unavailable",
            )
        })?;
        inner.next_id += 1;
        inner.busy = true;
        inner.state.phase = phase.into();
        inner.state.activity.insert(0, activity);
        inner.state.activity.truncate(20);
        Ok((StatusCode::ACCEPTED, axum::Json(inner.state.clone())).into_response())
    }
}
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[derive(Clone)]
struct Security {
    authority: String,
    token: Arc<String>,
}
async fn secure(State(security): State<Security>, request: Request, next: Next) -> Response {
    let headers = request.headers();
    let hosts = headers.get_all(header::HOST).iter().collect::<Vec<_>>();
    let origins = headers.get_all(header::ORIGIN).iter().collect::<Vec<_>>();
    let expected_origin = format!("http://{}", security.authority);
    let host_ok = hosts.len() == 1 && hosts[0].to_str().is_ok_and(|h| h == security.authority);
    let origin_ok = origins.is_empty()
        || (origins.len() == 1 && origins[0].to_str().is_ok_and(|o| o == expected_origin));
    let is_api = request.uri().path() == "/api" || request.uri().path().starts_with("/api/");
    let authorization = headers
        .get_all(header::AUTHORIZATION)
        .iter()
        .collect::<Vec<_>>();
    let expected = format!("Bearer {}", security.token);
    let authorized = authorization.len() == 1
        && authorization[0]
            .to_str()
            .is_ok_and(|v| constant_time_equal(v.as_bytes(), expected.as_bytes()));
    let mut response = if !host_ok || !origin_ok {
        ApiError::new(StatusCode::FORBIDDEN, "unexpected Host or Origin").into_response()
    } else if is_api && !authorized {
        ApiError::new(StatusCode::UNAUTHORIZED, "valid session token required").into_response()
    } else {
        next.run(request).await
    };
    for (name, value) in [
        ("cache-control", "no-store"),
        (
            "content-security-policy",
            "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; font-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
        ),
        ("x-content-type-options", "nosniff"),
        ("x-frame-options", "DENY"),
        ("referrer-policy", "no-referrer"),
    ] {
        response
            .headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }
    response
}
fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}
struct ApiError {
    status: StatusCode,
    error: String,
    field: Option<&'static str>,
}
impl ApiError {
    fn new(status: StatusCode, error: impl Into<String>) -> Self {
        Self {
            status,
            error: error.into(),
            field: None,
        }
    }
    fn field(field: &'static str, error: &str) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            error: error.into(),
            field: Some(field),
        }
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut body = json!({"error":self.error});
        if let Some(field) = self.field {
            body["field"] = field.into();
        }
        (self.status, axum::Json(body)).into_response()
    }
}
async fn payload(request: Request) -> std::result::Result<Value, ApiError> {
    if !request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|v| v.trim() == "application/json")
        })
    {
        return Err(ApiError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Content-Type must be application/json",
        ));
    }
    let bytes = to_bytes(request.into_body(), 8192)
        .await
        .map_err(|_| ApiError::new(StatusCode::PAYLOAD_TOO_LARGE, "JSON body exceeds 8 KiB"))?;
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::new(StatusCode::BAD_REQUEST, "malformed JSON body"))?;
    if !value.is_object() {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "JSON body must be an object",
        ));
    }
    Ok(value)
}
fn peer(value: &Value, own: &str) -> std::result::Result<EndpointId, ApiError> {
    let peer = value
        .get("peer_id")
        .and_then(Value::as_str)
        .and_then(|v| v.parse::<EndpointId>().ok())
        .ok_or_else(|| ApiError::field("peer_id", "enter a valid peer endpoint ID"))?;
    if peer.to_string() == own {
        return Err(ApiError::field(
            "peer_id",
            "peer must be a different device",
        ));
    }
    Ok(peer)
}
async fn state(State(app): State<WebApp>) -> axum::Json<Snapshot> {
    axum::Json(app.snapshot().await)
}
async fn scan(
    State(app): State<WebApp>,
    request: Request,
) -> std::result::Result<Response, ApiError> {
    payload(request).await?;
    app.accept(Command::Scan).await
}
async fn stop(
    State(app): State<WebApp>,
    request: Request,
) -> std::result::Result<Response, ApiError> {
    payload(request).await?;
    app.accept(Command::Stop).await
}
async fn start(
    State(app): State<WebApp>,
    request: Request,
) -> std::result::Result<Response, ApiError> {
    let value = payload(request).await?;
    let peer = peer(&value, &app.snapshot().await.endpoint_id)?;
    app.accept(Command::Start { peer }).await
}
async fn sync(
    State(app): State<WebApp>,
    request: Request,
) -> std::result::Result<Response, ApiError> {
    let value = payload(request).await?;
    let peer = peer(&value, &app.snapshot().await.endpoint_id)?;
    let address = value
        .get("direct_address")
        .and_then(Value::as_str)
        .and_then(|v| v.parse::<SocketAddr>().ok())
        .filter(|v| {
            v.port() != 0
                && !v.ip().is_unspecified()
                && !v.ip().is_multicast()
                && v.ip() != std::net::IpAddr::V4(std::net::Ipv4Addr::BROADCAST)
        })
        .ok_or_else(|| {
            ApiError::field(
                "direct_address",
                "enter a concrete IP address and nonzero port (IPv6 uses [address]:port)",
            )
        })?;
    if value.get("confirm") != Some(&Value::Bool(true)) {
        return Err(ApiError::field(
            "confirm",
            "confirm bidirectional file changes before synchronizing",
        ));
    }
    app.accept(Command::Sync { peer, address }).await
}
