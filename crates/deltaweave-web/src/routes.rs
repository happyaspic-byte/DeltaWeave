use crate::auth::{Auth, COOKIE};
use anyhow::{Result, ensure};
use axum::{
    Json, Router,
    body::Body,
    extract::{
        DefaultBodyLimit, Path, Query, Request, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post, put},
};
use deltaweave_control::{DeviceInput, FolderCommand, FolderInput, Manager, Settings};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashSet, convert::Infallible, net::SocketAddr, path::PathBuf, sync::Arc,
    time::Duration,
};

mod share_routes;

pub(crate) struct AppState {
    pub manager: Arc<Manager>,
    pub auth: Arc<Auth>,
    pub hosts: HostPolicy,
}

pub(crate) struct HostPolicy {
    names: HashSet<String>,
    authorities: HashSet<String>,
    port: u16,
}
impl HostPolicy {
    pub(crate) fn new(bind: SocketAddr, allowed: &[String]) -> Result<Self> {
        ensure!(
            !bind.ip().is_unspecified() || !allowed.is_empty(),
            "binding all interfaces requires --allow-host with an explicit reachable host"
        );
        let mut names = HashSet::from([
            "localhost".to_owned(),
            "127.0.0.1".to_owned(),
            "[::1]".to_owned(),
        ]);
        if !bind.ip().is_unspecified() {
            names.insert(if bind.is_ipv6() {
                format!("[{}]", bind.ip())
            } else {
                bind.ip().to_string()
            });
        }
        let mut authorities = HashSet::new();
        for value in allowed {
            let authority = parse_authority(value)
                .ok_or_else(|| anyhow::anyhow!("invalid allowed host: {value}"))?;
            if authority.port_u16().is_some() {
                authorities.insert(authority.as_str().to_ascii_lowercase());
            } else {
                names.insert(authority.host().to_ascii_lowercase());
            }
        }
        Ok(Self {
            names,
            authorities,
            port: bind.port(),
        })
    }
    fn allows(&self, host: &str) -> bool {
        let Some(authority) = parse_authority(host) else {
            return false;
        };
        self.authorities.contains(&host.to_ascii_lowercase())
            || (authority.port_u16().is_none()
                && [80, 443].iter().any(|port| {
                    self.authorities
                        .contains(&format!("{}:{port}", authority.host().to_ascii_lowercase()))
                }))
            || (self.names.contains(&authority.host().to_ascii_lowercase())
                && (self.port == 0 || authority.port_u16().unwrap_or(80) == self.port))
    }
}
fn parse_authority(value: &str) -> Option<axum::http::uri::Authority> {
    if value.is_empty() || value.contains(['@', '*', '/', '\\', ' ', '#', '?', ',']) {
        return None;
    }
    let authority = value.parse::<axum::http::uri::Authority>().ok()?;
    if authority.host().is_empty() {
        return None;
    }
    Some(authority)
}
fn session_id(headers: &HeaderMap) -> Option<String> {
    let values: Vec<_> = headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|part| part.trim().split_once('='))
        .filter(|(name, _)| *name == COOKIE)
        .map(|(_, value)| value.to_owned())
        .collect();
    if values.len() == 1 {
        values.into_iter().next()
    } else {
        None
    }
}
fn error(status: StatusCode, message: impl ToString) -> Response {
    (status, Json(json!({"error": message.to_string()}))).into_response()
}
fn same_origin(headers: &HeaderMap, host: &str, hosts: &HostPolicy) -> bool {
    let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Ok(uri) = origin.parse::<axum::http::Uri>() else {
        return false;
    };
    let default_port = match uri.scheme_str() {
        Some("http") => 80,
        Some("https") => 443,
        _ => return false,
    };
    let Some(host) = parse_authority(host) else {
        return false;
    };
    uri.authority().is_some_and(|origin| {
        origin.host().eq_ignore_ascii_case(host.host())
            && origin.port_u16().unwrap_or(default_port) == host.port_u16().unwrap_or(default_port)
            && hosts.allows(&format!(
                "{}:{}",
                origin.host(),
                origin.port_u16().unwrap_or(default_port)
            ))
    }) && uri.path_and_query().is_none_or(|path| path.as_str() == "/")
}

pub(crate) fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/v1/session", get(session).post(login).delete(logout))
        .route("/api/v1/state", get(snapshot))
        .route("/api/v1/events", get(events))
        // The managed-share router registers its fixed paths before the
        // `/{share_id}` paths so preview/validate/join cannot be parsed as IDs.
        .merge(share_routes::router())
        .route("/api/v1/folders", post(add_folder))
        .route(
            "/api/v1/folders/{id}",
            put(update_folder).delete(remove_folder),
        )
        .route("/api/v1/folders/{id}/{command}", post(command))
        .route("/api/v1/devices", post(add_device))
        .route(
            "/api/v1/devices/{id}",
            put(update_device).delete(remove_device),
        )
        .route("/api/v1/settings", put(settings))
        .route("/api/v1/activities/export", get(export))
        .route("/api/v1/browse", get(browse))
        .fallback(fallback)
        .method_not_allowed_fallback(|| async {
            error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
        })
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(Arc::clone(&state), protect))
        .with_state(state)
}
async fn protect(State(state): State<Arc<AppState>>, request: Request, next: Next) -> Response {
    let is_api = request.uri().path().starts_with("/api/");
    let result = authorize(&state, &request);
    let mut response = match result {
        Ok(()) => {
            let (parts, body) = request.into_parts();
            match tokio::time::timeout(
                Duration::from_secs(10),
                axum::body::to_bytes(body, 64 * 1024),
            )
            .await
            {
                Ok(Ok(body)) => next.run(Request::from_parts(parts, Body::from(body))).await,
                Ok(Err(_)) => error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "request body exceeds 64 KiB or could not be read",
                ),
                Err(_) => error(StatusCode::REQUEST_TIMEOUT, "request body timed out"),
            }
        }
        Err((status, message)) => error(status, message),
    };
    if is_api
        && (response.status().is_client_error() || response.status().is_server_error())
        && !response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    {
        let status = response.status();
        let message = axum::body::to_bytes(response.into_body(), 8192)
            .await
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_else(|_| "request could not be processed".into());
        response = error(status, message);
    }
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static("default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'; object-src 'none'"));
    headers.insert(
        "permissions-policy",
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    response
}
fn authorize(
    state: &AppState,
    request: &Request,
) -> std::result::Result<(), (StatusCode, &'static str)> {
    let error = |status: StatusCode, message: &'static str| (status, message);
    let headers = request.headers();
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok());
    if headers.get_all(header::HOST).iter().count() != 1
        || !host.is_some_and(|host| state.hosts.allows(host))
    {
        return Err(error(StatusCode::FORBIDDEN, "HTTP Host is not allowed"));
    }
    if headers
        .get("sec-fetch-site")
        .is_some_and(|site| site == "cross-site")
    {
        return Err(error(
            StatusCode::FORBIDDEN,
            "cross-site requests are not allowed",
        ));
    }
    let mutation = !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    );
    let path = request.uri().path();
    let public_session =
        path == "/api/v1/session" && matches!(*request.method(), Method::GET | Method::POST);
    if mutation && !same_origin(headers, host.unwrap(), &state.hosts) {
        return Err(error(
            StatusCode::FORBIDDEN,
            "same-origin Origin header is required",
        ));
    }
    if path.starts_with("/api/") && !public_session {
        let id = session_id(headers)
            .filter(|id| state.auth.session(id).is_some())
            .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "administrator session required"))?;
        if mutation
            && !headers
                .get("x-deltaweave-csrf")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|csrf| state.auth.valid_csrf(&id, csrf))
        {
            return Err(error(StatusCode::FORBIDDEN, "valid CSRF token required"));
        }
    }
    if let Some(length) = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        && length > 64 * 1024
    {
        return Err(error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "request body exceeds 64 KiB",
        ));
    }
    Ok(())
}
async fn session(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Json<Value> {
    Json(
        match session_id(&headers).and_then(|id| state.auth.session(&id)) {
            Some(csrf) => json!({"authenticated": true, "csrf_token": csrf}),
            None => json!({"authenticated": false}),
        },
    )
}
#[derive(Deserialize)]
struct Login {
    token: String,
}
async fn login(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    input: std::result::Result<Json<Login>, JsonRejection>,
) -> Response {
    let input = match input {
        Ok(Json(input)) => input,
        Err(error) => return json_rejection(error),
    };
    let Some((id, csrf)) = state.auth.login(input.token.trim()) else {
        return error(
            StatusCode::UNAUTHORIZED,
            "invalid or expired access key; session limit may have been reached",
        );
    };
    let mut response = Json(json!({"authenticated": true, "csrf_token": csrf})).into_response();
    let secure = secure_cookie(&headers);
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&format!(
            "{COOKIE}={id}; Path=/; HttpOnly; SameSite=Strict; Max-Age=43200{secure}"
        ))
        .unwrap(),
    );
    response
}
// The mutation middleware has verified this Origin against an allowed Host. Forwarded headers are ignored.
fn secure_cookie(headers: &HeaderMap) -> &'static str {
    if headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|origin| origin.starts_with("https://"))
    {
        "; Secure"
    } else {
        ""
    }
}
async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Some(id) = session_id(&headers) {
        state.auth.logout(&id);
    }
    let mut response = Json(json!({"authenticated": false})).into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&format!(
            "deltaweave_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0{}",
            secure_cookie(&headers)
        ))
        .unwrap(),
    );
    response
}
async fn snapshot(State(state): State<Arc<AppState>>) -> Json<deltaweave_control::AppSnapshot> {
    Json(state.manager.snapshot().await)
}
fn json_rejection(rejection: JsonRejection) -> Response {
    error(rejection.status(), rejection.body_text())
}
fn managed<T: serde::Serialize>(result: anyhow::Result<T>) -> Response {
    match result {
        Ok(value) => Json(value).into_response(),
        Err(err) => error(
            if err.to_string().contains("not found")
                || err.to_string().contains("unknown folder")
                || err.to_string().contains("unknown device")
            {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::BAD_REQUEST
            },
            format!("{err:#}"),
        ),
    }
}
async fn add_folder(
    State(state): State<Arc<AppState>>,
    input: std::result::Result<Json<FolderInput>, JsonRejection>,
) -> Response {
    match input {
        Ok(Json(input)) => managed(state.manager.add_folder(input).await),
        Err(err) => json_rejection(err),
    }
}
async fn update_folder(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    input: std::result::Result<Json<FolderInput>, JsonRejection>,
) -> Response {
    match input {
        Ok(Json(input)) => managed(state.manager.update_folder(&id, input).await),
        Err(err) => json_rejection(err),
    }
}
async fn remove_folder(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    managed(
        state
            .manager
            .remove_folder(&id)
            .await
            .map(|()| json!({"accepted": true})),
    )
}
async fn command(
    State(state): State<Arc<AppState>>,
    Path((id, command)): Path<(String, String)>,
) -> Response {
    let command = match command.as_str() {
        "sync" => FolderCommand::Sync,
        "pause" => FolderCommand::Pause,
        "resume" => FolderCommand::Resume,
        _ => return error(StatusCode::NOT_FOUND, "unknown folder command"),
    };
    managed(
        state
            .manager
            .command(&id, command)
            .await
            .map(|()| json!({"accepted": true})),
    )
}
async fn add_device(
    State(state): State<Arc<AppState>>,
    input: std::result::Result<Json<DeviceInput>, JsonRejection>,
) -> Response {
    match input {
        Ok(Json(input)) => managed(state.manager.add_device(input).await),
        Err(err) => json_rejection(err),
    }
}
async fn update_device(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    input: std::result::Result<Json<DeviceInput>, JsonRejection>,
) -> Response {
    match input {
        Ok(Json(input)) => managed(state.manager.update_device(&id, input).await),
        Err(err) => json_rejection(err),
    }
}
async fn remove_device(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    managed(
        state
            .manager
            .remove_device(&id)
            .await
            .map(|()| json!({"accepted": true})),
    )
}
async fn settings(
    State(state): State<Arc<AppState>>,
    input: std::result::Result<Json<Settings>, JsonRejection>,
) -> Response {
    match input {
        Ok(Json(input)) => managed(state.manager.update_settings(input).await),
        Err(err) => json_rejection(err),
    }
}
async fn export(State(state): State<Arc<AppState>>) -> Response {
    let mut response = Json(state.manager.snapshot().await.activities).into_response();
    response.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment; filename=deltaweave-activities.json"),
    );
    response
}
#[derive(Deserialize)]
struct Browse {
    path: Option<String>,
}
async fn browse(
    State(state): State<Arc<AppState>>,
    query: std::result::Result<Query<Browse>, QueryRejection>,
) -> Response {
    let query = match query {
        Ok(Query(query)) => query,
        Err(err) => return error(err.status(), err.body_text()),
    };
    match state.manager.browse(query.path.map(PathBuf::from)).await {
        Ok(result) => managed(Ok(result)),
        Err(_) => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "directory is unavailable or not allowed",
        ),
    }
}
async fn events(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(id) = session_id(&headers) else {
        return error(StatusCode::UNAUTHORIZED, "administrator session required");
    };
    let stream =
        futures_util::stream::unfold((state, id, None), |(state, id, revision)| async move {
            loop {
                state.auth.session(&id)?;
                let snapshot = state.manager.snapshot().await;
                if revision != Some(snapshot.revision) {
                    let next_revision = Some(snapshot.revision);
                    let event = Event::default()
                        .event("state")
                        .json_data(&snapshot)
                        .unwrap_or_else(|_| {
                            Event::default()
                                .event("error")
                                .data("snapshot serialization failed")
                        });
                    return Some((Ok::<_, Infallible>(event), (state, id, next_revision)));
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("heartbeat"),
        )
        .into_response()
}
async fn fallback(request: Request<Body>) -> Response {
    if request.uri().path().starts_with("/api/") || request.uri().path() == "/api" {
        return error(StatusCode::NOT_FOUND, "API route not found");
    }
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        return error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed");
    }
    crate::assets::serve(request.uri().path(), request.method() == Method::HEAD)
}

#[cfg(test)]
mod tests;
