use super::*;
use axum::http::Request as HttpRequest;
use http_body_util::BodyExt;
use tower::ServiceExt;

struct Harness {
    app: Router,
    state: Arc<AppState>,
    directory: tempfile::TempDir,
    bootstrap: String,
    admin: String,
}
impl Harness {
    async fn new() -> Self {
        let directory = tempfile::TempDir::new().unwrap();
        let manager = Manager::open(directory.path().to_path_buf()).await.unwrap();
        let (auth, bootstrap) = Auth::open(directory.path()).unwrap();
        let admin = std::fs::read_to_string(directory.path().join("admin-token"))
            .unwrap()
            .trim()
            .to_owned();
        let state = Arc::new(AppState {
            manager,
            auth: Arc::new(auth),
            hosts: HostPolicy::new("127.0.0.1:8390".parse().unwrap(), &[]).unwrap(),
        });
        Self {
            app: router(Arc::clone(&state)),
            state,
            directory,
            bootstrap,
            admin,
        }
    }
    #[allow(clippy::too_many_arguments)] // Explicit headers make security cases reviewable.
    async fn request(
        &self,
        method: &str,
        uri: &str,
        body: Value,
        cookie: Option<&str>,
        csrf: Option<&str>,
        origin: Option<&str>,
        host: &str,
    ) -> Response {
        let mut builder = HttpRequest::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, host);
        if let Some(cookie) = cookie {
            builder = builder.header(header::COOKIE, cookie);
        }
        if let Some(csrf) = csrf {
            builder = builder.header("x-deltaweave-csrf", csrf);
        }
        if let Some(origin) = origin {
            builder = builder.header(header::ORIGIN, origin);
        }
        let body = if body.is_null() {
            Body::empty()
        } else {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(body.to_string())
        };
        self.app
            .clone()
            .oneshot(builder.body(body).unwrap())
            .await
            .unwrap()
    }
    async fn login(&self, token: &str) -> (String, String) {
        let response = self
            .request(
                "POST",
                "/api/v1/session",
                json!({"token": token}),
                None,
                None,
                Some("http://localhost:8390"),
                "localhost:8390",
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let set_cookie = response.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .to_owned();
        assert!(set_cookie.contains("HttpOnly") && set_cookie.contains("SameSite=Strict"));
        let cookie = set_cookie.split(';').next().unwrap().to_owned();
        let body = json_body(response).await;
        (cookie, body["csrf_token"].as_str().unwrap().to_owned())
    }
}
async fn json_body(response: Response) -> Value {
    assert!(
        response.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .contains("application/json")
    );
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

#[tokio::test]
async fn protected_data_requires_an_authenticated_session() {
    let h = Harness::new().await;
    for uri in [
        "/api/v1/state",
        "/api/v1/events",
        "/api/v1/browse?path=/",
        "/api/v1/activities/export",
    ] {
        let response = h
            .request("GET", uri, Value::Null, None, None, None, "localhost:8390")
            .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{uri}");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        assert!(json_body(response).await["error"].is_string());
    }
    let response = h
        .request(
            "GET",
            "/api/v1/session",
            Value::Null,
            None,
            None,
            None,
            "localhost:8390",
        )
        .await;
    assert_eq!(json_body(response).await, json!({"authenticated": false}));
    h.state.manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_origin_csrf_login_replay_logout_and_expiry_are_enforced() {
    let h = Harness::new().await;
    for host in [
        "evil.example:8390",
        "localhost:9999",
        "localhost.evil:8390",
        "localhost:8390@evil.example",
    ] {
        assert_eq!(
            h.request(
                "GET",
                "/api/v1/session",
                Value::Null,
                None,
                None,
                None,
                host
            )
            .await
            .status(),
            StatusCode::FORBIDDEN
        );
    }
    for origin in [
        None,
        Some("null"),
        Some("http://evil.example:8390"),
        Some("http://localhost:9999"),
    ] {
        assert_eq!(
            h.request(
                "POST",
                "/api/v1/session",
                json!({"token": h.admin}),
                None,
                None,
                origin,
                "localhost:8390"
            )
            .await
            .status(),
            StatusCode::FORBIDDEN
        );
    }
    let (cookie, csrf) = h.login(&h.bootstrap).await;
    assert_eq!(
        h.request(
            "POST",
            "/api/v1/session",
            json!({"token": h.bootstrap}),
            None,
            None,
            Some("http://localhost:8390"),
            "localhost:8390"
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        h.request(
            "GET",
            "/api/v1/state",
            Value::Null,
            Some(&cookie),
            None,
            None,
            "localhost:8390"
        )
        .await
        .status(),
        StatusCode::OK
    );
    for token in [None, Some("wrong")] {
        assert_eq!(
            h.request(
                "DELETE",
                "/api/v1/session",
                Value::Null,
                Some(&cookie),
                token,
                Some("http://localhost:8390"),
                "localhost:8390"
            )
            .await
            .status(),
            StatusCode::FORBIDDEN
        );
    }
    assert_eq!(
        h.request(
            "DELETE",
            "/api/v1/session",
            Value::Null,
            Some(&cookie),
            Some(&csrf),
            Some("http://localhost:8390"),
            "localhost:8390"
        )
        .await
        .status(),
        StatusCode::OK
    );
    assert_eq!(
        h.request(
            "GET",
            "/api/v1/state",
            Value::Null,
            Some(&cookie),
            None,
            None,
            "localhost:8390"
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    let (cookie, _) = h.login(&h.admin).await;
    h.state.auth.expire(cookie.split_once('=').unwrap().1);
    assert_eq!(
        h.request(
            "GET",
            "/api/v1/state",
            Value::Null,
            Some(&cookie),
            None,
            None,
            "localhost:8390"
        )
        .await
        .status(),
        StatusCode::UNAUTHORIZED
    );
    h.state.manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn authenticated_control_browse_and_exports_use_real_manager_state() {
    let h = Harness::new().await;
    let (cookie, csrf) = h.login(&h.admin).await;
    let response = h
        .request(
            "PUT",
            "/api/v1/settings",
            json!({"node_name":"HTTP test node", "poll_interval_seconds":15, "history_limit":100}),
            Some(&cookie),
            Some(&csrf),
            Some("http://localhost:8390"),
            "localhost:8390",
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let response = h
        .request(
            "GET",
            "/api/v1/state",
            Value::Null,
            Some(&cookie),
            None,
            None,
            "localhost:8390",
        )
        .await;
    let snapshot = json_body(response).await;
    assert_eq!(snapshot["node"]["name"], "HTTP test node");
    assert_eq!(snapshot["folders"], json!([]));
    assert!(!snapshot.to_string().contains(&h.admin));
    let folder_root = tempfile::TempDir::new().unwrap();
    std::fs::create_dir(folder_root.path().join("visible-directory")).unwrap();
    std::fs::write(folder_root.path().join("secret.txt"), "private contents").unwrap();
    let response = h
        .request(
            "GET",
            &format!("/api/v1/browse?path={}", folder_root.path().display()),
            Value::Null,
            Some(&cookie),
            None,
            None,
            "localhost:8390",
        )
        .await;
    let listing = json_body(response).await;
    assert_eq!(listing["entries"].as_array().unwrap().len(), 1);
    assert_eq!(listing["entries"][0]["name"], "visible-directory");
    assert!(!listing.to_string().contains("secret.txt"));
    let response = h.request("POST", "/api/v1/folders", json!({"name":"HTTP receiver", "root":folder_root.path(), "role":"receive", "enabled":false, "bind":"127.0.0.1:0"}), Some(&cookie), Some(&csrf), Some("http://localhost:8390"), "localhost:8390").await;
    assert_eq!(response.status(), StatusCode::OK);
    let folder = json_body(response).await;
    let id = folder["id"].as_str().unwrap();
    let response = h
        .request(
            "DELETE",
            &format!("/api/v1/folders/{id}"),
            Value::Null,
            Some(&cookie),
            Some(&csrf),
            Some("http://localhost:8390"),
            "localhost:8390",
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        std::fs::read_to_string(folder_root.path().join("secret.txt")).unwrap(),
        "private contents"
    );
    let response = h
        .request(
            "GET",
            "/api/v1/activities/export",
            Value::Null,
            Some(&cookie),
            None,
            None,
            "localhost:8390",
        )
        .await;
    assert!(
        response.headers()[header::CONTENT_DISPOSITION]
            .to_str()
            .unwrap()
            .contains("attachment")
    );
    assert!(json_body(response).await.is_array());
    assert!(h.directory.path().join("admin-token").is_file());
    h.state.manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn directory_browse_rejects_management_private_state_without_echoing_it() {
    let h = Harness::new().await;
    let (cookie, _) = h.login(&h.admin).await;
    let private_path = h.directory.path().display().to_string();
    let response = h
        .request(
            "GET",
            &format!("/api/v1/browse?path={private_path}"),
            Value::Null,
            Some(&cookie),
            None,
            None,
            "localhost:8390",
        )
        .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = json_body(response).await.to_string();
    assert!(!body.contains(&private_path));
    h.state.manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn events_stop_after_logout_and_expiration() {
    let h = Harness::new().await;
    for expire in [false, true] {
        let (cookie, csrf) = h.login(&h.admin).await;
        let response = h
            .request(
                "GET",
                "/api/v1/events",
                Value::Null,
                Some(&cookie),
                None,
                None,
                "localhost:8390",
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body();
        let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
        assert!(String::from_utf8_lossy(&first).contains("event: state"));
        if expire {
            h.state.auth.expire(cookie.split_once('=').unwrap().1);
        } else {
            assert_eq!(
                h.request(
                    "DELETE",
                    "/api/v1/session",
                    Value::Null,
                    Some(&cookie),
                    Some(&csrf),
                    Some("http://localhost:8390"),
                    "localhost:8390"
                )
                .await
                .status(),
                StatusCode::OK
            );
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(2), body.frame())
                .await
                .unwrap()
                .is_none()
        );
    }
    h.state.manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_oversized_and_unknown_api_requests_return_json_errors() {
    let h = Harness::new().await;
    let (cookie, csrf) = h.login(&h.admin).await;
    let response = h
        .request(
            "POST",
            "/api/v1/session",
            json!({"token":"x".repeat(70_000)}),
            None,
            None,
            Some("http://localhost:8390"),
            "localhost:8390",
        )
        .await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(json_body(response).await["error"].is_string());
    let response = h
        .request(
            "PUT",
            "/api/v1/settings",
            json!({"node_name":7}),
            Some(&cookie),
            Some(&csrf),
            Some("http://localhost:8390"),
            "localhost:8390",
        )
        .await;
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert!(json_body(response).await["error"].is_string());
    let response = h
        .request(
            "GET",
            "/api/v1/no-such-route",
            Value::Null,
            Some(&cookie),
            None,
            None,
            "localhost:8390",
        )
        .await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(json_body(response).await["error"].is_string());
    h.state.manager.shutdown().await.unwrap();
}

#[test]
fn unspecified_bind_requires_explicit_exact_hosts() {
    assert!(HostPolicy::new("0.0.0.0:8390".parse().unwrap(), &[]).is_err());
    assert!(HostPolicy::new("0.0.0.0:8390".parse().unwrap(), &["*.example.com".into()]).is_err());
    let hosts = HostPolicy::new(
        "0.0.0.0:8390".parse().unwrap(),
        &["preview.example.com".into()],
    )
    .unwrap();
    assert!(hosts.allows("preview.example.com:8390"));
    assert!(!hosts.allows("preview.example.com.evil:8390"));
    assert!(!hosts.allows("evil.example.com:8390"));
    assert!(!hosts.allows("preview.example.com:4444"));
}

#[tokio::test]
async fn real_tcp_login_state_and_shutdown_revoke_event_streams() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let h = Harness::new().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = h.app.clone();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let auth = Arc::clone(&h.state.auth);
    let serving = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = stopped.await;
                auth.revoke_all();
            })
            .await
            .unwrap();
    });
    async fn exchange(address: SocketAddr, request: String) -> String {
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        socket.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }
    let response = exchange(
        address,
        "GET /api/v1/state HTTP/1.1\r\nHost: localhost:8390\r\nConnection: close\r\n\r\n".into(),
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 401"));
    assert!(response.contains("administrator session required"));
    let body = json!({"token":h.admin}).to_string();
    let response = exchange(address, format!("POST /api/v1/session HTTP/1.1\r\nHost: localhost:8390\r\nOrigin: http://localhost:8390\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())).await;
    assert!(response.starts_with("HTTP/1.1 200"));
    let cookie = response
        .lines()
        .find_map(|line| line.strip_prefix("set-cookie: "))
        .unwrap()
        .split(';')
        .next()
        .unwrap();
    let response = exchange(address, format!("GET /api/v1/state HTTP/1.1\r\nHost: localhost:8390\r\nCookie: {cookie}\r\nConnection: close\r\n\r\n")).await;
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("\"folders\":[]"));
    let mut event_socket = tokio::net::TcpStream::connect(address).await.unwrap();
    event_socket
        .write_all(
            format!(
                "GET /api/v1/events HTTP/1.1\r\nHost: localhost:8390\r\nCookie: {cookie}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut chunk = [0; 4096];
    let count = event_socket.read(&mut chunk).await.unwrap();
    assert!(String::from_utf8_lossy(&chunk[..count]).starts_with("HTTP/1.1 200"));
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), serving)
        .await
        .unwrap()
        .unwrap();
    assert!(
        h.state
            .auth
            .session(cookie.split_once('=').unwrap().1)
            .is_none()
    );
    h.state.manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_path_errors_still_follow_json_contract() {
    let h = Harness::new().await;
    let (cookie, csrf) = h.login(&h.admin).await;
    let response = h
        .request(
            "DELETE",
            "/api/v1/folders/%FF",
            Value::Null,
            Some(&cookie),
            Some(&csrf),
            Some("http://localhost:8390"),
            "localhost:8390",
        )
        .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(json_body(response).await["error"].is_string());
    h.state.manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn https_same_host_login_sets_secure_cookie_without_trusting_forwarded_headers() {
    let h = Harness::new().await;
    let response = h
        .request(
            "POST",
            "/api/v1/session",
            json!({"token":h.admin}),
            None,
            None,
            Some("https://localhost:8390"),
            "localhost:8390",
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("; Secure")
    );
    assert_eq!(
        h.request(
            "POST",
            "/api/v1/session",
            json!({"token":h.admin}),
            None,
            None,
            Some("https://evil.example:8390"),
            "localhost:8390"
        )
        .await
        .status(),
        StatusCode::FORBIDDEN
    );
    let proxy = HostPolicy::new(
        "127.0.0.1:8390".parse().unwrap(),
        &["preview.example:443".into()],
    )
    .unwrap();
    assert!(proxy.allows("preview.example"));
    assert!(proxy.allows("preview.example:443"));
    assert!(!proxy.allows("preview.example:8080"));
    h.state.manager.shutdown().await.unwrap();
}
