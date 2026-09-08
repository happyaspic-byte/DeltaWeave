use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use deltaweave_web::{Config, WebApp};
use serde_json::{Value, json};
use std::{fs, time::Duration};
use tempfile::TempDir;
use tower::ServiceExt;

async fn fixture() -> (TempDir, WebApp) {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir(temp.path().join("root")).unwrap();
    let app = WebApp::new(Config {
        root: temp.path().join("root"),
        state: temp.path().join("private"),
        identity: None,
        bind: "127.0.0.1:7840".parse().unwrap(),
        peer_bind: "127.0.0.1:0".parse().unwrap(),
    })
    .await
    .unwrap();
    (temp, app)
}
async fn request(app: &WebApp, path: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut req = Request::builder()
        .uri(path)
        .header("host", "127.0.0.1:7840")
        .header("authorization", format!("Bearer {}", app.token()));
    let body = match body {
        Some(v) => {
            req = req
                .method("POST")
                .header("content-type", "application/json");
            v.to_string()
        }
        None => String::new(),
    };
    let response = app
        .router("127.0.0.1:7840")
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let value = serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap())
        .unwrap();
    (status, value)
}
async fn idle(app: &WebApp) -> Value {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let (_, s) = request(app, "/api/state", None).await;
            if s["phase"] == "idle" {
                return s;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap()
}
async fn wait_for_receiving(app: &WebApp) {
    let readiness = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if request(app, "/api/state", None).await.1["phase"] == "receiving" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    if readiness.is_err() {
        let state = request(app, "/api/state", None).await.1;
        let phase = state["phase"].as_str().unwrap_or("invalid");
        let activity_status = state["activity"][0]["status"].as_str().unwrap_or("missing");
        let activity_error = state["activity"][0]["error"].as_str().unwrap_or("");
        let error_class = if activity_error
            .to_ascii_lowercase()
            .contains("address already in use")
        {
            "address_in_use"
        } else if activity_error
            .to_ascii_lowercase()
            .contains("permission denied")
        {
            "permission_denied"
        } else if activity_error
            .to_ascii_lowercase()
            .contains("failed to bind")
        {
            "bind_failure"
        } else if activity_error.is_empty() {
            "missing"
        } else {
            "other"
        };
        eprintln!("receiver readiness timeout: phase={phase} activity_status={activity_status}");
        eprintln!("receiver readiness error class: {error_class}");
    }
    readiness.unwrap();
}
#[tokio::test]
async fn authenticates_and_blocks_cross_origin_and_rebinding() {
    let (_temp, app) = fixture().await;
    for (host, origin, token, expected) in [
        ("127.0.0.1:7840", None, None, StatusCode::UNAUTHORIZED),
        (
            "evil.example:7840",
            None,
            Some(app.token()),
            StatusCode::FORBIDDEN,
        ),
        (
            "127.0.0.1:7840",
            Some("https://evil.example"),
            Some(app.token()),
            StatusCode::FORBIDDEN,
        ),
        (
            "127.0.0.1:7840",
            Some("null"),
            Some(app.token()),
            StatusCode::FORBIDDEN,
        ),
        (
            "127.0.0.1:7840",
            Some("http://127.0.0.1:7840"),
            Some(app.token()),
            StatusCode::OK,
        ),
    ] {
        let mut req = Request::builder().uri("/api/state").header("host", host);
        if let Some(origin) = origin {
            req = req.header("origin", origin);
        }
        if let Some(token) = token {
            req = req.header("authorization", format!("Bearer {token}"));
        }
        let response = app
            .router("127.0.0.1:7840")
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert!(response.headers().contains_key("content-security-policy"));
        assert!(
            !response
                .headers()
                .contains_key("access-control-allow-origin")
        );
    }
    app.shutdown().await.unwrap();
}
#[tokio::test]
async fn validates_fields_and_bounds_json() {
    let (_temp, app) = fixture().await;
    let peer = iroh::SecretKey::generate().public().to_string();
    for (payload, field) in [
        (
            json!({"peer_id":"bad","direct_address":"127.0.0.1:1","confirm":true}),
            "peer_id",
        ),
        (
            json!({"peer_id":peer,"direct_address":"hostname:1","confirm":true}),
            "direct_address",
        ),
        (
            json!({"peer_id":peer,"direct_address":"0.0.0.0:1","confirm":true}),
            "direct_address",
        ),
        (
            json!({"peer_id":peer,"direct_address":"127.0.0.1:1","confirm":false}),
            "confirm",
        ),
    ] {
        let (status, value) = request(&app, "/api/sync", Some(payload)).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(value["field"], field);
    }
    let (status, state) = request(&app, "/api/state", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        request(
            &app,
            "/api/receiver/start",
            Some(json!({"peer_id":state["endpoint_id"]}))
        )
        .await
        .0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(
        request(&app, "/api/scan", Some(json!({"huge":"x".repeat(9000)})))
            .await
            .0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    app.shutdown().await.unwrap();
}
#[tokio::test]
async fn scans_real_files_and_reuses_index_across_receiver_mode() {
    let (temp, app) = fixture().await;
    fs::write(temp.path().join("root/hello.txt"), "actual contents").unwrap();
    let (status, accepted) = request(&app, "/api/scan", Some(json!({}))).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(accepted["phase"], "scanning");
    let s = idle(&app).await;
    assert_eq!(s["scan"]["report"]["live_records"], 1);
    assert_eq!(s["scan"]["total_records"], 1);
    assert_eq!(s["activity"][0]["status"], "success");
    let peer = iroh::SecretKey::generate().public().to_string();
    assert_eq!(
        request(&app, "/api/receiver/start", Some(json!({"peer_id":peer})))
            .await
            .0,
        StatusCode::ACCEPTED
    );
    wait_for_receiving(&app).await;
    assert_eq!(
        request(&app, "/api/scan", Some(json!({}))).await.0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        request(&app, "/api/receiver/stop", Some(json!({}))).await.0,
        StatusCode::ACCEPTED
    );
    idle(&app).await;
    request(&app, "/api/scan", Some(json!({}))).await;
    let s = idle(&app).await;
    assert_eq!(s["activity"][0]["status"], "success");
    assert_eq!(s["scan"]["report"]["unchanged"], 1);
    app.shutdown().await.unwrap();
}
#[tokio::test]
async fn rejects_overlapping_or_missing_roots_before_creating_identity() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir(temp.path().join("root")).unwrap();
    for (root, state, identity) in [
        (
            temp.path().join("missing"),
            temp.path().join("private"),
            None,
        ),
        (
            temp.path().join("root"),
            temp.path().join("root/private"),
            None,
        ),
        (temp.path().join("root"), temp.path().to_path_buf(), None),
        (
            temp.path().join("root"),
            temp.path().join("private"),
            Some(temp.path().join("root/secret.key")),
        ),
    ] {
        assert!(
            WebApp::new(Config {
                root,
                state,
                identity,
                bind: "127.0.0.1:0".parse().unwrap(),
                peer_bind: "127.0.0.1:0".parse().unwrap()
            })
            .await
            .is_err()
        );
    }
    assert!(!temp.path().join("root/secret.key").exists());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn syncs_two_real_peers_and_denies_unlisted_identity() {
    let (left_temp, left) = fixture().await;
    let (right_temp, right) = fixture().await;
    let (_denied_temp, denied) = fixture().await;
    fs::write(left_temp.path().join("root/from-left.txt"), "left data").unwrap();
    fs::write(right_temp.path().join("root/from-right.txt"), "right data").unwrap();
    let left_id = request(&left, "/api/state", None).await.1["endpoint_id"].clone();
    request(
        &right,
        "/api/receiver/start",
        Some(json!({"peer_id":left_id})),
    )
    .await;
    wait_for_receiving(&right).await;
    let receiver = request(&right, "/api/state", None).await.1["receiver"].clone();
    let payload = json!({"peer_id":receiver["endpoint_id"],"direct_address":receiver["direct_addresses"][0],"confirm":true});
    assert_eq!(
        request(&left, "/api/sync", Some(payload.clone())).await.0,
        StatusCode::ACCEPTED
    );
    let result = idle(&left).await;
    assert_eq!(result["activity"][0]["status"], "success", "{result:#}");
    assert_eq!(
        result["sync"]["verified_local_root"],
        result["sync"]["verified_remote_root"]
    );
    assert_eq!(
        fs::read_to_string(left_temp.path().join("root/from-right.txt")).unwrap(),
        "right data"
    );
    assert_eq!(
        fs::read_to_string(right_temp.path().join("root/from-left.txt")).unwrap(),
        "left data"
    );
    request(&denied, "/api/sync", Some(payload)).await;
    let rejected = idle(&denied).await;
    assert_eq!(rejected["activity"][0]["status"], "error");
    assert!(rejected["sync"].is_null());
    request(&right, "/api/receiver/stop", Some(json!({}))).await;
    idle(&right).await;
    request(&right, "/api/scan", Some(json!({}))).await;
    assert_eq!(idle(&right).await["scan"]["report"]["live_records"], 2);
    left.shutdown().await.unwrap();
    right.shutdown().await.unwrap();
    denied.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_symlinked_private_paths_into_public_root() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("root");
    fs::create_dir(&root).unwrap();
    symlink(&root, temp.path().join("state-link")).unwrap();
    let config = Config {
        root: root.clone(),
        state: temp.path().join("state-link/new-private"),
        identity: None,
        bind: "127.0.0.1:0".parse().unwrap(),
        peer_bind: "127.0.0.1:0".parse().unwrap(),
    };
    assert!(WebApp::new(config).await.is_err());
    symlink(root.join("key"), temp.path().join("identity-link")).unwrap();
    let config = Config {
        root: root.clone(),
        state: temp.path().join("private"),
        identity: Some(temp.path().join("identity-link")),
        bind: "127.0.0.1:0".parse().unwrap(),
        peer_bind: "127.0.0.1:0".parse().unwrap(),
    };
    assert!(WebApp::new(config).await.is_err());
    assert!(!root.join("key").exists());
    assert!(!root.join("new-private").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_joins_accepted_scan_and_rejects_further_operations() {
    let (temp, app) = fixture().await;
    for i in 0..100 {
        fs::write(
            temp.path().join(format!("root/file-{i}.txt")),
            vec![b'x'; 1000],
        )
        .unwrap();
    }
    assert_eq!(
        request(&app, "/api/scan", Some(json!({}))).await.0,
        StatusCode::ACCEPTED
    );
    app.shutdown().await.unwrap();
    let s = app.snapshot().await;
    assert_eq!(s.activity[0].status, "success");
    assert_eq!(s.scan.unwrap()["total_records"], 100);
    assert_eq!(
        request(&app, "/api/scan", Some(json!({}))).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn session_tokens_are_fresh_and_static_assets_do_not_expose_them() {
    let (_temp, app) = fixture().await;
    let (_temp2, other) = fixture().await;
    assert_ne!(app.token(), other.token());
    assert_eq!(app.token().len(), 64);
    for path in ["/", "/styles.css", "/app.js", "/model.js"] {
        let req = Request::builder()
            .uri(path)
            .header("host", "127.0.0.1:7840")
            .body(Body::empty())
            .unwrap();
        let response = app.router("127.0.0.1:7840").oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let content = String::from_utf8(
            to_bytes(response.into_body(), 1024 * 1024)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        assert!(!content.contains(app.token()));
    }
    app.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_scan_preserves_previous_results_then_recovers() {
    let (temp, app) = fixture().await;
    let root = temp.path().join("root");
    fs::write(root.join("retained.txt"), "content").unwrap();
    request(&app, "/api/scan", Some(json!({}))).await;
    let successful = idle(&app).await;
    fs::rename(&root, temp.path().join("moved")).unwrap();
    request(&app, "/api/scan", Some(json!({}))).await;
    let failed = idle(&app).await;
    assert_eq!(failed["activity"][0]["status"], "error");
    assert_eq!(failed["scan"], successful["scan"]);
    fs::rename(temp.path().join("moved"), root).unwrap();
    request(&app, "/api/scan", Some(json!({}))).await;
    assert_eq!(idle(&app).await["activity"][0]["status"], "success");
    app.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_scan_keeps_state_responsive_and_rejects_overlapping_work() {
    let (temp, app) = fixture().await;
    for i in 0..250 {
        fs::write(
            temp.path().join(format!("root/{i}.bin")),
            vec![b'x'; 100_000],
        )
        .unwrap();
    }
    request(&app, "/api/scan", Some(json!({}))).await;
    let (status, state) = tokio::time::timeout(
        Duration::from_millis(500),
        request(&app, "/api/state", None),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(state["phase"], "scanning");
    assert_eq!(
        request(&app, "/api/scan", Some(json!({}))).await.0,
        StatusCode::CONFLICT
    );
    let result = idle(&app).await;
    assert_eq!(result["scan"]["total_records"], 250);
    assert_eq!(result["scan"]["records"].as_array().unwrap().len(), 200);
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_json_and_unknown_api_routes_have_json_errors() {
    let (_temp, app) = fixture().await;
    for (path, method, content_type, body, expected) in [
        (
            "/api/scan",
            "POST",
            "application/json",
            "{",
            StatusCode::BAD_REQUEST,
        ),
        (
            "/api/scan",
            "POST",
            "text/plain",
            "{}",
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
        (
            "/api/state",
            "DELETE",
            "application/json",
            "{}",
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        (
            "/api/no-such-route",
            "GET",
            "application/json",
            "",
            StatusCode::NOT_FOUND,
        ),
    ] {
        let req = Request::builder()
            .uri(path)
            .method(method)
            .header("host", "127.0.0.1:7840")
            .header("content-type", content_type)
            .header("authorization", format!("Bearer {}", app.token()))
            .body(Body::from(body))
            .unwrap();
        let response = app.router("127.0.0.1:7840").oneshot(req).await.unwrap();
        assert_eq!(response.status(), expected);
        let error: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1024).await.unwrap()).unwrap();
        assert!(error["error"].is_string());
    }
    app.shutdown().await.unwrap();
}

#[tokio::test]
async fn refuses_non_loopback_management_bind() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir(temp.path().join("root")).unwrap();
    let result = WebApp::new(Config {
        root: temp.path().join("root"),
        state: temp.path().join("private"),
        identity: None,
        bind: "0.0.0.0:7840".parse().unwrap(),
        peer_bind: "0.0.0.0:0".parse().unwrap(),
    })
    .await;
    assert!(result.is_err());
    assert!(!temp.path().join("private").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn rejected_scan_reports_collision_paths_and_preserves_last_success_until_repaired() {
    let (temp, app) = fixture().await;
    let root = temp.path().join("root");
    fs::write(root.join("A.txt"), "first file").unwrap();
    request(&app, "/api/scan", Some(json!({}))).await;
    let successful = idle(&app).await;
    assert_eq!(successful["activity"][0]["status"], "success");
    fs::write(root.join("a.txt"), "second file").unwrap();
    request(&app, "/api/scan", Some(json!({}))).await;
    let rejected = idle(&app).await;
    assert_eq!(rejected["activity"][0]["status"], "error");
    assert_eq!(rejected["scan"], successful["scan"]);
    let error = rejected["activity"][0]["error"].as_str().unwrap();
    assert!(error.contains("A.txt"), "missing affected path: {error}");
    assert!(error.contains("a.txt"), "missing affected path: {error}");
    assert!(error.contains("collision"), "missing reason: {error}");
    fs::rename(root.join("a.txt"), root.join("B.txt")).unwrap();
    request(&app, "/api/scan", Some(json!({}))).await;
    let repaired = idle(&app).await;
    assert_eq!(repaired["activity"][0]["status"], "success", "{repaired}");
    assert_eq!(repaired["scan"]["report"]["live_records"], 2);
    app.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn rejected_scan_bounds_issue_details_and_counts_omissions() {
    let (temp, app) = fixture().await;
    for i in 0..12 {
        fs::write(temp.path().join(format!("root/invalid:{i:02}.txt")), "data").unwrap();
    }
    request(&app, "/api/scan", Some(json!({}))).await;
    let rejected = idle(&app).await;
    assert_eq!(rejected["activity"][0]["status"], "error");
    let error = rejected["activity"][0]["error"].as_str().unwrap();
    assert!(error.contains("invalid:00.txt"), "missing path: {error}");
    assert!(
        error.contains("4 additional issues omitted"),
        "missing omission count: {error}"
    );
    assert!(error.len() < 16_384);
    app.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn rejected_scan_includes_persistent_retry_path_and_reason() {
    use std::os::unix::fs::PermissionsExt;
    let (temp, app) = fixture().await;
    let locked = temp.path().join("root/locked.txt");
    fs::write(&locked, "unreadable data").unwrap();
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
    // Root and capability-enabled test runners bypass Unix permission checks.
    if fs::read(&locked).is_ok() {
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o600)).unwrap();
        app.shutdown().await.unwrap();
        return;
    }
    request(&app, "/api/scan", Some(json!({}))).await;
    let rejected = idle(&app).await;
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(rejected["activity"][0]["status"], "error");
    let error = rejected["activity"][0]["error"].as_str().unwrap();
    assert!(
        error.contains("retry: locked.txt"),
        "missing actual retry path: {error}"
    );
    assert!(
        error.to_lowercase().contains("permission denied"),
        "missing retry reason: {error}"
    );
    app.shutdown().await.unwrap();
}
