use crate::routes::AppState;
use axum::{
    Json, Router,
    extract::{Path, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use deltaweave_control::{
    CreateShareInput, IssueKeyInput, JoinShareInput, PreviewKeyInput, RemoveShareInput,
    ResumeMembershipInput, RevokeKeyInput, RevokeMemberInput, RotateKeyInput, ShareCommand,
    ShareCommandInput, ValidateKeyInput, classify_managed_error,
};
use deltaweave_net::share::{InvitationId, Permission, ShareId};
use serde::Deserialize;
use serde_json::json;
use std::{path::PathBuf, sync::Arc};

#[derive(Clone, Debug, Deserialize)]
struct CreateBody {
    request_id: String,
    name: String,
    root: String,
    min_free_space_mib: Option<u64>,
}

#[derive(Deserialize)]
struct KeyBody {
    request_id: String,
    key: String,
}

#[derive(Deserialize)]
struct JoinBody {
    request_id: String,
    key: String,
    destination_root: String,
}

#[derive(Clone, Debug, Deserialize)]
struct ResumeBody {
    request_id: String,
    share_id: String,
}

#[derive(Clone, Debug, Deserialize)]
struct IssueBody {
    request_id: String,
    permission: Permission,
    expires_at: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
struct RotateBody {
    request_id: String,
    expires_at: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
struct RequestBody {
    request_id: String,
}

pub(crate) fn router() -> Router<AppState> {
    // Keep static paths before the share-id routes. This is also documented in
    // the contract because "preview" must never be parsed as a share ID.
    Router::new()
        .route("/api/v1/shares/preview", post(preview))
        .route("/api/v1/shares/validate", post(validate))
        .route("/api/v1/shares/join", post(join))
        .route("/api/v1/shares/resume", post(resume_membership))
        .route("/api/v1/shares", get(list).post(create))
        .route(
            "/api/v1/shares/{share_id}/keys/{invitation_id}/rotate",
            post(rotate_key),
        )
        .route(
            "/api/v1/shares/{share_id}/keys/{invitation_id}/revoke",
            post(revoke_key),
        )
        .route(
            "/api/v1/shares/{share_id}/members/{member_id}/revoke",
            post(revoke_member),
        )
        .route(
            "/api/v1/shares/{share_id}/keys",
            get(list_keys).post(issue_key),
        )
        .route("/api/v1/shares/{share_id}/members", get(list_members))
        .route("/api/v1/shares/{share_id}/sync", post(sync))
        .route("/api/v1/shares/{share_id}/pause", post(pause))
        .route("/api/v1/shares/{share_id}/resume", post(resume))
        .route("/api/v1/shares/{share_id}", get(get_one).delete(remove))
}

fn valid_opaque(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && !value
            .chars()
            .any(|ch| ch.is_control() || matches!(ch, '/' | '\\'))
}

fn valid_key(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 16_384 && !value.chars().any(char::is_control)
}

fn validate_body(request_id: &str) -> Option<Response> {
    if valid_opaque(request_id) {
        None
    } else {
        Some(client_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request_id",
            "요청 ID 형식이 올바르지 않습니다.",
            None,
        ))
    }
}

fn validate_key_body(request_id: &str, key: &str) -> Option<Response> {
    validate_body(request_id).or_else(|| {
        (!valid_key(key)).then(|| {
            client_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "invalid_ticket",
                "공유 키 형식이 올바르지 않습니다.",
                Some(request_id),
            )
        })
    })
}

fn parse_id<T>(value: &str, label: &'static str, make: fn([u8; 32]) -> T) -> Result<T, Response> {
    if value.len() != 64
        || value
            .chars()
            .any(|ch| !ch.is_ascii_hexdigit() || ch.is_ascii_uppercase())
    {
        return Err(client_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_id",
            label,
            None,
        ));
    }
    let bytes = hex::decode(value)
        .map_err(|_| client_error(StatusCode::UNPROCESSABLE_ENTITY, "invalid_id", label, None))?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| client_error(StatusCode::UNPROCESSABLE_ENTITY, "invalid_id", label, None))?;
    Ok(make(bytes))
}

fn share_id(value: &str) -> Result<ShareId, Response> {
    parse_id(value, "공유 ID 형식이 올바르지 않습니다.", ShareId)
}

fn invitation_id(value: &str) -> Result<InvitationId, Response> {
    parse_id(value, "초대 ID 형식이 올바르지 않습니다.", InvitationId)
}

fn json_error(rejection: JsonRejection) -> Response {
    client_error(
        rejection.status(),
        "invalid_json",
        "요청 형식을 읽을 수 없습니다.",
        None,
    )
}

fn client_error(
    status: StatusCode,
    code: &'static str,
    message: &'static str,
    request_id: Option<&str>,
) -> Response {
    let mut value = json!({"error": message, "error_code": code});
    if let Some(request_id) = request_id.filter(|id| valid_opaque(id)) {
        value["request_id"] = json!(request_id);
    }
    (status, Json(value)).into_response()
}

fn managed_error(error: anyhow::Error, request_id: Option<&str>) -> Response {
    let summary = classify_managed_error(&error);
    let status = match summary.code.as_str() {
        "unauthorized" | "owner_only" | "permission_denied" | "path_denied" | "not_member"
        | "member_revoked" | "owner_mismatch" => StatusCode::FORBIDDEN,
        "not_found" | "unknown_share" | "unknown_invitation" | "unknown_member" => {
            StatusCode::NOT_FOUND
        }
        "idempotency_conflict"
        | "duplicate"
        | "busy"
        | "key_response_expired"
        | "replica_claim_rejected" => StatusCode::CONFLICT,
        "offline"
        | "private_state_unavailable"
        | "state_unavailable"
        | "transfer_failed"
        | "shutdown"
        | "idempotency_capacity" => StatusCode::SERVICE_UNAVAILABLE,
        "invalid_ticket"
        | "unsupported_version"
        | "invitation_revoked"
        | "expired"
        | "invalid_path"
        | "invalid_request" => StatusCode::UNPROCESSABLE_ENTITY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let mut value = json!({
        "error": summary.message,
        "error_code": summary.code,
    });
    if let Some(request_id) = request_id.filter(|id| valid_opaque(id)) {
        value["request_id"] = json!(request_id);
    }
    (status, Json(value)).into_response()
}

fn result<T: serde::Serialize>(value: anyhow::Result<T>, request_id: Option<&str>) -> Response {
    match value {
        Ok(value) => Json(value).into_response(),
        Err(error) => managed_error(error, request_id),
    }
}

async fn list(State(state): State<Arc<AppState>>) -> Response {
    result(state.manager.list_shares().await, None)
}

async fn create(
    State(state): State<Arc<AppState>>,
    input: Result<Json<CreateBody>, JsonRejection>,
) -> Response {
    let Json(input) = match input {
        Ok(value) => value,
        Err(error) => return json_error(error),
    };
    if let Some(error) = validate_body(&input.request_id) {
        return error;
    }
    if input.name.trim().is_empty()
        || input.name.len() > 255
        || input.name.chars().any(char::is_control)
        || input.root.trim().is_empty()
    {
        return client_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_request",
            "공유 이름과 폴더를 확인하세요.",
            Some(&input.request_id),
        );
    }
    let request_id = input.request_id.clone();
    let value = state
        .manager
        .create_share(CreateShareInput {
            request_id: input.request_id,
            name: input.name.trim().to_owned(),
            root: PathBuf::from(input.root.trim()),
            min_free_space_mib: input.min_free_space_mib,
        })
        .await;
    match value {
        Ok(value) => (StatusCode::CREATED, Json(value)).into_response(),
        Err(error) => managed_error(error, Some(&request_id)),
    }
}

async fn preview(
    State(state): State<Arc<AppState>>,
    input: Result<Json<KeyBody>, JsonRejection>,
) -> Response {
    let Json(input) = match input {
        Ok(value) => value,
        Err(error) => return json_error(error),
    };
    if let Some(error) = validate_key_body(&input.request_id, &input.key) {
        return error;
    }
    let request_id = input.request_id.clone();
    result(
        state
            .manager
            .preview_share_key(PreviewKeyInput {
                request_id: input.request_id,
                encoded_key: input.key,
            })
            .await,
        Some(&request_id),
    )
}

async fn validate(
    State(state): State<Arc<AppState>>,
    input: Result<Json<KeyBody>, JsonRejection>,
) -> Response {
    let Json(input) = match input {
        Ok(value) => value,
        Err(error) => return json_error(error),
    };
    if let Some(error) = validate_key_body(&input.request_id, &input.key) {
        return error;
    }
    let request_id = input.request_id.clone();
    result(
        state
            .manager
            .validate_share_key(ValidateKeyInput {
                request_id: input.request_id,
                encoded_key: input.key,
            })
            .await,
        Some(&request_id),
    )
}

async fn join(
    State(state): State<Arc<AppState>>,
    input: Result<Json<JoinBody>, JsonRejection>,
) -> Response {
    let Json(input) = match input {
        Ok(value) => value,
        Err(error) => return json_error(error),
    };
    if let Some(error) = validate_key_body(&input.request_id, &input.key) {
        return error;
    }
    if input.destination_root.trim().is_empty() {
        return client_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_path",
            "저장 폴더를 선택하세요.",
            Some(&input.request_id),
        );
    }
    let request_id = input.request_id.clone();
    let value = state
        .manager
        .join_share(JoinShareInput {
            request_id: input.request_id,
            encoded_key: input.key,
            destination_root: PathBuf::from(input.destination_root.trim()),
        })
        .await;
    match value {
        Ok(value) => {
            let status = if value.enrollment == deltaweave_control::EnrollmentState::Waiting {
                StatusCode::ACCEPTED
            } else {
                StatusCode::OK
            };
            (status, Json(value)).into_response()
        }
        Err(error) => managed_error(error, Some(&request_id)),
    }
}

async fn resume_membership(
    State(state): State<Arc<AppState>>,
    input: Result<Json<ResumeBody>, JsonRejection>,
) -> Response {
    let Json(input) = match input {
        Ok(value) => value,
        Err(error) => return json_error(error),
    };
    if let Some(error) = validate_body(&input.request_id) {
        return error;
    }
    let share = match share_id(&input.share_id) {
        Ok(value) => value,
        Err(error) => return error,
    };
    let request_id = input.request_id.clone();
    result(
        state
            .manager
            .resume_membership(ResumeMembershipInput {
                request_id: input.request_id,
                share,
            })
            .await,
        Some(&request_id),
    )
}

async fn get_one(State(state): State<Arc<AppState>>, Path(raw_share): Path<String>) -> Response {
    if share_id(&raw_share).is_err() {
        return client_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_id",
            "공유 ID 형식이 올바르지 않습니다.",
            None,
        );
    }
    match state.manager.list_shares().await {
        Ok(shares) => shares
            .into_iter()
            .find(|share| share.share_id == raw_share)
            .map_or_else(
                || {
                    client_error(
                        StatusCode::NOT_FOUND,
                        "unknown_share",
                        "공유를 찾을 수 없습니다.",
                        None,
                    )
                },
                |share| Json(share).into_response(),
            ),
        Err(error) => managed_error(error, None),
    }
}

async fn list_keys(State(state): State<Arc<AppState>>, Path(raw_share): Path<String>) -> Response {
    let share = match share_id(&raw_share) {
        Ok(value) => value,
        Err(error) => return error,
    };
    result(state.manager.list_keys(share).await, None)
}

async fn issue_key(
    State(state): State<Arc<AppState>>,
    Path(raw_share): Path<String>,
    input: Result<Json<IssueBody>, JsonRejection>,
) -> Response {
    let share = match share_id(&raw_share) {
        Ok(value) => value,
        Err(error) => return error,
    };
    let Json(input) = match input {
        Ok(value) => value,
        Err(error) => return json_error(error),
    };
    if let Some(error) = validate_body(&input.request_id) {
        return error;
    }
    let request_id = input.request_id.clone();
    result(
        state
            .manager
            .issue_key(IssueKeyInput {
                request_id: input.request_id,
                share,
                permission: input.permission,
                expires_at: input.expires_at,
            })
            .await,
        Some(&request_id),
    )
}

async fn rotate_key(
    State(state): State<Arc<AppState>>,
    Path((raw_share, raw_invitation)): Path<(String, String)>,
    input: Result<Json<RotateBody>, JsonRejection>,
) -> Response {
    let share = match share_id(&raw_share) {
        Ok(value) => value,
        Err(error) => return error,
    };
    let invitation = match invitation_id(&raw_invitation) {
        Ok(value) => value,
        Err(error) => return error,
    };
    let Json(input) = match input {
        Ok(value) => value,
        Err(error) => return json_error(error),
    };
    if let Some(error) = validate_body(&input.request_id) {
        return error;
    }
    let request_id = input.request_id.clone();
    result(
        state
            .manager
            .rotate_key(RotateKeyInput {
                request_id: input.request_id,
                share,
                invitation,
                expires_at: input.expires_at,
            })
            .await,
        Some(&request_id),
    )
}

async fn revoke_key(
    State(state): State<Arc<AppState>>,
    Path((raw_share, raw_invitation)): Path<(String, String)>,
    input: Result<Json<RequestBody>, JsonRejection>,
) -> Response {
    let share = match share_id(&raw_share) {
        Ok(value) => value,
        Err(error) => return error,
    };
    let invitation = match invitation_id(&raw_invitation) {
        Ok(value) => value,
        Err(error) => return error,
    };
    let Json(input) = match input {
        Ok(value) => value,
        Err(error) => return json_error(error),
    };
    if let Some(error) = validate_body(&input.request_id) {
        return error;
    }
    let request_id = input.request_id.clone();
    result(
        state
            .manager
            .revoke_key(RevokeKeyInput {
                request_id: input.request_id,
                share,
                invitation,
            })
            .await,
        Some(&request_id),
    )
}

async fn list_members(
    State(state): State<Arc<AppState>>,
    Path(raw_share): Path<String>,
) -> Response {
    let share = match share_id(&raw_share) {
        Ok(value) => value,
        Err(error) => return error,
    };
    result(state.manager.list_members(share).await, None)
}

async fn revoke_member(
    State(state): State<Arc<AppState>>,
    Path((raw_share, member_id)): Path<(String, String)>,
    input: Result<Json<RequestBody>, JsonRejection>,
) -> Response {
    let share = match share_id(&raw_share) {
        Ok(value) => value,
        Err(error) => return error,
    };
    if !valid_opaque(&member_id) {
        return client_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_member_id",
            "멤버 ID 형식이 올바르지 않습니다.",
            None,
        );
    }
    let Json(input) = match input {
        Ok(value) => value,
        Err(error) => return json_error(error),
    };
    if let Some(error) = validate_body(&input.request_id) {
        return error;
    }
    let request_id = input.request_id.clone();
    result(
        state
            .manager
            .revoke_member(RevokeMemberInput {
                request_id: input.request_id,
                share,
                member_id,
            })
            .await,
        Some(&request_id),
    )
}

async fn remove(
    State(state): State<Arc<AppState>>,
    Path(raw_share): Path<String>,
    input: Result<Json<RequestBody>, JsonRejection>,
) -> Response {
    let share = match share_id(&raw_share) {
        Ok(value) => value,
        Err(error) => return error,
    };
    let Json(input) = match input {
        Ok(value) => value,
        Err(error) => return json_error(error),
    };
    if let Some(error) = validate_body(&input.request_id) {
        return error;
    }
    let request_id = input.request_id.clone();
    result(
        state
            .manager
            .remove_share(RemoveShareInput {
                request_id: input.request_id,
                share,
            })
            .await,
        Some(&request_id),
    )
}

async fn command(
    state: Arc<AppState>,
    raw_share: String,
    input: Result<Json<RequestBody>, JsonRejection>,
    command: ShareCommand,
) -> Response {
    let share = match share_id(&raw_share) {
        Ok(value) => value,
        Err(error) => return error,
    };
    let Json(input) = match input {
        Ok(value) => value,
        Err(error) => return json_error(error),
    };
    if let Some(error) = validate_body(&input.request_id) {
        return error;
    }
    let request_id = input.request_id.clone();
    result(
        state
            .manager
            .share_command(ShareCommandInput {
                request_id: input.request_id,
                share,
                command,
            })
            .await,
        Some(&request_id),
    )
}

async fn sync(
    State(state): State<Arc<AppState>>,
    Path(raw_share): Path<String>,
    input: Result<Json<RequestBody>, JsonRejection>,
) -> Response {
    command(state, raw_share, input, ShareCommand::Sync).await
}

async fn pause(
    State(state): State<Arc<AppState>>,
    Path(raw_share): Path<String>,
    input: Result<Json<RequestBody>, JsonRejection>,
) -> Response {
    command(state, raw_share, input, ShareCommand::Pause).await
}

async fn resume(
    State(state): State<Arc<AppState>>,
    Path(raw_share): Path<String>,
    input: Result<Json<RequestBody>, JsonRejection>,
) -> Response {
    command(state, raw_share, input, ShareCommand::Resume).await
}
