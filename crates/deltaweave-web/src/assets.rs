use axum::{
    body::Body,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
include!(concat!(env!("OUT_DIR"), "/web_assets.rs"));
pub(crate) fn available() -> bool {
    AVAILABLE
}
pub(crate) fn serve(path: &str, head: bool) -> Response {
    if !available() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "Browser assets are missing. Build web/dist and rebuild DeltaWeave.",
        )
            .into_response();
    }
    let asset = ASSETS.iter().find(|(name, _)| *name == path).or_else(|| {
        if path.starts_with("/assets/")
            || path
                .rsplit('/')
                .next()
                .is_some_and(|name| name.contains('.'))
        {
            None
        } else {
            ASSETS.iter().find(|(name, _)| *name == "/index.html")
        }
    });
    let Some((name, bytes)) = asset else {
        return (StatusCode::NOT_FOUND, "Asset not found").into_response();
    };
    let mime = match name.rsplit('.').next().unwrap_or("") {
        "html" => "text/html; charset=utf-8",
        "js" => "text/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "ico" => "image/x-icon",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "webp" => "image/webp",
        _ => "application/octet-stream",
    };
    Response::builder()
        .header(header::CONTENT_TYPE, mime)
        .header(header::CONTENT_LENGTH, bytes.len())
        .body(if head {
            Body::empty()
        } else {
            Body::from(*bytes)
        })
        .unwrap()
}
