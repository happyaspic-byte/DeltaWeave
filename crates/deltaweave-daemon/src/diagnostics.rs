//! Redacted diagnostic bundles for GUI export.

use anyhow::Result;
use serde::Serialize;

/// Strips pairing tickets and private-key material from diagnostic text.
#[must_use]
pub fn redact_diagnostics(raw: &str) -> String {
    let without_tickets = redact_prefix_payloads(raw, "dwpair1:");
    let without_identity = redact_labeled_value(&without_tickets, "identity=");
    redact_labeled_value(&without_identity, "private_key=")
}

/// Builds a JSON diagnostic bundle with secrets already stripped.
pub fn diagnostic_bundle_json(
    job_name: &str,
    error_code: &str,
    os: &str,
    version: &str,
    details: &str,
) -> Result<String> {
    #[derive(Serialize)]
    struct Bundle<'a> {
        job_name: &'a str,
        error_code: &'a str,
        os: &'a str,
        version: &'a str,
        details: String,
    }

    Ok(serde_json::to_string(&Bundle {
        job_name,
        error_code,
        os,
        version,
        details: redact_diagnostics(details),
    })?)
}

fn redact_prefix_payloads(raw: &str, prefix: &str) -> String {
    let mut out = String::new();
    let mut rest = raw;
    while let Some(index) = rest.find(prefix) {
        out.push_str(&rest[..index]);
        out.push_str("[redacted-ticket]");
        rest = &rest[index + prefix.len()..];
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
}

fn redact_labeled_value(raw: &str, label: &str) -> String {
    let mut out = String::new();
    let mut rest = raw;
    while let Some(index) = rest.find(label) {
        out.push_str(&rest[..index]);
        out.push_str(label);
        out.push_str("[redacted]");
        rest = &rest[index + label.len()..];
        let end = rest.find(char::is_whitespace).unwrap_or(rest.len());
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_bundle_redacts_ticket_codes_and_private_keys() {
        let raw = r#"issued dwpair1:deadbeef identity=not-hex/private-key winner_hash=abcd1234"#;
        let redacted = redact_diagnostics(raw);
        assert!(!redacted.contains("dwpair1:"));
        assert!(!redacted.contains("not-hex/private-key"));
        assert!(redacted.contains("winner_hash=abcd1234"));
    }

    #[test]
    fn structured_bundle_keeps_safe_operational_fields() {
        let bundle = diagnostic_bundle_json(
            "DeltaWeave-Private",
            "permission_denied",
            "windows",
            "0.3.0",
            "ticket=dwpair1:secret private_key=abcdef winner_hash=1234",
        )
        .unwrap();
        assert!(bundle.contains("DeltaWeave-Private"));
        assert!(bundle.contains("permission_denied"));
        assert!(bundle.contains("winner_hash=1234"));
        assert!(!bundle.contains("dwpair1:"));
        assert!(!bundle.contains("abcdef"));
    }
}
