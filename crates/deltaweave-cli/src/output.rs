//! Human-readable presentation alongside the stable JSON output contract.

use std::{
    env,
    io::{self, IsTerminal, Write},
};

use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
pub enum OutputFormat {
    #[default]
    Json,
    Text,
}

#[derive(Clone, Copy)]
pub struct Output {
    format: OutputFormat,
    command: &'static str,
}

impl Output {
    pub const fn new(format: OutputFormat, command: &'static str) -> Self {
        Self { format, command }
    }

    pub fn print(&self, value: &impl Serialize) -> Result<()> {
        let stdout = io::stdout();
        self.write(value, &mut stdout.lock(), use_color(stdout.is_terminal()))
    }

    pub fn progress(&self) {
        let _ = self.write_progress(&mut io::stderr().lock());
    }

    pub fn error(&self, error: &anyhow::Error) -> io::Result<()> {
        let stderr = io::stderr();
        self.write_error(error, &mut stderr.lock(), use_color(stderr.is_terminal()))
    }

    fn write(&self, value: &impl Serialize, writer: &mut impl Write, color: bool) -> Result<()> {
        match self.format {
            OutputFormat::Json => {
                // Serialize the original type: conversion through Value would reorder struct fields.
                serde_json::to_writer_pretty(&mut *writer, value)?;
                writeln!(writer)?;
            }
            OutputFormat::Text => {
                let value = serde_json::to_value(value)?;
                heading(
                    writer,
                    &format!("DeltaWeave · {}", title(self.command, &value)),
                    0,
                    color,
                )?;
                render_value(writer, &value, 2, color)?;
                writeln!(writer)?;
            }
        }
        Ok(())
    }

    fn write_progress(&self, writer: &mut impl Write) -> io::Result<()> {
        if matches!(self.format, OutputFormat::Text) {
            let action = match self.command {
                "init" => "Loading node identity",
                "manifest" => "Hashing file and building manifest",
                "serve" => "Starting receiver",
                "push" => "Connecting and transferring file",
                "scan" => "Scanning local directory",
                "watch" => "Starting directory watcher",
                "sync-once" => "Reconciling folders",
                "sync" => "Starting continuous synchronization",
                "self-test" => "Running isolated end-to-end checks",
                "fault-test" => "Running restart and network fault checks",
                _ => "Working",
            };
            writeln!(writer, "{action}…")?;
        }
        Ok(())
    }

    fn write_error(
        &self,
        error: &anyhow::Error,
        writer: &mut impl Write,
        color: bool,
    ) -> io::Result<()> {
        heading(
            writer,
            &format!("Error · {}", escape(self.command)),
            0,
            color,
        )?;
        for (index, cause) in error.chain().enumerate() {
            let label = if index == 0 { "" } else { "Caused by: " };
            writeln!(writer, "  {label}{}", escape(&cause.to_string()))?;
        }
        Ok(())
    }
}

fn use_color(terminal: bool) -> bool {
    terminal
        && env::var_os("NO_COLOR").is_none()
        && env::var_os("TERM").is_none_or(|term| term != "dumb")
}

fn heading(writer: &mut impl Write, text: &str, indent: usize, color: bool) -> io::Result<()> {
    if color {
        writeln!(writer, "{:indent$}\x1b[1;36m{text}\x1b[0m", "")
    } else {
        writeln!(writer, "{:indent$}{text}", "")
    }
}

fn has_items(value: &Value, key: &str) -> bool {
    value.get(key).is_some_and(|value| match value {
        Value::Array(items) => !items.is_empty(),
        Value::Number(number) => number.as_u64().is_some_and(|count| count > 0),
        _ => false,
    })
}

fn needs_attention(value: &Value) -> bool {
    ["issues", "collisions", "retries", "retries_queued"]
        .iter()
        .any(|key| has_items(value, key))
}

fn title(command: &str, value: &Value) -> &'static str {
    let report = value.get("report").unwrap_or(value);
    let status = value.get("status").and_then(Value::as_str);
    let event = value.get("event").and_then(Value::as_str);
    let attention = needs_attention(value) || needs_attention(report);
    let fallback = status == Some("polling_fallback")
        || value.get("watcher_degraded").and_then(Value::as_bool) == Some(true)
        || value.get("local_change_detection").and_then(Value::as_str) == Some("polling_fallback")
        || value
            .get("watcher_error")
            .is_some_and(|error| !error.is_null());

    if status == Some("stopped") || event == Some("shutdown") {
        return "Stopped";
    }
    if status == Some("retrying") || event == Some("sync_error") {
        return "Retrying synchronization";
    }
    if matches!(status, Some("fail" | "failed" | "forced_failure")) {
        return match command {
            "self-test" => "Self-test failed",
            "fault-test" => "Fault test failed",
            _ => "Operation failed",
        };
    }
    if status == Some("ready") {
        return "Ready to receive";
    }
    if status == Some("synchronizing") || event == Some("local_change") {
        return "Synchronizing local changes";
    }
    if command == "watch" || event == Some("sync_started") {
        return match (fallback, attention) {
            (true, true) => "Polling fallback · scan needs attention",
            (true, false) => "Polling fallback · watching for changes",
            (false, true) => "Watching · scan needs attention",
            (false, false) => "Watching for changes",
        };
    }
    match command {
        "init" if value.get("created").and_then(Value::as_bool) == Some(false) => {
            "Using existing identity"
        }
        "init" => "Identity created",
        "manifest" => "Manifest ready",
        "push" => "Transfer verified",
        "scan" if attention => "Scan complete · needs attention",
        "scan" => "Scan complete",
        "sync-once" | "sync" if has_items(report, "conflicts") => {
            "Folders reconciled · conflicts recorded"
        }
        "sync-once" | "sync" if attention => "Synchronization needs attention",
        "sync-once" | "sync" => "Folders synchronized · roots verified",
        "self-test" => "Self-test passed",
        "fault-test" => "Fault test passed",
        _ => "Result",
    }
}

/// Render all fields, including unfamiliar ones, so new backend data is never silently omitted.
fn render_value(
    writer: &mut impl Write,
    value: &Value,
    indent: usize,
    color: bool,
) -> io::Result<()> {
    match value {
        Value::Object(fields) if !fields.is_empty() => {
            let mut fields: Vec<_> = fields.iter().collect();
            fields.sort_by_key(|(key, value)| field_order(key, value));
            for (key, value) in fields {
                render_field(writer, key, value, indent, color)?;
            }
        }
        Value::Array(items) if !items.is_empty() => {
            for (index, item) in items.iter().enumerate() {
                if item.is_object() || item.is_array() {
                    writeln!(writer, "{:indent$}[{}]", "", index + 1)?;
                    render_value(writer, item, indent + 2, color)?;
                } else {
                    writeln!(writer, "{:indent$}- {}", "", scalar("", item))?;
                }
            }
        }
        _ => writeln!(writer, "{:indent$}{}", "", scalar("", value))?,
    }
    Ok(())
}

fn field_order<'a>(key: &'a str, value: &Value) -> (u8, &'a str) {
    let rank = match key {
        "status" | "event" => 0,
        _ if value.is_number() => 1,
        _ if value.is_boolean() => 2,
        _ if !value.is_object() && !value.is_array() => 3,
        "report" => 4,
        _ if value.is_object() => 5,
        "records" | "chunks" | "operations" => 7,
        _ => 6,
    };
    (rank, key)
}

fn render_field(
    writer: &mut impl Write,
    key: &str,
    value: &Value,
    indent: usize,
    color: bool,
) -> io::Result<()> {
    let label = label(key);
    match value {
        Value::Array(items) => {
            if items.is_empty() {
                let empty = if key == "changes" {
                    "No changes"
                } else {
                    "None"
                };
                writeln!(writer, "{:indent$}{label} (0): {empty}", "")?;
            } else {
                writeln!(writer)?;
                heading(writer, &format!("{label} ({})", items.len()), indent, color)?;
                render_value(writer, value, indent + 2, color)?;
            }
        }
        Value::Object(fields) if !fields.is_empty() => {
            writeln!(writer)?;
            heading(writer, &label, indent, color)?;
            render_value(writer, value, indent + 2, color)?;
        }
        _ => writeln!(writer, "{:indent$}{label}: {}", "", scalar(key, value))?,
    }
    Ok(())
}

fn scalar(key: &str, value: &Value) -> String {
    match value {
        Value::String(text) if text.is_empty() => "\"\"".into(),
        Value::String(text) => escape(text),
        Value::Null | Value::Object(_) | Value::Array(_) => "None".into(),
        Value::Number(number) if is_byte_count(key) => {
            if let Some(bytes) = number.as_u64() {
                format_bytes(bytes)
            } else {
                format!("{number} bytes")
            }
        }
        _ => value.to_string(),
    }
}

fn is_byte_count(key: &str) -> bool {
    key.ends_with("_bytes")
        || matches!(
            key,
            "size" | "final_size" | "length" | "offset" | "min_size" | "avg_size" | "max_size"
        )
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        return format!("{bytes} bytes");
    }
    let mut amount = bytes as f64;
    let mut unit = "bytes";
    for next in ["KiB", "MiB", "GiB", "TiB", "PiB", "EiB"] {
        amount /= 1024.0;
        unit = next;
        if amount < 1024.0 {
            break;
        }
    }
    format!("{amount:.1} {unit} ({bytes} bytes)")
}

/// Only schema field names are humanized. Map keys such as replica IDs remain intact.
fn label(key: &str) -> String {
    match key {
        "endpoint_id" => return "Endpoint ID".into(),
        "relay_urls" => return "Relay URLs".into(),
        _ => {}
    }
    const FIELDS: &[&str] = &[
        "architecture",
        "attempts",
        "avg_size",
        "barrier",
        "bundle",
        "changes",
        "chunks",
        "collision_key",
        "collisions",
        "conflict_path",
        "conflicts",
        "content_hash",
        "created",
        "desired_root",
        "destination",
        "direct_addresses",
        "error",
        "event",
        "faults",
        "file",
        "file_hash",
        "files_hashed",
        "final_merkle_root",
        "final_size",
        "first_transfer_bytes",
        "from",
        "generation",
        "hash",
        "identity",
        "identity_file",
        "index_initial_records",
        "index_rename_detected",
        "index_restart_verified",
        "index_tombstones",
        "issues",
        "killed_process",
        "kind",
        "last_error",
        "length",
        "live_records",
        "local_actions",
        "local_before_root",
        "local_change_detection",
        "loser_hash",
        "manifest_hash",
        "max_size",
        "merkle_queries",
        "message",
        "min_size",
        "modified_ns",
        "native_events",
        "not_before_ms",
        "offset",
        "operating_system",
        "operations",
        "path",
        "paths",
        "peer",
        "peer_logs",
        "pid",
        "profile",
        "pulled_bytes",
        "pulled_remote_files",
        "pushed_bytes",
        "readonly",
        "reason",
        "records",
        "remote_actions",
        "remote_before_root",
        "remote_poll_seconds",
        "report",
        "rescan_required",
        "restart_local_actions",
        "restart_remote_actions",
        "retries",
        "retries_queued",
        "retry_in_seconds",
        "reused_extents",
        "root",
        "roots",
        "schema_version",
        "second_transfer_bytes",
        "seed",
        "sequence",
        "size",
        "staged_local_files",
        "states",
        "status",
        "sync_bidirectional_verified",
        "sync_conflicts_preserved",
        "sync_delete_verified",
        "sync_restart_actions",
        "sync_verified_root",
        "temporary_data",
        "to",
        "tombstone",
        "tombstones",
        "transferred_bytes",
        "unchanged",
        "verified_local_root",
        "verified_remote_root",
        "version",
        "watcher_degraded",
        "watcher_error",
        "winner_hash",
    ];
    if FIELDS.contains(&key) {
        let mut label = key.replace('_', " ");
        label[..1].make_ascii_uppercase();
        label
    } else {
        escape(key)
    }
}

/// Escape control sequences and directional overrides before writing untrusted terminal text.
fn escape(text: &str) -> String {
    let mut safe = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '\\' => safe.push_str("\\\\"),
            '\n' => safe.push_str("\\n"),
            '\r' => safe.push_str("\\r"),
            '\t' => safe.push_str("\\t"),
            '\u{0000}'..='\u{001f}'
            | '\u{007f}'..='\u{009f}'
            | '\u{061c}'
            | '\u{200e}'
            | '\u{200f}'
            | '\u{2028}'..='\u{202e}'
            | '\u{2066}'..='\u{2069}' => {
                use std::fmt::Write as _;
                // Formatting into a String is infallible.
                write!(safe, "\\u{{{:04x}}}", u32::from(character))
                    .expect("String formatting cannot fail");
            }
            _ => safe.push(character),
        }
    }
    safe
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn text(command: &'static str, value: &Value) -> String {
        let mut bytes = Vec::new();
        Output::new(OutputFormat::Text, command)
            .write(value, &mut bytes, false)
            .unwrap();
        String::from_utf8(bytes).unwrap()
    }

    #[test]
    fn json_keeps_struct_order_indentation_and_trailing_newline() {
        #[derive(Serialize)]
        struct Fixture {
            zebra: u8,
            alpha: &'static str,
        }
        let mut bytes = Vec::new();
        Output::new(OutputFormat::Json, "init")
            .write(
                &Fixture {
                    zebra: 7,
                    alpha: "x\ny",
                },
                &mut bytes,
                true,
            )
            .unwrap();
        assert_eq!(bytes, b"{\n  \"zebra\": 7,\n  \"alpha\": \"x\\ny\"\n}\n");
    }

    #[test]
    fn scan_counters_precede_change_details_and_empty_changes_are_explicit() {
        let output = text(
            "scan",
            &json!({
                "generation": 3, "live_records": 4, "files_hashed": 2,
                "unchanged": 2, "tombstones": 0, "retries_queued": 0,
                "changes": [], "collisions": [], "issues": []
            }),
        );
        assert!(output.lines().next().unwrap().contains("Scan complete"));
        assert!(output.contains("No changes"));
        assert!(output.find("Files hashed: 2").unwrap() < output.find("Changes").unwrap());
        assert!(output.contains("Collisions (0): None"));
        assert!(output.contains("Issues (0): None"));
    }

    #[test]
    fn nested_records_retain_every_value_and_unknown_key() {
        let endpoint = "abcdef0123456789".repeat(4);
        let hash = "1234567890abcdef".repeat(4);
        let output = text(
            "scan",
            &json!({
                "report": {"generation": 9, "changes": [{"kind": "renamed", "from": "旧/notes.txt", "to": "新/notes.txt"}]},
                "records": [{"path": "新/notes.txt", "endpoint_id": endpoint, "hash": hash, "size": 2048,
                    "version": {"replica_with_underscores": 17}, "tombstone": false, "file": null}],
                "retries": [{"path": "retry.txt", "attempts": 3, "not_before_ms": 123456, "last_error": "busy"}]
            }),
        );
        for expected in [
            "旧/notes.txt",
            "新/notes.txt",
            endpoint.as_str(),
            hash.as_str(),
            "renamed",
            "replica_with_underscores: 17",
            "2048 bytes",
            "false",
            "File: None",
            "retry.txt",
            "Attempts: 3",
            "Not before ms: 123456",
            "busy",
        ] {
            assert!(output.contains(expected), "missing {expected:?}: {output}");
        }
        assert!(output.find("Generation: 9").unwrap() < output.find("Records (1)").unwrap());
    }

    #[test]
    fn scan_issues_collisions_and_queued_retries_require_attention() {
        for report in [
            json!({"issues": [{"path": "bad", "kind": "hash_failed", "message": "unreadable"}]}),
            json!({"collisions": [{"collision_key": "a", "paths": ["a", "A"]}]}),
            json!({"retries_queued": 1}),
            json!({"report": {"retries_queued": 0}, "retries": [{"path": "later"}]}),
        ] {
            let output = text("scan", &report);
            assert!(
                output.lines().next().unwrap().contains("attention"),
                "{output}"
            );
        }
    }

    #[test]
    fn sync_conflicts_are_visible_in_the_status() {
        let output = text(
            "sync-once",
            &json!({
                "status": "converged", "local_actions": 1, "remote_actions": 1,
                "conflicts": [{"path": "notes.txt", "conflict_path": "notes.conflict.txt"}]
            }),
        );
        assert!(output.lines().next().unwrap().contains("conflicts"));
        assert!(output.contains("notes.conflict.txt"));
    }

    #[test]
    fn daemon_events_describe_the_current_state() {
        for (command, event, expected) in [
            (
                "serve",
                json!({"status": "ready", "endpoint_id": "full-id"}),
                "Ready",
            ),
            (
                "watch",
                json!({"event": "initial_scan", "status": "watching", "report": {"changes": []}}),
                "Watching",
            ),
            (
                "watch",
                json!({"event": "fallback_scan", "watcher_degraded": true, "report": {"changes": []}}),
                "Polling fallback",
            ),
            (
                "sync",
                json!({"event": "sync_started", "local_change_detection": "native_watcher", "watcher_error": null}),
                "Watching",
            ),
            (
                "sync",
                json!({"event": "sync_error", "status": "retrying", "retry_in_seconds": 4, "error": "peer unavailable"}),
                "Retrying",
            ),
            (
                "sync",
                json!({"event": "local_change", "status": "synchronizing", "native_events": 2}),
                "Synchronizing",
            ),
            (
                "watch",
                json!({"event": "shutdown", "status": "stopped"}),
                "Stopped",
            ),
        ] {
            let output = text(command, &event);
            assert!(
                output.lines().next().unwrap().contains(expected),
                "{output}"
            );
            assert!(!output.contains('\r'));
        }
    }

    #[test]
    fn identity_reuse_and_failed_tests_have_honest_titles() {
        assert!(
            text("init", &json!({"created": false}))
                .lines()
                .next()
                .unwrap()
                .contains("existing")
        );
        let failure = text(
            "fault-test",
            &json!({"status": "fail", "error": "injected failure", "bundle": "/evidence"}),
        );
        assert!(failure.lines().next().unwrap().contains("failed"));
        assert!(failure.contains("/evidence"));
    }

    #[test]
    fn forced_failure_keeps_its_failed_status() {
        let output = text(
            "fault-test",
            &json!({"status": "forced_failure", "error": null}),
        );
        assert!(output.lines().next().unwrap().contains("failed"));
        assert!(!output.lines().next().unwrap().contains("passed"));
    }

    #[test]
    fn strings_and_dynamic_keys_cannot_inject_terminal_controls() {
        let output = text(
            "manifest",
            &json!({
                "path": "資料/ok\nforged\r\t\u{1b}[31m\u{85}\u{202e}txt",
                "extra\u{1b}]0;title\u{7}": "literal\\n",
                "chunks": []
            }),
        );
        for forbidden in ['\r', '\t', '\u{1b}', '\u{7}', '\u{85}', '\u{202e}'] {
            assert!(!output.contains(forbidden));
        }
        assert!(output.contains("資料/ok\\nforged\\r\\t\\u{001b}[31m\\u{0085}\\u{202e}txt"));
        assert!(output.contains("extra\\u{001b}]0;title\\u{0007}"));
        assert!(output.contains("literal\\\\n"));
    }

    #[test]
    fn runtime_errors_include_command_and_sanitized_complete_chain() {
        let error =
            anyhow::anyhow!("permission denied\u{1b}[31m").context("cannot read 資料\nfile");
        let mut bytes = Vec::new();
        Output::new(OutputFormat::Text, "manifest")
            .write_error(&error, &mut bytes, false)
            .unwrap();
        let output = String::from_utf8(bytes).unwrap();
        assert!(output.lines().next().unwrap().contains("manifest"));
        assert!(output.contains("cannot read 資料\\nfile"));
        assert!(output.contains("permission denied\\u{001b}[31m"));
        assert!(!output.contains('\u{1b}'));
    }

    #[test]
    fn progress_is_only_emitted_for_text() {
        let mut json_progress = Vec::new();
        Output::new(OutputFormat::Json, "scan")
            .write_progress(&mut json_progress)
            .unwrap();
        assert!(json_progress.is_empty());
        let mut text_progress = Vec::new();
        Output::new(OutputFormat::Text, "scan")
            .write_progress(&mut text_progress)
            .unwrap();
        assert!(!text_progress.is_empty());
        assert!(!text_progress.contains(&b'\r'));
        assert!(!text_progress.contains(&0x1b));
    }
}
