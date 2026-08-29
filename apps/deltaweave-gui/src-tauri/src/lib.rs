//! Tauri 2 light shell: tray, close-to-tray, autostart, daemon sidecar.

#![forbid(unsafe_code)]

use deltaweave_daemon_api::{Command, ConflictAction};
use tauri::{
    Manager, WindowEvent,
    menu::{Menu, MenuItem},
    tray::TrayIconBuilder,
};
use tauri_plugin_shell::ShellExt;

/// Starts the desktop application.
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(
            tauri_plugin_autostart::Builder::new()
                .args(["--hidden"])
                .build(),
        )
        .setup(|app| {
            if std::env::args().any(|arg| arg == "--hidden")
                && let Some(window) = app.get_webview_window("main")
            {
                window.hide()?;
            }
            spawn_daemon_if_needed(app.handle().clone());
            let open = MenuItem::with_id(app, "open", "Open DeltaWeave", true, None::<&str>)?;
            let sync_all = MenuItem::with_id(app, "sync_all", "Sync all now", true, None::<&str>)?;
            let pause_all = MenuItem::with_id(app, "pause_all", "Pause all", true, None::<&str>)?;
            let quit = MenuItem::with_id(app, "quit", "Quit Sync Engine", true, None::<&str>)?;
            let menu = Menu::with_items(app, &[&open, &sync_all, &pause_all, &quit])?;
            let _tray = TrayIconBuilder::new()
                .menu(&menu)
                .show_menu_on_left_click(true)
                .on_menu_event(|app, event| match event.id().as_ref() {
                    "open" => {
                        if let Some(window) = app.get_webview_window("main") {
                            let _ = window.show();
                            let _ = window.set_focus();
                        }
                    }
                    "sync_all" => tray_command(app, TrayAction::SyncAll),
                    "pause_all" => tray_command(app, TrayAction::PauseAll),
                    "quit" => quit_sync_engine(app),
                    _ => {}
                })
                .build(app)?;
            Ok(())
        })
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { api, .. } = event {
                api.prevent_close();
                let _ = window.hide();
            }
        })
        .invoke_handler(tauri::generate_handler![
            resolve_conflict,
            create_job,
            redeem_ticket
        ])
        .run(tauri::generate_context!())
        .expect("error while running DeltaWeave");
}

#[tauri::command]
async fn create_job(
    name: String,
    local_root: String,
    peer_endpoint_id: String,
    peer_address: Option<String>,
    direction: String,
    preview_confirmed: bool,
) -> Result<(), String> {
    let direction = match direction.as_str() {
        "bidirectional" => deltaweave_daemon_api::Direction::Bidirectional,
        "send_only" => deltaweave_daemon_api::Direction::SendOnly,
        "receive_only" => deltaweave_daemon_api::Direction::ReceiveOnly,
        _ => return Err("invalid sync direction".into()),
    };
    send_daemon(Command::CreateJob {
        name,
        local_root,
        peer_endpoint_id,
        peer_address,
        direction,
        preview_confirmed,
    })
    .await
    .map(|_| ())
}

#[tauri::command]
async fn redeem_ticket(code: String) -> Result<serde_json::Value, String> {
    let result = send_daemon(Command::RedeemTicket { code }).await?;
    serde_json::to_value(result).map_err(|error| error.to_string())
}

#[tauri::command]
async fn resolve_conflict(id: String, path: String, action: String) -> Result<(), String> {
    let action = match action.as_str() {
        "keep_local" => ConflictAction::KeepLocal,
        "keep_remote" => ConflictAction::KeepRemote,
        "keep_both" => ConflictAction::KeepBoth,
        _ => return Err("invalid conflict action".into()),
    };
    send_daemon(Command::ResolveConflict { id, path, action })
        .await
        .map(|_| ())
}

async fn send_daemon(command: Command) -> Result<deltaweave_daemon_api::CommandResult, String> {
    let data_dir = deltaweave_daemon::default_data_dir().map_err(|error| error.to_string())?;
    let socket = deltaweave_daemon::ipc_path(data_dir);
    deltaweave_daemon::send_command(&socket, command)
        .await
        .map_err(|error| error.to_string())
}

enum TrayAction {
    SyncAll,
    PauseAll,
}

fn tray_command(_app: &tauri::AppHandle, action: TrayAction) {
    tauri::async_runtime::spawn(async move {
        let jobs = match send_daemon(Command::ListJobs).await {
            Ok(deltaweave_daemon_api::CommandResult::Jobs { jobs }) => jobs,
            _ => return,
        };
        for job in jobs {
            let command = match action {
                TrayAction::SyncAll => Command::SyncNow { id: job.id },
                TrayAction::PauseAll => Command::PauseJob { id: job.id },
            };
            let _ = send_daemon(command).await;
        }
    });
}

fn spawn_daemon_if_needed(app: tauri::AppHandle) {
    tauri::async_runtime::spawn(async move {
        if let Ok(data_dir) = deltaweave_daemon::default_data_dir() {
            let socket = deltaweave_daemon::ipc_path(data_dir);
            if deltaweave_daemon::connect_and_hello(&socket).await.is_ok() {
                return;
            }
        }
        if let Ok(sidecar) = app.shell().sidecar("deltaweave-daemon") {
            let _ = sidecar.spawn();
        }
    });
}

fn quit_sync_engine(app: &tauri::AppHandle) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        if let Ok(data_dir) = deltaweave_daemon::default_data_dir() {
            let socket = deltaweave_daemon::ipc_path(data_dir);
            let _ = deltaweave_daemon::send_command(&socket, deltaweave_daemon_api::Command::Stop)
                .await;
        }
        app.exit(0);
    });
}

#[cfg(test)]
mod tests {
    #[test]
    fn stylesheet_is_light_only() {
        let css = include_str!("../../ui/src/styles.css");
        assert!(css.contains("#f3f5f7"));
        assert!(css.contains("#1b2430"));
        assert!(!css.to_lowercase().contains("prefers-color-scheme: dark"));
    }
}
