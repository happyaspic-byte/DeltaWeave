//! Tauri 2 light shell: tray, close-to-tray, autostart, daemon sidecar.

#![forbid(unsafe_code)]

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
        .run(tauri::generate_context!())
        .expect("error while running DeltaWeave");
}

fn spawn_daemon_if_needed(app: tauri::AppHandle) {
    tauri::async_runtime::spawn(async move {
        #[cfg(unix)]
        {
            let live = deltaweave_daemon::default_data_dir()
                .map(|dir| deltaweave_daemon::ipc_path(&dir))
                .ok()
                .map(|socket| async move {
                    deltaweave_daemon::connect_and_hello(&socket).await.is_ok()
                });
            if let Some(check) = live
                && check.await
            {
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
        #[cfg(unix)]
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
