//! DeltaWeave desktop window and tray.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
#![forbid(unsafe_code)]

fn main() {
    deltaweave_gui_lib::run();
}
