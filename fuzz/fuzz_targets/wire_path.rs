#![no_main]

use deltaweave_core::WirePath;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(input) = std::str::from_utf8(data) {
        let _ = WirePath::new(input);
    }
});
