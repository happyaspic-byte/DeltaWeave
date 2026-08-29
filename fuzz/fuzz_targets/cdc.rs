#![no_main]

use deltaweave_cdc::manifest_from_reader;
use deltaweave_core::{ChunkingProfile, Hash32};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let manifest = manifest_from_reader(data, ChunkingProfile::DEFAULT)
        .expect("default CDC profile accepts arbitrary bytes");
    assert!(manifest.validate().is_ok());
    assert_eq!(manifest.size, data.len() as u64);
    assert_eq!(manifest.file_hash, Hash32::digest(data));

    let mut offset = 0_u64;
    for chunk in &manifest.chunks {
        assert_eq!(chunk.offset, offset);
        offset += u64::from(chunk.length);
    }
    assert_eq!(offset, manifest.size);
});
