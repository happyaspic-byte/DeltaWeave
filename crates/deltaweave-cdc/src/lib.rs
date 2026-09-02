//! Streaming content-defined chunking and manifest construction.

#![forbid(unsafe_code)]

use std::{
    collections::HashSet,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use deltaweave_core::{
    ChunkDescriptor, ChunkingProfile, FileManifest, Hash32, MANIFEST_SCHEMA_V1, ManifestError,
    ProfileError,
};
use fastcdc::v2020::StreamCDC;
use thiserror::Error;

/// Builds a deterministic manifest while holding at most one maximum-sized chunk in memory.
pub fn manifest_from_reader<R: Read>(
    reader: R,
    profile: ChunkingProfile,
) -> Result<FileManifest, CdcError> {
    profile.validate().map_err(CdcError::Profile)?;

    let mut file_hasher = blake3::Hasher::new();
    let mut chunks = Vec::new();
    let mut expected_offset = 0_u64;
    let chunker = StreamCDC::new(
        reader,
        profile.min_size as usize,
        profile.avg_size as usize,
        profile.max_size as usize,
    );

    for result in chunker {
        let chunk = result.map_err(CdcError::FastCdc)?;
        if chunk.offset != expected_offset {
            return Err(CdcError::UnexpectedOffset {
                expected: expected_offset,
                actual: chunk.offset,
            });
        }
        let length =
            u32::try_from(chunk.length).map_err(|_| CdcError::ChunkLengthOverflow(chunk.length))?;
        let hash = Hash32::digest(&chunk.data);
        file_hasher.update(&chunk.data);
        chunks.push(ChunkDescriptor {
            offset: chunk.offset,
            length,
            hash,
        });
        expected_offset = expected_offset
            .checked_add(u64::from(length))
            .ok_or(CdcError::FileSizeOverflow)?;
    }

    let manifest = FileManifest {
        schema_version: MANIFEST_SCHEMA_V1,
        size: expected_offset,
        file_hash: Hash32::from_bytes(*file_hasher.finalize().as_bytes()),
        profile,
        chunks,
    };
    manifest.validate().map_err(CdcError::Manifest)?;
    Ok(manifest)
}

/// Opens a file and creates its streaming FastCDC manifest.
pub fn manifest_from_path(
    path: impl AsRef<Path>,
    profile: ChunkingProfile,
) -> Result<FileManifest, CdcError> {
    let file = File::open(path).map_err(CdcError::Io)?;
    let size = file.metadata().map_err(CdcError::Io)?.len();
    manifest_from_reader(file, profile.for_file_size(size))
}

/// Reads and verifies exactly one manifest chunk from a seekable source.
pub fn read_chunk<R: Read + Seek>(
    reader: &mut R,
    descriptor: &ChunkDescriptor,
) -> Result<Vec<u8>, CdcError> {
    reader
        .seek(SeekFrom::Start(descriptor.offset))
        .map_err(CdcError::Io)?;
    let mut bytes = vec![0_u8; descriptor.length as usize];
    reader.read_exact(&mut bytes).map_err(CdcError::Io)?;
    verify_chunk(descriptor, &bytes)?;
    Ok(bytes)
}

/// Verifies a chunk's length and BLAKE3 digest.
pub fn verify_chunk(descriptor: &ChunkDescriptor, bytes: &[u8]) -> Result<(), CdcError> {
    let actual_length =
        u32::try_from(bytes.len()).map_err(|_| CdcError::ChunkLengthOverflow(bytes.len()))?;
    if actual_length != descriptor.length {
        return Err(CdcError::LengthMismatch {
            expected: descriptor.length,
            actual: actual_length,
        });
    }
    let actual = Hash32::digest(bytes);
    if actual != descriptor.hash {
        return Err(CdcError::HashMismatch {
            expected: descriptor.hash,
            actual,
        });
    }
    Ok(())
}

/// Minimal set of unique chunks that a receiver still needs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaPlan {
    /// Unique missing chunk hashes, in first-use file order.
    pub missing: Vec<Hash32>,
    /// Total payload bytes for unique missing chunks.
    pub transfer_bytes: u64,
    /// Number of manifest extents already present at the receiver.
    pub reused_extents: usize,
}

impl DeltaPlan {
    /// Compares a manifest with the receiver's content-addressed inventory.
    #[must_use]
    pub fn build(manifest: &FileManifest, available: impl IntoIterator<Item = Hash32>) -> Self {
        let available: HashSet<_> = available.into_iter().collect();
        let mut scheduled = HashSet::new();
        let mut missing = Vec::new();
        let mut transfer_bytes = 0_u64;
        let mut reused_extents = 0_usize;

        for chunk in &manifest.chunks {
            if available.contains(&chunk.hash) {
                reused_extents += 1;
            } else if scheduled.insert(chunk.hash) {
                missing.push(chunk.hash);
                transfer_bytes = transfer_bytes.saturating_add(u64::from(chunk.length));
            }
        }
        Self {
            missing,
            transfer_bytes,
            reused_extents,
        }
    }
}

/// Failure while chunking or validating file content.
#[derive(Debug, Error)]
pub enum CdcError {
    /// Invalid FastCDC profile.
    #[error("invalid FastCDC profile: {0}")]
    Profile(ProfileError),
    /// The generated or supplied manifest was invalid.
    #[error("invalid file manifest: {0}")]
    Manifest(ManifestError),
    /// The streaming FastCDC implementation failed.
    #[error("FastCDC stream failed: {0}")]
    FastCdc(fastcdc::v2020::Error),
    /// File I/O failed.
    #[error("file I/O failed: {0}")]
    Io(std::io::Error),
    /// A platform-sized chunk could not fit in the protocol's 32-bit length.
    #[error("chunk length {0} does not fit in u32")]
    ChunkLengthOverflow(usize),
    /// Streaming FastCDC returned a non-contiguous offset.
    #[error("FastCDC returned offset {actual}, expected {expected}")]
    UnexpectedOffset {
        /// Required next offset.
        expected: u64,
        /// Returned offset.
        actual: u64,
    },
    /// The complete file length overflowed `u64`.
    #[error("file size overflow")]
    FileSizeOverflow,
    /// The supplied bytes had the wrong length.
    #[error("chunk length mismatch: expected {expected}, got {actual}")]
    LengthMismatch {
        /// Manifest length.
        expected: u32,
        /// Supplied length.
        actual: u32,
    },
    /// The supplied bytes had the wrong digest.
    #[error("chunk hash mismatch: expected {expected}, got {actual}")]
    HashMismatch {
        /// Manifest digest.
        expected: Hash32,
        /// Supplied digest.
        actual: Hash32,
    },
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Cursor};

    use super::*;

    fn fixture(length: usize) -> Vec<u8> {
        let mut state = 0x6a09_e667_f3bc_c908_u64;
        (0..length)
            .map(|index| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state as u8).wrapping_add(index as u8)
            })
            .collect()
    }

    #[test]
    fn manifest_is_deterministic_and_covers_input() {
        let bytes = fixture(2 * 1024 * 1024 + 317);
        let first = manifest_from_reader(Cursor::new(&bytes), ChunkingProfile::DEFAULT)
            .expect("fixture can be chunked");
        let second = manifest_from_reader(Cursor::new(&bytes), ChunkingProfile::DEFAULT)
            .expect("fixture can be chunked");
        assert_eq!(first, second);
        assert_eq!(first.size, bytes.len() as u64);
        assert_eq!(first.file_hash, Hash32::digest(&bytes));
        assert_eq!(first.validate(), Ok(()));
    }

    #[test]
    fn chunks_reconstruct_the_original_file() {
        let bytes = fixture(3 * 1024 * 1024 + 19);
        let manifest = manifest_from_reader(Cursor::new(&bytes), ChunkingProfile::DEFAULT)
            .expect("fixture can be chunked");
        let mut reader = Cursor::new(&bytes);
        let mut reconstructed = Vec::new();
        for descriptor in &manifest.chunks {
            reconstructed.extend_from_slice(
                &read_chunk(&mut reader, descriptor).expect("manifest chunk is readable"),
            );
        }
        assert_eq!(reconstructed, bytes);
    }

    #[test]
    fn localized_insertion_reuses_content_defined_chunks() {
        let original = fixture(4 * 1024 * 1024);
        let mut modified = original.clone();
        modified.splice(500_000..500_000, b"localized insertion".iter().copied());
        let old_manifest = manifest_from_reader(Cursor::new(&original), ChunkingProfile::DEFAULT)
            .expect("original can be chunked");
        let new_manifest = manifest_from_reader(Cursor::new(&modified), ChunkingProfile::DEFAULT)
            .expect("modified file can be chunked");
        let inventory = old_manifest.chunks.iter().map(|chunk| chunk.hash);
        let plan = DeltaPlan::build(&new_manifest, inventory);
        assert!(plan.reused_extents > 0);
        assert!(plan.transfer_bytes < modified.len() as u64);
    }

    #[test]
    fn delta_plan_deduplicates_repeated_chunks() {
        let bytes = fixture(512 * 1024);
        let manifest = manifest_from_reader(Cursor::new(&bytes), ChunkingProfile::DEFAULT)
            .expect("fixture can be chunked");
        let mut repeated = manifest.clone();
        let first = repeated
            .chunks
            .first()
            .expect("fixture has a chunk")
            .clone();
        repeated.size += u64::from(first.length);
        repeated.chunks.push(ChunkDescriptor {
            offset: manifest.size,
            ..first
        });
        let plan = DeltaPlan::build(&repeated, []);
        let unique: HashMap<_, _> = repeated
            .chunks
            .iter()
            .map(|chunk| (chunk.hash, chunk.length))
            .collect();
        assert_eq!(plan.missing.len(), unique.len());
    }

    #[test]
    fn path_manifest_leaves_the_file_handle_usable_for_metadata() {
        let directory = tempfile::tempdir().expect("temporary directory can be created");
        let path = directory.path().join("handle.bin");
        std::fs::write(&path, fixture(1024)).expect("file can be written");
        let mut file = File::open(&path).expect("file can be opened");
        let before = file
            .metadata()
            .expect("metadata can be read before chunking");
        let manifest = manifest_from_reader(&mut file, ChunkingProfile::DEFAULT)
            .expect("open file handle can be chunked");
        let after = file
            .metadata()
            .expect("same file handle remains usable after chunking");
        assert_eq!(manifest.size, before.len());
        assert_eq!(after.len(), before.len());
    }

    #[test]
    fn small_files_keep_the_requested_default_profile() {
        let directory = tempfile::tempdir().expect("temporary directory can be created");
        let path = directory.path().join("small.bin");
        std::fs::write(&path, fixture(1024)).expect("small file can be written");

        let manifest =
            manifest_from_path(&path, ChunkingProfile::DEFAULT).expect("small file can be chunked");
        assert_eq!(manifest.profile, ChunkingProfile::DEFAULT);
        assert_eq!(manifest.size, 1024);
    }

    #[test]
    fn corrupted_chunk_is_rejected() {
        let descriptor = ChunkDescriptor {
            offset: 0,
            length: 4,
            hash: Hash32::digest(b"good"),
        };
        assert!(matches!(
            verify_chunk(&descriptor, b"evil"),
            Err(CdcError::HashMismatch { .. })
        ));
    }
}
