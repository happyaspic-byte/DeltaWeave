//! Stable data types shared by every DeltaWeave layer.

#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use thiserror::Error;

/// Current on-disk and wire manifest schema.
pub const MANIFEST_SCHEMA_V1: u16 = 1;

/// A BLAKE3 digest used for chunks, files, and manifests.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Hash32([u8; 32]);

impl Hash32 {
    /// Computes a BLAKE3 digest over `bytes`.
    #[must_use]
    pub fn digest(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    /// Constructs a digest from its raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the raw digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns the lowercase hexadecimal representation.
    #[must_use]
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Display for Hash32 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for Hash32 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Hash32({self})")
    }
}

impl FromStr for Hash32 {
    type Err = ParseHashError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 64 {
            return Err(ParseHashError::Length(value.len()));
        }
        let mut bytes = [0_u8; 32];
        hex::decode_to_slice(value, &mut bytes).map_err(ParseHashError::Hex)?;
        Ok(Self(bytes))
    }
}

impl Serialize for Hash32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Hash32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(D::Error::custom)
    }
}

/// Error returned when a hexadecimal BLAKE3 digest is invalid.
#[derive(Debug, Error)]
pub enum ParseHashError {
    /// A BLAKE3 digest must contain exactly 64 hexadecimal characters.
    #[error("expected 64 hexadecimal characters, got {0}")]
    Length(usize),
    /// The text was not valid hexadecimal.
    #[error("invalid hexadecimal digest: {0}")]
    Hex(hex::FromHexError),
}

/// Versioned FastCDC parameters that must travel with every manifest.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChunkingProfile {
    /// Profile schema version.
    pub version: u16,
    /// Smallest permitted chunk.
    pub min_size: u32,
    /// Desired average chunk size.
    pub avg_size: u32,
    /// Largest permitted chunk.
    pub max_size: u32,
}

impl ChunkingProfile {
    /// DeltaWeave's default 64 KiB / 256 KiB / 1 MiB profile.
    pub const DEFAULT: Self = Self {
        version: 1,
        min_size: 64 * 1024,
        avg_size: 256 * 1024,
        max_size: 1024 * 1024,
    };

    /// Validates the profile against FastCDC v2020 limits and DeltaWeave invariants.
    pub fn validate(self) -> Result<(), ProfileError> {
        if self.version != 1 {
            return Err(ProfileError::UnsupportedVersion(self.version));
        }
        if !(64..=1_048_576).contains(&self.min_size) {
            return Err(ProfileError::MinimumOutOfRange(self.min_size));
        }
        if !(256..=4_194_304).contains(&self.avg_size) {
            return Err(ProfileError::AverageOutOfRange(self.avg_size));
        }
        if !(1_024..=16_777_216).contains(&self.max_size) {
            return Err(ProfileError::MaximumOutOfRange(self.max_size));
        }
        if !(self.min_size < self.avg_size && self.avg_size < self.max_size) {
            return Err(ProfileError::InvalidOrdering);
        }
        if [self.min_size, self.avg_size, self.max_size]
            .iter()
            .any(|size| size % 2 != 0)
        {
            return Err(ProfileError::OddSize);
        }
        Ok(())
    }
}

impl Default for ChunkingProfile {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Invalid FastCDC profile.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ProfileError {
    /// The profile schema is unknown.
    #[error("unsupported chunking profile version {0}")]
    UnsupportedVersion(u16),
    /// The minimum size is outside FastCDC's supported range.
    #[error("minimum chunk size {0} is outside 64..=1048576")]
    MinimumOutOfRange(u32),
    /// The average size is outside FastCDC's supported range.
    #[error("average chunk size {0} is outside 256..=4194304")]
    AverageOutOfRange(u32),
    /// The maximum size is outside FastCDC's supported range.
    #[error("maximum chunk size {0} is outside 1024..=16777216")]
    MaximumOutOfRange(u32),
    /// The sizes must be strictly increasing.
    #[error("chunk sizes must satisfy min < avg < max")]
    InvalidOrdering,
    /// FastCDC v2020 requires even boundary sizes.
    #[error("FastCDC v2020 chunk sizes must be even")]
    OddSize,
}

/// One content-addressed extent inside a file.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChunkDescriptor {
    /// Byte offset from the start of the file.
    pub offset: u64,
    /// Chunk length in bytes.
    pub length: u32,
    /// BLAKE3 digest of the chunk bytes.
    pub hash: Hash32,
}

/// Deterministic description of a file's content-defined chunks.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FileManifest {
    /// Manifest format version.
    pub schema_version: u16,
    /// Logical file length.
    pub size: u64,
    /// BLAKE3 digest of the complete file.
    pub file_hash: Hash32,
    /// FastCDC parameters used to produce this manifest.
    pub profile: ChunkingProfile,
    /// Ordered, contiguous file chunks.
    pub chunks: Vec<ChunkDescriptor>,
}

impl FileManifest {
    /// Checks structural invariants before a manifest is stored or trusted.
    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.schema_version != MANIFEST_SCHEMA_V1 {
            return Err(ManifestError::UnsupportedVersion(self.schema_version));
        }
        self.profile.validate().map_err(ManifestError::Profile)?;

        if self.size == 0 {
            if !self.chunks.is_empty() {
                return Err(ManifestError::ChunksForEmptyFile);
            }
            if self.file_hash != Hash32::digest(&[]) {
                return Err(ManifestError::InvalidEmptyHash);
            }
            return Ok(());
        }
        if self.chunks.is_empty() {
            return Err(ManifestError::MissingChunks);
        }

        let mut expected_offset = 0_u64;
        for (index, chunk) in self.chunks.iter().enumerate() {
            if chunk.offset != expected_offset {
                return Err(ManifestError::NonContiguous {
                    index,
                    expected: expected_offset,
                    actual: chunk.offset,
                });
            }
            if chunk.length == 0 {
                return Err(ManifestError::EmptyChunk(index));
            }
            if chunk.length > self.profile.max_size {
                return Err(ManifestError::ChunkTooLarge {
                    index,
                    length: chunk.length,
                    maximum: self.profile.max_size,
                });
            }
            expected_offset = expected_offset
                .checked_add(u64::from(chunk.length))
                .ok_or(ManifestError::SizeOverflow)?;
        }
        if expected_offset != self.size {
            return Err(ManifestError::SizeMismatch {
                declared: self.size,
                described: expected_offset,
            });
        }
        Ok(())
    }

    /// Computes a domain-separated deterministic identity for this manifest.
    #[must_use]
    pub fn manifest_hash(&self) -> Hash32 {
        let mut hasher = blake3::Hasher::new_derive_key("deltaweave manifest v1");
        hasher.update(&self.schema_version.to_be_bytes());
        hasher.update(&self.size.to_be_bytes());
        hasher.update(self.file_hash.as_bytes());
        hasher.update(&self.profile.version.to_be_bytes());
        hasher.update(&self.profile.min_size.to_be_bytes());
        hasher.update(&self.profile.avg_size.to_be_bytes());
        hasher.update(&self.profile.max_size.to_be_bytes());
        for chunk in &self.chunks {
            hasher.update(&chunk.offset.to_be_bytes());
            hasher.update(&chunk.length.to_be_bytes());
            hasher.update(chunk.hash.as_bytes());
        }
        Hash32::from_bytes(*hasher.finalize().as_bytes())
    }
}

/// Structurally invalid file manifest.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ManifestError {
    /// The manifest format is unknown.
    #[error("unsupported manifest schema {0}")]
    UnsupportedVersion(u16),
    /// The embedded chunking profile is invalid.
    #[error("invalid chunking profile: {0}")]
    Profile(ProfileError),
    /// A zero-byte file cannot contain chunks.
    #[error("zero-byte file contains chunks")]
    ChunksForEmptyFile,
    /// A zero-byte file must carry BLAKE3's empty-input digest.
    #[error("zero-byte file carries an invalid digest")]
    InvalidEmptyHash,
    /// A non-empty file must contain at least one chunk.
    #[error("non-empty file does not contain chunks")]
    MissingChunks,
    /// Chunk offsets must cover the file without gaps or overlaps.
    #[error("chunk {index} starts at {actual}, expected {expected}")]
    NonContiguous {
        /// Chunk index.
        index: usize,
        /// Required offset.
        expected: u64,
        /// Supplied offset.
        actual: u64,
    },
    /// Chunks cannot be empty.
    #[error("chunk {0} is empty")]
    EmptyChunk(usize),
    /// A chunk exceeded the configured maximum.
    #[error("chunk {index} length {length} exceeds maximum {maximum}")]
    ChunkTooLarge {
        /// Chunk index.
        index: usize,
        /// Supplied length.
        length: u32,
        /// Configured maximum.
        maximum: u32,
    },
    /// Chunk offsets overflowed `u64`.
    #[error("manifest size overflow")]
    SizeOverflow,
    /// The chunks did not describe the declared file size.
    #[error("manifest declares {declared} bytes but describes {described}")]
    SizeMismatch {
        /// Declared size.
        declared: u64,
        /// Sum of chunk sizes.
        described: u64,
    },
}

/// Cross-platform relative path accepted by the wire protocol.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct WirePath(String);

impl WirePath {
    /// Validates and constructs a portable relative path.
    pub fn new(value: impl Into<String>) -> Result<Self, WirePathError> {
        let value = value.into();
        validate_wire_path(&value)?;
        Ok(Self(value))
    }

    /// Returns the protocol path using `/` separators.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Iterates over validated path components.
    pub fn components(&self) -> impl Iterator<Item = &str> {
        self.0.split('/')
    }
}

impl fmt::Display for WirePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for WirePath {
    type Err = WirePathError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for WirePath {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

fn validate_wire_path(value: &str) -> Result<(), WirePathError> {
    if value.is_empty() {
        return Err(WirePathError::Empty);
    }
    if value.len() > 4096 {
        return Err(WirePathError::TooLong);
    }
    if value.starts_with('/') || value.contains('\\') {
        return Err(WirePathError::NotRelative);
    }

    const ILLEGAL: [char; 9] = ['<', '>', ':', '"', '|', '?', '*', '\0', '\r'];
    const RESERVED: [&str; 22] = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];

    for component in value.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(WirePathError::InvalidComponent(component.to_owned()));
        }
        if component.len() > 255 {
            return Err(WirePathError::ComponentTooLong(component.to_owned()));
        }
        if component.ends_with('.') || component.ends_with(' ') {
            return Err(WirePathError::InvalidComponent(component.to_owned()));
        }
        if component
            .chars()
            .any(|character| character.is_control() || ILLEGAL.contains(&character))
        {
            return Err(WirePathError::InvalidComponent(component.to_owned()));
        }
        let stem = component.split('.').next().unwrap_or(component);
        if RESERVED
            .iter()
            .any(|reserved| stem.eq_ignore_ascii_case(reserved))
        {
            return Err(WirePathError::ReservedName(component.to_owned()));
        }
    }
    Ok(())
}

/// Invalid cross-platform path.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum WirePathError {
    /// A path must contain at least one component.
    #[error("path is empty")]
    Empty,
    /// Absolute paths and backslashes are forbidden on the wire.
    #[error("path must be relative and use '/' separators")]
    NotRelative,
    /// The complete path is unreasonably long.
    #[error("path exceeds 4096 UTF-8 bytes")]
    TooLong,
    /// A component is empty, dot-relative, or not portable.
    #[error("invalid path component {0:?}")]
    InvalidComponent(String),
    /// A component exceeds the cross-platform limit.
    #[error("path component exceeds 255 UTF-8 bytes: {0:?}")]
    ComponentTooLong(String),
    /// A Windows device name was used.
    #[error("reserved Windows path component {0:?}")]
    ReservedName(String),
}

/// Stable replica identity used by version vectors.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ReplicaId(pub Hash32);

/// Causal relationship between two version vectors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CausalRelation {
    /// Both vectors contain the same knowledge.
    Equal,
    /// The left-hand vector happened before the right-hand vector.
    Before,
    /// The left-hand vector happened after the right-hand vector.
    After,
    /// Neither vector dominates the other.
    Concurrent,
}

/// Per-replica logical counters used to detect concurrent edits without wall-clock time.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct VersionVector(BTreeMap<ReplicaId, u64>);

impl VersionVector {
    /// Increments a replica's logical counter and returns the new value.
    pub fn increment(&mut self, replica: ReplicaId) -> Result<u64, ClockError> {
        let counter = self.0.entry(replica).or_default();
        *counter = counter.checked_add(1).ok_or(ClockError::CounterOverflow)?;
        Ok(*counter)
    }

    /// Returns the known counter for a replica.
    #[must_use]
    pub fn get(&self, replica: ReplicaId) -> u64 {
        self.0.get(&replica).copied().unwrap_or_default()
    }

    /// Merges all knowledge from another vector.
    pub fn merge(&mut self, other: &Self) {
        for (&replica, &counter) in &other.0 {
            let current = self.0.entry(replica).or_default();
            *current = (*current).max(counter);
        }
    }

    /// Compares two vectors using partial causal ordering.
    #[must_use]
    pub fn relation(&self, other: &Self) -> CausalRelation {
        let replicas: BTreeSet<_> = self.0.keys().chain(other.0.keys()).copied().collect();
        let mut less = false;
        let mut greater = false;
        for replica in replicas {
            let left = self.get(replica);
            let right = other.get(replica);
            less |= left < right;
            greater |= left > right;
        }
        match (less, greater) {
            (false, false) => CausalRelation::Equal,
            (true, false) => CausalRelation::Before,
            (false, true) => CausalRelation::After,
            (true, true) => CausalRelation::Concurrent,
        }
    }
}

/// Logical clock failure.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ClockError {
    /// A replica counter cannot advance beyond `u64::MAX`.
    #[error("version-vector counter overflow")]
    CounterOverflow,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(offset: u64, bytes: &[u8]) -> ChunkDescriptor {
        ChunkDescriptor {
            offset,
            length: u32::try_from(bytes.len()).expect("test data fits in u32"),
            hash: Hash32::digest(bytes),
        }
    }

    #[test]
    fn hash_text_round_trip() {
        let hash = Hash32::digest(b"deltaweave");
        assert_eq!(
            hash.to_string()
                .parse::<Hash32>()
                .expect("valid hash text parses"),
            hash
        );
        assert!("abc".parse::<Hash32>().is_err());
    }

    #[test]
    fn valid_manifest_has_stable_identity() {
        let first = b"alpha";
        let second = b"bravo";
        let mut complete = Vec::from(*first);
        complete.extend_from_slice(second);
        let manifest = FileManifest {
            schema_version: MANIFEST_SCHEMA_V1,
            size: 10,
            file_hash: Hash32::digest(&complete),
            profile: ChunkingProfile::DEFAULT,
            chunks: vec![descriptor(0, first), descriptor(5, second)],
        };
        assert_eq!(manifest.validate(), Ok(()));
        assert_eq!(manifest.manifest_hash(), manifest.manifest_hash());
    }

    #[test]
    fn manifest_rejects_gaps() {
        let manifest = FileManifest {
            schema_version: MANIFEST_SCHEMA_V1,
            size: 10,
            file_hash: Hash32::digest(b"0123456789"),
            profile: ChunkingProfile::DEFAULT,
            chunks: vec![descriptor(1, b"0123456789")],
        };
        assert!(matches!(
            manifest.validate(),
            Err(ManifestError::NonContiguous { .. })
        ));
    }

    #[test]
    fn wire_paths_reject_traversal_and_windows_devices() {
        assert!(WirePath::new("documents/report.txt").is_ok());
        assert!(WirePath::new("../secrets.txt").is_err());
        assert!(WirePath::new("folder\\escape.txt").is_err());
        assert!(WirePath::new("con.txt").is_err());
    }

    #[test]
    fn wire_path_deserialization_cannot_bypass_validation() {
        assert!(serde_json::from_str::<WirePath>(r#""safe/file.txt""#).is_ok());
        assert!(serde_json::from_str::<WirePath>(r#""../escape.txt""#).is_err());
    }

    #[test]
    fn version_vectors_detect_concurrency() {
        let left_id = ReplicaId(Hash32::digest(b"left"));
        let right_id = ReplicaId(Hash32::digest(b"right"));
        let mut base = VersionVector::default();
        base.increment(left_id).expect("counter can increment");

        let mut left = base.clone();
        left.increment(left_id).expect("counter can increment");
        let mut right = base;
        right.increment(right_id).expect("counter can increment");

        assert_eq!(left.relation(&right), CausalRelation::Concurrent);
        left.merge(&right);
        assert_eq!(left.relation(&right), CausalRelation::After);
    }
}
