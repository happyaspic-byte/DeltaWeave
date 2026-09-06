//! The existing causal algorithm accepts either legacy or issuer-pinned shared transport.
use super::*;
use deltaweave_net::{RemoteSnapshot, share::ShareSession};

pub(crate) trait ReconcileTransport: Sync {
    fn fetch_snapshot(
        &self,
        local: &MerkleTree,
    ) -> impl std::future::Future<Output = Result<RemoteSnapshot>> + Send;
    fn pull_record_to_with_budget(
        &self,
        record: SyncRecord,
        store: Arc<Store>,
        root: PathBuf,
        reserve: u64,
        pending: u64,
    ) -> impl std::future::Future<Output = Result<PullReceipt>> + Send;
    fn push_record(
        &self,
        source: PathBuf,
        record: SyncRecord,
        profile: ChunkingProfile,
    ) -> impl std::future::Future<Output = Result<SyncApplyReceipt>> + Send;
    fn apply_metadata(
        &self,
        record: SyncRecord,
    ) -> impl std::future::Future<Output = Result<SyncApplyReceipt>> + Send;
}

macro_rules! transport {
    ($kind:ty) => {
        impl ReconcileTransport for $kind {
            async fn fetch_snapshot(&self, local: &MerkleTree) -> Result<RemoteSnapshot> {
                <$kind>::fetch_snapshot(self, local).await
            }
            async fn pull_record_to_with_budget(
                &self,
                record: SyncRecord,
                store: Arc<Store>,
                root: PathBuf,
                reserve: u64,
                pending: u64,
            ) -> Result<PullReceipt> {
                <$kind>::pull_record_to_with_budget(self, record, store, root, reserve, pending)
                    .await
            }
            async fn push_record(
                &self,
                source: PathBuf,
                record: SyncRecord,
                profile: ChunkingProfile,
            ) -> Result<SyncApplyReceipt> {
                <$kind>::push_record(self, source, record, profile).await
            }
            async fn apply_metadata(&self, record: SyncRecord) -> Result<SyncApplyReceipt> {
                <$kind>::apply_metadata(self, record).await
            }
        }
    };
}
transport!(SyncSession);
transport!(ShareSession);
