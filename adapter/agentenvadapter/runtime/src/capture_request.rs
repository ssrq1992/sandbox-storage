//! A storage-triggered request still executes the original native snapshot.
//! The durable start marker prevents a lost HTTP response from freezing twice.
use crate::{client, endpoint, StorageContext};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::Path,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CaptureRequest {
    pub id: String,
    pub request_id: String,
    pub owner_uid: String,
    pub run_uid: String,
    pub native_id: String,
    pub barrier: String,
}
tokio::task_local! { static BARRIER: String; }
pub fn barrier_id() -> String {
    BARRIER
        .try_with(Clone::clone)
        .unwrap_or_else(|_| uuid::Uuid::now_v7().to_string())
}
pub async fn scope<T>(barrier: String, future: impl std::future::Future<Output = T>) -> T {
    BARRIER.scope(barrier, future).await
}
impl CaptureRequest {
    pub async fn authorize(&self, storage: &StorageContext, native: &str) -> Result<()> {
        anyhow::ensure!(
            self.native_id == native
                && self.request_id == storage.request_id
                && self.owner_uid == storage.owner_uid
                && self.run_uid == storage.run_uid,
            "storage capture incarnation mismatch"
        );
        anyhow::ensure!(
            !self.id.is_empty()
                && self.barrier == format!("external_{:x}", Sha256::digest(self.id.as_bytes())),
            "storage capture barrier mismatch"
        );
        client()?.post(endpoint("/storage/control")?).json(&serde_json::json!({
            "ticket":storage.ticket,"action":"capture.request", "capture":{"barrier_id":self.barrier,"artifact":"","digest":""}
        })).send().await?.error_for_status()?;
        Ok(())
    }
    /// Once persisted, this request is never invoked again, even after process
    /// restart. The provider recovers a completed barrier from its ledger; an
    /// unknown unfinished operation is not permission to take a newer snapshot.
    pub fn begin(&self) -> Result<()> {
        let root = crate::host().config().state_dir.join("capture_requests");
        self.begin_at(&root)
    }
    fn begin_at(&self, root: &Path) -> Result<()> {
        fs::create_dir_all(root)?;
        if let Some(parent) = root.parent() {
            File::open(parent)?.sync_all()?;
        }
        let name = format!("{:x}.json", Sha256::digest(self.id.as_bytes()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(root.join(name))
            .context("capture already started; inspect its original durable barrier")?;
        file.write_all(&serde_json::to_vec(self)?)?;
        file.sync_all()?;
        File::open(root)?.sync_all()?;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn persisted_start_never_reexecutes_after_response_loss() {
        let dir = tempfile::tempdir().unwrap();
        let q = CaptureRequest {
            id: "snapshot".into(),
            request_id: "request".into(),
            owner_uid: "owner".into(),
            run_uid: "run".into(),
            native_id: "native".into(),
            barrier: "barrier".into(),
        };
        q.begin_at(dir.path()).unwrap();
        let reopened: CaptureRequest =
            serde_json::from_slice(&serde_json::to_vec(&q).unwrap()).unwrap();
        assert!(reopened.begin_at(dir.path()).is_err());
        let mut changed = q;
        changed.run_uid = "replacement".into();
        assert!(changed.begin_at(dir.path()).is_err());
    }
    #[tokio::test]
    async fn explicit_barrier_scopes_only_requested_snapshot() {
        assert_eq!(scope("fixed".into(), async { barrier_id() }).await, "fixed");
        assert_ne!(barrier_id(), "fixed");
    }
}
