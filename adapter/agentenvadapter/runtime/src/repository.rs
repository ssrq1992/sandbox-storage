//! Durable generic repository receipts. The provider verifies repository state
//! before accepting completion, and owns no native artifact deletion.
use super::{client, endpoint};

use anyhow::Result;

pub fn completion(repository: &str, id: &str, action: &str) -> Result<()> {
    uuid::Uuid::parse_str(id)?;
    anyhow::ensure!(
        matches!(action, "published" | "deleted"),
        "invalid repository completion"
    );
    let root = crate::host().config().state_dir.join("publications");
    super::artifacts::persist_repository_observation(&root, repository, id, action)?;
    Ok(())
}

pub async fn begin_delete(repository: &str, id: &str) -> Result<bool> {
    let reply: serde_json::Value = client()?
        .post(endpoint("/storage/checkpoints")?)
        .json(&serde_json::json!({"action": "delete_begin", "data": {"native_id": id, "repository_id": repository}}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let managed = reply
        .get("managed")
        .and_then(serde_json::Value::as_bool)
        .ok_or_else(|| anyhow::anyhow!("provider deletion receipt missing"))?;
    if managed {
        // Persist before native delete. Until the repository actually reports
        // absence, the provider retains all references and retries this task.
        completion(repository, id, "deleted")?;
    }
    Ok(managed)
}
