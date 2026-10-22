//! Optional generic storage provider. Kubernetes, CSI, shared use accounting
//! and placement live in the provider plugin; the native runtime owns the VMM.
pub mod artifact_gc;
pub mod artifacts;
pub mod devices;
pub mod freeze;
pub mod repository;
pub mod transition;
pub use devices::PreparedExternalDrives;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, future::Future};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManagedMount {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pvc: Option<VolumeReference>,
    pub path: String,
}

/// Opaque provider reference; resolution and authorization stay in the plugin.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VolumeReference {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub name: String,
}

impl ManagedMount {
    pub fn request_key(&self) -> String {
        match &self.pvc {
            Some(source) => format!(
                "pvc:{}/{}",
                source.namespace.as_deref().unwrap_or(""),
                source.name
            ),
            None => self.name.clone(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct StorageContext {
    #[serde(default)]
    pub replace_mounts: bool,
    #[serde(default)]
    pub relocations: Vec<transition::MountRelocation>,
    pub ticket: String,
    pub owner_uid: String,
    pub run_uid: String,
    pub request_id: String,
    #[serde(default)]
    pub template_pin: String,
    #[serde(default)]
    pub checkpoint_source: String,
    #[serde(default)]
    pub managed_mounts: Vec<ManagedMount>,
    #[serde(default)]
    pub native_id: String,
}
impl fmt::Debug for StorageContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageContext")
            .field("owner_uid", &self.owner_uid)
            .field("run_uid", &self.run_uid)
            .field("request_id", &self.request_id)
            .field("managed_mounts", &self.managed_mounts)
            .finish_non_exhaustive()
    }
}

impl StorageContext {
    pub async fn control(&self, action: &str) -> Result<serde_json::Value> {
        if action == "delete" {
            self.persist_delete()?;
        }
        let result = async {
            let response = client()?
                .post(endpoint("/storage/control")?)
                .json(&serde_json::json!({"ticket": self.ticket, "action": action}))
                .send()
                .await?
                .error_for_status()?;
            Ok::<serde_json::Value, anyhow::Error>(response.json().await?)
        }
        .await;
        match result {
            Err(error) if action == "delete" => {
                // The local outbox is already fsynced. Stop the original VMM
                // even when control transport is down; release still requires
                // the node's independently verified stop receipt.
                tracing::warn!(request_id=%self.request_id,%error,"storage delete delivery deferred to durable outbox");
                Ok(serde_json::json!({"queued":true}))
            }
            other => other,
        }
    }

    // Persist before contacting the loopback provider. The provider imports
    // these tasks into its own journal after either process restarts.
    pub fn persist_delete(&self) -> Result<()> {
        use std::fs::{self, File, OpenOptions};
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        anyhow::ensure!(
            !self.request_id.is_empty()
                && self.request_id.len() <= 128
                && self
                    .request_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
            "invalid provider request identity"
        );
        let root = crate::host().config().state_dir.join("control");
        fs::create_dir_all(&root)?;
        if let Some(parent) = root.parent() {
            File::open(parent)?.sync_all()?;
        }
        let target = root.join(format!("{}.json", self.request_id));
        let temporary = root.join(format!("{}.{}.tmp", self.request_id, uuid::Uuid::now_v7()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec(&serde_json::json!({
            "version": 1, "action": "delete", "ticket": self.ticket,
            "request_id": self.request_id, "owner_uid": self.owner_uid, "run_uid": self.run_uid,
        }))?)?;
        file.sync_all()?;
        fs::rename(&temporary, &target)?;
        File::open(&root)?.sync_all()?;
        Ok(())
    }

    pub async fn prepare_fork(
        &self,
        child: impl ToString,
        checkpoint: Option<&serde_json::Value>,
    ) -> Result<Self> {
        let next: Self = client()?
            .post(endpoint("/storage/control")?)
            .json(&serde_json::json!({
                "ticket": self.ticket, "action": "fork",
                "child_id": child.to_string(), "checkpoint": checkpoint,
            }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        anyhow::ensure!(
            !next.owner_uid.is_empty()
                && next.owner_uid != self.owner_uid
                && !next.run_uid.is_empty()
                && next.run_uid != self.run_uid
                && !next.request_id.is_empty()
                && next.request_id != self.request_id
                && next.ticket.len() >= 32
                && next.native_id.is_empty(),
            "provider returned invalid fork identity"
        );
        Ok(next)
    }

    pub async fn prepare_resume(&self) -> Result<Self> {
        let next: Self = serde_json::from_value(self.control("resume").await?)?;
        anyhow::ensure!(
            next.owner_uid == self.owner_uid
                && next.checkpoint_source == self.checkpoint_source
                && next.run_uid != self.run_uid
                && !next.request_id.is_empty()
                && next.ticket.len() >= 32
                && next.native_id.is_empty(),
            "provider returned invalid resume identity"
        );
        Ok(next)
    }
}

/// A native temporary VMM owns its cleanup obligation even if construction,
/// startup or a background recording task fails before normal deletion.
pub struct AdmissionCleanup(pub Option<StorageContext>);
impl AdmissionCleanup {
    pub fn disarm(&mut self) {
        self.0 = None;
    }
}
impl Drop for AdmissionCleanup {
    fn drop(&mut self) {
        if let Some(context) = self.0.take() {
            if let Err(error) = context.persist_delete() {
                tracing::error!(%error, "failed to persist temporary VMM cleanup");
            }
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = context.control("delete").await;
                });
            }
        }
    }
}

pub async fn admit_compute(
    id: impl ToString,
    cpu_count: u32,
    memory_mib: u32,
    checkpoint: Option<&serde_json::Value>,
) -> Result<StorageContext> {
    let context: StorageContext = client()?
        .post(endpoint("/storage/compute")?)
        .json(&serde_json::json!({"native_id": id.to_string(),
            "resources": {"cpu": cpu_count, "memory_mib": memory_mib},
            "checkpoint": checkpoint}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    anyhow::ensure!(
        !context.owner_uid.is_empty()
            && !context.run_uid.is_empty()
            && !context.request_id.is_empty()
            && context.ticket.len() >= 32
            && context.native_id.is_empty(),
        "invalid native compute admission"
    );
    Ok(context)
}

tokio::task_local! {
    static REQUEST_CONTEXT: Option<StorageContext>;
}
pub fn current_context() -> Option<StorageContext> {
    REQUEST_CONTEXT.try_with(Clone::clone).ok().flatten()
}
pub async fn scope<F: Future>(context: Option<StorageContext>, f: F) -> F::Output {
    REQUEST_CONTEXT.scope(context, f).await
}
fn endpoint(path: &str) -> Result<String> {
    let cfg = &crate::host().config();
    anyhow::ensure!(cfg.enabled, "storage provider disabled");
    let base = cfg
        .url
        .as_deref()
        .context("storage provider URL required")?;
    let parsed = reqwest::Url::parse(base)?;
    anyhow::ensure!(
        parsed.scheme() == "http"
            && matches!(parsed.host_str(), Some("127.0.0.1" | "localhost" | "::1")),
        "storage provider must be Pod loopback"
    );
    Ok(format!("{}{}", base.trim_end_matches('/'), path))
}
pub fn client() -> Result<reqwest::Client> {
    let cfg = &crate::host().config();
    let token = std::fs::read_to_string(&cfg.token_file)?;
    anyhow::ensure!(token.trim().len() >= 32, "storage provider token too short");
    let mut headers = reqwest::header::HeaderMap::new();
    let mut authorization =
        reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token.trim()))?;
    authorization.set_sensitive(true);
    headers.insert(reqwest::header::AUTHORIZATION, authorization);
    Ok(reqwest::Client::builder()
        .default_headers(headers)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_millis(cfg.timeout_ms))
        .build()?)
}
/// Volume management is a generic provider command. The native runtime does
/// not interpret PVCs, StorageClasses, CSI capabilities or ledger references.
pub async fn volume_command(action: &str, data: serde_json::Value) -> Result<serde_json::Value> {
    Ok(client()?
        .post(endpoint("/storage/volumes")?)
        .json(&serde_json::json!({"action": action, "data": data}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

pub async fn admit(context: &str, body: &[u8]) -> Result<StorageContext> {
    let digest = format!("{:x}", Sha256::digest(body));
    let result = client()?
        .post(endpoint("/storage/admit")?)
        .json(&serde_json::json!({"context": context, "body_digest": digest}))
        .send()
        .await?
        .error_for_status()?
        .json::<StorageContext>()
        .await?;
    anyhow::ensure!(
        !result.request_id.is_empty()
            && !result.owner_uid.is_empty()
            && !result.run_uid.is_empty()
            && result.ticket.len() >= 32,
        "provider returned incomplete launch identity"
    );
    Ok(result)
}

/// Standalone admission uses the same consumer and exact native request body.
/// Private headers supplied by a public client are never used as authority.
async fn admit_local(path: &str, body: &[u8]) -> Result<StorageContext> {
    use base64::Engine;
    let result = client()?
        .post(endpoint("/storage/local-admit")?)
        .json(&serde_json::json!({
            "request_id": uuid::Uuid::new_v4().to_string(),
            "path": path,
            "body": base64::engine::general_purpose::STANDARD.encode(body),
        }))
        .send()
        .await?
        .error_for_status()?
        .json::<StorageContext>()
        .await?;
    anyhow::ensure!(
        !result.request_id.is_empty()
            && !result.owner_uid.is_empty()
            && !result.run_uid.is_empty()
            && result.ticket.len() >= 32,
        "provider returned incomplete launch identity"
    );
    Ok(result)
}

/// Runs after native API-key authentication. The original body bytes and all
/// public API fields remain unchanged; opaque storage context stays internal.
fn strip_private_headers(headers: &mut axum::http::HeaderMap) -> Option<String> {
    let context = headers
        .get("x-agentenv-storage-context")
        .map(|value| value.to_str().unwrap_or_default().to_owned());
    let private = headers
        .keys()
        .filter(|key| key.as_str().starts_with("x-agentenv-storage-"))
        .cloned()
        .collect::<Vec<_>>();
    for key in private {
        headers.remove(key);
    }
    context
}

pub async fn request_scope(
    mut request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::{
        body::{to_bytes, Body},
        http::StatusCode,
        response::IntoResponse,
    };
    let launch = request.method() == axum::http::Method::POST
        && matches!(
            request.uri().path().trim_end_matches('/'),
            "/sandboxes" | "/sandboxes-cold" | "/v2/sandboxes"
        );
    let supplied_context = strip_private_headers(request.headers_mut());
    if !launch {
        return next.run(request).await;
    }
    let enabled = crate::host().config().enabled;
    if !enabled {
        if supplied_context.is_some() {
            return (
                StatusCode::CONFLICT,
                axum::Json(serde_json::json!({"code":409,"message":"storage provider disabled"})),
            )
                .into_response();
        }
        return next.run(request).await;
    }
    let header = supplied_context.unwrap_or_default();
    let (parts, body) = request.into_parts();
    let body = match to_bytes(body, 512 * 1024).await {
        Ok(body) => body,
        Err(_) => return (StatusCode::PAYLOAD_TOO_LARGE, axum::Json(serde_json::json!({"code":413,"message":"native launch body exceeds storage limit"}))).into_response(),
    };
    let admission = if crate::host().config().standalone {
        admit_local(parts.uri.path().trim_end_matches('/'), &body).await
    } else {
        admit(&header, &body).await
    };
    match admission {
        Ok(context) => {
            let response = scope(Some(context.clone()),
                next.run(axum::http::Request::from_parts(parts, Body::from(body)))).await;
            if !response.status().is_success() {
                // Revocation serializes against runtime prepare. Once a VMM
                // may have started, the provider requires real stop evidence.
                if let Err(error) = context.control("cancel_unstarted").await {
                    tracing::warn!(request_id = %context.request_id, error = %error,
                        "storage admission cleanup requires runtime reconciliation");
                }
            }
            response
        },
        Err(err) => (StatusCode::CONFLICT, axum::Json(serde_json::json!({"code":409,"message":format!("storage admission failed: {err:#}")}))).into_response(),
    }
}

/// Retain storage before publishing a native immutable snapshot. The provider
/// descriptor stays opaque; the runtime understands neither CSI nor its pins.
pub async fn publish_checkpoint(
    checkpoint: &serde_json::Value,
    native_id: &str,
    repository_id: &str,
) -> Result<serde_json::Value> {
    Ok(client()?
        .post(endpoint("/storage/checkpoints")?)
        .json(&serde_json::json!({"action": "publish", "data": {
            "checkpoint": checkpoint, "native_id": native_id, "repository_id": repository_id,
        }}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

#[cfg(test)]
mod header_tests {
    use super::*;
    #[test]
    fn private_headers_are_removed_from_every_native_request() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-api-key", "native".parse().unwrap());
        headers.insert("x-agentenv-storage-context", "signed".parse().unwrap());
        headers.insert("x-agentenv-storage-owner", "forged".parse().unwrap());
        assert_eq!(
            strip_private_headers(&mut headers).as_deref(),
            Some("signed")
        );
        assert_eq!(headers.len(), 1);
        assert_eq!(headers["x-api-key"], "native");
    }
}

/// Native operations remain behind a deliberately narrow host interface. No
/// AgentENV private types, Kubernetes client or CSI implementation enter here.
#[derive(Clone)]
pub struct ProviderConfig {
    pub enabled: bool,
    pub standalone: bool,
    pub url: Option<String>,
    pub token_file: std::path::PathBuf,
    pub state_dir: std::path::PathBuf,
    pub device_root: std::path::PathBuf,
    pub timeout_ms: u64,
}
pub trait RuntimeHost: Send + Sync {
    fn config(&self) -> ProviderConfig;
    fn validate_device(&self, device: &devices::ExternalBlockDevice) -> Result<()>;
    /// Called only for a locally persisted, independently verified retirement.
    /// The host must constrain this to its native managed capture directory.
    fn remove_capture_root(&self, root: &std::path::Path) -> Result<()>;
}
static HOST: std::sync::OnceLock<std::sync::Arc<dyn RuntimeHost>> = std::sync::OnceLock::new();
pub fn install_host(host: std::sync::Arc<dyn RuntimeHost>) {
    let _ = HOST.set(host);
}
fn host() -> &'static dyn RuntimeHost {
    HOST.get()
        .expect("native storage host must be installed during config initialization")
        .as_ref()
}

pub mod guest;

pub mod capture_request;
