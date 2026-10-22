//! Assemble the original repository transport; do not duplicate OSS algorithms.
use aenv_volume_core::{
    compact::{CompactOutput, CompressionAlgorithm},
    engine::VolumeRepository,
    head_repository::{HeadRepository, ObjectHeadStore},
    heads::PosixHeadStore,
    oss_client::OssClient,
    repositories::{ObjectVolumeRepository, PosixVolumeRepository},
};
use object_store_operator::{
    credential_source_from_fields, AddressingStyle, CredentialFields, CredentialSourceOptions,
};
use serde::Deserialize;
use std::{path::PathBuf, sync::Arc};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OssConfig {
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub prefix: String,
    pub access_key_id_file: Option<PathBuf>,
    pub secret_access_key_file: Option<PathBuf>,
    pub security_token_file: Option<PathBuf>,
    pub credential_process: Option<String>,
    #[serde(default)]
    pub anonymous: bool,
    pub addressing_style: Option<String>,
    #[serde(default = "raw_compression")]
    pub compression: String,
    #[serde(default = "one_worker")]
    pub compression_workers: usize,
}
fn raw_compression() -> String {
    "raw".into()
}
fn one_worker() -> usize {
    1
}
fn secret(path: &Option<PathBuf>) -> Result<Option<String>, Box<dyn std::error::Error>> {
    match path {
        None => Ok(None),
        Some(path) if path.is_absolute() => {
            Ok(Some(std::fs::read_to_string(path)?.trim().to_owned()))
        }
        Some(_) => Err("credential file paths must be absolute".into()),
    }
}
pub type Repositories = (Arc<dyn VolumeRepository>, Arc<dyn HeadRepository>);
pub fn assemble(
    backend: &str,
    root: Option<PathBuf>,
    oss: Option<OssConfig>,
) -> Result<Repositories, Box<dyn std::error::Error>> {
    match backend {
        "posix_fs" => {
            if oss.is_some() {
                return Err("OSS configuration supplied for POSIX repository".into());
            }
            let root = root.ok_or("POSIX repository_root required")?;
            Ok((
                Arc::new(PosixVolumeRepository::new(root.clone())),
                Arc::new(PosixHeadStore::new(root.join("published-heads"))?),
            ))
        }
        "oss" => {
            let c = oss.ok_or("OSS repository configuration required")?;
            // A dedicated nonempty prefix is mandatory; legacy root namespaces
            // must never be adopted merely by changing a profile/StorageClass.
            if c.bucket.is_empty()
                || c.prefix.is_empty()
                || c.prefix
                    .split('/')
                    .any(|part| part.is_empty() || part == "." || part == "..")
                || c.prefix.contains('\\')
                || c.compression_workers == 0
            {
                return Err(
                    "isolated canonical OSS prefix and nonzero compression workers required".into(),
                );
            }
            let id = secret(&c.access_key_id_file)?;
            let key = secret(&c.secret_access_key_file)?;
            let token = secret(&c.security_token_file)?;
            let credentials = credential_source_from_fields(
                CredentialFields {
                    access_key_id: id.as_deref(),
                    secret_access_key: key.as_deref(),
                    security_token: token.as_deref(),
                    credential_process: c.credential_process.as_deref(),
                },
                CredentialSourceOptions {
                    scope: "OverlayBD CSI repository",
                    allow_anonymous: c.anonymous,
                    required_access_key_id_label: "access_key_id_file",
                    required_secret_access_key_label: "secret_access_key_file",
                },
            )?;
            let addressing = match c.addressing_style.as_deref() {
                None => None,
                Some("path") => Some(AddressingStyle::Path),
                Some("virtual") => Some(AddressingStyle::Virtual),
                Some(_) => return Err("unknown OSS addressing style".into()),
            };
            let compression = match c.compression.as_str() {
                "raw" => CompactOutput::Raw,
                "lz4" => CompactOutput::ZFile {
                    algorithm: CompressionAlgorithm::Lz4,
                    workers: c.compression_workers,
                },
                "zstd" => CompactOutput::ZFile {
                    algorithm: CompressionAlgorithm::Zstd,
                    workers: c.compression_workers,
                },
                _ => return Err("unknown publication compression".into()),
            };
            let client = Arc::new(OssClient::new(
                c.bucket,
                c.endpoint,
                c.region,
                c.prefix,
                credentials,
                addressing,
            )?);
            let layers = client.managed_layers_repo_blob_url();
            Ok((
                Arc::new(ObjectVolumeRepository::new(
                    client.clone(),
                    layers,
                    compression,
                )),
                Arc::new(ObjectHeadStore::new(client)),
            ))
        }
        _ => Err("repository_backend must be posix_fs or oss".into()),
    }
}
