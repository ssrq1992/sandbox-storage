//! Private native worker. Device ownership stays in the dedicated daemon when
//! this process exits; graceful RPC shutdown does not delete live devices.
use aenv_volume_core::{
    engine::VolumeLimits,
    journal::{LockedOperation, NativeIntent},
};
use serde::Deserialize;
mod repository_config;
fn posix_backend() -> String {
    "posix_fs".into()
}
use ssf_overlaybd_native::{
    device_host::DeviceHost, protocol::native_storage_backend_server::NativeStorageBackendServer,
    service::BackendService, volume_host::VolumeHost,
};
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, PermissionsExt};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio_stream::wrappers::UnixListenerStream;
use uvm_ublk_daemon::client::ManagedDaemonClient;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    repository_id: String,
    repository_root: Option<PathBuf>,
    #[serde(default = "posix_backend")]
    repository_backend: String,
    oss: Option<repository_config::OssConfig>,
    state_root: PathBuf,
    socket: PathBuf,
    node_id: String,
    daemon_socket: PathBuf,
    overlaybd_config: PathBuf,
    accessible_nodes: Vec<String>,
    max_size_mb: u64,
    max_mounts: usize,
    request_timeout_seconds: u64,
}
fn private_directory(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true).mode(0o700).create(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(format!("private directory required: {}", path.display()).into());
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let config_path = args
        .next()
        .ok_or("usage: ssf-overlaybd-native CONFIG.json")?;
    if args.next().is_some() {
        return Err("exactly one configuration file required".into());
    }
    let config: Config = serde_json::from_slice(&std::fs::read(config_path)?)?;
    if !aenv_volume_core::is_valid_volume_component(&config.repository_id)
        || config.node_id.is_empty()
        || config.max_size_mb == 0
        || config.max_mounts == 0
        || config.request_timeout_seconds == 0
    {
        return Err("invalid native worker configuration".into());
    }
    let mut paths = vec![
        &config.state_root,
        &config.socket,
        &config.daemon_socket,
        &config.overlaybd_config,
    ];
    if let Some(root) = &config.repository_root {
        paths.push(root);
    }
    for path in paths {
        if !path.is_absolute()
            || path
                .components()
                .any(|p| matches!(p, std::path::Component::ParentDir))
        {
            return Err("canonical absolute native worker paths required".into());
        }
    }
    private_directory(&config.state_root)?;
    let parent = config.socket.parent().ok_or("socket parent required")?;
    private_directory(parent)?;
    // The lock survives for the entire listener lifetime. Only its holder may
    // remove a stale socket; another worker cannot replace an active listener.
    let _socket_owner = LockedOperation::open(
        parent,
        NativeIntent {
            operation_id: "backend-listener".into(),
            input_digest: "0".repeat(64),
            volume_id: "backend-listener".into(),
            repository_id: config.repository_id.clone(),
            kind: "listener".into(),
        },
    )?;
    match std::fs::symlink_metadata(&config.socket) {
        Ok(meta) if meta.file_type().is_socket() => std::fs::remove_file(&config.socket)?,
        Ok(_) => return Err("refusing to replace non-socket backend endpoint".into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    let (repository, heads) = repository_config::assemble(
        &config.repository_backend,
        config.repository_root,
        config.oss,
    )?;
    let volumes = Arc::new(VolumeHost::new(
        config.repository_id.clone(),
        config.state_root.join("volume-operations"),
        config.state_root.join("volume-work"),
        heads,
        repository,
        VolumeLimits {
            max_size_mb: config.max_size_mb,
            max_mounts: config.max_mounts,
        },
        config.accessible_nodes,
    )?);
    let devices = Arc::new(DeviceHost {
        node_id: config.node_id,
        journal_root: config.state_root.join("device-operations"),
        work_root: config.state_root.join("devices"),
        global_config: config.overlaybd_config,
        daemon: ManagedDaemonClient::connect(
            config.daemon_socket,
            Duration::from_secs(config.request_timeout_seconds),
        )?,
        volumes: volumes.clone(),
    });
    let capabilities = devices.daemon.capabilities().await?;
    for required in [
        "device-session-v1",
        "layer-capture-v1",
        "head-publication-v1",
        "durable-restack-v1",
    ] {
        if !capabilities.iter().any(|v| v == required) {
            return Err(format!("native daemon missing {required}").into());
        }
    }
    let offline_expansion = capabilities.iter().any(|v| v == "offline-expansion-v1");
    let listener = tokio::net::UnixListener::bind(&config.socket)?;
    std::fs::set_permissions(&config.socket, std::fs::Permissions::from_mode(0o600))?;
    let service = NativeStorageBackendServer::new(BackendService::new(
        config.repository_id,
        volumes,
        devices,
        offline_expansion,
    ))
    .max_decoding_message_size(4 << 20)
    .max_encoding_message_size(4 << 20);
    let result = tonic::transport::Server::builder()
        .add_service(service)
        .serve_with_incoming_shutdown(UnixListenerStream::new(listener), async {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install termination signal handler");
            tokio::select! { _ = term.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
        })
        .await;
    std::fs::remove_file(&config.socket)?;
    result?;
    Ok(())
}
