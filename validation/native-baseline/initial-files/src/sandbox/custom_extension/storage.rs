//! Optional, generic external block-device provider. This module has no knowledge
//! of Kubernetes, CSI or any particular storage framework.
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Mutex;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::CustomExtensionParams;
use crate::cfg::ConfigManager;
use crate::sandbox::extra_drive::{normalize_mount_path, DriveMount};
use crate::types::SandboxId;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ExternalBlockDevice {
    pub id: String,
    pub path: PathBuf,
    pub guest_path: PathBuf,
    pub readonly: bool,
    pub major: u32,
    pub minor: u32,
    pub bytes: u64,
    pub uuid: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Event {
    sequence: u64,
    ticket: String,
    sandbox_id: String,
    instance_id: String,
    event: String,
    owner_pid: u32,
    owner_start: String,
    pid: u32,
    process_start: String,
    boot_id: String,
    #[serde(default)]
    devices: Vec<ExternalBlockDevice>,
}
#[derive(Deserialize)]
struct Prepared { devices: Vec<ExternalBlockDevice>, resources: Resources }
#[derive(Deserialize)]
struct Resources { cpu: u32, memory_mib: u64 }

pub(crate) struct ExternalStorage {
    event: Mutex<Event>,
    root: PathBuf,
    url: String,
    client: reqwest::Client,
    pub devices: Vec<ExternalBlockDevice>,
}

pub(crate) fn requested(params: Option<&CustomExtensionParams>) -> bool {
    params.is_some_and(|p| p.contains_key("external_storage"))
}
pub(crate) fn reject_managed(params: Option<&CustomExtensionParams>) -> Result<()> {
    anyhow::ensure!(!requested(params), "operation unsupported for externally managed storage");
    Ok(())
}
fn start_time(pid: u32) -> Result<String> {
    let data = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let (_, fields) = data.rsplit_once(')').context("invalid process identity")?;
    Ok(fields.split_whitespace().nth(19).context("short process identity")?.to_owned())
}

impl ExternalStorage {
    pub async fn prepare(id: SandboxId, params: Option<&CustomExtensionParams>, cpu: u32, memory_mib: u64) -> Result<Option<Self>> {
        let cfg = &ConfigManager::global_config().custom_extension;
        if !requested(params) {
            anyhow::ensure!(!cfg.storage_required, "external storage launch authorization required on this node");
            return Ok(None);
        }
        let ticket = params.and_then(|p| p.get("external_storage"))
            .and_then(|v| v.get("ticket")).and_then(|v| v.as_str())
            .filter(|v| v.len() >= 32).context("external storage ticket required")?.to_owned();
        let url = cfg.url.clone().context("external storage provider is not configured")?;
        let parsed = reqwest::Url::parse(&url)?;
        anyhow::ensure!(parsed.scheme() == "http" && matches!(parsed.host_str(), Some("127.0.0.1" | "localhost" | "::1")), "storage provider must use node-local loopback");
        let root = cfg.storage_state_dir.clone();
        fs::create_dir_all(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        let owner_pid = std::process::id();
        let mut this = Self {
            event: Mutex::new(Event {
                sequence: 1, ticket, sandbox_id: id.to_string(), instance_id: Uuid::now_v7().to_string(),
                event: "prepare".into(), owner_pid, owner_start: start_time(owner_pid)?, pid: 0,
                process_start: String::new(), boot_id: fs::read_to_string("/proc/sys/kernel/random/boot_id")?.trim().into(), devices: Vec::new(),
            }), root, url, client: reqwest::Client::builder().timeout(std::time::Duration::from_millis(cfg.timeout_ms)).build()?, devices: Vec::new(),
        };
        this.persist()?;
        let result = async {
            let ev = this.event.lock().unwrap().clone();
            let response = this.client.post(format!("{}/storage/prepare", this.url.trim_end_matches('/')))
                .json(&ev).send().await?.error_for_status()?.json::<Prepared>().await?;
            anyhow::ensure!(response.resources.cpu == cpu && response.resources.memory_mib == memory_mib, "actual VM resources differ from the reserved launch");
            anyhow::ensure!(!response.devices.is_empty(), "external storage provider returned no devices");
            let device_root = fs::canonicalize(&cfg.storage_device_root)?;
            for d in &response.devices {
                anyhow::ensure!(d.uuid.len() == 36 && d.uuid.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-'), "invalid filesystem UUID");
                anyhow::ensure!(d.id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'), "invalid device ID");
                let canonical = fs::canonicalize(&d.path)?;
                anyhow::ensure!(canonical == d.path && canonical.starts_with(&device_root), "external device outside provider root");
                let m = fs::metadata(&canonical)?;
                anyhow::ensure!(m.file_type().is_block_device() && nix::libc::major(m.rdev()) == d.major && nix::libc::minor(m.rdev()) == d.minor, "external block identity differs");
                anyhow::ensure!(normalize_mount_path(d.guest_path.clone())? == d.guest_path && d.bytes > 0 && d.bytes % 512 == 0, "invalid external mount or capacity");
            }
            Ok::<_, anyhow::Error>(response.devices)
        }.await;
        match result {
            Ok(devices) => { this.devices = devices; Ok(Some(this)) }
            Err(e) => { let _ = this.stopped().await; Err(e) }
        }
    }

    fn persist(&self) -> Result<()> {
        let ev = self.event.lock().unwrap().clone();
        let target = self.root.join(format!("{}.json", ev.instance_id));
        let temp = self.root.join(format!("{}.tmp", ev.instance_id));
        let mut file = OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(&temp)?;
        file.write_all(&serde_json::to_vec(&ev)?)?;
        file.sync_all()?;
        fs::rename(temp, target)?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
    async fn notify(&self) -> Result<()> {
        let ev = self.event.lock().unwrap().clone();
        self.client.post(format!("{}/storage/event", self.url.trim_end_matches('/')))
            .json(&ev).send().await?.error_for_status()?;
        Ok(())
    }
    pub async fn before_launch(&self) -> Result<()> {
        { let mut ev=self.event.lock().unwrap(); ev.sequence+=1; ev.event="launching".into(); }
        self.persist()?;
        if let Err(e)=self.notify().await {
            // This function has not allowed a spawn yet.
            { let mut ev=self.event.lock().unwrap();ev.sequence+=1;ev.event="stopped".into(); }
            self.persist()?;
            return Err(e);
        }
        Ok(())
    }
    pub async fn starting(&self, pid: u32) -> Result<()> {
        { let mut ev = self.event.lock().unwrap(); ev.sequence += 1; ev.event = "starting".into(); ev.pid = pid; ev.process_start = start_time(pid)?; }
        self.persist()?;
        self.notify().await
    }
    pub async fn ready(&self) -> Result<()> {
        { let mut ev = self.event.lock().unwrap(); if ev.event == "ready" { return Ok(()); } ev.sequence += 1; ev.event = "ready".into(); ev.devices = self.devices.clone(); }
        self.persist()?;
        self.notify().await
    }
    pub async fn stopped(&self) -> Result<()> {
        { let mut ev = self.event.lock().unwrap();
          anyhow::ensure!(ev.event != "launching" || ev.pid != 0, "spawn outcome unknown; retain external storage until process fencing");
          anyhow::ensure!(ev.pid == 0 || start_time(ev.pid).ok().as_deref() != Some(ev.process_start.as_str()), "external storage still owned by VMM");
          if ev.event != "stopped" { ev.sequence += 1; ev.event = "stopped".into(); }
        }
        self.persist()?;
        // The fsynced record is also consumed by the sidecar recovery worker.
        if let Err(e) = self.notify().await { tracing::warn!(error=%e, "external storage stop notification deferred to durable journal"); }
        Ok(())
    }
    pub fn assignments(&self, slots: usize, readonly_slots: usize, occupied: usize) -> Result<Vec<(usize, &ExternalBlockDevice)>> {
        anyhow::ensure!(readonly_slots <= slots && occupied <= slots - readonly_slots, "incompatible external storage slot manifest");
        let mut rw = occupied;
        let mut ro = slots - readonly_slots;
        let mut out = Vec::new();
        for d in &self.devices {
            let index = if d.readonly { let i = ro; ro += 1; anyhow::ensure!(i < slots, "template has no free readonly block slot"); i }
                else { let i = rw; rw += 1; anyhow::ensure!(i < slots - readonly_slots, "template has no free writable block slot"); i };
            out.push((index, d));
        }
        Ok(out)
    }
    pub fn mount(d: &ExternalBlockDevice) -> DriveMount {
        DriveMount { drive_id: d.id.clone(), attachment_path: d.path.clone(), read_only: d.readonly }
    }
}

impl Drop for ExternalStorage {
    fn drop(&mut self) {
        // Before process launch there is no asynchronous actor that may acquire
        // these devices. After launch, only observed process exit is evidence.
        let mut ev = self.event.lock().unwrap();
        if ev.pid == 0 && ev.event == "prepare" { ev.sequence += 1; ev.event = "stopped".into(); drop(ev); let _ = self.persist(); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_path_and_lifecycle_restrictions() {
        assert!(!requested(None));
        assert!(reject_managed(None).is_ok());
        let params = serde_json::from_value(serde_json::json!({
            "external_storage": {"ticket": "generic-opaque-reference"}
        })).unwrap();
        assert!(requested(Some(&params)));
        assert!(reject_managed(Some(&params)).is_err());
    }

    #[test]
    fn readonly_devices_use_only_readonly_slots() {
        let device = ExternalBlockDevice {
            id: "disk".into(), path: "/device".into(), guest_path: "/data".into(),
            readonly: true, major: 1, minor: 1, bytes: 4096,
            uuid: "00000000-0000-0000-0000-000000000000".into(),
        };
        let session = ExternalStorage {
            event: Mutex::new(Event {
                sequence: 1, ticket: "test".into(), sandbox_id: "sandbox".into(),
                instance_id: "instance".into(), event: "starting".into(), owner_pid: 1,
                owner_start: "1".into(), pid: 1, process_start: "1".into(),
                boot_id: "boot".into(), devices: Vec::new(),
            }),
            root: PathBuf::new(), url: String::new(), client: reqwest::Client::new(),
            devices: vec![device],
        };
        assert!(session.assignments(8, 0, 0).is_err());
        assert_eq!(session.assignments(8, 2, 0).unwrap()[0].0, 6);
        assert!(session.assignments(8, 2, 7).is_err());
    }
}
