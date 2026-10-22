//! Optional, generic external block-device provider. This module has no knowledge
//! of Kubernetes, CSI or any particular storage framework.
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::StorageContext;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalBlockDevice {
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    capture: Option<CaptureReceipt>,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaptureReceipt {
    barrier_id: String,
    artifact: String,
    digest: String,
}

#[derive(Deserialize)]
struct Prepared {
    devices: Vec<ExternalBlockDevice>,
    resources: Resources,
}
#[derive(Deserialize)]
struct Resources {
    cpu: u32,
    memory_mib: u64,
}

pub struct PreparedExternalDrives {
    event: Mutex<Event>,
    root: PathBuf,
    url: String,
    client: reqwest::Client,
    context: StorageContext,
    pause_retained: AtomicBool,
    pub devices: Vec<ExternalBlockDevice>,
}

fn start_time(pid: u32) -> Result<String> {
    let data = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let (_, fields) = data.rsplit_once(')').context("invalid process identity")?;
    Ok(fields
        .split_whitespace()
        .nth(19)
        .context("short process identity")?
        .to_owned())
}

impl PreparedExternalDrives {
    pub async fn prepare(
        id: impl ToString,
        params: Option<&StorageContext>,
        cpu: u32,
        memory_mib: u64,
    ) -> Result<Option<Self>> {
        let cfg = &crate::host().config();
        let Some(context) = params else {
            return Ok(None);
        };
        anyhow::ensure!(
            cfg.enabled,
            "storage context supplied while provider disabled"
        );
        let ticket = context.ticket.clone();
        anyhow::ensure!(ticket.len() >= 32, "storage ticket required");
        let url = cfg
            .url
            .clone()
            .context("storage provider is not configured")?;
        let parsed = reqwest::Url::parse(&url)?;
        anyhow::ensure!(
            parsed.scheme() == "http"
                && matches!(parsed.host_str(), Some("127.0.0.1" | "localhost" | "::1")),
            "storage provider must use node-local loopback"
        );
        let root = cfg.state_dir.clone();
        fs::create_dir_all(&root)?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
        let owner_pid = std::process::id();
        let mut this = Self {
            event: Mutex::new(Event {
                capture: None,
                sequence: 1,
                ticket,
                sandbox_id: id.to_string(),
                instance_id: Uuid::now_v7().to_string(),
                event: "prepare".into(),
                owner_pid,
                owner_start: start_time(owner_pid)?,
                pid: 0,
                process_start: String::new(),
                boot_id: fs::read_to_string("/proc/sys/kernel/random/boot_id")?
                    .trim()
                    .into(),
                devices: Vec::new(),
            }),
            root,
            url,
            client: super::client()?,
            context: context.clone(),
            pause_retained: AtomicBool::new(false),
            devices: Vec::new(),
        };
        this.persist()?;
        let result = async {
            let ev = this.event.lock().unwrap().clone();
            let response = this
                .client
                .post(format!(
                    "{}/storage/prepare",
                    this.url.trim_end_matches('/')
                ))
                .json(&ev)
                .send()
                .await?
                .error_for_status()?
                .json::<Prepared>()
                .await?;
            anyhow::ensure!(
                response.resources.cpu == cpu && response.resources.memory_mib == memory_mib,
                "actual VM resources differ from the reserved launch"
            );
            anyhow::ensure!(
                !response.devices.is_empty() || context.managed_mounts.is_empty(),
                "storage provider omitted managed devices"
            );
            for device in &response.devices {
                crate::host().validate_device(device)?;
            }
            Ok::<_, anyhow::Error>(response.devices)
        }
        .await;
        match result {
            Ok(devices) => {
                this.devices = devices;
                Ok(Some(this))
            }
            Err(e) => {
                let _ = this.stopped().await;
                Err(e)
            }
        }
    }

    fn persist(&self) -> Result<()> {
        let ev = self.event.lock().unwrap().clone();
        let target = self.root.join(format!("{}.json", ev.instance_id));
        let temp = self.root.join(format!("{}.tmp", ev.instance_id));
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&temp)?;
        file.write_all(&serde_json::to_vec(&ev)?)?;
        file.sync_all()?;
        fs::rename(temp, target)?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }
    async fn notify(&self) -> Result<()> {
        let ev = self.event.lock().unwrap().clone();
        self.client
            .post(format!("{}/storage/event", self.url.trim_end_matches('/')))
            .json(&ev)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
    /// Acquire the durable logical hold before creating paused memory or
    /// releasing this run's attachments. A local boolean is never the authority.
    pub async fn retain_pause(&self) -> Result<()> {
        self.context.control("pause").await?;
        self.pause_retained.store(true, Ordering::Release);
        Ok(())
    }
    pub fn pause_retained(&self) -> bool {
        self.pause_retained.load(Ordering::Acquire)
    }
    pub async fn cancel_pause(&self) -> Result<()> {
        self.context.control("cancel_pause").await?;
        self.pause_retained.store(false, Ordering::Release);
        Ok(())
    }

    pub async fn before_launch(&self) -> Result<()> {
        {
            let mut ev = self.event.lock().unwrap();
            ev.sequence += 1;
            ev.event = "launching".into();
        }
        self.persist()?;
        if let Err(e) = self.notify().await {
            // This function has not allowed a spawn yet.
            {
                let mut ev = self.event.lock().unwrap();
                ev.sequence += 1;
                ev.event = "stopped".into();
            }
            self.persist()?;
            return Err(e);
        }
        Ok(())
    }
    pub async fn starting(&self, pid: u32) -> Result<()> {
        {
            let mut ev = self.event.lock().unwrap();
            ev.sequence += 1;
            ev.event = "starting".into();
            ev.pid = pid;
            ev.process_start = start_time(pid)?;
        }
        self.persist()?;
        self.notify().await
    }
    pub async fn ready(&self) -> Result<()> {
        {
            let mut ev = self.event.lock().unwrap();
            if ev.event != "ready" {
                ev.sequence += 1;
                ev.event = "ready".into();
                ev.capture = None;
                ev.devices = self.devices.clone();
            }
            // A persisted ready event is not a controller acknowledgement.
            // Retry delivery after an HTTP timeout before reporting VM ready.
        }
        self.persist()?;
        self.notify().await
    }
    /// Called only while the original VMM is paused and every writable guest
    /// volume is frozen. CSI details remain opaque to the native runtime.
    pub fn retain_capture(&self, root: &Path, barrier: &str) -> Result<()> {
        super::artifact_gc::retain(root, &self.context, barrier)
    }
    pub async fn capture(
        &self,
        barrier_id: String,
        artifact: String,
        digest: String,
    ) -> Result<Option<serde_json::Value>> {
        match self.seal_capture(barrier_id, artifact, digest).await? {
            Some(receipt) => self.publish_capture(&receipt).await.map(Some),
            None => Ok(None),
        }
    }

    /// Only a durably sealed reply permits the original runtime to resume.
    /// Generic CSI participants remain frozen until their snapshots are ready.
    pub async fn seal_capture(
        &self,
        barrier_id: String,
        artifact: String,
        digest: String,
    ) -> Result<Option<CaptureReceipt>> {
        if self.devices.is_empty() {
            return Ok(None);
        }
        let receipt = CaptureReceipt {
            barrier_id,
            artifact,
            digest,
        };
        {
            let mut ev = self.event.lock().unwrap();
            anyhow::ensure!(ev.event == "ready", "capture requires a ready native run");
            ev.sequence += 1;
            ev.event = "capturing".into();
            ev.capture = Some(receipt.clone());
            ev.devices = self.devices.clone();
        }
        self.persist()?;
        self.notify().await?;
        self.await_capture("capture.seal", &receipt).await?;
        Ok(Some(receipt))
    }

    pub async fn publish_capture(&self, receipt: &CaptureReceipt) -> Result<serde_json::Value> {
        self.await_capture("capture", receipt)
            .await?
            .get("checkpoint")
            .cloned()
            .context("missing joint checkpoint")
    }

    async fn await_capture(
        &self,
        action: &str,
        receipt: &CaptureReceipt,
    ) -> Result<serde_json::Value> {
        tokio::time::timeout(std::time::Duration::from_secs(300), async {
            loop {
                let result: serde_json::Value = self.client
                    .post(format!("{}/storage/control", self.url.trim_end_matches('/')))
                    .json(&serde_json::json!({"ticket": self.context.ticket, "action": action, "capture": receipt}))
                    .send().await?.error_for_status()?.json().await?;
                match result.get("ready").and_then(serde_json::Value::as_bool) {
                    Some(true) => return Ok(result),
                    Some(false) => tokio::time::sleep(std::time::Duration::from_millis(250)).await,
                    None => anyhow::bail!("invalid provider capture response"),
                }
            }
        }).await.context("joint storage capture timed out")?
    }

    /// Invalidate the durable barrier before resuming vCPUs. A controller that
    /// races an HTTP timeout must not observe a stale capturing receipt.
    pub async fn begin_thaw(&self) -> Result<()> {
        {
            let mut ev = self.event.lock().unwrap();
            if ev.event != "capturing" && ev.event != "thawing" {
                return Ok(());
            }
            if ev.event != "thawing" {
                ev.sequence += 1;
                ev.event = "thawing".into();
                ev.capture = None;
            }
        }
        self.persist()?;
        self.notify().await
    }

    pub async fn stopped(&self) -> Result<()> {
        {
            let mut ev = self.event.lock().unwrap();
            anyhow::ensure!(
                ev.event != "launching" || ev.pid != 0,
                "spawn outcome unknown; retain external storage until process fencing"
            );
            anyhow::ensure!(
                ev.pid == 0
                    || start_time(ev.pid).ok().as_deref() != Some(ev.process_start.as_str()),
                "external storage still owned by VMM"
            );
            if ev.event != "stopped" {
                ev.sequence += 1;
                ev.event = "stopped".into();
            }
        }
        self.persist()?;
        // The fsynced record is also consumed by the sidecar recovery worker.
        if let Err(e) = self.notify().await {
            tracing::warn!(error=%e, "external storage stop notification deferred to durable journal");
        }
        Ok(())
    }
    pub fn validate_native_drives(
        &self,
        native: &[PathBuf],
        physical_count: usize,
        slots: usize,
        readonly_slots: usize,
    ) -> Result<()> {
        anyhow::ensure!(
            physical_count <= native.len(),
            "invalid physical drive count"
        );
        anyhow::ensure!(
            physical_count.checked_add(slots).is_some_and(|n| n <= 24),
            "external volume slots exceed guest block-device names"
        );
        self.assignments(slots, readonly_slots, native.len() - physical_count)?;
        let mut paths = native.iter().cloned().collect::<Vec<_>>();
        let mut ids = std::collections::HashSet::new();
        for device in &self.devices {
            anyhow::ensure!(
                ids.insert(device.id.as_str()),
                "duplicate external device ID"
            );
            for path in &paths {
                anyhow::ensure!(
                    !path.starts_with(&device.guest_path) && !device.guest_path.starts_with(path),
                    "external mount overlaps another drive: {}",
                    device.guest_path.display()
                );
            }
            paths.push(device.guest_path.clone());
        }
        Ok(())
    }

    pub fn assignments(
        &self,
        slots: usize,
        readonly_slots: usize,
        occupied: usize,
    ) -> Result<Vec<(usize, &ExternalBlockDevice)>> {
        anyhow::ensure!(
            readonly_slots <= slots && occupied <= slots - readonly_slots,
            "incompatible external storage slot manifest"
        );
        let mut rw = occupied;
        let mut ro = slots - readonly_slots;
        let mut out = Vec::new();
        for d in &self.devices {
            let index = if d.readonly {
                let i = ro;
                ro += 1;
                anyhow::ensure!(i < slots, "template has no free readonly block slot");
                i
            } else {
                let i = rw;
                rw += 1;
                anyhow::ensure!(
                    i < slots - readonly_slots,
                    "template has no free writable block slot"
                );
                i
            };
            out.push((index, d));
        }
        Ok(out)
    }
    pub fn assignments_layout(
        &self,
        slots: usize,
        readonly_slots: usize,
        occupied: usize,
        layout: &std::collections::BTreeMap<String, usize>,
    ) -> Result<Vec<(usize, &ExternalBlockDevice)>> {
        if layout.is_empty() {
            return self.assignments(slots, readonly_slots, occupied);
        }
        let mut used = std::collections::HashSet::new();
        self.devices
            .iter()
            .map(|device| {
                let path = device
                    .guest_path
                    .to_str()
                    .context("invalid guest device path")?;
                let slot = *layout
                    .get(path)
                    .context("external device missing from stable layout")?;
                anyhow::ensure!(
                    slot < slots && used.insert(slot),
                    "external device slot collision"
                );
                anyhow::ensure!(
                    (slot >= slots - readonly_slots) == device.readonly,
                    "external readonly slot mismatch"
                );
                Ok((slot, device))
            })
            .collect()
    }
}

impl Drop for PreparedExternalDrives {
    fn drop(&mut self) {
        // Before process launch there is no asynchronous actor that may acquire
        // these devices. After launch, only observed process exit is evidence.
        let mut ev = self.event.lock().unwrap();
        if ev.pid == 0 && ev.event == "prepare" {
            ev.sequence += 1;
            ev.event = "stopped".into();
            drop(ev);
            let _ = self.persist();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readonly_devices_use_only_readonly_slots() {
        let device = ExternalBlockDevice {
            id: "disk".into(),
            path: "/device".into(),
            guest_path: "/data".into(),
            readonly: true,
            major: 1,
            minor: 1,
            bytes: 4096,
            uuid: "00000000-0000-0000-0000-000000000000".into(),
        };
        let mut session = PreparedExternalDrives {
            event: Mutex::new(Event {
                capture: None,
                sequence: 1,
                ticket: "test".into(),
                sandbox_id: "sandbox".into(),
                instance_id: "instance".into(),
                event: "starting".into(),
                owner_pid: 1,
                owner_start: "1".into(),
                pid: 1,
                process_start: "1".into(),
                boot_id: "boot".into(),
                devices: Vec::new(),
            }),
            root: PathBuf::new(),
            url: String::new(),
            client: reqwest::Client::new(),
            context: StorageContext {
                ticket: "test".into(),
                owner_uid: "owner".into(),
                run_uid: "run".into(),
                request_id: "request".into(),
                template_pin: String::new(),
                checkpoint_source: String::new(),
                managed_mounts: Vec::new(),
                native_id: String::new(),
                replace_mounts: false,
                relocations: Vec::new(),
            },
            pause_retained: AtomicBool::new(false),
            devices: vec![device],
        };
        assert!(session.assignments(8, 0, 0).is_err());
        assert_eq!(session.assignments(5, 1, 4).unwrap()[0].0, 4);
        assert_eq!(session.assignments(8, 2, 0).unwrap()[0].0, 6);
        assert!(session.assignments(8, 2, 7).is_err());
        assert!(session.validate_native_drives(&[], 0, 8, 2).is_ok());
        assert!(session.validate_native_drives(&[], 1, 8, 2).is_err());
        assert!(session.validate_native_drives(&[], 0, 25, 2).is_err());
        let mut overlapping = session.devices[0].clone();
        overlapping.id = "second".into();
        overlapping.guest_path = "/data/child".into();
        session.devices.push(overlapping);
        assert!(session.validate_native_drives(&[], 0, 8, 2).is_err());
    }
}
