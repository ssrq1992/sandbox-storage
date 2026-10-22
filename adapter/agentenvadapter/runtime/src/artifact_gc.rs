//! Generic native artifact retirement. Only the original runtime removes files;
//! provider receipts protect checkpoint consumers and contain no filesystem paths.
use super::{client, endpoint, StorageContext};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::Once,
};

#[derive(Clone, Serialize, Deserialize)]
struct Capture {
    context: StorageContext,
    barrier: String,
    directory: PathBuf,
}
#[derive(Serialize, Deserialize)]
struct Task {
    root: PathBuf,
    pid: u32,
    start: String,
    wanted: bool,
    latest: Capture,
    #[serde(default)]
    captures: Vec<Capture>,
}

fn task_root() -> PathBuf {
    crate::host().config().state_dir.join("artifact_gc")
}
fn task_path(root: &Path) -> PathBuf {
    task_root().join(format!(
        "{:x}.json",
        Sha256::digest(root.as_os_str().as_encoded_bytes())
    ))
}
fn process_start(pid: u32) -> Result<String> {
    let value = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    Ok(value
        .rsplit_once(')')
        .context("process identity missing")?
        .1
        .split_whitespace()
        .nth(19)
        .context("process identity truncated")?
        .to_owned())
}
fn write_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("task parent missing")?;
    fs::create_dir_all(parent)?;
    if let Some(ancestor) = parent.parent() {
        File::open(ancestor)?.sync_all()?;
    }
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(&serde_json::to_vec(value)?)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

pub fn retain(directory: &Path, context: &StorageContext, barrier: &str) -> Result<()> {
    let directory = directory.canonicalize()?;
    let root = directory
        .parent()
        .context("capture root missing")?
        .to_path_buf();
    let capture = Capture {
        context: context.clone(),
        barrier: barrier.to_owned(),
        directory: directory.clone(),
    };
    let mut captures = match fs::read(task_path(&root)) {
        Ok(raw) => {
            let prior: Task = serde_json::from_slice(&raw)?;
            anyhow::ensure!(prior.root == root, "artifact root identity changed");
            let mut captures = prior.captures;
            if !captures
                .iter()
                .any(|capture| capture.directory == prior.latest.directory)
            {
                captures.push(prior.latest);
            }
            captures
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(
        !captures
            .iter()
            .any(|capture| capture.directory == directory),
        "capture directory reused"
    );
    captures.push(capture.clone());
    // Persist recovery identity before marking or issuing the provider request.
    write_atomic(
        &task_path(&root),
        &Task {
            root: root.clone(),
            pid: std::process::id(),
            start: process_start(std::process::id())?,
            wanted: false,
            latest: capture.clone(),
            captures,
        },
    )?;
    super::artifacts::retain_capture(&directory)?;
    write_atomic(&directory.join(super::artifacts::RETAIN_MARKER), &capture)?;
    Ok(())
}

pub fn request(root: &Path) -> Result<()> {
    let root = root.canonicalize()?;
    let path = task_path(&root);
    let mut task: Task = serde_json::from_slice(&fs::read(&path)?)?;
    anyhow::ensure!(task.root == root, "artifact task identity changed");
    task.wanted = true;
    write_atomic(&path, &task)
}

async fn retire(capture: &Capture) -> Result<bool> {
    let reply:serde_json::Value=client()?.post(endpoint("/storage/control")?)
        .json(&serde_json::json!({"ticket":capture.context.ticket,"action":"capture.retire","capture":{"barrier_id":capture.barrier}}))
        .send().await?.error_for_status()?.json().await?;
    reply
        .get("collectible")
        .and_then(serde_json::Value::as_bool)
        .context("artifact retirement receipt missing")
}

async fn advance(path: &Path) -> Result<()> {
    anyhow::ensure!(
        fs::metadata(path)?.len() <= 64 * 1024 * 1024,
        "oversized artifact task"
    );
    let raw = fs::read(path)?;
    anyhow::ensure!(raw.len() <= 64 * 1024 * 1024, "oversized artifact task");
    let task: Task = serde_json::from_slice(&raw)?;
    anyhow::ensure!(
        task_path(&task.root) == path && task.root.is_absolute(),
        "invalid artifact task path"
    );
    if !task.wanted && process_start(task.pid).is_ok_and(|start| start == task.start) {
        return Ok(());
    }
    if !task.root.try_exists()? {
        for capture in task.captures.iter().chain(std::iter::once(&task.latest)) {
            if !retire(capture).await? {
                return Ok(());
            }
        }
        fs::remove_file(path)?;
        File::open(task_root())?.sync_all()?;
        return Ok(());
    }
    anyhow::ensure!(
        task.root.canonicalize()? == task.root,
        "artifact root became a symlink"
    );
    let mut captures = task.captures.clone();
    for child in fs::read_dir(&task.root)? {
        let child = child?;
        if !child.file_type()?.is_dir() {
            continue;
        }
        let marker = child.path().join(super::artifacts::RETAIN_MARKER);
        if !marker.try_exists()? {
            continue;
        }
        anyhow::ensure!(
            fs::symlink_metadata(&marker)?.is_file(),
            "artifact marker is not a regular file"
        );
        let raw = fs::read(&marker)?;
        anyhow::ensure!(raw.len() <= 1024 * 1024, "oversized artifact marker");
        let capture: Capture = match serde_json::from_slice(&raw) {
            Ok(capture) => capture,
            Err(_) if child.path() == task.latest.directory => task.latest.clone(),
            Err(error) => return Err(error.into()),
        };
        anyhow::ensure!(
            capture.directory == child.path(),
            "capture directory identity changed"
        );
        if let Some(prior) = captures
            .iter()
            .find(|prior| prior.directory == capture.directory)
        {
            anyhow::ensure!(
                prior.barrier == capture.barrier
                    && prior.context.request_id == capture.context.request_id,
                "capture marker identity changed"
            );
        } else {
            captures.push(capture);
        }
    }
    if !captures
        .iter()
        .any(|capture| capture.directory == task.latest.directory)
    {
        captures.push(task.latest.clone());
    }
    for capture in &captures {
        if !retire(capture).await? {
            return Ok(());
        }
    }
    // All original owner runs are terminal and no template/child retains the
    // native artifact. The immutable root cannot be reused by a new sandbox.
    anyhow::ensure!(
        task.root.canonicalize()? == task.root,
        "artifact root changed during retirement"
    );
    crate::host().remove_capture_root(&task.root)?;
    if let Some(parent) = task.root.parent() {
        File::open(parent)?.sync_all()?;
    }
    fs::remove_file(path)?;
    File::open(task_root())?.sync_all()?;
    Ok(())
}

pub fn start() {
    static START: Once = Once::new();
    if !crate::host().config().enabled {
        return;
    }
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    START.call_once(|| {
        handle.spawn(async {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                tick.tick().await;
                let entries = match fs::read_dir(task_root()) {
                    Ok(entries) => entries,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => {
                        tracing::warn!(%error,"artifact retirement scan failed");
                        continue;
                    }
                };
                for entry in entries {
                    let Ok(entry) = entry else {
                        continue;
                    };
                    if entry.file_type().is_ok_and(|kind| kind.is_file())
                        && entry
                            .path()
                            .extension()
                            .is_some_and(|extension| extension == "json")
                    {
                        if let Err(error) = advance(&entry.path()).await {
                            tracing::debug!(%error,"native artifact retirement retained");
                        }
                    }
                }
            }
        });
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn artifact_task_atomic_write_preserves_all_capture_identities() -> Result<()> {
        let temporary = tempfile::tempdir()?;
        let path = temporary.path().join("tasks/task.json");
        let context = StorageContext {
            ticket: "private-ticket".into(),
            owner_uid: "owner".into(),
            run_uid: "run".into(),
            request_id: "request".into(),
            template_pin: String::new(),
            checkpoint_source: String::new(),
            managed_mounts: Vec::new(),
            native_id: String::new(),
            replace_mounts: false,
            relocations: Vec::new(),
        };
        let first = Capture {
            context: context.clone(),
            barrier: "one".into(),
            directory: temporary.path().join("root/one"),
        };
        let second = Capture {
            context,
            barrier: "two".into(),
            directory: temporary.path().join("root/two"),
        };
        let mut task = Task {
            root: temporary.path().join("root"),
            pid: std::process::id(),
            start: "start".into(),
            wanted: false,
            latest: second.clone(),
            captures: vec![first, second],
        };
        write_atomic(&path, &task)?;
        task.wanted = true;
        write_atomic(&path, &task)?;
        let reopened: Task = serde_json::from_slice(&fs::read(&path)?)?;
        assert!(reopened.wanted);
        assert_eq!(reopened.captures.len(), 2);
        assert_eq!(reopened.captures[0].barrier, "one");
        assert_eq!(reopened.latest.barrier, "two");
        assert_eq!(fs::read_dir(path.parent().unwrap())?.count(), 1);
        Ok(())
    }
    #[test]
    #[cfg(target_os = "linux")]
    fn artifact_owner_identity_requires_live_exact_process() -> Result<()> {
        assert!(!process_start(std::process::id())?.is_empty());
        assert!(process_start(u32::MAX).is_err());
        Ok(())
    }
}
