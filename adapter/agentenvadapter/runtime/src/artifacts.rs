//! A local marker prevents native artifact GC from outrunning provider
//! checkpoint reconciliation. Only explicit checkpoint GC may remove it.
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

pub const RETAIN_MARKER: &str = ".storage-checkpoint-retained";

pub fn persist_repository_observation(
    root: &Path,
    repository: &str,
    id: &str,
    action: &str,
) -> io::Result<()> {
    if repository.len() != 64
        || !repository
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        || !matches!(action, "published" | "deleted")
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid repository task",
        ));
    }
    fs::create_dir_all(root)?;
    if let Some(parent) = root.parent() {
        File::open(parent)?.sync_all()?;
    }
    let target = root.join(format!("{repository}_{id}-{action}.json"));
    // NamedTempFile creates a private, unique file in the same directory, so
    // concurrent retries cannot truncate each other's incomplete writes.
    let mut temporary = tempfile::NamedTempFile::new_in(root)?;
    temporary.write_all(&serde_json::to_vec(&serde_json::json!({
        "version": 1, "action": action, "native_id": id, "repository_id": repository,
    }))?)?;
    temporary.as_file().sync_all()?;
    temporary.persist(&target).map_err(|error| error.error)?;
    File::open(root)?.sync_all()?;
    Ok(())
}

pub fn retained(root: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(root.join(RETAIN_MARKER)) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

pub fn retain_capture(root: &Path) -> io::Result<()> {
    // Both the per-capture leaf and native per-sandbox root can be GC targets.
    // Mark the ancestor first so a crash never leaves an unprotected subtree.
    let parent = root
        .parent()
        .ok_or_else(|| io::Error::other("capture parent required"))?;
    for directory in [parent, root] {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(directory.join(RETAIN_MARKER))?;
        file.write_all(b"provider checkpoint reconciliation required\n")?;
        file.sync_all()?;
        File::open(directory)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn repository_observation_is_atomic_and_replayable() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let root = temporary.path().join("publications");
        let repository = "a".repeat(64);
        for _ in 0..2 {
            persist_repository_observation(&root, &repository, "snapshot-id", "published")?;
        }
        let raw = fs::read(root.join(format!("{repository}_snapshot-id-published.json")))?;
        let task: serde_json::Value = serde_json::from_slice(&raw)?;
        assert_eq!(task["native_id"], "snapshot-id");
        assert_eq!(task["action"], "published");
        assert_eq!(task["version"], 1);
        assert_eq!(fs::read_dir(&root)?.count(), 1);
        for (id, action) in [
            ("../escape", "published"),
            ("snapshot-id", "../deleted"),
            ("", "deleted"),
        ] {
            assert!(persist_repository_observation(&root, &repository, id, action).is_err());
        }
        persist_repository_observation(&root, &repository, "snapshot-id", "deleted")?;
        assert!(root
            .join(format!("{repository}_snapshot-id-deleted.json"))
            .exists());
        Ok(())
    }
    #[test]
    fn capture_and_parent_remain_marked_across_reopen() -> io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let parent = temporary.path().join("sandbox");
        let capture = parent.join("capture");
        fs::create_dir_all(&capture)?;
        assert!(!retained(&parent)?);
        retain_capture(&capture)?;
        assert!(retained(&parent)?);
        assert!(retained(&capture)?);
        retain_capture(&capture)?;
        assert!(retained(&capture)?);
        Ok(())
    }
}
