//! Guest filesystem obligations captured with VMM memory. No host device paths
//! or provider credentials are persisted here.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Component, PathBuf};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrozenFilesystem {
    pub mount: PathBuf,
    /// Guest st_dev, checked again before thaw. Empty means an interrupted,
    /// unknown freeze outcome and must never be exported as a runnable capture.
    pub device: String,
}

pub fn validate_frozen_filesystems(mounts: &[FrozenFilesystem]) -> Result<()> {
    let mut seen = HashSet::new();
    ensure!(mounts.len() <= 24, "too many frozen guest filesystems");
    for entry in mounts {
        ensure!(
            entry.mount.is_absolute() && entry.mount != std::path::Path::new("/"),
            "frozen filesystem must be a non-root guest path"
        );
        ensure!(
            entry
                .mount
                .components()
                .all(|c| matches!(c, Component::RootDir | Component::Normal(_)))
                && entry.mount.to_str().is_some_and(|p| !p.contains('\0')),
            "invalid frozen guest path"
        );
        ensure!(
            seen.insert(&entry.mount),
            "duplicate frozen guest filesystem"
        );
        ensure!(
            !entry.device.is_empty()
                && entry.device.bytes().all(|b| b.is_ascii_digit())
                && entry.device.parse::<u64>().is_ok_and(|d| d != 0),
            "unknown frozen guest device identity"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn entry(path: &str, device: &str) -> FrozenFilesystem {
        FrozenFilesystem {
            mount: path.into(),
            device: device.into(),
        }
    }
    #[test]
    fn capture_requires_known_device_and_safe_unique_paths() {
        assert!(validate_frozen_filesystems(&[]).is_ok());
        assert!(validate_frozen_filesystems(&[entry("/data", "64512")]).is_ok());
        for invalid in [
            entry("/", "1"),
            entry("data", "1"),
            entry("/data/../etc", "1"),
            entry("/data", ""),
            entry("/data", "0"),
            entry("/data", "1;reboot"),
            entry("/data", "18446744073709551616"),
        ] {
            assert!(validate_frozen_filesystems(&[invalid]).is_err());
        }
        assert!(validate_frozen_filesystems(&[entry("/data", "1"), entry("/data", "1")]).is_err());
        assert!(validate_frozen_filesystems(
            &(0..25)
                .map(|i| entry(&format!("/d{i}"), "1"))
                .collect::<Vec<_>>()
        )
        .is_err());
    }
    #[test]
    fn obligations_round_trip_without_host_capabilities() {
        let entries = vec![entry("/data", "64512")];
        let bytes = serde_json::to_vec(&entries).unwrap();
        assert_eq!(
            serde_json::from_slice::<Vec<FrozenFilesystem>>(&bytes).unwrap(),
            entries
        );
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value[0].as_object().unwrap().len(), 2);
    }
}
