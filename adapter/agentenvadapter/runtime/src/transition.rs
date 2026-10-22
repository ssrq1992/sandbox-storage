//! Generic frozen-memory volume layout. A replacement retains old devices in
//! their original slots while moving their mounts behind a private guest path.
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

pub const RETAINED_PREFIX: &str = "/.agentenv-storage-retained/";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MountRelocation {
    pub source: String,
    pub target: String,
}

pub fn retained(path: &str) -> bool {
    path == RETAINED_PREFIX.trim_end_matches('/') || path.starts_with(RETAINED_PREFIX)
}

pub fn relocate_layout(
    original: &BTreeMap<String, usize>,
    moves: &[MountRelocation],
) -> Result<BTreeMap<String, usize>> {
    let mut layout = original.clone();
    let mut sources = HashSet::new();
    let mut targets = HashSet::new();
    for relocation in moves {
        anyhow::ensure!(
            relocation.source.starts_with('/')
                && !retained(&relocation.source)
                && retained(&relocation.target)
                && !relocation.target.contains("/../")
                && !relocation.target.ends_with("/..")
                && sources.insert(relocation.source.clone())
                && targets.insert(relocation.target.clone()),
            "invalid frozen mount relocation"
        );
        let slot = layout
            .remove(&relocation.source)
            .context("relocated mount missing from captured device layout")?;
        anyhow::ensure!(
            layout.insert(relocation.target.clone(), slot).is_none(),
            "retained mount path collision"
        );
    }
    Ok(layout)
}

/// Each tuple is (mount path, read-only, external block device). Legacy readonly
/// OverlayBD is enforced by its native backend; external readonly devices also
/// require the immutable readonly virtio slot advertised by the template.
pub fn allocate_layout(
    inherited: &BTreeMap<String, usize>,
    drives: &[(String, bool, bool)],
    slots: usize,
    readonly_slots: usize,
) -> Result<BTreeMap<String, usize>> {
    anyhow::ensure!(
        readonly_slots <= slots && slots <= 24,
        "invalid reserved device slots"
    );
    let writable = slots - readonly_slots;
    let mut paths = HashSet::new();
    for (path, _, _) in drives {
        anyhow::ensure!(paths.insert(path.clone()), "duplicate volume mount path");
    }
    anyhow::ensure!(
        inherited.keys().all(|path| paths.contains(path)),
        "captured device was removed from frozen memory"
    );
    let mut used = HashSet::new();
    let mut layout = inherited.clone();
    for (path, readonly, external) in drives {
        if let Some(slot) = inherited.get(path) {
            anyhow::ensure!(
                *slot < slots && used.insert(*slot),
                "captured volume slot collision"
            );
            anyhow::ensure!(
                !(*external && *readonly && *slot < writable) && !(!readonly && *slot >= writable),
                "captured readonly device constraint changed"
            );
        }
    }
    for (path, readonly, external) in drives {
        if layout.contains_key(path) {
            continue;
        }
        let range = if *readonly && *external {
            writable..slots
        } else {
            0..writable
        };
        let slot = range
            .into_iter()
            .find(|slot| !used.contains(slot))
            .context("template has insufficient free slots for safe volume replacement")?;
        used.insert(slot);
        layout.insert(path.clone(), slot);
    }
    Ok(layout)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replacement_preserves_old_slots_and_allocates_distinct_new_devices() -> Result<()> {
        let old = BTreeMap::from([("/data".into(), 1), ("/readonly".into(), 5)]);
        let hidden = format!("{RETAINED_PREFIX}request/data");
        let relocations = vec![MountRelocation {
            source: "/data".into(),
            target: hidden.clone(),
        }];
        let inherited = relocate_layout(&old, &relocations)?;
        let layout = allocate_layout(
            &inherited,
            &[
                (hidden.clone(), false, true),
                ("/readonly".into(), true, true),
                ("/data".into(), false, true),
            ],
            6,
            1,
        )?;
        assert_eq!(layout[&hidden], 1);
        assert_eq!(layout["/readonly"], 5);
        assert_ne!(layout["/data"], 1);
        assert!(allocate_layout(&inherited, &[("/data".into(), false, true)], 6, 1).is_err());
        assert!(allocate_layout(
            &BTreeMap::from([("/ro".into(), 0)]),
            &[("/ro".into(), true, true)],
            2,
            1
        )
        .is_err());
        assert!(relocate_layout(
            &old,
            &[MountRelocation {
                source: "/data".into(),
                target: "/public".into()
            }]
        )
        .is_err());
        Ok(())
    }
}
