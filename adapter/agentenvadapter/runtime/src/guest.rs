//! Guest mount policy. Commands are executed only by the native guest bridge.
//! Paths and UUIDs remain argv values and never become shell source.
pub struct GuestCommand {
    pub program: String,
    pub args: Vec<String>,
}
pub fn retain(source: &str, target: &str, replacing: bool) -> GuestCommand {
    let script = r#"set -eu
BB=/agentenv/bin/busybox
source="$1"; hidden="$2"; replacing="$3"
"$BB" mkdir -p "$hidden"
test "$("$BB" readlink -f "$source")" = "$source"
test "$("$BB" readlink -f "$hidden")" = "$hidden"
"$BB" mountpoint -q "$source"
if ! "$BB" mountpoint -q "$hidden"; then "$BB" mount --bind "$source" "$hidden"; fi
test "$("$BB" stat -Lc %d "$source")" = "$("$BB" stat -Lc %d "$hidden")"
# Keep an old open filesystem on its original device. A replacement overlays
# its existing mount atomically; omission exposes the original rootfs directory.
if [ "$replacing" = 0 ]; then "$BB" umount -l "$source"; fi
"#;
    let args = vec![
        "sh".into(),
        "-c".into(),
        script.into(),
        "--".into(),
        source.to_owned(),
        target.to_owned(),
        if replacing { "1" } else { "0" }.into(),
    ];
    GuestCommand {
        program: "/agentenv/bin/busybox".into(),
        args,
    }
}

pub fn mount(d: &super::devices::ExternalBlockDevice, device: &str, replace: bool) -> GuestCommand {
    let script = r#"set -eu
BB=/agentenv/bin/busybox
dev="$1"; target="$2"; uuid="$3"; sectors="$4"; readonly="$5"; replace="$6"
test "$("$BB" cat "/sys/class/block/$dev/size")" = "$sectors"
"$BB" blkid "/dev/$dev" | "$BB" grep -F "UUID=\"$uuid\"" >/dev/null
if [ "$readonly" = 1 ]; then
    test "$("$BB" cat "/sys/class/block/$dev/ro")" = 1
    opts=ro
else
    opts=rw
fi
"$BB" mkdir -p "$target"
if [ "$replace" = 1 ] || ! "$BB" mountpoint -q "$target"; then
    "$BB" mount -t ext4 -o "$opts" "/dev/$dev" "$target"
fi
"$BB" awk -v t="$target" -v d="/dev/$dev" -v ro="$readonly" '
 $2==t { seen=1; mdev=$1; mfs=$3; ok=0; n=split($4,a,","); for(i=1;i<=n;i++) if(a[i]==(ro==1?"ro":"rw")) ok=1 }
 END { if (!seen) exit 3; if (mdev!=d || mfs!="ext4") exit 2; if (!ok) exit 3 }' /proc/mounts
"#;
    let args = vec![
        "sh".into(),
        "-c".into(),
        script.into(),
        "--".into(),
        device.to_owned(),
        d.guest_path.to_string_lossy().into_owned(),
        d.uuid.clone(),
        (d.bytes / 512).to_string(),
        if d.readonly { "1" } else { "0" }.into(),
        if replace { "1" } else { "0" }.into(),
    ];
    GuestCommand {
        program: "/agentenv/bin/busybox".into(),
        args,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn guest_paths_remain_argv_and_overlay_check_uses_last_mount() {
        let device = crate::devices::ExternalBlockDevice {
            id: "ext_data".into(),
            path: "/provider/block".into(),
            guest_path: "/data/$(touch injected)".into(),
            readonly: true,
            major: 1,
            minor: 1,
            bytes: 4096,
            uuid: "11111111-2222-3333-4444-555555555555".into(),
        };
        let command = mount(&device, "vdc", true);
        assert!(!command.args[2].contains("touch injected"));
        assert_eq!(command.args[5], "/data/$(touch injected)");
        assert!(command.args[2].contains("mdev=$1; mfs=$3"));
        assert_eq!(command.args[8], "1");
        assert_eq!(command.args[9], "1");
    }
}
