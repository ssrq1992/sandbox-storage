use crate::protocol::ResolveVolumeRequest;
use aenv_volume_core::{VolumeMode, VolumeRecord, VolumeStatus};
use tonic::Status;

pub(crate) fn validate(
    record: &VolumeRecord,
    request: &ResolveVolumeRequest,
) -> Result<(), Status> {
    if request.require_writable && request.require_readonly {
        return Err(Status::invalid_argument(
            "conflicting registration access modes",
        ));
    }
    if request.require_idle {
        if !request.head_revision.is_empty()
            || record.deleting
            || record.status != VolumeStatus::Ready
            || record.reserved_by_sandbox_id.is_some()
            || !record.read_only_mounts.is_empty()
        {
            return Err(Status::failed_precondition(
                "static registration requires an idle current managed volume",
            ));
        }
        if request.require_writable && record.mode != VolumeMode::Exclusive {
            return Err(Status::permission_denied(
                "native readonly volume cannot be registered writable",
            ));
        }
        if request.require_readonly && record.mode != VolumeMode::ReadOnly {
            return Err(Status::permission_denied(
                "writable native volume cannot become a shared readonly registration",
            ));
        }
    } else if request.require_writable || request.require_readonly {
        return Err(Status::invalid_argument(
            "writable registration check requires idle inspection",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record() -> VolumeRecord {
        VolumeRecord {
            id: "volume".into(),
            name: "volume".into(),
            mode: VolumeMode::Exclusive,
            size_mb: 1024,
            status: VolumeStatus::Ready,
            reserved_by_sandbox_id: None,
            backing_image_config: None,
            backing_layers: vec![],
            read_only_mounts: vec![],
            deleting: false,
        }
    }
    #[test]
    fn registration_requires_idle_current_and_exact_access() {
        let mut q = ResolveVolumeRequest {
            require_idle: true,
            require_writable: true,
            ..Default::default()
        };
        assert!(validate(&record(), &q).is_ok());
        for n in 0..5 {
            let mut r = record();
            match n {
                0 => r.deleting = true,
                1 => r.status = VolumeStatus::Uploading,
                2 => r.reserved_by_sandbox_id = Some("sandbox".into()),
                3 => r.read_only_mounts.push("reader".into()),
                _ => r.mode = VolumeMode::ReadOnly,
            }
            assert!(validate(&r, &q).is_err());
        }
        q.head_revision = "previous-head".into();
        assert!(validate(&record(), &q).is_err());
        q.head_revision.clear();
        q.require_idle = false;
        assert!(validate(&record(), &q).is_err());
        q.require_idle = true;
        q.require_readonly = true;
        assert!(validate(&record(), &q).is_err());
        q.require_writable = false;
        assert!(validate(&record(), &q).is_err());
        let mut r = record();
        r.mode = VolumeMode::ReadOnly;
        assert!(validate(&r, &q).is_ok());
    }
    #[test]
    fn normal_resolve_keeps_existing_semantics() {
        let mut r = record();
        r.reserved_by_sandbox_id = Some("running".into());
        assert!(validate(&r, &ResolveVolumeRequest::default()).is_ok());
    }
}
