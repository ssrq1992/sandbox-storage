//! Native host contract. The service implementation must use the original
//! AgentENV storage crates; this crate never imports the AgentENV server.
pub mod protocol {
    tonic::include_proto!("ssf.overlaybd.v1");
}

use aenv_volume_core::journal::{LockedOperation, NativeIntent};
use protocol::OperationIdentity;
use std::path::Path;

pub const PROTOCOL: &str = "ssf.overlaybd.backend.v1";

/// Pin the Go controller's operation identity before calling any native storage
/// side effect. Inspection reopens this record rather than allocating a new ID.
pub fn open_operation(
    root: &Path,
    kind: &str,
    operation: Option<&OperationIdentity>,
) -> Result<LockedOperation, tonic::Status> {
    let operation =
        operation.ok_or_else(|| tonic::Status::invalid_argument("operation required"))?;
    LockedOperation::open(
        root,
        NativeIntent {
            operation_id: operation.operation_id.clone(),
            input_digest: operation.input_digest.clone(),
            volume_id: operation.volume_id.clone(),
            repository_id: operation.repository_id.clone(),
            kind: kind.to_owned(),
        },
    )
    .map_err(|error| tonic::Status::failed_precondition(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn protocol_roundtrip_keeps_operation_and_epoch() {
        let operation = OperationIdentity {
            operation_id: "op_a".into(),
            input_digest: "a".repeat(64),
            repository_id: "repo_a".into(),
            volume_id: "vol_a".into(),
        };
        let request = protocol::PrepareDeviceRequest {
            operation: Some(operation),
            session_id: "session_a".into(),
            grant: Some(protocol::UseGrant {
                epoch: 9,
                ..Default::default()
            }),
        };
        let decoded =
            protocol::PrepareDeviceRequest::decode(request.encode_to_vec().as_slice()).unwrap();
        assert_eq!(request, decoded);
        let dir = tempfile::TempDir::new().unwrap();
        let mut record = open_operation(dir.path(), "prepare", decoded.operation.as_ref()).unwrap();
        record.begin().unwrap();
        drop(record);
        assert!(
            open_operation(dir.path(), "prepare", decoded.operation.as_ref())
                .unwrap()
                .begin()
                .is_err()
        );
    }
}

#[cfg(feature = "volume-host")]
pub mod volume_host;

#[cfg(feature = "metadata")]
mod intent;

#[cfg(feature = "device-host")]
pub mod device_host;

#[cfg(feature = "device-host")]
pub mod service;

mod static_registration;
