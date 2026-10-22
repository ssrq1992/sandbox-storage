use crate::protocol::EnsureVolumeRequest;
use std::collections::BTreeMap;
use tonic::Status;
fn storage_error(error: impl std::fmt::Display) -> Status {
    Status::invalid_argument(error.to_string())
}
// Go's canonical CreateIntent uses lexicographically sorted keys and integer
// JSON numbers. Use BTreeMap explicitly even with serde_json/preserve_order.
pub(crate) fn input_digest(request: &EnsureVolumeRequest) -> String {
    let mut input = BTreeMap::new();
    input.insert(
        "repository_id",
        serde_json::json!(request
            .operation
            .as_ref()
            .map(|o| o.repository_id.as_str())
            .unwrap_or_default()),
    );
    input.insert("capacity_bytes", serde_json::json!(request.capacity_bytes));
    input.insert("readonly", serde_json::json!(request.readonly));
    input.insert(
        "profile_revision",
        serde_json::json!(request.profile_revision),
    );
    if let Some(source) = &request.source {
        let mut value = BTreeMap::new();
        if !source.volume_id.is_empty() {
            value.insert("volume_id", &source.volume_id);
        }
        if !source.head_revision.is_empty() {
            value.insert("head_revision", &source.head_revision);
        }
        if !source.snapshot_id.is_empty() {
            value.insert("snapshot_id", &source.snapshot_id);
        }
        input.insert(
            "source",
            serde_json::to_value(value).expect("string map serializes"),
        );
    }
    aenv_volume_core::digest::sha256_digest(&serde_json::to_vec(&input).expect("intent serializes"))
        .trim_start_matches("sha256:")
        .to_owned()
}
pub(crate) fn volume_filesystem_identity(
    identity: &crate::protocol::OperationIdentity,
) -> Result<String, Status> {
    let bytes = serde_json::to_vec(&(
        identity.repository_id.as_str(),
        identity.volume_id.as_str(),
        identity.operation_id.as_str(),
        identity.input_digest.as_str(),
    ))
    .map_err(storage_error)?;
    let digest = aenv_volume_core::digest::sha256_digest(&bytes);
    filesystem_identity(digest.trim_start_matches("sha256:"))
}

pub(crate) fn filesystem_identity(digest: &str) -> Result<String, Status> {
    if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(Status::invalid_argument("SHA-256 digest required"));
    }
    let raw = &digest[..32];
    // Stable per immutable operation. Set RFC 4122 version/variant bits.
    let mut bytes = [0u8; 16];
    for (i, out) in bytes.iter_mut().enumerate() {
        *out = u8::from_str_radix(&raw[i * 2..i * 2 + 2], 16).map_err(storage_error)?;
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(uuid::Uuid::from_bytes(bytes).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{OperationIdentity, VolumeSource};
    #[test]
    fn matches_go_canonical_intent_without_integer_rounding() {
        let fixtures: serde_json::Value =
            serde_json::from_str(include_str!("../../testdata/create-intent.json")).unwrap();
        for fixture in fixtures.as_array().unwrap() {
            let input = &fixture["input"];
            let request = EnsureVolumeRequest {
                operation: Some(OperationIdentity {
                    repository_id: input["repository_id"].as_str().unwrap().into(),
                    ..Default::default()
                }),
                capacity_bytes: input["capacity_bytes"].as_u64().unwrap(),
                readonly: input["readonly"].as_bool().unwrap(),
                profile_revision: input["profile_revision"].as_str().unwrap().into(),
                source: input.get("source").map(|s| VolumeSource {
                    volume_id: s["volume_id"].as_str().unwrap().into(),
                    head_revision: s["head_revision"].as_str().unwrap().into(),
                    snapshot_id: s["snapshot_id"].as_str().unwrap().into(),
                }),
            };
            let digest = input_digest(&request);
            assert_eq!(digest, fixture["digest"].as_str().unwrap());
            let uuid = filesystem_identity(&digest).unwrap();
            assert_eq!(uuid, filesystem_identity(&digest).unwrap());
            assert!(!uuid::Uuid::parse_str(&uuid).unwrap().is_nil());
        }
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;
    #[test]
    fn equal_capacity_volumes_have_distinct_stable_filesystems() {
        let mut a = crate::protocol::OperationIdentity {
            repository_id: "repo".into(),
            volume_id: "volume_a".into(),
            operation_id: "create_a".into(),
            input_digest: "a".repeat(64),
        };
        let first = volume_filesystem_identity(&a).unwrap();
        assert_eq!(first, volume_filesystem_identity(&a).unwrap());
        a.volume_id = "volume_b".into();
        assert_ne!(first, volume_filesystem_identity(&a).unwrap());
    }
}
