//! Maps authenticated host requests to the native managed daemon protocol.
use crate::{open_operation, protocol::*, volume_host::VolumeHost};
use aenv_volume_core::{
    is_valid_volume_component,
    journal::{LockedOperation, NativeOperationState},
};
use prost::Message;
use std::{path::PathBuf, sync::Arc};
use tonic::Status;
use uvm_ublk_daemon::{
    client::ManagedDaemonClient,
    protocol::{DaemonResponse, ManagedDeviceAction, ManagedDevicePlan, ManagedDeviceView},
};

pub struct DeviceHost {
    pub node_id: String,
    pub journal_root: PathBuf,
    pub work_root: PathBuf,
    pub global_config: PathBuf,
    pub daemon: ManagedDaemonClient,
    pub volumes: Arc<VolumeHost>,
}
impl DeviceHost {
    pub async fn expand(&self, request: ExpandVolumeRequest) -> Result<VolumeReceipt, Status> {
        let identity = request
            .operation
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("maintenance operation required"))?;
        if !digest_valid(&request.maintenance_authorization_digest)
            || !digest_valid(&request.expected_head_revision)
            || request.capacity_bytes == 0
            || request.capacity_bytes % (1 << 30) != 0
        {
            return Err(Status::permission_denied(
                "maintenance authority and growing GiB capacity required",
            ));
        }
        let mut operation =
            open_operation(&self.journal_root, "offline-expansion-v1", Some(identity))?;
        operation
            .checkpoint("request", serde_json::to_value(&request).map_err(error)?)
            .map_err(error)?;
        if operation.record().state == NativeOperationState::Completed {
            return serde_json::from_value(
                operation
                    .record()
                    .receipt
                    .clone()
                    .ok_or_else(|| Status::data_loss("maintenance receipt missing"))?,
            )
            .map_err(error);
        }
        let owner = format!("maintenance_{}", identity.operation_id);
        let root = self
            .work_root
            .join("maintenance")
            .join(&identity.operation_id);
        let source: aenv_volume_core::heads::PublishedHead =
            if let Some(value) = operation.record().checkpoints.get("source") {
                serde_json::from_value(value.clone()).map_err(error)?
            } else {
                let source = self
                    .volumes
                    .expansion_source(
                        identity,
                        &request.expected_head_revision,
                        &owner,
                        request.capacity_bytes,
                        &root.join("source.json"),
                    )
                    .await?;
                operation
                    .checkpoint("source", serde_json::to_value(&source).map_err(error)?)
                    .map_err(error)?;
                source
            };
        if operation.record().state == NativeOperationState::Intent {
            operation.begin().map_err(error)?;
        }
        let receipt: VolumeReceipt =
            if let Some(value) = operation.record().checkpoints.get("published") {
                serde_json::from_value(value.clone()).map_err(error)?
            } else {
                let view: uvm_ublk_daemon::protocol::ManagedExpansionView =
                    if let Some(value) = operation.record().checkpoints.get("sealed") {
                        serde_json::from_value(value.clone()).map_err(error)?
                    } else {
                        let plan = uvm_ublk_daemon::protocol::ManagedExpansionPlan {
                            operation_id: identity.operation_id.clone(),
                            repository_id: identity.repository_id.clone(),
                            volume_id: identity.volume_id.clone(),
                            authorization_digest: request.maintenance_authorization_digest.clone(),
                            expected_head: source.revision.clone(),
                            source_capacity: source.capacity_bytes,
                            target_capacity: request.capacity_bytes,
                            filesystem_uuid: source.filesystem_uuid.clone(),
                            source_image_config: root.join("source.json"),
                            runtime_dir: root.join("runtime"),
                            global_config: self.global_config.clone(),
                        };
                        let view = self.daemon.expand(plan).await.map_err(error)?;
                        if view.operation_id != identity.operation_id
                            || view.capacity_bytes != request.capacity_bytes
                            || view.filesystem_uuid != source.filesystem_uuid
                            || !view.device_released
                            || view.device_identity.is_empty()
                            || view.image_config_path != root.join("runtime/expanded.json")
                        {
                            return Err(Status::data_loss(
                                "maintenance device receipt identity mismatch",
                            ));
                        }
                        operation
                            .checkpoint("sealed", serde_json::to_value(&view).map_err(error)?)
                            .map_err(error)?;
                        view
                    };
                let head = if let Some(value) = operation.record().checkpoints.get("head") {
                    serde_json::from_value(value.clone()).map_err(error)?
                } else {
                    let head = self
                        .volumes
                        .expanded_head(
                            identity,
                            &source,
                            request.capacity_bytes,
                            &view.image_config_path,
                        )
                        .await?;
                    operation
                        .checkpoint("head", serde_json::to_value(&head).map_err(error)?)
                        .map_err(error)?;
                    head
                };
                let receipt = self
                    .volumes
                    .commit_expanded_head(identity, &owner, &source, &head)
                    .await?;
                operation
                    .checkpoint("published", serde_json::to_value(&receipt).map_err(error)?)
                    .map_err(error)?;
                receipt
            };
        self.volumes
            .release_run(&identity.volume_id, &owner)
            .await?;
        operation
            .complete(serde_json::to_value(&receipt).map_err(error)?)
            .map_err(error)?;
        Ok(receipt)
    }
    fn operation(&self, identity: Option<&OperationIdentity>) -> Result<LockedOperation, Status> {
        open_operation(&self.journal_root, "device-session", identity)
    }
    fn original(
        &self,
        record: &LockedOperation,
        identity: Option<&OperationIdentity>,
        session: &str,
    ) -> Result<(PrepareDeviceRequest, ManagedDevicePlan, String), Status> {
        let request: PrepareDeviceRequest = serde_json::from_value(
            record
                .record()
                .checkpoints
                .get("request")
                .cloned()
                .ok_or_else(|| {
                    Status::failed_precondition("device session has not been prepared")
                })?,
        )
        .map_err(error)?;
        if request.operation.as_ref() != identity || request.session_id != session {
            return Err(Status::permission_denied(
                "device session identity mismatch",
            ));
        }
        let plan = serde_json::from_value(
            record
                .record()
                .checkpoints
                .get("daemon_plan")
                .cloned()
                .ok_or_else(|| Status::data_loss("daemon plan missing"))?,
        )
        .map_err(error)?;
        let uuid = record
            .record()
            .checkpoints
            .get("filesystem_uuid")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Status::data_loss("device filesystem identity missing"))?
            .to_owned();
        Ok((request, plan, uuid))
    }
    pub async fn prepare(&self, request: PrepareDeviceRequest) -> Result<DeviceReceipt, Status> {
        let identity = request
            .operation
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("operation required"))?;
        let grant = request
            .grant
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("use grant required"))?;
        if !matches!(grant.purpose.as_str(), "runtime" | "preparation")
            || grant.node_id != self.node_id
            || grant.epoch == 0
            || grant.owner_id.is_empty()
            || grant.run_id.is_empty()
            || !is_valid_volume_component(&request.session_id)
            || !digest_valid(&grant.authorization_digest)
        {
            return Err(Status::permission_denied("invalid native use scope"));
        }
        let mut unsigned = request.clone();
        unsigned.operation.as_mut().unwrap().input_digest.clear();
        let digest = aenv_volume_core::digest::sha256_digest(&unsigned.encode_to_vec());
        if digest.trim_start_matches("sha256:") != identity.input_digest {
            return Err(Status::invalid_argument("device request digest mismatch"));
        }
        if !self.work_root.is_absolute() || !self.global_config.is_absolute() {
            return Err(Status::failed_precondition(
                "configured native device paths must be absolute",
            ));
        }
        let mut operation = self.operation(Some(identity))?;
        if operation.record().state == NativeOperationState::Completed
            || operation
                .record()
                .checkpoints
                .contains_key("release-request")
        {
            return Err(Status::failed_precondition(
                "device session is releasing or released",
            ));
        }
        operation
            .checkpoint("request", serde_json::to_value(&request).map_err(error)?)
            .map_err(error)?;
        if operation.record().state == NativeOperationState::Intent {
            let materialized = self
                .work_root
                .join("sources")
                .join(&request.session_id)
                .join("image.json");
            let head = self
                .volumes
                .materialize_head(&identity.volume_id, &grant.head_revision, &materialized)
                .await?;
            operation
                .checkpoint("source_head", serde_json::to_value(&head).map_err(error)?)
                .map_err(error)?;
            let plan = ManagedDevicePlan {
                purpose: grant.purpose.clone(),
                session_id: request.session_id.clone(),
                operation_id: identity.operation_id.clone(),
                repository_id: identity.repository_id.clone(),
                volume_id: identity.volume_id.clone(),
                owner_id: grant.owner_id.clone(),
                run_id: grant.run_id.clone(),
                epoch: grant.epoch,
                node_id: grant.node_id.clone(),
                head_revision: grant.head_revision.clone(),
                source_image_config: materialized,
                global_config: self.global_config.clone(),
                runtime_dir: self.work_root.join("runtime").join(&request.session_id),
                read_only: grant.readonly,
                virtual_size: head.capacity_bytes,
            };
            operation
                .checkpoint("daemon_plan", serde_json::to_value(&plan).map_err(error)?)
                .map_err(error)?;
            operation
                .checkpoint("filesystem_uuid", serde_json::json!(head.filesystem_uuid))
                .map_err(error)?;
            self.volumes
                .reserve_run(&identity.volume_id, &grant.run_id, grant.readonly)
                .await?;
            operation.begin().map_err(error)?;
        }
        let (_, plan, uuid) = self.original(&operation, Some(identity), &request.session_id)?;
        // Native daemon serializes this immutable plan, including retries whose
        // earlier transport result was unknown. It never creates twice.
        let response = self
            .daemon
            .execute(plan, ManagedDeviceAction::Prepare)
            .await
            .map_err(error)?;
        let DaemonResponse::ManagedDevice { view } = response else {
            return Err(Status::data_loss("unexpected prepare receipt"));
        };
        let receipt = receipt(&request, &uuid, view)?;
        operation
            .checkpoint("prepared", serde_json::to_value(&receipt).map_err(error)?)
            .map_err(error)?;
        Ok(receipt)
    }
    pub async fn inspect(&self, request: InspectDeviceRequest) -> Result<DeviceReceipt, Status> {
        let operation = self.operation(request.operation.as_ref())?;
        if operation.record().state == NativeOperationState::Intent {
            // No daemon side effect may precede begin(). A persisted partial
            // plan can therefore resume only through the same Prepare request.
            return Ok(DeviceReceipt {
                operation: request.operation,
                session_id: request.session_id,
                state: "Pending".into(),
                ..Default::default()
            });
        }
        let (original, plan, uuid) =
            self.original(&operation, request.operation.as_ref(), &request.session_id)?;
        if operation.record().state == NativeOperationState::Completed {
            return Ok(DeviceReceipt {
                operation: request.operation,
                session_id: request.session_id,
                grant: original.grant,
                state: "Released".into(),
                ..Default::default()
            });
        }
        let DaemonResponse::ManagedDevice { view } = self
            .daemon
            .execute(plan, ManagedDeviceAction::Inspect)
            .await
            .map_err(error)?
        else {
            return Err(Status::data_loss("unexpected inspect receipt"));
        };
        receipt(&original, &uuid, view)
    }
    pub async fn capture(&self, request: CaptureDeviceRequest) -> Result<CaptureReceipt, Status> {
        let mut operation = self.operation(request.operation.as_ref())?;
        let (original, plan, _) =
            self.original(&operation, request.operation.as_ref(), &request.session_id)?;
        if request.grant != original.grant
            || original
                .grant
                .as_ref()
                .is_none_or(|g| g.purpose != "runtime")
            || operation.record().state == NativeOperationState::Completed
            || operation
                .record()
                .checkpoints
                .contains_key("release-request")
        {
            return Err(Status::permission_denied(
                "capture grant is stale or session released",
            ));
        }
        if !is_valid_volume_component(&request.capture_id) || request.capture_id.len() > 96 {
            return Err(Status::invalid_argument("invalid capture identity"));
        }
        if !request.stopped_evidence_digest.is_empty()
            && !digest_valid(&request.stopped_evidence_digest)
        {
            return Err(Status::invalid_argument(
                "invalid final stopped evidence digest",
            ));
        }
        let key = format!("capture-{}", request.capture_id);
        operation
            .checkpoint(
                &format!("capture-request-{}", request.capture_id),
                serde_json::to_value(&request).map_err(error)?,
            )
            .map_err(error)?;
        let response = self
            .daemon
            .execute(
                plan,
                ManagedDeviceAction::Capture {
                    capture_id: request.capture_id.clone(),
                    barrier_id: request.barrier_id.clone(),
                    freeze_evidence_digest: request.freeze_evidence_digest.clone(),
                    stopped_evidence_digest: request.stopped_evidence_digest.clone(),
                },
            )
            .await
            .map_err(error)?;
        let DaemonResponse::ManagedCapture { capture } = response else {
            return Err(Status::data_loss("unexpected capture receipt"));
        };
        if capture.capture_id != request.capture_id
            || capture.barrier_id != request.barrier_id
            || capture.session_id != request.session_id
        {
            return Err(Status::data_loss("capture evidence mismatch"));
        }
        operation
            .checkpoint(&key, serde_json::to_value(&capture).map_err(error)?)
            .map_err(error)?;
        Ok(CaptureReceipt {
            operation: request.operation,
            session_id: request.session_id,
            capture_id: request.capture_id,
            barrier_id: request.barrier_id,
            state: "Captured".into(),
            sealed_revision: capture
                .content_digest
                .trim_start_matches("sha256:")
                .to_owned(),
            content_digest: capture
                .content_digest
                .trim_start_matches("sha256:")
                .to_owned(),
            reason: String::new(),
        })
    }
    pub async fn publish(&self, request: PublishHeadRequest) -> Result<VolumeReceipt, Status> {
        let mut operation = self.operation(request.operation.as_ref())?;
        let original: PrepareDeviceRequest = serde_json::from_value(
            operation
                .record()
                .checkpoints
                .get("request")
                .cloned()
                .ok_or_else(|| Status::failed_precondition("session not prepared"))?,
        )
        .map_err(error)?;
        let (original, _, _) =
            self.original(&operation, request.operation.as_ref(), &original.session_id)?;
        if original.grant != request.grant
            || original
                .grant
                .as_ref()
                .is_none_or(|g| g.readonly || g.purpose != "runtime")
            || operation.record().state == NativeOperationState::Completed
            || operation
                .record()
                .checkpoints
                .contains_key("release-request")
        {
            return Err(Status::permission_denied("publication grant mismatch"));
        }
        if !is_valid_volume_component(&request.capture_id) || request.capture_id.len() > 96 {
            return Err(Status::invalid_argument(
                "invalid publication capture identity",
            ));
        }
        let key = format!("publication-{}", request.capture_id);
        operation
            .checkpoint(
                &format!("publication-input-{}", request.capture_id),
                serde_json::to_value(&request).map_err(error)?,
            )
            .map_err(error)?;
        if let Some(value) = operation.record().checkpoints.get(&key) {
            return serde_json::from_value(value.clone()).map_err(error);
        }
        let capture: uvm_ublk_daemon::protocol::ManagedCaptureView = serde_json::from_value(
            operation
                .record()
                .checkpoints
                .get(&format!("capture-{}", request.capture_id))
                .cloned()
                .ok_or_else(|| Status::failed_precondition("native capture not pinned"))?,
        )
        .map_err(error)?;
        let identity = request
            .operation
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("operation missing"))?;
        let head_key = format!("publication-head-{}", request.capture_id);
        let head = if let Some(value) = operation.record().checkpoints.get(&head_key) {
            serde_json::from_value(value.clone()).map_err(error)?
        } else {
            let head = self
                .volumes
                .capture_head(
                    identity,
                    &request.expected_head_revision,
                    &capture.image_config_path,
                )
                .await?;
            operation
                .checkpoint(&head_key, serde_json::to_value(&head).map_err(error)?)
                .map_err(error)?;
            head
        };
        let receipt = self
            .volumes
            .commit_capture(identity, &original.grant.as_ref().unwrap().run_id, &head)
            .await?;
        operation
            .checkpoint(&key, serde_json::to_value(&receipt).map_err(error)?)
            .map_err(error)?;
        Ok(receipt)
    }
    pub async fn release(&self, request: ReleaseDeviceRequest) -> Result<OperationReceipt, Status> {
        let mut operation = self.operation(request.operation.as_ref())?;
        let (original, plan, _) =
            self.original(&operation, request.operation.as_ref(), &request.session_id)?;
        if request.grant != original.grant {
            return Err(Status::permission_denied("release grant mismatch"));
        }
        if operation
            .record()
            .checkpoints
            .get("release-request")
            .is_some_and(|value| *value != serde_json::to_value(&request).unwrap_or_default())
        {
            return Err(Status::permission_denied(
                "release request changed during recovery",
            ));
        }
        if operation.record().state == NativeOperationState::Completed {
            return Ok(OperationReceipt {
                operation: request.operation,
                state: "Released".into(),
                reason: String::new(),
            });
        }
        if original
            .grant
            .as_ref()
            .is_some_and(|grant| grant.purpose == "runtime" && !grant.readonly)
        {
            let capture: CaptureDeviceRequest = serde_json::from_value(
                operation
                    .record()
                    .checkpoints
                    .get(&format!("capture-request-{}", request.final_capture_id))
                    .cloned()
                    .ok_or_else(|| {
                        Status::failed_precondition("final stopped capture required before release")
                    })?,
            )
            .map_err(error)?;
            let published: VolumeReceipt = serde_json::from_value(
                operation
                    .record()
                    .checkpoints
                    .get(&format!("publication-{}", request.final_capture_id))
                    .cloned()
                    .ok_or_else(|| {
                        Status::failed_precondition("final capture publication incomplete")
                    })?,
            )
            .map_err(error)?;
            if capture.stopped_evidence_digest.is_empty()
                || capture.stopped_evidence_digest != request.stopped_evidence_digest
                || published.head_revision != request.published_head_revision
            {
                return Err(Status::permission_denied(
                    "release is not tied to final stopped publication",
                ));
            }
        }
        operation
            .checkpoint(
                "release-request",
                serde_json::to_value(&request).map_err(error)?,
            )
            .map_err(error)?;
        let response = self
            .daemon
            .execute(
                plan,
                ManagedDeviceAction::Release {
                    preparation_release_digest: request.preparation_release_digest,
                    stopped_evidence_digest: request.stopped_evidence_digest,
                    borrowed_refs_released_digest: request.borrowed_refs_released_digest,
                },
            )
            .await
            .map_err(error)?;
        let DaemonResponse::ManagedDevice { view } = response else {
            return Err(Status::data_loss("unexpected release receipt"));
        };
        if view.session_id != request.session_id || view.state != "Released" {
            return Err(Status::failed_precondition(
                "native device release is not proven",
            ));
        }
        let result = OperationReceipt {
            operation: request.operation,
            state: "Released".into(),
            reason: String::new(),
        };
        self.volumes
            .release_run(
                &original.operation.as_ref().unwrap().volume_id,
                &original.grant.as_ref().unwrap().run_id,
            )
            .await?;
        operation
            .complete(serde_json::to_value(&result).map_err(error)?)
            .map_err(error)?;
        Ok(result)
    }
}
fn error(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(error.to_string())
}
fn digest_valid(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}
fn receipt(
    request: &PrepareDeviceRequest,
    uuid: &str,
    view: ManagedDeviceView,
) -> Result<DeviceReceipt, Status> {
    if view.session_id != request.session_id {
        return Err(Status::data_loss("native daemon session mismatch"));
    }
    Ok(DeviceReceipt {
        operation: request.operation.clone(),
        session_id: request.session_id.clone(),
        grant: request.grant.clone(),
        state: view.state,
        device_path: view
            .device_path
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default(),
        major: u32::try_from(view.major).map_err(error)?,
        minor: u32::try_from(view.minor).map_err(error)?,
        capacity_bytes: view.actual_virtual_size,
        fs_uuid: uuid.into(),
        node_boot_id: view.node_boot_id,
        daemon_instance_id: view.daemon_instance_id,
        device_identity: view.device_identity,
        reason: String::new(),
    })
}
