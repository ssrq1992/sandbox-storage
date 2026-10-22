//! Native volume operations using the original AgentENV engine and repository.
use crate::intent::{input_digest, volume_filesystem_identity};
use crate::{open_operation, protocol::*};
use aenv_volume_core::{
    engine::{VolumeEngine, VolumeLimits, VolumeRepository},
    head_repository::HeadRepository,
    heads::PublishedHead,
    journal::{LockedOperation, NativeIntent, NativeOperationState},
    VolumeMode, VolumeRecord, VolumeStatus, BYTES_PER_MB,
};
use std::{path::PathBuf, sync::Arc};
use tonic::Status;

pub struct VolumeHost {
    repository_id: String,
    journal_root: PathBuf,
    engine: VolumeEngine,
    heads: Arc<dyn HeadRepository>,
    accessible_nodes: Vec<String>,
}
impl VolumeHost {
    pub(crate) async fn expansion_source(
        &self,
        identity: &OperationIdentity,
        revision: &str,
        owner: &str,
        capacity: u64,
        destination: &std::path::Path,
    ) -> Result<PublishedHead, Status> {
        self.identity(Some(identity))?;
        if capacity % BYTES_PER_MB != 0 || capacity / BYTES_PER_MB > self.engine.max_size_mb() {
            return Err(Status::out_of_range(
                "maintenance capacity exceeds native volume limit",
            ));
        }
        let source = self
            .heads
            .current(&identity.volume_id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| Status::not_found("maintenance source head missing"))?;
        let record = self
            .engine
            .get(&identity.volume_id)
            .await
            .map_err(storage_error)?;
        if source.revision != revision
            || source.capacity_bytes >= capacity
            || record.mode != VolumeMode::Exclusive
            || record.status != VolumeStatus::Ready
            || record.deleting
            || record.size_mb.checked_mul(BYTES_PER_MB) != Some(source.capacity_bytes)
            || record.backing_layers != source.layers
            || source.capacity_generation == u64::MAX
        {
            return Err(Status::failed_precondition(
                "maintenance source identity or capacity changed",
            ));
        }
        self.reserve_run(&identity.volume_id, owner, false).await?;
        self.materialize_head(&identity.volume_id, revision, destination)
            .await
    }

    pub(crate) async fn expanded_head(
        &self,
        identity: &OperationIdentity,
        source: &PublishedHead,
        capacity: u64,
        config: &std::path::Path,
    ) -> Result<PublishedHead, Status> {
        self.identity(Some(identity))?;
        let layers = self
            .engine
            .publish_captured_backing(&identity.volume_id, config)
            .await
            .map_err(storage_error)?;
        PublishedHead {
            version: 1,
            volume_id: identity.volume_id.clone(),
            revision: String::new(),
            parent: Some(source.revision.clone()),
            capacity_bytes: capacity,
            capacity_generation: source
                .capacity_generation
                .checked_add(1)
                .ok_or_else(|| Status::out_of_range("capacity generation exhausted"))?,
            filesystem_uuid: source.filesystem_uuid.clone(),
            layers,
            operation_id: identity.operation_id.clone(),
            input_digest: identity.input_digest.clone(),
        }
        .seal()
        .map_err(storage_error)
    }

    pub(crate) async fn commit_expanded_head(
        &self,
        identity: &OperationIdentity,
        owner: &str,
        source: &PublishedHead,
        head: &PublishedHead,
    ) -> Result<VolumeReceipt, Status> {
        self.identity(Some(identity))?;
        if head.volume_id != identity.volume_id
            || head.operation_id != identity.operation_id
            || head.input_digest != identity.input_digest
            || head.capacity_bytes % BYTES_PER_MB != 0
        {
            return Err(Status::permission_denied(
                "maintenance publication identity mismatch",
            ));
        }
        head.validate_transition(Some(source), Some(&source.revision))
            .map_err(storage_error)?;
        self.heads
            .publish(Some(&source.revision), head)
            .await
            .map_err(storage_error)?;
        self.engine
            .commit_expansion(
                &identity.volume_id,
                owner,
                source.capacity_bytes / BYTES_PER_MB,
                &source.layers,
                head.capacity_bytes / BYTES_PER_MB,
                &head.layers,
            )
            .await
            .map_err(storage_error)?;
        Ok(VolumeReceipt {
            operation: Some(identity.clone()),
            state: "ContentReady".into(),
            head_revision: head.revision.clone(),
            capacity_bytes: head.capacity_bytes,
            capacity_generation: head.capacity_generation,
            fs_uuid: head.filesystem_uuid.clone(),
            content_digest: head.revision.clone(),
            accessible_nodes: self.accessible_nodes.clone(),
            reason: String::new(),
        })
    }
    pub fn new(
        repository_id: String,
        journal_root: PathBuf,
        work_root: PathBuf,
        heads: Arc<dyn HeadRepository>,
        repository: Arc<dyn VolumeRepository>,
        limits: VolumeLimits,
        accessible_nodes: Vec<String>,
    ) -> Result<Self, Status> {
        if !aenv_volume_core::is_valid_volume_component(&repository_id)
            || !journal_root.is_absolute()
            || !work_root.is_absolute()
        {
            return Err(Status::invalid_argument(
                "configured repository and absolute host state paths required",
            ));
        }
        Ok(Self {
            repository_id,
            journal_root,
            engine: VolumeEngine::new(work_root, repository, limits),
            heads,
            accessible_nodes,
        })
    }
    pub(crate) async fn materialize_head(
        &self,
        volume: &str,
        revision: &str,
        destination: &std::path::Path,
    ) -> Result<PublishedHead, Status> {
        let head = self
            .heads
            .read(volume, revision)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| Status::not_found("immutable native head missing"))?;
        self.engine
            .materialize_captured_backing(volume, &head.layers, destination)
            .await
            .map_err(storage_error)?;
        Ok(head)
    }
    pub(crate) async fn reserve_run(
        &self,
        id: &str,
        run: &str,
        readonly: bool,
    ) -> Result<(), Status> {
        let record = self.engine.get(id).await.map_err(storage_error)?;
        if record.mode == VolumeMode::ReadOnly && !readonly {
            return Err(Status::permission_denied(
                "readonly native volume cannot admit a writer",
            ));
        }
        self.engine.reserve(id, run).await.map_err(storage_error)
    }
    pub(crate) async fn release_run(&self, id: &str, run: &str) -> Result<(), Status> {
        self.engine
            .replace_owner_for(run, None, &[id.to_owned()])
            .await
            .map_err(storage_error)
    }
    pub(crate) async fn capture_head(
        &self,
        identity: &OperationIdentity,
        expected: &str,
        config: &std::path::Path,
    ) -> Result<PublishedHead, Status> {
        self.identity(Some(identity))?;
        let prior = self
            .heads
            .read(&identity.volume_id, expected)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| Status::not_found("expected native head missing"))?;
        let layers = self
            .engine
            .publish_captured_backing(&identity.volume_id, config)
            .await
            .map_err(storage_error)?;
        PublishedHead {
            version: 1,
            volume_id: identity.volume_id.clone(),
            revision: String::new(),
            parent: Some(expected.to_owned()),
            capacity_bytes: prior.capacity_bytes,
            capacity_generation: prior.capacity_generation,
            filesystem_uuid: prior.filesystem_uuid,
            layers,
            operation_id: identity.operation_id.clone(),
            input_digest: identity.input_digest.clone(),
        }
        .seal()
        .map_err(storage_error)
    }
    pub(crate) async fn commit_capture(
        &self,
        identity: &OperationIdentity,
        run: &str,
        head: &PublishedHead,
    ) -> Result<VolumeReceipt, Status> {
        self.identity(Some(identity))?;
        if head.volume_id != identity.volume_id
            || head.operation_id != identity.operation_id
            || head.input_digest != identity.input_digest
        {
            return Err(Status::permission_denied(
                "publication head identity mismatch",
            ));
        }
        self.heads
            .publish(head.parent.as_deref(), head)
            .await
            .map_err(storage_error)?;
        self.engine
            .commit_captured_layers(&head.volume_id, run, head.layers.clone())
            .await
            .map_err(storage_error)?;
        Ok(VolumeReceipt {
            operation: Some(identity.clone()),
            state: "ContentReady".into(),
            head_revision: head.revision.clone(),
            capacity_bytes: head.capacity_bytes,
            fs_uuid: head.filesystem_uuid.clone(),
            capacity_generation: head.capacity_generation,
            content_digest: head.revision.clone(),
            reason: String::new(),
            accessible_nodes: self.accessible_nodes.clone(),
        })
    }
    pub async fn delete(&self, request: DeleteVolumeRequest) -> Result<OperationReceipt, Status> {
        let identity = self.identity(request.operation.as_ref())?;
        if request.drain_authorization_digest.len() != 64
            || !request
                .drain_authorization_digest
                .bytes()
                .all(|b| b.is_ascii_hexdigit())
        {
            return Err(Status::permission_denied("drain authorization required"));
        }
        let mut operation = open_operation(&self.journal_root, "delete", Some(identity))?;
        operation
            .checkpoint(
                "delete_request",
                serde_json::to_value(&request).map_err(storage_error)?,
            )
            .map_err(storage_error)?;
        if operation.record().state == NativeOperationState::Completed {
            let receipt: OperationReceipt = serde_json::from_value(
                operation
                    .record()
                    .receipt
                    .clone()
                    .ok_or_else(|| Status::data_loss("delete receipt missing"))?,
            )
            .map_err(storage_error)?;
            if receipt.operation.as_ref() != Some(identity) || receipt.state != "Deleted" {
                return Err(Status::data_loss("delete receipt identity mismatch"));
            }
            return Ok(receipt);
        }
        if operation.record().state == NativeOperationState::Intent {
            operation.begin().map_err(storage_error)?;
        }
        // SSF has already fenced new uses and authorized this drain. The native
        // catalog additionally refuses active reservations; immutable heads and
        // content-addressed layers remain retained for snapshot references.
        match self.engine.delete(&identity.volume_id).await {
            Ok(()) | Err(aenv_volume_core::VolumeError::NotFound(_)) => (),
            Err(error) => return Err(storage_error(error)),
        }
        let result = OperationReceipt {
            operation: Some(identity.clone()),
            state: "Deleted".into(),
            reason: String::new(),
        };
        operation
            .complete(serde_json::to_value(&result).map_err(storage_error)?)
            .map_err(storage_error)?;
        Ok(result)
    }
    pub async fn resolve(&self, request: ResolveVolumeRequest) -> Result<VolumeReceipt, Status> {
        if request.repository_id != self.repository_id {
            return Err(Status::permission_denied(
                "repository not served by this worker",
            ));
        }
        // Retained snapshot heads do not resurrect a deleted live volume.
        let record = self
            .engine
            .get(&request.volume_id)
            .await
            .map_err(storage_error)?;
        crate::static_registration::validate(&record, &request)?;
        let head = if request.head_revision.is_empty() {
            self.heads.current(&request.volume_id).await
        } else {
            self.heads
                .read(&request.volume_id, &request.head_revision)
                .await
        }
        .map_err(storage_error)?
        .ok_or_else(|| Status::not_found("published native head not found"))?;
        Ok(VolumeReceipt {
            operation: Some(OperationIdentity {
                operation_id: head.operation_id,
                input_digest: head.input_digest,
                repository_id: self.repository_id.clone(),
                volume_id: head.volume_id,
            }),
            state: "ContentReady".into(),
            head_revision: head.revision.clone(),
            capacity_bytes: head.capacity_bytes,
            fs_uuid: head.filesystem_uuid,
            capacity_generation: head.capacity_generation,
            content_digest: head.revision,
            reason: String::new(),
            accessible_nodes: self.accessible_nodes.clone(),
        })
    }
    fn identity<'a>(
        &self,
        identity: Option<&'a OperationIdentity>,
    ) -> Result<&'a OperationIdentity, Status> {
        let identity = identity.ok_or_else(|| Status::invalid_argument("operation required"))?;
        if identity.repository_id != self.repository_id {
            return Err(Status::permission_denied(
                "repository is not served by this worker",
            ));
        }
        Ok(identity)
    }
    // A second operation cannot adopt a first operation's just-published
    // catalog during the crash window before its head/receipt is committed.
    fn claim(&self, identity: &OperationIdentity) -> Result<LockedOperation, Status> {
        let mut claim = LockedOperation::open(
            &self.journal_root.join("volume-claims"),
            NativeIntent {
                operation_id: identity.volume_id.clone(),
                volume_id: identity.volume_id.clone(),
                repository_id: identity.repository_id.clone(),
                input_digest: identity.input_digest.clone(),
                kind: "initial-create-claim".into(),
            },
        )
        .map_err(storage_error)?;
        claim
            .checkpoint(
                "creator",
                serde_json::to_value(identity).map_err(storage_error)?,
            )
            .map_err(storage_error)?;
        Ok(claim)
    }
    async fn source(&self, request: &EnsureVolumeRequest) -> Result<Option<PublishedHead>, Status> {
        match request.source.as_ref() {
            None => Ok(None),
            Some(source) => Ok(Some(
                self.heads
                    .read(&source.volume_id, &source.head_revision)
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| Status::failed_precondition("pinned source head missing"))?,
            )),
        }
    }
    pub async fn ensure(&self, request: EnsureVolumeRequest) -> Result<VolumeReceipt, Status> {
        let identity = self.identity(request.operation.as_ref())?;
        if request.capacity_bytes == 0
            || request.capacity_bytes % BYTES_PER_MB != 0
            || request.profile_revision != "agentenv-overlaybd-ext4-v1"
        {
            return Err(Status::invalid_argument(
                "native ext4 requires MiB-aligned capacity and OverlayBD profile",
            ));
        }
        if request.readonly && request.source.is_none() {
            return Err(Status::invalid_argument(
                "cannot initialize a readonly blank volume",
            ));
        }
        if input_digest(&request) != identity.input_digest {
            return Err(Status::invalid_argument(
                "native create input digest mismatch",
            ));
        }
        let _claim = self.claim(identity)?;
        let mut operation = open_operation(&self.journal_root, "create", Some(identity))?;
        if operation.record().state == NativeOperationState::Completed {
            return decode_receipt(operation.record().receipt.as_ref(), identity);
        }
        if operation.record().state != NativeOperationState::Intent {
            return Err(Status::failed_precondition(
                "native create already dispatched; inspect this operation",
            ));
        }
        let source = self.source(&request).await?;
        if source
            .as_ref()
            .is_some_and(|source| source.capacity_bytes != request.capacity_bytes)
        {
            return Err(Status::failed_precondition(
                "clone capacity must match pinned head; expand through offline maintenance",
            ));
        }
        operation
            .checkpoint(
                "create_request",
                serde_json::to_value(&request).map_err(storage_error)?,
            )
            .map_err(storage_error)?;
        operation.begin().map_err(storage_error)?;
        let mode = if request.readonly {
            VolumeMode::ReadOnly
        } else {
            VolumeMode::Exclusive
        };
        let result = match source {
            Some(source) => {
                self.engine
                    .register_snapshot(VolumeRecord {
                        id: identity.volume_id.clone(),
                        name: identity.volume_id.clone(),
                        mode,
                        size_mb: request.capacity_bytes / BYTES_PER_MB,
                        status: VolumeStatus::Ready,
                        reserved_by_sandbox_id: None,
                        backing_image_config: None,
                        backing_layers: source.layers,
                        read_only_mounts: Vec::new(),
                        deleting: false,
                    })
                    .await
            }
            None => {
                self.engine
                    .create_identified(
                        &identity.volume_id,
                        &volume_filesystem_identity(identity)?,
                        mode,
                        None,
                        request.capacity_bytes / BYTES_PER_MB,
                    )
                    .await
            }
        };
        if let Err(error) = result {
            operation.mark_unknown().map_err(storage_error)?;
            return Err(Status::failed_precondition(format!(
                "native create requires same-operation inspection: {error}"
            )));
        }
        self.finish_create(&mut operation, &request).await
    }
    pub async fn inspect(&self, request: InspectVolumeRequest) -> Result<VolumeReceipt, Status> {
        let identity = self.identity(request.operation.as_ref())?;
        let _claim = self.claim(identity)?;
        let mut operation = open_operation(&self.journal_root, "create", Some(identity))?;
        if operation.record().state == NativeOperationState::Completed {
            return decode_receipt(operation.record().receipt.as_ref(), identity);
        }
        if matches!(
            operation.record().state,
            NativeOperationState::Started | NativeOperationState::NeedsRecovery
        ) {
            let input: EnsureVolumeRequest = serde_json::from_value(
                operation
                    .record()
                    .checkpoints
                    .get("create_request")
                    .cloned()
                    .ok_or_else(|| {
                        Status::data_loss("dispatched native create lacks pinned input")
                    })?,
            )
            .map_err(storage_error)?;
            if input.operation.as_ref() != Some(identity)
                || input_digest(&input) != identity.input_digest
            {
                return Err(Status::data_loss("pinned native request mismatch"));
            }
            // This completes publication if the native catalog was already written.
            // It never creates a new backing or reruns a formatter.
            return self.finish_create(&mut operation, &input).await;
        }
        Ok(VolumeReceipt {
            operation: Some(identity.clone()),
            state: "Pending".into(),
            reason: "operation has not been dispatched".into(),
            ..Default::default()
        })
    }
    async fn finish_create(
        &self,
        operation: &mut LockedOperation,
        request: &EnsureVolumeRequest,
    ) -> Result<VolumeReceipt, Status> {
        let identity = self.identity(request.operation.as_ref())?;
        let record = self
            .engine
            .get(&identity.volume_id)
            .await
            .map_err(storage_error)?;
        if record.id != identity.volume_id
            || record.name != identity.volume_id
            || record.status != VolumeStatus::Ready
            || record.deleting
            || record.backing_layers.is_empty()
            || record.size_mb.checked_mul(BYTES_PER_MB) != Some(request.capacity_bytes)
            || (record.mode == VolumeMode::ReadOnly) != request.readonly
        {
            return Err(Status::failed_precondition(
                "native published catalog does not match create intent",
            ));
        }
        let source = self.source(request).await?;
        if source
            .as_ref()
            .is_some_and(|s| s.layers != record.backing_layers)
        {
            return Err(Status::data_loss(
                "restored native layers differ from pinned head",
            ));
        }
        let fs_uuid = match source {
            Some(source) => source.filesystem_uuid,
            None => volume_filesystem_identity(identity)?,
        };
        let head = PublishedHead {
            version: 1,
            volume_id: record.id,
            revision: String::new(),
            parent: None,
            capacity_bytes: request.capacity_bytes,
            capacity_generation: 1,
            filesystem_uuid: fs_uuid,
            layers: record.backing_layers,
            operation_id: identity.operation_id.clone(),
            input_digest: identity.input_digest.clone(),
        }
        .seal()
        .map_err(storage_error)?;
        operation
            .checkpoint(
                "published_head",
                serde_json::to_value(&head).map_err(storage_error)?,
            )
            .map_err(storage_error)?;
        self.heads
            .publish(None, &head)
            .await
            .map_err(storage_error)?;
        let receipt = VolumeReceipt {
            operation: Some(identity.clone()),
            state: "ContentReady".into(),
            head_revision: head.revision.clone(),
            capacity_bytes: head.capacity_bytes,
            fs_uuid: head.filesystem_uuid,
            capacity_generation: head.capacity_generation,
            content_digest: head.revision,
            reason: String::new(),
            accessible_nodes: self.accessible_nodes.clone(),
        };
        operation
            .complete(serde_json::to_value(&receipt).map_err(storage_error)?)
            .map_err(storage_error)?;
        Ok(receipt)
    }
}
fn decode_receipt(
    value: Option<&serde_json::Value>,
    identity: &OperationIdentity,
) -> Result<VolumeReceipt, Status> {
    let receipt: VolumeReceipt = serde_json::from_value(
        value
            .cloned()
            .ok_or_else(|| Status::data_loss("native receipt missing"))?,
    )
    .map_err(storage_error)?;
    if receipt.operation.as_ref() != Some(identity) || receipt.state != "ContentReady" {
        return Err(Status::data_loss("native completion identity mismatch"));
    }
    Ok(receipt)
}
fn storage_error(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(error.to_string())
}
