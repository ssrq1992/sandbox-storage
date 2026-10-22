//! Private UDS backend: a serialized native operation lane avoids blocking
//! executor threads on the original cross-process POSIX operation locks.
use crate::{device_host::DeviceHost, protocol::*, volume_host::VolumeHost, PROTOCOL};
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct BackendService {
    volumes: Arc<VolumeHost>,
    devices: Arc<DeviceHost>,
    identity: BackendIdentity,
    lane: tokio::sync::Mutex<()>,
}
impl BackendService {
    pub fn new(
        repository: String,
        volumes: Arc<VolumeHost>,
        devices: Arc<DeviceHost>,
        offline_expansion: bool,
    ) -> Self {
        let mut capabilities = vec![
            "native-volume-v1".into(),
            "static-registration-v1".into(),
            "device-session-v1".into(),
            "layer-capture-v1".into(),
            "head-publication-v1".into(),
            "durable-restack-v1".into(),
        ];
        if offline_expansion {
            capabilities.push("offline-expansion-v1".into());
        }
        Self {
            volumes,
            devices,
            lane: tokio::sync::Mutex::new(()),
            identity: BackendIdentity {
                protocol: PROTOCOL.into(),
                instance_id: uuid::Uuid::now_v7().to_string(),
                repository_ids: vec![repository],
                capabilities,
            },
        }
    }
}
#[tonic::async_trait]
impl native_storage_backend_server::NativeStorageBackend for BackendService {
    async fn describe(
        &self,
        _: Request<DescribeRequest>,
    ) -> Result<Response<BackendIdentity>, Status> {
        Ok(Response::new(self.identity.clone()))
    }
    async fn ensure_volume(
        &self,
        request: Request<EnsureVolumeRequest>,
    ) -> Result<Response<VolumeReceipt>, Status> {
        let _lane = self.lane.lock().await;
        self.volumes
            .ensure(request.into_inner())
            .await
            .map(Response::new)
    }
    async fn inspect_volume(
        &self,
        request: Request<InspectVolumeRequest>,
    ) -> Result<Response<VolumeReceipt>, Status> {
        let _lane = self.lane.lock().await;
        self.volumes
            .inspect(request.into_inner())
            .await
            .map(Response::new)
    }
    async fn resolve_volume(
        &self,
        request: Request<ResolveVolumeRequest>,
    ) -> Result<Response<VolumeReceipt>, Status> {
        let _lane = self.lane.lock().await;
        self.volumes
            .resolve(request.into_inner())
            .await
            .map(Response::new)
    }
    async fn delete_volume(
        &self,
        request: Request<DeleteVolumeRequest>,
    ) -> Result<Response<OperationReceipt>, Status> {
        let _lane = self.lane.lock().await;
        self.volumes
            .delete(request.into_inner())
            .await
            .map(Response::new)
    }
    async fn prepare_device(
        &self,
        request: Request<PrepareDeviceRequest>,
    ) -> Result<Response<DeviceReceipt>, Status> {
        let _lane = self.lane.lock().await;
        self.devices
            .prepare(request.into_inner())
            .await
            .map(Response::new)
    }
    async fn inspect_device(
        &self,
        request: Request<InspectDeviceRequest>,
    ) -> Result<Response<DeviceReceipt>, Status> {
        let _lane = self.lane.lock().await;
        self.devices
            .inspect(request.into_inner())
            .await
            .map(Response::new)
    }
    async fn capture_device(
        &self,
        request: Request<CaptureDeviceRequest>,
    ) -> Result<Response<CaptureReceipt>, Status> {
        let _lane = self.lane.lock().await;
        self.devices
            .capture(request.into_inner())
            .await
            .map(Response::new)
    }
    async fn publish_head(
        &self,
        request: Request<PublishHeadRequest>,
    ) -> Result<Response<VolumeReceipt>, Status> {
        let _lane = self.lane.lock().await;
        self.devices
            .publish(request.into_inner())
            .await
            .map(Response::new)
    }
    async fn release_device(
        &self,
        request: Request<ReleaseDeviceRequest>,
    ) -> Result<Response<OperationReceipt>, Status> {
        let _lane = self.lane.lock().await;
        self.devices
            .release(request.into_inner())
            .await
            .map(Response::new)
    }
    async fn expand_volume(
        &self,
        request: Request<ExpandVolumeRequest>,
    ) -> Result<Response<VolumeReceipt>, Status> {
        if !self
            .identity
            .capabilities
            .iter()
            .any(|c| c == "offline-expansion-v1")
        {
            return Err(Status::failed_precondition(
                "daemon has not negotiated offline expansion",
            ));
        }
        let _lane = self.lane.lock().await;
        self.devices
            .expand(request.into_inner())
            .await
            .map(Response::new)
    }
}
