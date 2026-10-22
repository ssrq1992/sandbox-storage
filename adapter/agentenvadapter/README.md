# AgentENV SSF adapter

保留原 Gateway、Scheduler、Orchestrator 和全部原生功能。公共 Consumption/Preparation/Ledger 由 `ssf-coordinator` 独立服务运行，代码和运行入口都在 `sandbox-storage`；原 Scheduler 只使用标准库轻量客户端。

- 控制映射：`control_service.go`、`native_*.go`；不另建公开沙箱 API。
- VolumePreparation：profile/空白 ext4/检查和释放；Kubernetes 管理 PVC/PV/供卷。
- RuntimeMount：节点插件与原宿主 hook 协作接入 Firecracker；VMM 由原 Orchestrator 管理。
- Rust adapter：`runtime/`，无 AgentENV 私有类型依赖；保留同步 outbox 和停止事实。
- 部署：`deploy/k8s` 引用原 base；`deploy/Dockerfile.*` 联合构建；`deploy/standalone` 保留原服务并增加独立协调服务。

[当前实现](../../../design/26-independent-coordinator-implementation.md) · [架构](../../../design/23-current-implementation-architecture.md) · [安装](../../../design/13-agentenvadapter-installation-and-deployment.md) · [验收](../../../design/14-agentenvadapter-real-environment-test-plan.md)。本机验证报告在 `validation/control-service`，真实 KVM/K8S/CSI 未执行项单列。
