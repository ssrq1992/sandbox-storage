# SSF 公共模块与运行时扩展点

更新：2026-10-21。当前逻辑图、部署图和调用图统一见 [23](23-current-implementation-architecture.md)，架构约束见 [03](03-framework-design.md)。

| 分层 | 公共实现 | 运行时需要实现 |
| --- | --- | --- |
| 卷管理 | Kubernetes/CSI 原生体系 | 可选 facade，不直接供卷或回收后端 |
| 内容准备 | `preparation` 的状态、互斥、节点会话、回执与释放 | profile Describe/Plan/Prepare/Inspect/ReleasePreparation；AgentENV 是 Block/ext4 |
| 消费控制 | `consume`、`kubernetes`、`ledger`、`coordinator` | `coordinator.Runtime.Call` 注册，映射原请求、模板、生命周期和原生计算准入 |
| 原生调度衔接 | `sdk/go/control` 的版本化 mTLS 线协议 | 原发现/健康和原选择策略；只读 ObserveNodes，Resolve/Prepare/Inspect 调用 |
| 节点卷处理 | `node` 的 Stage/Publish/WAL/证据/Release | RuntimeMount 将 Endpoint 接入原 VMM 和 guest |
| 原生宿主 | framework adapter 的 Rust DTO/策略/outbox | RuntimeHost 的 config、设备验证、受限工件删除，以及原生命周期同步 hook |
| 保护 | 独立 Guard、共享 PostgreSQL | 原管理 API 权限映射和真实身份，不创建第二份 writer 权威 |

`coordinator` 仅依赖中立 DTO 和接口；AgentENV 专用映射及表在 `adapter/agentenvadapter`。新增运行时需要注册自己的实现，不能在公共核心导入 AgentENV 私有类型。服务式和内嵌式是不同宿主形态，当前 AgentENV 只保留服务式执行入口。

原生 hook 是实际副作用边界，不能用旁路代理猜测 VMM 是否停止或冻结。减少侵入以抽离协议、策略和恢复实现为主，保留必要的原生控制权。完整代码和验证入口见 [26](26-independent-coordinator-implementation.md)。
