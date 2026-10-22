# SSF 实现覆盖矩阵

代码目录：[SSF](../sandbox-storage/README.md)。此表区分软件实现、验证环境与项目扩展，不能把测试替身当作真实 Driver/VM 验收。

| 设计能力 | 实现位置 | 验证与边界 |
| --- | --- | --- |
| 2026-10-21：卷自动准备、两部分 adapter、PVC 直接引用 | [设计 21](21-ssf-volume-provisioning-and-runtime-adapter-design.md)；实现待交付 | 仅设计，现有测试报告不覆盖 |
| 标准 protobuf/gRPC、Go/Rust 客户端 | `api/ssf/v1`、`sdk`、`tools/generate` | Go 生成/构建检查；Rust 需安装工具链后执行 |
| 规范身份、参数、FileTree/BlockDevice | `core`、`auth` | CSI oneof JSON 往返、签名、节点/boot/有效期、路径意图测试 |
| 共享事务账本与 writer 排他 | `ledger` | 专用 PostgreSQL 并发 writer/排空/暂停/恢复测试已执行；见 18 的报告 |
| 请求驱动消费与恢复 | `consume` | 同一 request/operation 重入；保留未知结果；恢复回调衔接项目生命周期 |
| Kubernetes 管理 facade | `kubernetes` | 原生 PVC、snapshot、clone、restore、expand、Retain/Delete API；不调用 Driver Create/Delete |
| Immediate 与 WFFC | `kubernetes`、`ledger/selection.go` | 单测覆盖 sticky node、静态 UID 预留；真实四种动态组合测试需 kind |
| Driver 会话和标准 CSI | `drivers/csi`、`config` | role/revision 长连接；身份/Probe/真实 node_id；能力失败关闭准入 |
| 共享 Attach 与最终 Detach | `ledger/attachment.go`、`consume` | 持久 Attaching/Attached/Detaching/Detached；PostgreSQL 生命周期测试已执行；厂商 CSI 未运行 |
| Stage/Publish/WAL 与恢复 | `node` | mock CSI 故障/重试、共享引用、取消重放、WAL 重开；Linux 实际资源需 fixture |
| 原生卷消费保护 | `guard` | AdmissionReview 单测；真实 API-server webhook、证书、RBAC 待集群验证 |
| 条件 Gateway | `gateway` | 拒绝管理 RPC；真实账本审批、绑定参数、node_id 校验；部署需共享账本只读连接 |
| Runtime、计算 lease | `runtime`、`examples/integration` | 接口与验证适配器；真正鉴权、Reserve/Renew/Commit、VMM/guest/evidence 由项目实现 |
| 扩容 | `kubernetes/expansion.go`、NodeExpand | 节点只接受已被原生 PVC/PV 授权的容量；guest 扩容仍由 Runtime 适配 |
| 观测与诊断 | `observe`、`ssfctl`、服务日志 | bounded-cardinality 步骤计数/耗时/错误，InspectOperation；无实测 VM SLO |
| 部署与复现 | `deploy`、`scripts`、`testing` | 固定版本脚本、镜像、清单；本机只做静态/构建检查 |

## 具体实现选择

- 生产共享账本固定 PostgreSQL；不交付可被误用为生产权威的内存账本。bbolt 用于本机 WAL。
- Binding ID 永不复用；owner/run 使用持久 epoch，接续须确认旧运行停止。旧记录作为 tombstone 保留，不允许仅增加 epoch 抢占旧 writer。
- Draining 是单向屏障，当前不提供自动取消 drain 后重新准入。因此回收许可使用固定 PVC UID、动作、有效期，并结合不可变的 PV 身份与单向屏障验证；没有宣称数据库 epoch 能完成数据面 fencing。
- 保护基础是 fail-closed Guard、管理权限边界、默认 Retain 和账本屏障。本次没有增加 SSF 自有 finalizer，也不移除原生 finalizer；设计中自有 finalizer 的移除顺序用于未来增加辅助 finalizer 时遵循。
- Node 网络 API 首期采用 mTLS；本机 Driver 使用 UDS。暂不提供独立的 Node UDS peer-credential server。
- 配置 revision 用于会话分离；gRPC 负责连接恢复。配置/证书更新按排空后滚动重启操作，尚未实现无中断热轮换。
- WFFC 对未知供卷/预绑定结果保持原 PVC、原节点和候选 PV。没有自动切换到新节点的管理捷径；确需换节点时必须先证明原 attempt 已终结。
- 共享消费服务、热资源池、自动跨权威导入/移交、强制 fencing、真实 VM 一致性 checkpoint 的运行验证仍未完成；当前通过明确拒绝或项目接口保留边界。

## 不能由本次结果推导的结论

不能据此宣称厂商 Driver 全量兼容、真实 VM 文件/块卷已验收、PostgreSQL 故障矩阵已通过，或达到设计中的 p99 启动目标。详细执行证据见 [验证报告](10-implementation-validation.md)。


## AgentENV 原生接入补充

AgentENV 原生插件已实现创建、计算预留、暂停/恢复、自动恢复、联合捕获、Fork、模板省略/清空/替换、引用保护与自动内部回收、离线扩容和 standalone；协商 `agentenv-capture-clone-v1` / hook version 3。实现路径、本机结果和未执行真机矩阵见 [18](18-native-integration-implementation-status.md)。该能力 profile 不代替 Linux/KVM/K8s/真实 CSI 验收。
