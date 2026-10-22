# AgentENV 原生插件实现与验证状态

> 2026-10-21 独立化更新：本页保留此前设计/阶段记录。当前 AgentENV 的消费与准备协调、账本事务在 `sandbox-storage` 的独立 `ssf-coordinator` 中执行；Scheduler 为轻量客户端，standalone 同样独立。以 [03](03-framework-design.md)、[23](23-current-implementation-architecture.md)、[26](26-independent-coordinator-implementation.md) 和 [13 部署](13-agentenvadapter-installation-and-deployment.md) 为准。旧内嵌宿主、`go.ssf.mod`、`-tags ssf`、原仓库 SSF overlay 不再适用。本次保留 Block/ext4 和全部原生功能，未新增 OverlayBD CSI Driver。

> 2026-10-21 自动卷准备增量：实现、部署、验证结果及未完成边界统一见 [22](22-volume-preparation-implementation.md)；直接 PVC 两步场景见 [20](20-kubernetes-pvc-agentenv-mount-validation.md)。本文件原生命周期说明继续适用，旧未启用 profile 的卷策略不代表新 profile 规则。

更新：2026-10-20。按 [设计 16](16-agentenvadapter-native-integration-design.md) 和 [实施计划 17](17-agentenvadapter-refactoring-and-validation.md) 交付当前工作区的软件实现、部署材料和测试工具。原 Gateway、Scheduler、节点服务、SDK 使用的公开沙箱接口和 Orchestrator 保留。没有提交、推送或外部部署；CubeSandbox/substrate 不在本次修改范围。

**本机软件交付与真实环境验收分开记录。** 下列代码路径已经实现，真实 Linux/KVM、Kubernetes、厂商 CSI 和 systemd 执行尚未完成；不能据本机通过项宣称生产验收通过。

## 实现覆盖

| 计划项 | 当前代码与行为 | 本机证据 / 真机边界 |
| --- | --- | --- |
| 2026-10-21 卷准备与直接 PVC 来源 | 待实现：公共 watcher/profile/准备证据、两部分 adapter、原生 PVC 来源及删除 finalizer 流程 | 仅设计；没有新增软件测试或真机通过结果 |
| 基线与最小扩展 | 保留初始未提交 diff/hash；独立可关闭 StorageProvider；Rust 通用设备/hook/outbox；不含 K8s/CSI/账本 | 初始快照、当前补丁精确重放；完整 Rust 运行未执行 |
| 原生 Gateway/Scheduler | 原 body、native ID、201、鉴权/路由/代理；签名完整计划；原发现/健康/策略 + 持久计算预留；独立 Go modfile/tag | 原 Go 普通/SSF 测试、race、vet、Linux 构建、OpenAPI 比对 |
| 公共消费框架 | 公共拓扑、四种绑定组合、owner/run/epoch、UseHold、可信 Ready、未知派发/补偿/tombstone | Go/真实 PostgreSQL 测试；K8s 客户端和设备为 fixture |
| VolumeProviderMux | legacy 默认；动态/静态 PVC、UID 固定导入、SSF 不回退；普通删沙箱保留用户卷 | provider/初始化/删除测试；真实四种组合未执行 |
| 暂停/恢复/自动策略 | 逻辑 hold 保留、真实停止后接续新 run；原生 ID、autoPause、connect-resume 与驱逐链路 | DB epoch/hold/恢复测试；原生完整测试已类型检查，Linux 执行未完成 |
| 联合快照 | native memory/rootfs + legacy + CSI 同冻结屏障，UID/pin、thaw、联合发布及失败补偿 | barrier/CheckpointSet/引用保护 DB 测试；VMM 一致性未执行 |
| Fork | 原 ID/父节点放置/部分成功；每 child 独立计算与 owner/run；RW 独立 PVC，RO 共享；未知 stop 保留 child handle/Killing | Go Fork/clone DB 测试；原生失败所有权测试已类型检查，未在 Linux 执行 |
| 模板语义 | 原 SnapshotManager/P2P；别名 pin 不可变版本；省略继承、`[]` 清空、显式替换 | DB 幂等克隆/替换测试 + 槽位转换可移植 Rust 测试；guest 打开 FD 转换 E2E 已交付未执行 |
| 原生仓库保护 | POSIX/OSS 仓库身份；只读投影、发布/删除 outbox、明确仓库观察证据 | fixture/DB/便携元数据测试；原生仓库真机运行未执行 |
| 持久删除与回收 | 真正 Stopped 证据先于 Unpublish/Unstage/Detach；用户卷保留；内部失败克隆/隐藏旧盘/临时 compute/capture 无引用才回收 | stop/WAL/outbox/GC/pin DB 测试；未知异步结果保留 tombstone |
| 离线扩容 | 原 Volume expand；维护屏障排斥 writer/hold/attachment；PVC Controller → 启动前 NodeExpand/e2fsck/resize2fs/UUID 校验 | DB 屏障/Controller 崩溃恢复测试；真实 ext4/CSI 扩容未执行 |
| standalone | 保留原 service；本地插件同一消费库/固定物理宿主；认证 local-admit；无替代公开控制面 | Go loopback/body/固定身份测试；systemd/KVM 运行未执行 |
| 原生增量部署 | 原 Kustomize base + overlay 保留 Gateway/Scheduler/node/headless/心跳/P2P/探针/drain；新增 sidecar/WAL/RBAC/传播 | 两个 namespace 渲染、凭据角色分离检查；镜像和集群部署未执行 |
| 协议和生成 | `ssf.runtime.v2`、`agentenv.storage.v2`、hook 4、完整 profile `agentenv-capture-clone-v1`；幂等迁移 | protobuf 重生成、公开 API 兼容、12 个 OpenAPI Rust 文件逐一重生成比对 |
| 旧原型移除 | 删除独立沙箱 API/control、静态 workers/templates、专用客户端和 custom-extension 存储通道 | 当前 CLI 仅 native node；旧原型已先归档，无兼容层 |

完整 profile 表示软件 hook 能力，不是厂商 Driver 认证。模板设备槽位遵守原生 Firecracker 数量/只读约束；显式替换需同时容纳保留旧盘和新盘，超限明确拒绝。首规格 Block/ext4，不新增 FileTree、跨节点内存迁移或强制 OverlayBD CSI 化。

## 安全实现中的关键行为

- capacity 总额与 native IDs 在同一元数据锁内投影，避免旧心跳和新路由样本混合导致重复计算准入；暂停实例和 dormant builder 不计活动资源。
- 新卷初始化不能因重试误格式化：新 profile 的导入与直接 PVC 共用内容准备规则，恢复来源永不授权 mkfs；格式意图按原 operation 恢复。未启用 profile 的旧卷仍保留原初始化策略。
- pause hold 与维护锁、writer、attachment 和回收共用权威事务。待处理扩容不会堵住原 owner 恢复；只有空闲时真正进入维护屏障。
- 冻结内存可保留打开 FD。模板清空/替换将旧文件系统保留在独立克隆和原槽位，guest 隐藏挂载并安装新盘后才 thaw，不将旧内存直接接到新数据盘。
- 原生创建、等待 Ready 或 Fork child 失败但停止未知时，原 Orchestrator 保留 VMM handle 和 Killing 元数据。删除 outbox 先持久化，原生重试 stop；插件不能通过 HTTP 404、租约或响应失败推断已停止。
- checkpoint、仓库、clone 与 capture 目录各自持久引用相互接续。内部克隆回收公平轮询；成功可见用户卷不随沙箱删除。未知晚到 Snapshot/Content 和未知发布保留 tombstone，不能按时间超期强删。

## 检查结果与工件

| 检查 | 当前结果 | 报告 |
| --- | --- | --- |
| Go 单测/race/vet、普通/SSF 构建、Linux Go 构建、protobuf/API 兼容 | 通过项及 skip 逐项记录 | [foundation](../sandbox-storage/validation/native-plugin-progress/report.json) |
| 真实 PostgreSQL ledger/consume/native 集成 | 通过，无 skip；K8s/Node 为 fixture | [database](../sandbox-storage/validation/native-database/report.json) |
| 只读故障断言工具 | 正常数据 exit 0；故意双 writer fixture exit 1，预期拒绝 | 同一 database 报告 |
| Rust Linux all-target 交叉类型检查 | 通过，源码稳定；未链接/执行原生测试 | [cross](../sandbox-storage/validation/native-rust-cross/current/report.json) |
| 可移植 Rust 安全模块 | 5 个测试实际执行通过 | [portable](../sandbox-storage/validation/native-rust-modules/report.json) |
| 原生 overlay/凭据离线检查 | 两个 namespace 通过，未部署 | [deployment](../sandbox-storage/validation/native-deployment/report.json) |
| OpenAPI 固定版本生成一致性 | 7.22.0，12 个 Rust 文件逐一一致 | `scripts/check_native_openapi.py`，汇总报告记录 |
| AgentENV 补丁 | 76 个变更路径精确重放通过，包含原始用户未提交改动 | [patch](../sandbox-storage/adapter/agentenvadapter/patches/agentenv-native-progress.patch.json) |
| 文档/依赖边界/保护参考文件 | 链接、公共核心不导入 adapter、参考文件哈希通过 | [artifacts](../sandbox-storage/validation/artifacts/report.json) |
| 汇总 | 按 scope 保留 passed/failed/not_executed，不合并推导真机成功 | [delivery](../sandbox-storage/validation/native-delivery/report.json) |

foundation 未设置测试 DSN 的数据库 skip 由独立 database 报告补充；未配置的 Redis 测试仍是 skip，未声明通过。foundation 自身不运行 Rust 或 KVM，应结合单独报告阅读。补丁/foundation JSON 的 `complete_plan=false` 是这些工具不判定整体验收的保守字段，不表示可将其单独当作最终验收证书。

初始 diff/hash 另存 `SSF/validation/native-baseline/`；重构前旧原型归档同存 `SSF/validation/native-baseline/retired-prototype.tar.gz`（会话原备份仍在 `/private/tmp/ssf-retired-prototype-baseline/`）。受保护 CSI 参考文档 SHA256 保持 `9733935a2c0e2d144b67c8404361c835e7f8ed45d2b508992cb230da31b3d5dc`。

## 已交付的真机工具与未执行矩阵

- `native_preflight.py`：只读环境检查，cluster/standalone 分开，不冒充运行验证。
- `native_compatibility_e2e.py`：原生 cold/v1/v2/list/get/delete/timeout/autoPause/connect/template/envd 流代理；custom extension/HTTP/WS/template build/P2P 按该环境 SDK 场景接入，未提供场景明确记未执行。
- `native_lifecycle_e2e.py`：真实 guest 数据、双 writer 拒绝、pause hold、resume、快照/Fork/模板、混合 legacy/SSF、扩容、旧 FD 保留；动态与静态、Immediate/WFFC 分别配置独立测试资源执行。
- `native_fault_e2e.py`：显式 context/execute、分阶段场景、实际注入计数、强制恢复/断言、未覆盖 stage 列表；CSI 故障仅在 disposable fixture 显式开启。
- `native_invariants.py`：只读 DB 事务断言；不将账本一致性冒充实际 VMM/FD/guest 证明。
- `native_evidence.py`、`native_cleanup.py`：采集已知环境证据；按报告已知 native ID 审核/清理，默认不删用户卷和导入卷，不重试未知 create。
- `check_agentenv_linux.sh`：Linux 全目标类型检查及原生生命周期、设备、冻结、outbox/GC 等测试入口。

**未执行：** 完整 Linux Rust 测试与链接、Linux/KVM 实际 VMM、Kubernetes API-server Guard/RBAC/drain、镜像构建部署、四种绑定组合真实数据语义、只读共享、跨卷一致性、自动策略/故障矩阵、systemd、真实 CSI 厂商兼容和性能。fixture 结果与 vendor 结果分别标记。实际验收步骤和证据判据见 [14](14-agentenvadapter-real-environment-test-plan.md)。
